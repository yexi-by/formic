//! 单单元执行：读分片 → 装配 prompt → 「LLM ↔ 工具调用」多轮循环 → 发布最终
//! 回合的文本或给出诊断。一切工具调用经调度器准入；连续 3 次完全相同的工具
//! 调用触发停滞检测；瞬时故障按预算重试；取消令牌随时可收敛本单元。

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::llm::{Finish, LlmClient, LlmError, LlmEvent, Message, ToolCallReq};
use crate::output::{self, AuditEntry};
use crate::plan::{PlanUnit, Shard};
use crate::prompt::{self, ShardContent};
use crate::scheduler::{Scheduler, SchedulerGone};

/// 停滞检测阈值：连续相同 (name, arguments) 调用达到此次数即终止（内部常量）。
const STALL_LIMIT: u32 = 3;

/// 单次 LLM 调用的总尝试次数上限（内部常量；瞬时故障重试，语义性失败不重试）。
const RETRY_BUDGET: u32 = 3;

/// 第 attempt 次尝试失败后的退避（1 起始）。无实测证据，不引入指数/抖动策略。
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(attempt as u64)
}

/// 一个作业的运行上下文：全部单元共享的只读事实与依赖。
pub struct JobContext {
    pub data_root: PathBuf,
    pub task: String,
    pub listing: Vec<String>,
    pub llm: LlmClient,
    pub scheduler: Scheduler,
    pub out_dir: PathBuf,
}

/// 单元结局：发布成功，或被取消（在途丢弃，不算失败）。
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Published,
    Cancelled,
}

/// 单元失败：带对象与直接原因，细节留在审计文件。
#[derive(thiserror::Error, Debug)]
pub enum UnitFailure {
    #[error("读取分片文件 {path} 失败：{source}")]
    ReadShard {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("分片文件 {path} 不是合法 UTF-8，无法装配进 prompt")]
    ShardEncoding { path: PathBuf },
    #[error(transparent)]
    Llm(#[from] LlmError),
    #[error("模型给出的工具参数不是合法 JSON：{0}")]
    BadToolArguments(String),
    #[error("停滞：连续 {STALL_LIMIT} 次相同的工具调用 {name}（参数 {arguments}）")]
    Stalled { name: String, arguments: String },
    #[error("模型输出达到协议长度上限，产出被截断")]
    Truncated,
    #[error("模型未产出任何文本")]
    EmptyOutput,
    #[error("{cause}（重试 {retries} 次后仍失败）")]
    RetriesExhausted {
        retries: u32,
        cause: Box<UnitFailure>,
    },
    #[error("调度器故障：{0}")]
    Scheduler(#[from] SchedulerGone),
    #[error("内部错误：worker task panic")]
    Panicked,
    #[error("写入输出区失败：{0}")]
    Output(#[from] std::io::Error),
}

/// 一次 LLM 调用的失败按可重试性分类。
enum CallFailure {
    Cancelled,
    Retryable(UnitFailure),
    Fatal(UnitFailure),
}

fn classify(failure: UnitFailure) -> CallFailure {
    match failure {
        // 429 与 5xx 之外的客户错误属配置/请求问题，重试无意义
        UnitFailure::Llm(LlmError::Http { status, .. }) if status != 429 && status < 500 => {
            CallFailure::Fatal(failure)
        }
        UnitFailure::Llm(_) => CallFailure::Retryable(failure),
        UnitFailure::BadToolArguments(_) => CallFailure::Retryable(failure),
        other => CallFailure::Fatal(other),
    }
}

/// 执行一个单元。发布成功或取消都会照常落审计；失败不留下完成记录。
pub async fn run_unit(
    ctx: &JobContext,
    unit: &PlanUnit,
    cancel: CancellationToken,
) -> Result<Outcome, UnitFailure> {
    let shard = read_shard(&ctx.data_root, &unit.shard)?;
    let user = prompt::build_user_message(&ctx.task, &ctx.listing, &shard);
    let mut history = vec![Message::User(user)];
    let mut audit = Vec::new();

    let outcome = drive_loop(ctx, &mut history, &mut audit, &cancel).await;
    // 证据完整是契约要求：取消与失败的现场也落盘，由审计还原经过。
    output::write_audit(&ctx.out_dir, unit.unit, &audit)?;
    match outcome? {
        LoopEnd::Published(text) => {
            output::publish(&ctx.out_dir, unit.unit, &text)?;
            Ok(Outcome::Published)
        }
        LoopEnd::Cancelled => Ok(Outcome::Cancelled),
    }
}

enum LoopEnd {
    Published(String),
    Cancelled,
}

/// 「LLM ↔ 工具调用」循环。
async fn drive_loop(
    ctx: &JobContext,
    history: &mut Vec<Message>,
    audit: &mut Vec<AuditEntry>,
    cancel: &CancellationToken,
) -> Result<LoopEnd, UnitFailure> {
    let mut last_call: Option<(String, String)> = None;
    let mut same_calls = 0u32;

    loop {
        // 单次 LLM 调用 + 重试预算：同一历史重发，每次尝试独立留痕
        let mut attempt = 0u32;
        let turn = loop {
            attempt += 1;
            match one_turn(ctx, history, audit, attempt, cancel).await {
                Ok(turn) => break turn,
                Err(CallFailure::Cancelled) => return Ok(LoopEnd::Cancelled),
                Err(CallFailure::Fatal(f)) => return Err(f),
                Err(CallFailure::Retryable(f)) => {
                    if attempt >= RETRY_BUDGET {
                        return Err(UnitFailure::RetriesExhausted {
                            retries: attempt - 1,
                            cause: Box::new(f),
                        });
                    }
                    eprintln!("第 {attempt} 次调用失败（{f}），退避后重试");
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(LoopEnd::Cancelled),
                        _ = tokio::time::sleep(backoff(attempt)) => {}
                    }
                }
            }
        };

        match turn {
            TurnEnd::FinalText(text) => return Ok(LoopEnd::Published(text)),
            TurnEnd::ToolCalls { text, calls } => {
                history.push(Message::Assistant {
                    text,
                    tool_calls: calls.iter().map(|c| c.req.clone()).collect(),
                });
                for prepared in calls {
                    let tc = prepared.req;
                    audit.push(AuditEntry::ToolCall {
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                    });
                    let current = (tc.name.clone(), tc.arguments.clone());
                    if last_call.as_ref() == Some(&current) {
                        same_calls += 1;
                    } else {
                        same_calls = 1;
                        last_call = Some(current);
                    }
                    if same_calls >= STALL_LIMIT {
                        return Err(UnitFailure::Stalled {
                            name: tc.name,
                            arguments: tc.arguments,
                        });
                    }
                    // 参数 JSON 已在 one_turn 校验（LLM 边界），这里直接信任
                    let result = tokio::select! {
                        _ = cancel.cancelled() => return Ok(LoopEnd::Cancelled),
                        r = ctx.scheduler.execute(&tc.name, prepared.arguments) => r?,
                    };
                    audit.push(AuditEntry::ToolResult(result.clone()));
                    history.push(Message::ToolResult {
                        call_id: tc.call_id,
                        content: result,
                    });
                }
            }
        }
    }
}

/// 一个回合的结局：最终文本（可发布）或待执行的工具调用（参数已校验）。
enum TurnEnd {
    FinalText(String),
    ToolCalls {
        text: String,
        calls: Vec<PreparedCall>,
    },
}

struct PreparedCall {
    req: ToolCallReq,
    arguments: serde_json::Value,
}

/// 一个回合：发起一次流式调用，收完事件，解释结局。可取消。
async fn one_turn(
    ctx: &JobContext,
    history: &[Message],
    audit: &mut Vec<AuditEntry>,
    attempt: u32,
    cancel: &CancellationToken,
) -> Result<TurnEnd, CallFailure> {
    let specs = ctx.scheduler.specs();
    let mut call = tokio::select! {
        _ = cancel.cancelled() => return Err(CallFailure::Cancelled),
        r = ctx.llm.call(prompt::INSTRUCTIONS, history, &specs) => r,
    }
    .map_err(|e| classify(e.into()))?;
    audit.push(AuditEntry::LlmRequest {
        attempt,
        body: call.request_body.clone(),
    });

    let mut text = String::new();
    let mut tool_calls: Vec<ToolCallReq> = Vec::new();
    let mut finish = None;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => return Err(CallFailure::Cancelled),
            r = call.next_event() => r,
        };
        match next {
            Ok(Some(LlmEvent::TextDelta(delta))) => text.push_str(&delta),
            Ok(Some(LlmEvent::ToolCall(tc))) => tool_calls.push(tc),
            Ok(Some(LlmEvent::Finished(f))) => finish = Some(f),
            Ok(None) => break,
            Err(e) => {
                audit.extend(call.raw_log().iter().cloned().map(AuditEntry::LlmEvent));
                return Err(classify(e.into()));
            }
        }
    }
    audit.extend(call.raw_log().iter().cloned().map(AuditEntry::LlmEvent));

    if finish == Some(Finish::MaxTokens) {
        return Err(CallFailure::Fatal(UnitFailure::Truncated));
    }
    if tool_calls.is_empty() {
        return match finish {
            Some(Finish::Stop) => {
                if text.is_empty() {
                    Err(CallFailure::Fatal(UnitFailure::EmptyOutput))
                } else {
                    Ok(TurnEnd::FinalText(text))
                }
            }
            Some(Finish::ToolUse) => Err(CallFailure::Retryable(UnitFailure::Llm(
                LlmError::protocol("finish 声称工具调用但没有工具调用内容", ""),
            ))),
            // MaxTokens 已在上面返回
            _ => Err(CallFailure::Retryable(UnitFailure::Llm(
                LlmError::protocol("流结束但没有收到完成事件", ""),
            ))),
        };
    }

    // 参数 JSON 合法性在 LLM 边界校验一次，历史与执行都直接信任
    let mut calls = Vec::with_capacity(tool_calls.len());
    for tc in tool_calls {
        let arguments = serde_json::from_str(&tc.arguments)
            .map_err(|_| UnitFailure::BadToolArguments(tc.arguments.clone()))
            .map_err(classify)?;
        calls.push(PreparedCall { req: tc, arguments });
    }
    Ok(TurnEnd::ToolCalls { text, calls })
}

fn read_shard(root: &Path, shard: &Shard) -> Result<ShardContent, UnitFailure> {
    match shard {
        Shard::Files(files) => {
            let mut contents = Vec::with_capacity(files.len());
            for f in files {
                contents.push((prompt::slash_path(f), read_utf8(root, f)?));
            }
            Ok(ShardContent::Files(contents))
        }
        Shard::Lines { file, start, end } => {
            let text = read_utf8(root, file)?;
            let lines: Vec<&str> = text.lines().collect();
            let from = (*start as usize).saturating_sub(1);
            let to = (*end as usize).min(lines.len());
            Ok(ShardContent::Lines {
                file: prompt::slash_path(file),
                start: *start,
                end: *end,
                content: lines[from..to].join("\n"),
            })
        }
    }
}

fn read_utf8(root: &Path, rel: &Path) -> Result<String, UnitFailure> {
    let bytes = fs::read(root.join(rel)).map_err(|e| UnitFailure::ReadShard {
        path: rel.to_path_buf(),
        source: e,
    })?;
    String::from_utf8(bytes).map_err(|_| UnitFailure::ShardEncoding {
        path: rel.to_path_buf(),
    })
}

/// 递归列出数据根内全部文件，返回排序后的根内相对表示（`/` 分隔）。
pub fn list_files(root: &Path) -> std::io::Result<Vec<String>> {
    Ok(crate::tools::walk_files(root)?
        .iter()
        .map(|p| prompt::slash_path(p))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{LlmConfig, Protocol};
    use crate::tools::Roots;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 迷你一次性应答 server：Slow 长时间不响应；Status(code) 以固定状态码应答。
    /// 返回端口与请求计数。
    #[derive(Clone, Copy)]
    enum Mode {
        Slow,
        Status(u16),
    }

    fn mini_server(mode: Mode) -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let count = Arc::new(AtomicUsize::new(0));
        let shared = Arc::clone(&count);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                shared.fetch_add(1, Ordering::SeqCst);
                match mode {
                    Mode::Slow => {
                        std::thread::sleep(Duration::from_secs(30));
                    }
                    Mode::Status(code) => {
                        // 读完请求体再应答，避免对端写一半被重置
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        let mut line = String::new();
                        let mut len = 0usize;
                        while reader.read_line(&mut line).is_ok() {
                            let t = line.trim().to_string();
                            line.clear();
                            if t.is_empty() {
                                break;
                            }
                            let lower = t.to_ascii_lowercase();
                            if let Some(v) = lower.strip_prefix("content-length:") {
                                len = v.trim().parse().unwrap_or(0);
                            }
                        }
                        let mut body = vec![0u8; len];
                        reader.read_exact(&mut body).ok();
                        let body_text = "err";
                        let response = format!(
                            "HTTP/1.1 {code} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body_text}",
                            body_text.len()
                        );
                        stream.write_all(response.as_bytes()).ok();
                    }
                }
            }
        });
        (port, count)
    }

    fn fixture(port: u16) -> (tempfile::TempDir, JobContext) {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let out = dir.path().join("out");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&out).unwrap();
        fs::write(data.join("a.txt"), "苹果是水果。\n").unwrap();
        let ctx = JobContext {
            scheduler: Scheduler::start(Roots {
                input: data.clone(),
                output: out.clone(),
            }),
            data_root: data,
            task: "任务。".into(),
            listing: vec!["a.txt".into()],
            llm: LlmClient::new(LlmConfig {
                protocol: Protocol::Completions,
                base_url: format!("http://127.0.0.1:{port}"),
                model: "m".into(),
                api_key: None,
            }),
            out_dir: out,
        };
        (dir, ctx)
    }

    fn unit() -> PlanUnit {
        PlanUnit {
            unit: 1,
            shard: Shard::Files(vec![PathBuf::from("a.txt")]),
        }
    }

    #[tokio::test]
    async fn cancel_during_llm_call_converges() {
        let (port, _count) = mini_server(Mode::Slow);
        let (dir, ctx) = fixture(port);
        let token = CancellationToken::new();
        let trigger = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let outcome = run_unit(&ctx, &unit(), token).await.unwrap();
        assert_eq!(outcome, Outcome::Cancelled);
        assert!(
            !dir.path().join("out").join("1.md").exists(),
            "取消单元不得留下记录"
        );
        assert!(
            dir.path()
                .join("out")
                .join("audit")
                .join("1.jsonl")
                .exists(),
            "取消现场仍应落审计"
        );
    }

    #[tokio::test]
    async fn http_400_is_fatal_not_retried() {
        let (port, count) = mini_server(Mode::Status(400));
        let (_dir, ctx) = fixture(port);
        let result = run_unit(&ctx, &unit(), CancellationToken::new()).await;
        let Err(failure) = result else {
            panic!("400 应判失败")
        };
        assert!(failure.to_string().contains("400"), "{failure}");
        assert!(
            !failure.to_string().contains("重试"),
            "400 不重试：{failure}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1, "400 只应请求一次");
    }

    #[tokio::test]
    async fn http_500_retried_to_budget() {
        let (port, count) = mini_server(Mode::Status(500));
        let (_dir, ctx) = fixture(port);
        let result = run_unit(&ctx, &unit(), CancellationToken::new()).await;
        let Err(failure) = result else {
            panic!("500 应判失败")
        };
        let text = failure.to_string();
        assert!(text.contains("500"), "{text}");
        assert!(text.contains("重试 2 次"), "{text}");
        assert_eq!(
            count.load(Ordering::SeqCst),
            RETRY_BUDGET as usize,
            "500 应尝试到预算上限"
        );
    }
}
