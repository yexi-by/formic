//! 单单元执行：读分片 → 装配 prompt → 「LLM ↔ 工具调用」多轮循环 → 发布最终
//! 回合的文本或给出诊断。一切工具调用经调度器准入；连续 3 次完全相同的工具
//! 调用触发停滞检测，终止本单元。

use std::fs;
use std::path::{Path, PathBuf};

use crate::llm::{Finish, LlmClient, LlmError, LlmEvent, Message, ToolCallReq};
use crate::output::{self, AuditEntry};
use crate::plan::{PlanUnit, Shard};
use crate::prompt::{self, ShardContent};
use crate::scheduler::{Scheduler, SchedulerGone};

/// 停滞检测阈值：连续相同 (name, arguments) 调用达到此次数即终止（内部常量）。
const STALL_LIMIT: u32 = 3;

/// 一个作业的运行上下文：全部单元共享的只读事实与依赖。
pub struct JobContext {
    pub data_root: PathBuf,
    pub task: String,
    pub listing: Vec<String>,
    pub llm: LlmClient,
    pub scheduler: Scheduler,
    pub out_dir: PathBuf,
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
    #[error("调度器故障：{0}")]
    Scheduler(#[from] SchedulerGone),
    #[error("写入输出区失败：{0}")]
    Output(#[from] std::io::Error),
}

/// 执行一个单元：成功则产出已发布；失败不留下完成记录。
pub async fn run_unit(ctx: &JobContext, unit: &PlanUnit) -> Result<(), UnitFailure> {
    let shard = read_shard(&ctx.data_root, &unit.shard)?;
    let user = prompt::build_user_message(&ctx.task, &ctx.listing, &shard);
    let mut history = vec![Message::User(user)];
    let mut audit = Vec::new();

    let outcome = drive_loop(ctx, &mut history, &mut audit).await;
    // 证据完整是契约要求：审计写不进去时单元不算完成，由调用方续跑重做。
    output::write_audit(&ctx.out_dir, unit.unit, &audit)?;
    let text = outcome?;
    output::publish(&ctx.out_dir, unit.unit, &text)?;
    Ok(())
}

/// 「LLM ↔ 工具调用」循环：返回最终回合的文本。
async fn drive_loop(
    ctx: &JobContext,
    history: &mut Vec<Message>,
    audit: &mut Vec<AuditEntry>,
) -> Result<String, UnitFailure> {
    let mut last_call: Option<(String, String)> = None;
    let mut same_calls = 0u32;

    loop {
        let mut call = ctx
            .llm
            .call(prompt::INSTRUCTIONS, history, &ctx.scheduler.specs())
            .await?;
        audit.push(AuditEntry::LlmRequest(call.request_body.clone()));

        let mut text = String::new();
        let mut tool_calls: Vec<ToolCallReq> = Vec::new();
        let mut finish = None;
        while let Some(event) = call.next_event().await? {
            match event {
                LlmEvent::TextDelta(delta) => text.push_str(&delta),
                LlmEvent::ToolCall(tc) => tool_calls.push(tc),
                LlmEvent::Finished(f) => finish = Some(f),
            }
        }
        audit.extend(call.raw_log().iter().cloned().map(AuditEntry::LlmEvent));

        if finish == Some(Finish::MaxTokens) {
            return Err(UnitFailure::Truncated);
        }
        if tool_calls.is_empty() {
            return match finish {
                Some(Finish::Stop) => {
                    if text.is_empty() {
                        Err(UnitFailure::EmptyOutput)
                    } else {
                        Ok(text)
                    }
                }
                Some(Finish::ToolUse) => Err(UnitFailure::Llm(LlmError::protocol(
                    "finish 声称工具调用但没有工具调用内容",
                    "",
                ))),
                // MaxTokens 已在上面返回
                _ => Err(UnitFailure::Llm(LlmError::protocol(
                    "流结束但没有收到完成事件",
                    "",
                ))),
            };
        }

        history.push(Message::Assistant {
            text,
            tool_calls: tool_calls.clone(),
        });
        for tc in tool_calls {
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

            // JSON 合法性在进入历史前校验一次（LLM 边界）；之后的模块直接信任。
            let arguments: serde_json::Value = serde_json::from_str(&tc.arguments)
                .map_err(|_| UnitFailure::BadToolArguments(tc.arguments.clone()))?;
            let result = ctx.scheduler.execute(&tc.name, arguments).await?;
            audit.push(AuditEntry::ToolResult(result.clone()));
            history.push(Message::ToolResult {
                call_id: tc.call_id,
                content: result,
            });
        }
    }
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
