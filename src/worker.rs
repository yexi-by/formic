//! 单单元执行：读分片 → 装配 prompt → 「LLM ↔ 工具调用」多轮循环 → 发布最终
//! 回合的文本或给出诊断。一切工具调用经调度器准入；连续完全相同的工具调用
//! 达到配置阈值时触发停滞检测；瞬时故障按预算重试；取消令牌随时可收敛本单元。

use std::fmt::{self, Write as _};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::compaction::{CompactionCallUsage, CompactionError, CompactionOutcome};
use crate::config::ExecutionConfig;
use crate::llm::{
    Finish, LlmClient, LlmError, LlmEvent, Message, ProviderUsage, ToolCallReq, ToolSpec,
};
use crate::output::{self, AuditEntry, UnitStats, WorkerState};
use crate::plan::{PlanUnit, Shard};
use crate::prompt;
use crate::scheduler::{Scheduler, SchedulerError};
use crate::structured::{OutputContract, SUBMIT_RESULT_TOOL};
use crate::tokenize;
use crate::tools::ReadRoot;

/// 第 attempt 次尝试失败后的退避（1 起始）。无实测证据，不引入指数/抖动策略。
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(attempt as u64)
}

/// 一个作业的运行上下文：全部单元共享的只读事实与依赖。
pub struct JobContext {
    pub data_root: ReadRoot,
    pub task: Arc<str>,
    pub listing: Arc<[String]>,
    pub llm: LlmClient,
    pub scheduler: Scheduler,
    pub out_root: output::OutputRoot,
    pub output_contract: OutputContract,
    pub execution: ExecutionConfig,
    pub model_tools: Arc<[ToolSpec]>,
    pub worker_run: output::WorkerRun,
    /// 最终发布与作业取消的线性化边界：worker 持读锁完成审计和原子发布，
    /// 信号处理持写锁取消根令牌，保证两者有唯一先后顺序。
    pub publish_gate: Arc<tokio::sync::RwLock<()>>,
    /// 作业启动时按输出模式冻结，全部单元字节一致。
    pub instructions: String,
}

/// 单元结局：发布成功，或被取消（在途丢弃，不算失败）。
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Published,
    Cancelled,
}

/// 单元失败：带对象与直接原因，细节最终进入 worker 运行档案。
#[derive(thiserror::Error, Debug)]
pub enum UnitFailure {
    #[error("读取分片文件 {path} 失败：{source}")]
    ReadShard {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("分片文件 {path} 不是合法 UTF-8，无法装配进 prompt")]
    ShardEncoding { path: PathBuf },
    #[error(
        "分片在完整读取前已达到模型安全输入预算（保守估算 {estimated_tokens} token，预算 {input_budget} token）；请减小该单元的文件数量或行范围"
    )]
    ShardTooLarge {
        estimated_tokens: u64,
        input_budget: u64,
    },
    #[error(transparent)]
    Llm(#[from] LlmError),
    #[error("停滞：连续 {limit} 次相同的工具调用 {name}（参数 {arguments}）")]
    Stalled {
        limit: u32,
        name: String,
        arguments: String,
    },
    #[error("模型输出达到协议长度上限，产出被截断")]
    Truncated,
    #[error("模型未产出任何文本")]
    EmptyOutput,
    #[error("模型拒绝执行本单元")]
    Refused,
    #[error("结构化结果连续 {attempts} 次无效，未发布记录；最后错误：{last_error}")]
    StructuredExhausted { attempts: u32, last_error: String },
    #[error(transparent)]
    Compaction(#[from] CompactionError),
    #[error("{cause}（重试 {retries} 次后仍失败）")]
    RetriesExhausted {
        retries: u32,
        cause: Box<UnitFailure>,
    },
    #[error("调度器故障：{0}")]
    Scheduler(#[from] SchedulerError),
    #[error("内部错误：worker task panic")]
    Panicked,
    #[error("写入输出区失败：{0}")]
    Output(#[from] std::io::Error),
}

/// 一次 LLM 调用的失败按可重试性分类。
enum CallFailure {
    Cancelled,
    ContextLimit(UnitFailure),
    Retryable(UnitFailure),
    Fatal(UnitFailure),
}

fn classify(failure: UnitFailure) -> CallFailure {
    match failure {
        UnitFailure::Llm(LlmError::ContextLimit { .. }) => CallFailure::ContextLimit(failure),
        UnitFailure::Llm(LlmError::StreamLimit { .. }) => CallFailure::Fatal(failure),
        // 429 与 5xx 之外的客户错误属配置/请求问题，重试无意义
        UnitFailure::Llm(LlmError::Http { status, .. }) if status != 429 && status < 500 => {
            CallFailure::Fatal(failure)
        }
        UnitFailure::Llm(_) => CallFailure::Retryable(failure),
        other => CallFailure::Fatal(other),
    }
}

/// 执行一个单元。发布成功、失败或取消都会记录现场；失败不留下完成记录。
/// stats 随执行累计（指标分析的派生数据，token 为内部估算值）。
pub async fn run_unit(
    ctx: &JobContext,
    unit: &PlanUnit,
    cancel: CancellationToken,
    stats: &mut UnitStats,
) -> Result<Outcome, UnitFailure> {
    // 审计先于分片读取创建，使输入读取失败也能留下 worker 的状态现场。
    let mut audit = output::AuditLog::create(&ctx.worker_run, unit.unit)?;
    audit.push(&AuditEntry::State {
        state: WorkerState::Preparing,
        reason: "读取计划分片并构造首条用户消息".into(),
    })?;
    let root = ctx.data_root.clone();
    let planned_shard = unit.shard.clone();
    let read_cancel = cancel.clone();
    let task = Arc::clone(&ctx.task);
    let listing = Arc::clone(&ctx.listing);
    let input_budget = ctx.llm.input_budget(ctx.execution.context_safety_tokens);
    let empty_history = [Message::User(String::new())];
    let request_base_tokens =
        ctx.llm
            .estimate_request_tokens(&ctx.instructions, &empty_history, &ctx.model_tools);
    let reader = tokio::task::spawn_blocking(move || {
        read_shard(
            &root,
            &planned_shard,
            &task,
            &listing,
            request_base_tokens,
            input_budget,
            &read_cancel,
        )
    });
    let shard_result = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(ShardReadError::Cancelled),
        result = reader => result.unwrap_or_else(|_| {
            Err(ShardReadError::Failure(UnitFailure::Panicked))
        }),
    };
    let user = match shard_result {
        Ok(user) => user,
        Err(ShardReadError::Cancelled) => {
            audit.push(&AuditEntry::State {
                state: WorkerState::Cancelled,
                reason: "读取计划分片时收到取消信号".into(),
            })?;
            audit.finish()?;
            return Ok(Outcome::Cancelled);
        }
        Err(ShardReadError::Failure(failure)) => {
            audit.push(&AuditEntry::State {
                state: WorkerState::Failed,
                reason: failure.to_string(),
            })?;
            audit.finish()?;
            return Err(failure);
        }
    };
    let mut history = vec![Message::User(user)];
    let exact_tokens =
        ctx.llm
            .estimate_request_tokens(&ctx.instructions, &history, &ctx.model_tools);
    if exact_tokens > input_budget {
        let failure = UnitFailure::ShardTooLarge {
            estimated_tokens: exact_tokens,
            input_budget,
        };
        audit.push(&AuditEntry::State {
            state: WorkerState::Failed,
            reason: failure.to_string(),
        })?;
        audit.finish()?;
        return Err(failure);
    }
    let mut meter = Metering {
        tracked: message_size(&history[0]),
        history_tokens: tokenize::count_message(&history[0]),
        stats,
    };
    audit.push(&AuditEntry::State {
        state: WorkerState::Ready,
        reason: format!(
            "首条用户消息已构造，当前历史估算 {} token",
            meter.history_tokens
        ),
    })?;
    // 对话历史是当前内存主导项的候选，全程计量以验证（规模观测）
    crate::metrics::gauge_add(&crate::metrics::HISTORY_BYTES, meter.tracked);

    let mut outcome = drive_loop(
        ctx,
        unit.unit,
        &mut history,
        &mut audit,
        &cancel,
        &mut meter,
    )
    .await;
    let _publish_guard = if matches!(outcome, Ok(LoopEnd::Published(_))) {
        Some(ctx.publish_gate.read().await)
    } else {
        None
    };
    if matches!(outcome, Ok(LoopEnd::Published(_))) && cancel.is_cancelled() {
        outcome = Ok(LoopEnd::Cancelled);
    }
    crate::metrics::gauge_add(&crate::metrics::HISTORY_BYTES, -meter.tracked);
    match &outcome {
        Ok(LoopEnd::Published(_)) => audit.push(&AuditEntry::State {
            state: WorkerState::ReadyToPublish,
            reason: "最终结果已经满足当前输出契约".into(),
        })?,
        Ok(LoopEnd::Cancelled) => audit.push(&AuditEntry::State {
            state: WorkerState::Cancelled,
            reason: "收到取消信号，丢弃尚未发布的结果".into(),
        })?,
        Err(failure) => audit.push(&AuditEntry::State {
            state: WorkerState::Failed,
            reason: failure.to_string(),
        })?,
    }
    // 证据完整是契约要求：取消与失败的现场也落盘，由审计还原经过。
    audit.finish()?;
    match outcome? {
        LoopEnd::Published(text) => {
            output::publish(
                &ctx.out_root,
                unit.unit,
                &text,
                ctx.output_contract.format(),
            )?;
            Ok(Outcome::Published)
        }
        LoopEnd::Cancelled => Ok(Outcome::Cancelled),
    }
}

fn message_size(message: &Message) -> i64 {
    let size: usize = match message {
        Message::User(text) => text.len(),
        Message::Compaction(text) => text.len(),
        Message::Assistant { text, tool_calls } => {
            text.len()
                + tool_calls
                    .iter()
                    .map(|tc| tc.call_id.len() + tc.name.len() + tc.arguments.len())
                    .sum::<usize>()
        }
        Message::ResponseOutputItems(items) => items
            .iter()
            .map(|item| serde_json::to_string(item).map_or(0, |text| text.len()))
            .sum(),
        Message::ToolResult { call_id, content } => call_id.len() + content.len(),
    };
    size.try_into().unwrap_or(i64::MAX)
}

enum LoopEnd {
    Published(String),
    Cancelled,
}

/// 一个单元的计量：内存字节（metrics 观测）、历史 token（估算）、单元统计。
struct Metering<'a> {
    tracked: i64,
    history_tokens: u64,
    stats: &'a mut UnitStats,
}

/// 「LLM ↔ 工具调用」循环。
async fn drive_loop(
    ctx: &JobContext,
    unit: u64,
    history: &mut Vec<Message>,
    audit: &mut output::AuditLog,
    cancel: &CancellationToken,
    meter: &mut Metering<'_>,
) -> Result<LoopEnd, UnitFailure> {
    let mut last_call: Option<(String, String)> = None;
    let mut same_calls = 0u32;
    let mut invalid_submissions = 0u32;
    let mut emergency_compacted = false;

    loop {
        match crate::compaction::compact_if_needed(
            &ctx.llm,
            &ctx.instructions,
            &ctx.model_tools,
            &ctx.execution,
            history,
            audit,
            cancel,
            false,
        )
        .await
        {
            Ok(CompactionOutcome::Unchanged) => {}
            Ok(CompactionOutcome::Replaced {
                before_tokens,
                after_tokens,
                calls,
            }) => refresh_after_compaction(history, meter, before_tokens, after_tokens, &calls),
            Err(CompactionError::Cancelled) => {
                return Ok(LoopEnd::Cancelled);
            }
            Err(error) => return Err(error.into()),
        }
        // 单次 LLM 调用 + 重试预算：同一历史重发，每次尝试独立留痕
        let mut attempt = 0u32;
        let turn = loop {
            attempt += 1;
            meter.stats.llm_calls += 1;
            let request_tokens =
                ctx.llm
                    .estimate_request_tokens(&ctx.instructions, history, &ctx.model_tools);
            meter.stats.input_tokens += request_tokens;
            if attempt > 1 {
                meter.stats.retries += 1;
            }
            audit.push(&AuditEntry::State {
                state: WorkerState::RequestingModel,
                reason: format!(
                    "第 {attempt} 次尝试，消息 {} 条，完整请求估算 {request_tokens} token",
                    history.len()
                ),
            })?;
            match one_turn(ctx, history, audit, attempt, cancel).await {
                Ok(turn) => break turn,
                Err(CallFailure::Cancelled) => return Ok(LoopEnd::Cancelled),
                Err(CallFailure::ContextLimit(failure)) => {
                    if emergency_compacted {
                        return Err(failure);
                    }
                    emergency_compacted = true;
                    match crate::compaction::compact_if_needed(
                        &ctx.llm,
                        &ctx.instructions,
                        &ctx.model_tools,
                        &ctx.execution,
                        history,
                        audit,
                        cancel,
                        true,
                    )
                    .await
                    {
                        Ok(CompactionOutcome::Replaced {
                            before_tokens,
                            after_tokens,
                            calls,
                        }) => {
                            refresh_after_compaction(
                                history,
                                meter,
                                before_tokens,
                                after_tokens,
                                &calls,
                            );
                            continue;
                        }
                        Err(CompactionError::Cancelled) => {
                            return Ok(LoopEnd::Cancelled);
                        }
                        Ok(CompactionOutcome::Unchanged) => return Err(failure),
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(CallFailure::Fatal(f)) => return Err(f),
                Err(CallFailure::Retryable(f)) => {
                    if attempt >= ctx.execution.llm_attempts {
                        return Err(UnitFailure::RetriesExhausted {
                            retries: attempt - 1,
                            cause: Box::new(f),
                        });
                    }
                    let delay = backoff(attempt);
                    audit.push(&AuditEntry::Retry {
                        attempt,
                        next_attempt: attempt + 1,
                        delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        reason: f.to_string(),
                    })?;
                    audit.push(&AuditEntry::State {
                        state: WorkerState::RetryingModel,
                        reason: format!("第 {attempt} 次模型调用失败，等待后重试：{f}"),
                    })?;
                    eprintln!("第 {attempt} 次调用失败（{f}），退避后重试");
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(LoopEnd::Cancelled),
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        };
        let (turn, usage) = turn;
        meter.stats.record_provider_usage(&usage);
        meter.stats.turns += 1;
        audit.push(&AuditEntry::State {
            state: WorkerState::InterpretingModel,
            reason: match &turn {
                TurnEnd::FinalText { text, .. } => {
                    format!("模型返回最终文本，正文 {} bytes", text.len())
                }
                TurnEnd::ToolCalls { text, calls, .. } => format!(
                    "模型返回 {} 个工具调用，伴随文本 {} bytes",
                    calls.len(),
                    text.len()
                ),
            },
        })?;

        match turn {
            TurnEnd::FinalText {
                text,
                response_output_items,
            } => {
                meter.stats.output_tokens += tokenize::count(&text);
                if !ctx.output_contract.is_structured() {
                    return Ok(LoopEnd::Published(text));
                }
                let reason = "结构化模式不能用最终文本提交结果；请单独调用 formic_submit_result";
                audit.push(&AuditEntry::OutputValidation {
                    valid: false,
                    instance_path: None,
                    schema_path: None,
                    reason: reason.into(),
                })?;
                audit.push(&AuditEntry::State {
                    state: WorkerState::CorrectingOutput,
                    reason: reason.into(),
                })?;
                invalid_submissions += 1;
                meter.stats.structured_corrections += 1;
                if invalid_submissions >= ctx.execution.llm_attempts {
                    return Err(UnitFailure::StructuredExhausted {
                        attempts: invalid_submissions,
                        last_error: reason.into(),
                    });
                }
                push_history(
                    history,
                    meter,
                    Message::Assistant {
                        text,
                        tool_calls: Vec::new(),
                    },
                );
                push_response_output_items(history, meter, response_output_items);
                push_history(history, meter, Message::User(reason.into()));
            }
            TurnEnd::ToolCalls {
                text,
                calls,
                response_output_items,
            } => {
                meter.stats.output_tokens += tokenize::count(&text)
                    + calls
                        .iter()
                        .map(|c| tokenize::count_tool_call(&c.req))
                        .sum::<u64>();
                let has_submit = calls.iter().any(|call| call.req.name == SUBMIT_RESULT_TOOL);
                let mixed_submit = has_submit && (calls.len() != 1 || !text.trim().is_empty());
                let assistant = Message::Assistant {
                    text,
                    tool_calls: calls.iter().map(|c| c.req.clone()).collect(),
                };
                push_history(history, meter, assistant);
                push_response_output_items(history, meter, response_output_items);
                let mut submission_error = None;
                for prepared in calls {
                    let tc = prepared.req;
                    audit.push(&AuditEntry::ToolCall {
                        name: tc.name.clone(),
                        source: if tc.name == SUBMIT_RESULT_TOOL {
                            Some("internal".into())
                        } else {
                            ctx.scheduler.source(&tc.name).map(|source| match source {
                                crate::scheduler::ToolSource::Builtin => "builtin".to_string(),
                                crate::scheduler::ToolSource::Mcp {
                                    server,
                                    remote_name,
                                } => format!("mcp:{server}/{remote_name}"),
                            })
                        },
                        arguments: prepared.raw_arguments.clone(),
                    })?;
                    if tc.name == SUBMIT_RESULT_TOOL && ctx.output_contract.is_structured() {
                        last_call = None;
                        same_calls = 0;
                        let (validation, validation_issue) = if mixed_submit {
                            (
                                Err(
                                    "formic_submit_result 必须在不含文本和其他工具调用的回合中单独出现"
                                        .to_string(),
                                ),
                                None,
                            )
                        } else if let Some(error) = &prepared.argument_error {
                            (Err(error.clone()), None)
                        } else {
                            match ctx.output_contract.validate_submission(&prepared.arguments) {
                                Ok(content) => (Ok(content), None),
                                Err(issue) => (Err(issue.to_string()), Some(issue)),
                            }
                        };
                        match validation {
                            Ok(content) => {
                                audit.push(&AuditEntry::OutputValidation {
                                    valid: true,
                                    instance_path: None,
                                    schema_path: None,
                                    reason: "结构化结果验证通过".into(),
                                })?;
                                return Ok(LoopEnd::Published(content));
                            }
                            Err(reason) => {
                                let (instance_path, schema_path, audit_reason) = validation_issue
                                    .map(|issue| {
                                        (
                                            Some(issue.instance_path),
                                            Some(issue.schema_path),
                                            issue.reason,
                                        )
                                    })
                                    .unwrap_or_else(|| (None, None, reason.clone()));
                                audit.push(&AuditEntry::OutputValidation {
                                    valid: false,
                                    instance_path,
                                    schema_path,
                                    reason: audit_reason,
                                })?;
                                audit.push(&AuditEntry::State {
                                    state: WorkerState::CorrectingOutput,
                                    reason: reason.clone(),
                                })?;
                                let result = format!("错误：{reason}；请修正后单独提交");
                                audit.push(&AuditEntry::ToolResult(result.clone()))?;
                                push_history(
                                    history,
                                    meter,
                                    Message::ToolResult {
                                        call_id: tc.call_id,
                                        content: result,
                                    },
                                );
                                submission_error = Some(reason);
                            }
                        }
                        continue;
                    }
                    *meter.stats.tool_calls.entry(tc.name.clone()).or_default() += 1;
                    let current = (tc.name.clone(), prepared.raw_arguments.clone());
                    if last_call.as_ref() == Some(&current) {
                        same_calls += 1;
                    } else {
                        same_calls = 1;
                        last_call = Some(current);
                    }
                    if same_calls >= ctx.execution.identical_tool_call_limit {
                        return Err(UnitFailure::Stalled {
                            limit: ctx.execution.identical_tool_call_limit,
                            name: tc.name,
                            arguments: prepared.raw_arguments,
                        });
                    }
                    let result = if let Some(error) = prepared.argument_error {
                        audit.push(&AuditEntry::State {
                            state: WorkerState::CorrectingToolCall,
                            reason: format!("工具 {} 的参数无效：{error}", tc.name),
                        })?;
                        crate::scheduler::ToolResponse {
                            content: format!("错误：{error}"),
                            cache: crate::scheduler::CacheDisposition::Bypassed,
                            cache_evictions: 0,
                            wait_ms: 0,
                            execution_ms: 0,
                            mcp_server: None,
                            mcp_current_in_flight: None,
                            mcp_peak_in_flight: None,
                        }
                    } else {
                        audit.push(&AuditEntry::State {
                            state: WorkerState::WaitingForTool,
                            reason: format!("工具 {} 已进入调度器，等待准入和执行", tc.name),
                        })?;
                        tokio::select! {
                            _ = cancel.cancelled() => return Ok(LoopEnd::Cancelled),
                            result = ctx.scheduler.execute(unit, &tc.name, prepared.arguments, cancel.clone()) => result?,
                        }
                    };
                    audit.push(&AuditEntry::ToolExecution {
                        name: tc.name.clone(),
                        cache: cache_disposition_key(result.cache).into(),
                        wait_ms: result.wait_ms,
                        execution_ms: result.execution_ms,
                        result_bytes: result.content.len(),
                        mcp_server: result.mcp_server.clone(),
                    })?;
                    audit.push(&AuditEntry::ToolResult(result.content.clone()))?;
                    meter.stats.record_tool_response(&tc.name, &result);
                    push_history(
                        history,
                        meter,
                        Message::ToolResult {
                            call_id: tc.call_id,
                            content: result.content,
                        },
                    );
                    audit.push(&AuditEntry::State {
                        state: WorkerState::Ready,
                        reason: format!("工具 {} 的结果已附加到对话历史", tc.name),
                    })?;
                }
                if let Some(last_error) = submission_error {
                    invalid_submissions += 1;
                    meter.stats.structured_corrections += 1;
                    if invalid_submissions >= ctx.execution.llm_attempts {
                        return Err(UnitFailure::StructuredExhausted {
                            attempts: invalid_submissions,
                            last_error,
                        });
                    }
                }
            }
        }
    }
}

fn push_history(history: &mut Vec<Message>, meter: &mut Metering<'_>, message: Message) {
    let size = message_size(&message);
    let tokens = tokenize::count_message(&message);
    history.push(message);
    meter.tracked += size;
    meter.history_tokens += tokens;
    crate::metrics::gauge_add(&crate::metrics::HISTORY_BYTES, size);
}

fn push_response_output_items(
    history: &mut Vec<Message>,
    meter: &mut Metering<'_>,
    items: Vec<serde_json::Value>,
) {
    if !items.is_empty() {
        push_history(history, meter, Message::ResponseOutputItems(items));
    }
}

fn cache_disposition_key(disposition: crate::scheduler::CacheDisposition) -> &'static str {
    match disposition {
        crate::scheduler::CacheDisposition::Disabled => "disabled",
        crate::scheduler::CacheDisposition::Bypassed => "bypassed",
        crate::scheduler::CacheDisposition::Hit => "hit",
        crate::scheduler::CacheDisposition::Miss => "miss",
        crate::scheduler::CacheDisposition::Joined => "joined",
    }
}

fn refresh_after_compaction(
    history: &[Message],
    meter: &mut Metering<'_>,
    before_tokens: u64,
    after_tokens: u64,
    calls: &[CompactionCallUsage],
) {
    let new_tracked: i64 = history.iter().map(message_size).sum();
    crate::metrics::gauge_add(
        &crate::metrics::HISTORY_BYTES,
        new_tracked.saturating_sub(meter.tracked),
    );
    meter.tracked = new_tracked;
    meter.history_tokens = history.iter().map(tokenize::count_message).sum();
    meter.stats.compactions += 1;
    meter.stats.compaction_before_tokens += before_tokens;
    meter.stats.compaction_after_tokens += after_tokens;
    for call in calls {
        meter.stats.llm_calls += 1;
        meter.stats.input_tokens += call.estimated_input_tokens;
        meter.stats.record_provider_usage(&call.provider_usage);
    }
}

/// 一个回合的结局：最终文本（可发布）或待执行的工具调用（参数已校验）。
enum TurnEnd {
    FinalText {
        text: String,
        response_output_items: Vec<serde_json::Value>,
    },
    ToolCalls {
        text: String,
        calls: Vec<PreparedCall>,
        response_output_items: Vec<serde_json::Value>,
    },
}

struct PreparedCall {
    req: ToolCallReq,
    raw_arguments: String,
    arguments: serde_json::Value,
    argument_error: Option<String>,
}

/// 一个回合：发起一次流式调用，收完事件，解释结局。可取消。
async fn one_turn(
    ctx: &JobContext,
    history: &[Message],
    audit: &mut output::AuditLog,
    attempt: u32,
    cancel: &CancellationToken,
) -> Result<(TurnEnd, ProviderUsage), CallFailure> {
    let prepared = ctx
        .llm
        .prepare_call(&ctx.instructions, history, &ctx.model_tools);
    audit
        .push_llm_request(attempt, prepared.body())
        .map_err(|e| CallFailure::Fatal(UnitFailure::Output(e)))?;
    let mut call = tokio::select! {
        _ = cancel.cancelled() => return Err(CallFailure::Cancelled),
        r = ctx.llm.send(prepared) => r,
    }
    .map_err(|e| classify(e.into()))?;

    let mut text = String::new();
    let mut tool_calls: Vec<ToolCallReq> = Vec::new();
    let mut finish = None;
    let mut usage = ProviderUsage::default();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            r = call.next_event() => Some(r),
        };
        let Some(next) = next else {
            let snapshot = call.take_audit_snapshot();
            audit
                .push_llm_event_stream(&snapshot)
                .map_err(|e| CallFailure::Fatal(UnitFailure::Output(e)))?;
            return Err(CallFailure::Cancelled);
        };
        match next {
            Ok(Some(LlmEvent::TextDelta(delta))) => text.push_str(&delta),
            Ok(Some(LlmEvent::ToolCall(tc))) => tool_calls.push(tc),
            Ok(Some(LlmEvent::Usage(update))) => usage.merge(update),
            Ok(Some(LlmEvent::Finished(f))) => finish = Some(f),
            Ok(None) => break,
            Err(e) => {
                let snapshot = call.take_audit_snapshot();
                audit
                    .push_llm_event_stream(&snapshot)
                    .map_err(|e| CallFailure::Fatal(UnitFailure::Output(e)))?;
                return Err(classify(e.into()));
            }
        }
    }
    let snapshot = call.take_audit_snapshot();
    audit
        .push_llm_event_stream(&snapshot)
        .map_err(|e| CallFailure::Fatal(UnitFailure::Output(e)))?;
    let response_output_items = call.response_output_items().to_vec();

    if finish == Some(Finish::MaxTokens) {
        return Err(CallFailure::Fatal(UnitFailure::Truncated));
    }
    if finish == Some(Finish::Refusal) {
        return Err(CallFailure::Fatal(UnitFailure::Refused));
    }
    if tool_calls.is_empty() {
        return match finish {
            Some(Finish::Stop) => {
                if text.is_empty() && !ctx.output_contract.is_structured() {
                    Err(CallFailure::Fatal(UnitFailure::EmptyOutput))
                } else {
                    Ok((
                        TurnEnd::FinalText {
                            text,
                            response_output_items,
                        },
                        usage,
                    ))
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

    if finish != Some(Finish::ToolUse) {
        return Err(CallFailure::Retryable(UnitFailure::Llm(
            LlmError::protocol("收到工具调用内容，但没有收到明确的工具调用完成事件", ""),
        )));
    }

    // 参数在 LLM 边界只解析一次。非法 JSON 以合成工具结果回注，历史中使用合法空 object。
    let mut calls = Vec::with_capacity(tool_calls.len());
    for mut tc in tool_calls {
        let raw_arguments = tc.arguments.clone();
        let (arguments, argument_error) = match serde_json::from_str(&raw_arguments) {
            Ok(arguments) => (arguments, None),
            Err(error) => {
                tc.arguments = "{}".into();
                (
                    serde_json::json!({}),
                    Some(format!("模型给出的工具参数不是合法 JSON：{error}")),
                )
            }
        };
        calls.push(PreparedCall {
            req: tc,
            raw_arguments,
            arguments,
            argument_error,
        });
    }
    Ok((
        TurnEnd::ToolCalls {
            text,
            calls,
            response_output_items,
        },
        usage,
    ))
}

#[derive(Debug)]
enum ShardReadError {
    Cancelled,
    Failure(UnitFailure),
}

fn read_shard(
    root: &ReadRoot,
    shard: &Shard,
    task: &str,
    listing: &[String],
    request_base_tokens: u64,
    input_budget: u64,
    cancel: &CancellationToken,
) -> Result<String, ShardReadError> {
    let mut message = InitialMessageBuilder::new(request_base_tokens, input_budget)?;
    if prompt::write_user_prefix(&mut message, task, listing).is_err() {
        return Err(message.too_large());
    }
    match shard {
        Shard::Files(files) => {
            for (index, file) in files.iter().enumerate() {
                let path = prompt::slash_path(file);
                if prompt::write_file_header(&mut message, index, &path).is_err() {
                    return Err(message.too_large());
                }
                let content_start = message.len();
                let complete = crate::tools::stream_utf8_file(root, file, cancel, |text| {
                    message.write_str(text).is_ok()
                })
                .map_err(|error| shard_read_error(file, error))?;
                if !complete {
                    return Err(message.too_large());
                }
                message.trim_content_newlines(content_start);
                if message.write_char('\n').is_err() {
                    return Err(message.too_large());
                }
            }
        }
        Shard::Lines { file, start, end } => {
            let path = prompt::slash_path(file);
            if prompt::write_line_header(&mut message, &path, *start, *end).is_err() {
                return Err(message.too_large());
            }
            let content_start = message.len();
            let complete = crate::tools::stream_utf8_lines(
                root,
                file,
                *start,
                Some(*end),
                crate::tools::LineRender::Plain,
                cancel,
                |text| message.write_str(text).is_ok(),
            )
            .map_err(|error| shard_read_error(file, error))?;
            if !complete {
                return Err(message.too_large());
            }
            message.trim_content_newlines(content_start);
            if message.write_char('\n').is_err() {
                return Err(message.too_large());
            }
        }
    }
    Ok(message.finish())
}

struct InitialMessageBuilder {
    text: String,
    estimated_tokens: u64,
    rejected_tokens: u64,
    input_budget: u64,
}

impl InitialMessageBuilder {
    fn new(request_base_tokens: u64, input_budget: u64) -> Result<Self, ShardReadError> {
        let builder = Self {
            text: String::new(),
            estimated_tokens: request_base_tokens,
            rejected_tokens: request_base_tokens,
            input_budget,
        };
        if request_base_tokens > input_budget {
            Err(builder.too_large())
        } else {
            Ok(builder)
        }
    }

    fn len(&self) -> usize {
        self.text.len()
    }

    fn trim_content_newlines(&mut self, content_start: usize) {
        let trimmed = self.text[content_start..].trim_end_matches('\n').len();
        self.text.truncate(content_start + trimmed);
    }

    fn too_large(&self) -> ShardReadError {
        ShardReadError::Failure(UnitFailure::ShardTooLarge {
            estimated_tokens: self.rejected_tokens.max(self.estimated_tokens),
            input_budget: self.input_budget,
        })
    }

    fn finish(self) -> String {
        self.text
    }
}

impl fmt::Write for InitialMessageBuilder {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        // 每块独立按 JSON 字符串计数：这包含转义与额外引号，
        // 各块之和是最终请求的保守上界，不依赖字节数猜测 token。
        let encoded = serde_json::to_string(text).expect("str 可以序列化为 JSON");
        let added = tokenize::count(&encoded);
        let next = self.estimated_tokens.saturating_add(added);
        if next > self.input_budget {
            self.rejected_tokens = next;
            return Err(fmt::Error);
        }
        self.estimated_tokens = next;
        self.text.push_str(text);
        Ok(())
    }
}

fn shard_read_error(path: &Path, error: io::Error) -> ShardReadError {
    if error.kind() == io::ErrorKind::Interrupted {
        ShardReadError::Cancelled
    } else if error.kind() == io::ErrorKind::InvalidData {
        ShardReadError::Failure(UnitFailure::ShardEncoding {
            path: path.to_path_buf(),
        })
    } else {
        ShardReadError::Failure(UnitFailure::ReadShard {
            path: path.to_path_buf(),
            source: error,
        })
    }
}

/// 递归列出数据根内全部文件，返回排序后的根内相对表示（`/` 分隔）。
pub fn list_files(root: &ReadRoot) -> std::io::Result<Vec<String>> {
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
    use std::fs;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 迷你一次性应答 server：Slow 长时间不响应；Status(code) 以固定状态码应答。
    /// 返回端口与请求计数。
    #[derive(Clone, Copy)]
    enum Mode {
        Slow,
        Status(u16),
        Sse(&'static str),
        SseThenSlow(&'static str),
    }

    fn consume_request(stream: &TcpStream) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        let mut len = 0usize;
        while reader.read_line(&mut line).is_ok() {
            let trimmed = line.trim().to_string();
            line.clear();
            if trimmed.is_empty() {
                break;
            }
            let lower = trimmed.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("content-length:") {
                len = value.trim().parse().unwrap_or(0);
            }
        }
        let mut body = vec![0u8; len];
        reader.read_exact(&mut body).ok();
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
                        consume_request(&stream);
                        let body_text = "err";
                        let response = format!(
                            "HTTP/1.1 {code} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body_text}",
                            body_text.len()
                        );
                        stream.write_all(response.as_bytes()).ok();
                    }
                    Mode::Sse(body) => {
                        consume_request(&stream);
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).ok();
                    }
                    Mode::SseThenSlow(body) => {
                        consume_request(&stream);
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
                        );
                        stream.write_all(response.as_bytes()).ok();
                        stream.flush().ok();
                        std::thread::sleep(Duration::from_secs(30));
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
        let tools = crate::config::ToolsConfig {
            max_in_flight: 4,
            search: crate::config::SearchToolConfig {
                enabled: true,
                max_result_bytes: 32768,
                max_in_flight: 4,
                max_matches: 100,
                max_context_lines: 20,
            },
            read: crate::config::ReadToolConfig {
                enabled: true,
                max_result_bytes: 32768,
                max_in_flight: 4,
            },
        };
        let out_root = crate::output::OutputRoot::open(out.clone()).unwrap();
        let out_read_root = ReadRoot::from_dir(out_root.clone_dir().unwrap());
        let scheduler = Scheduler::start(
            crate::scheduler::ToolRegistry::builtins(&tools),
            Roots {
                input: ReadRoot::open(data.clone()).unwrap(),
                output: out_read_root,
                output_format: crate::output::RecordFormat::Markdown,
            },
            &tools,
            &crate::config::CacheConfig {
                enabled: true,
                max_bytes: 1024 * 1024,
            },
            4,
        );
        let model_tools = scheduler.specs();
        let worker_run = crate::output::WorkerRun::create(
            &out_root,
            crate::output::JobReportFacts {
                protocol: "completions".into(),
                model: "m".into(),
                context_window_tokens: 131072,
                anthropic_max_tokens: None,
                context_safety_tokens: 4096,
                concurrency: 4,
                output_format: crate::output::RecordFormat::Markdown,
                tools: model_tools.iter().map(|tool| tool.name.clone()).collect(),
            },
        )
        .unwrap();
        let ctx = JobContext {
            scheduler,
            data_root: ReadRoot::open(data).unwrap(),
            task: "任务。".into(),
            listing: vec!["a.txt".into()].into(),
            llm: LlmClient::new(LlmConfig {
                protocol: Protocol::Completions,
                base_url: format!("http://127.0.0.1:{port}"),
                model: "m".into(),
                api_key: None,
                context_window_tokens: 131072,
                anthropic_max_tokens: None,
            }),
            out_root,
            output_contract: OutputContract::Text,
            execution: ExecutionConfig {
                llm_attempts: 3,
                identical_tool_call_limit: 3,
                context_safety_tokens: 4096,
            },
            model_tools,
            worker_run,
            publish_gate: Arc::new(tokio::sync::RwLock::new(())),
            instructions: crate::prompt::instructions(false).to_string(),
        };
        (dir, ctx)
    }

    fn unit() -> PlanUnit {
        PlanUnit {
            unit: 1,
            shard: Shard::Files(vec![PathBuf::from("a.txt")]),
        }
    }

    #[test]
    fn line_shard_reads_only_the_planned_range() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("range.txt"), b"first\nsecond\n\xff").unwrap();
        let shard = Shard::Lines {
            file: PathBuf::from("range.txt"),
            start: 1,
            end: 2,
        };
        let root = ReadRoot::open(root.to_path_buf()).unwrap();
        let result = read_shard(
            &root,
            &shard,
            "任务。",
            &["range.txt".into()],
            0,
            u64::MAX,
            &CancellationToken::new(),
        )
        .expect("计划范围内的合法 UTF-8 应可读取");
        assert!(result.ends_with("first\nsecond\n"), "{result}");
    }

    #[test]
    fn streamed_file_message_keeps_the_existing_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("a.txt"), "alpha\n\n").unwrap();
        fs::write(root.join("b.txt"), "beta\r\n").unwrap();
        let read_root = ReadRoot::open(root.to_path_buf()).unwrap();
        let shard = Shard::Files(vec!["a.txt".into(), "b.txt".into()]);
        let task = "任务。\n";
        let listing = vec!["a.txt".into(), "b.txt".into()];
        let actual = read_shard(
            &read_root,
            &shard,
            task,
            &listing,
            0,
            u64::MAX,
            &CancellationToken::new(),
        )
        .unwrap();
        let expected = prompt::build_user_message(
            task,
            &listing,
            &crate::prompt::ShardContent::Files(vec![
                ("a.txt".into(), "alpha\n\n".into()),
                ("b.txt".into(), "beta\r\n".into()),
            ]),
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn large_files_share_one_model_input_budget() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("a.txt"), "first file\n").unwrap();
        fs::write(root.join("b.txt"), "甲乙丙丁".repeat(64 * 1024)).unwrap();
        let read_root = ReadRoot::open(root.to_path_buf()).unwrap();
        let shard = Shard::Files(vec!["a.txt".into(), "b.txt".into()]);
        let error = read_shard(
            &read_root,
            &shard,
            "任务。",
            &["a.txt".into(), "b.txt".into()],
            0,
            1_000,
            &CancellationToken::new(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                ShardReadError::Failure(UnitFailure::ShardTooLarge {
                    input_budget: 1_000,
                    ..
                })
            ),
            "{error:?}"
        );
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
        let outcome = run_unit(&ctx, &unit(), token, &mut UnitStats::default())
            .await
            .unwrap();
        assert_eq!(outcome, Outcome::Cancelled);
        assert!(
            !dir.path().join("out").join("1.md").exists(),
            "取消单元不得留下记录"
        );
        assert!(ctx.worker_run.audit_path(1).exists(), "取消现场仍应落审计");
    }

    #[tokio::test]
    async fn cancellation_wins_before_the_publish_commit_point() {
        const BODY: &str = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let (port, count) = mini_server(Mode::Sse(BODY));
        let (dir, ctx) = fixture(port);
        let ctx = Arc::new(ctx);
        let publish_guard = Arc::clone(&ctx.publish_gate).write_owned().await;
        let token = CancellationToken::new();
        let run_token = token.clone();
        let run_ctx = Arc::clone(&ctx);
        let worker = tokio::spawn(async move {
            run_unit(&run_ctx, &unit(), run_token, &mut UnitStats::default()).await
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while count.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        drop(publish_guard);

        let outcome = worker.await.unwrap().unwrap();
        assert_eq!(outcome, Outcome::Cancelled);
        assert!(!dir.path().join("out/1.md").exists());
        let audit = fs::read_to_string(ctx.worker_run.audit_path(1)).unwrap();
        assert!(audit.contains("\"state\":\"cancelled\""), "{audit}");
    }

    #[tokio::test]
    async fn tool_item_without_explicit_finish_is_not_accepted() {
        const BODY: &str = "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n";
        let (port, _count) = mini_server(Mode::Sse(BODY));
        let (_dir, mut ctx) = fixture(port);
        ctx.llm = LlmClient::new(LlmConfig {
            protocol: Protocol::Responses,
            base_url: format!("http://127.0.0.1:{port}"),
            model: "m".into(),
            api_key: None,
            context_window_tokens: 131072,
            anthropic_max_tokens: None,
        });
        let mut audit = output::AuditLog::create(&ctx.worker_run, 1).unwrap();
        let history = vec![Message::User("任务".into())];
        let result = one_turn(&ctx, &history, &mut audit, 1, &CancellationToken::new()).await;
        assert!(
            matches!(
                result,
                Err(CallFailure::Retryable(UnitFailure::Llm(
                    LlmError::Protocol { .. }
                )))
            ),
            "没有明确完成事件的工具调用必须作为协议失败"
        );
        audit.finish().unwrap();
    }

    #[tokio::test]
    async fn events_after_stop_cannot_replace_the_terminal_state_with_tool_use() {
        const BODY: &str = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n"
        );
        let (port, _count) = mini_server(Mode::Sse(BODY));
        let (_dir, ctx) = fixture(port);
        let mut audit = output::AuditLog::create(&ctx.worker_run, 1).unwrap();
        let history = vec![Message::User("任务".into())];

        let result = one_turn(&ctx, &history, &mut audit, 1, &CancellationToken::new()).await;

        assert!(
            matches!(
                result,
                Err(CallFailure::Retryable(UnitFailure::Llm(
                    LlmError::Protocol { .. }
                )))
            ),
            "Stop 后的工具事件必须使整回合成为协议错误，不能覆盖首个终态"
        );
        let report = audit.finish().unwrap();
        let text = fs::read_to_string(report).unwrap();
        assert!(text.contains("call_1"), "违规帧仍须进入原始审计：{text}");
    }

    #[tokio::test]
    async fn cancellation_persists_sse_received_before_the_signal() {
        const BODY: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"partial-before-cancel\"},\"finish_reason\":null}]}\n\n";
        let (port, _count) = mini_server(Mode::SseThenSlow(BODY));
        let (_dir, ctx) = fixture(port);
        let token = CancellationToken::new();
        let trigger = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let mut audit = output::AuditLog::create(&ctx.worker_run, 1).unwrap();
        let history = vec![Message::User("任务".into())];
        let result = one_turn(&ctx, &history, &mut audit, 1, &token).await;
        assert!(matches!(result, Err(CallFailure::Cancelled)));
        let report = audit.finish().unwrap();
        let text = fs::read_to_string(report).unwrap();
        assert!(text.contains("partial-before-cancel"), "{text}");
    }

    #[tokio::test]
    async fn cancellation_persists_an_incomplete_sse_frame() {
        const BODY: &str =
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial-without-delimiter\"}";
        let (port, _count) = mini_server(Mode::SseThenSlow(BODY));
        let (_dir, ctx) = fixture(port);
        let token = CancellationToken::new();
        let trigger = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let mut audit = output::AuditLog::create(&ctx.worker_run, 1).unwrap();
        let history = vec![Message::User("任务".into())];

        let result = one_turn(&ctx, &history, &mut audit, 1, &token).await;

        assert!(matches!(result, Err(CallFailure::Cancelled)));
        let report = audit.finish().unwrap();
        let entries: Vec<serde_json::Value> = fs::read_to_string(report)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let marker = entries
            .iter()
            .filter(|entry| entry["direction"] == "llm_event_data")
            .filter_map(|entry| entry["data"].as_str())
            .filter_map(|data| serde_json::from_str::<serde_json::Value>(data).ok())
            .find(|value| value["formic_audit_kind"] == "incomplete_sse_frame")
            .expect("取消时必须保存未闭合 SSE 半帧");
        assert_eq!(marker["encoding"], "utf-8");
        assert_eq!(marker["byte_length"], BODY.len());
    }

    #[tokio::test]
    async fn http_400_is_fatal_not_retried() {
        let (port, count) = mini_server(Mode::Status(400));
        let (_dir, ctx) = fixture(port);
        let result = run_unit(
            &ctx,
            &unit(),
            CancellationToken::new(),
            &mut UnitStats::default(),
        )
        .await;
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
        let result = run_unit(
            &ctx,
            &unit(),
            CancellationToken::new(),
            &mut UnitStats::default(),
        )
        .await;
        let Err(failure) = result else {
            panic!("500 应判失败")
        };
        let text = failure.to_string();
        assert!(text.contains("500"), "{text}");
        assert!(text.contains("重试 2 次"), "{text}");
        assert_eq!(
            count.load(Ordering::SeqCst),
            ctx.execution.llm_attempts as usize,
            "500 应尝试到预算上限"
        );
    }
}
