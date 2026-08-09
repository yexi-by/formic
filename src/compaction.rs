//! 上下文预算与历史压缩。普通工具在压缩调用中完全禁用，只暴露内部提交工具；
//! 历史只有在摘要有效、请求更小且重新进入预算时才一次性替换。

use std::io;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::config::ExecutionConfig;
use crate::llm::{
    Finish, LlmClient, LlmError, LlmEvent, Message, ProviderUsage, ToolCallReq, ToolSpec,
};
use crate::output::{AuditEntry, AuditLog, WorkerState};
use crate::structured::SUBMIT_RESULT_TOOL;

pub const SUBMIT_COMPACTION_TOOL: &str = "formic_submit_compaction";

const INSTRUCTIONS: &str = "\
你正在压缩 Formic 单元的旧对话历史。只能依据给出的原始历史提取事实，不得补充推测。\
普通工具不可用。完成后必须单独调用 formic_submit_compaction；不要同时输出文本。\
summary 简要说明已经完成的工作，verified_facts 只列已确认事实，evidence 保留可追溯的文件、行号、\
工具结果或原文定位，remaining_work 列出尚未完成的动作。";

#[derive(Debug)]
pub enum CompactionOutcome {
    Unchanged,
    Replaced {
        before_tokens: u64,
        after_tokens: u64,
        calls: Vec<CompactionCallUsage>,
    },
}

#[derive(Debug)]
pub struct CompactionCallUsage {
    pub estimated_input_tokens: u64,
    pub provider_usage: ProviderUsage,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("上下文压缩已取消")]
    Cancelled,
    #[error(
        "初始任务、数据分片或冻结工具目录本身已超过上下文安全预算；请减小分片、schema 或启用的工具范围"
    )]
    ImmutableTooLarge,
    #[error("上下文无法压缩：没有可压缩的完整 assistant-tool-result 组")]
    NoCompleteGroup,
    #[error("上下文无法压缩：压缩请求本身超过安全预算")]
    CompactionRequestTooLarge,
    #[error("上下文压缩调用失败：{0}")]
    Llm(#[from] LlmError),
    #[error("上下文压缩结果连续 {attempts} 次无效：{reason}")]
    InvalidResult { attempts: u32, reason: String },
    #[error("上下文无法压缩：摘要没有缩小请求或替换后仍超过安全预算")]
    NoProgress,
    #[error("写入上下文压缩审计失败：{0}")]
    Audit(#[from] io::Error),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompactionSubmission {
    summary: String,
    verified_facts: Vec<String>,
    evidence: Vec<String>,
    remaining_work: Vec<String>,
}

pub async fn compact_if_needed(
    llm: &LlmClient,
    normal_instructions: &str,
    normal_tools: &[ToolSpec],
    execution: &ExecutionConfig,
    history: &mut Vec<Message>,
    audit: &mut AuditLog,
    cancel: &CancellationToken,
    force: bool,
) -> Result<CompactionOutcome, CompactionError> {
    let budget = llm.input_budget(execution.context_safety_tokens);
    let before_tokens = llm.estimate_request_tokens(normal_instructions, history, normal_tools);
    let needs_compaction = before_tokens > budget || force;
    audit.push(&AuditEntry::ContextBudget {
        estimated_tokens: before_tokens,
        input_budget: budget,
        force,
        action: if needs_compaction {
            "compact".into()
        } else {
            "continue".into()
        },
    })?;
    if before_tokens <= budget && !force {
        return Ok(CompactionOutcome::Unchanged);
    }
    audit.push(&AuditEntry::State {
        state: WorkerState::CompactingContext,
        reason: if force {
            "供应商明确报告上下文超限，执行一次紧急压缩".into()
        } else {
            format!("预计输入 {before_tokens} token 超过安全输入预算 {budget} token")
        },
    })?;
    let immutable = &history[..history.len().min(1)];
    if llm.estimate_request_tokens(normal_instructions, immutable, normal_tools) > budget {
        return Err(CompactionError::ImmutableTooLarge);
    }

    let ends = complete_prefix_ends(history);
    if ends.is_empty() {
        return Err(CompactionError::NoCompleteGroup);
    }
    let tool = compaction_spec();
    let tools = [tool];
    let output_contract = normal_tools
        .iter()
        .find(|tool| tool.name == SUBMIT_RESULT_TOOL);
    let overflow = before_tokens.saturating_sub(budget);
    let mut chosen = None;
    let mut largest_fitting = None;
    for end in ends {
        let transcript = render_transcript(&history[1..end]);
        let compact_history = vec![Message::User(render_compaction_input(
            normal_instructions,
            &history[0],
            output_contract,
            &transcript,
        ))];
        let request_tokens = llm.estimate_request_tokens(INSTRUCTIONS, &compact_history, &tools);
        if request_tokens > budget {
            break;
        }
        largest_fitting = Some((end, compact_history));
        let removed_tokens: u64 = history[1..end]
            .iter()
            .map(crate::tokenize::count_message)
            .sum();
        if force || removed_tokens >= overflow.saturating_add(512) {
            chosen = largest_fitting.clone();
            break;
        }
    }
    let Some((cut, compact_history)) = chosen.or(largest_fitting) else {
        return Err(CompactionError::CompactionRequestTooLarge);
    };

    let (submission, calls) = request_compaction(
        llm,
        compact_history,
        &tools,
        execution.llm_attempts,
        budget,
        audit,
        cancel,
    )
    .await?;
    let summary = format!(
        "{}\n",
        serde_json::to_string_pretty(&submission).expect("压缩结果可序列化")
    );
    let mut candidate = Vec::with_capacity(history.len() - cut + 2);
    candidate.push(history[0].clone());
    candidate.push(Message::Compaction(summary));
    candidate.extend_from_slice(&history[cut..]);
    let after_tokens = llm.estimate_request_tokens(normal_instructions, &candidate, normal_tools);
    audit.push(&AuditEntry::ContextCompaction {
        valid: after_tokens < before_tokens && after_tokens <= budget,
        before_tokens,
        after_tokens,
        reason: if after_tokens < before_tokens && after_tokens <= budget {
            "摘要有效且替换后进入安全预算".into()
        } else {
            "摘要未能缩小到安全预算".into()
        },
    })?;
    if after_tokens >= before_tokens || after_tokens > budget {
        return Err(CompactionError::NoProgress);
    }
    *history = candidate;
    Ok(CompactionOutcome::Replaced {
        before_tokens,
        after_tokens,
        calls,
    })
}

fn complete_prefix_ends(history: &[Message]) -> Vec<usize> {
    if history.len() <= 1 {
        return Vec::new();
    }
    let mut index = 1;
    while matches!(history.get(index), Some(Message::Compaction(_))) {
        index += 1;
    }
    let mut ends = Vec::new();
    while let Some(Message::Assistant { tool_calls, .. }) = history.get(index) {
        if tool_calls.is_empty() {
            break;
        }
        let result_start = if matches!(
            history.get(index + 1),
            Some(Message::ResponseOutputItems(_))
        ) {
            index + 2
        } else {
            index + 1
        };
        let result_end = result_start + tool_calls.len();
        if result_end > history.len() {
            break;
        }
        let complete = tool_calls.iter().zip(&history[result_start..result_end]).all(
            |(call, message)| {
                matches!(message, Message::ToolResult { call_id, .. } if call_id == &call.call_id)
            },
        );
        if !complete {
            break;
        }
        index = result_end;
        ends.push(index);
    }
    ends
}

fn render_transcript(history: &[Message]) -> String {
    let mut output = String::from("# 待压缩的原始历史\n");
    for message in history {
        match message {
            Message::User(text) => output.push_str(&format!("\n[user]\n{text}\n")),
            Message::Compaction(text) => {
                output.push_str(&format!("\n[previous_compaction]\n{text}\n"));
            }
            Message::Assistant { text, tool_calls } => {
                output.push_str(&format!("\n[assistant]\n{text}\n"));
                for call in tool_calls {
                    output.push_str(&format!(
                        "[tool_call id={} name={}]\n{}\n",
                        call.call_id, call.name, call.arguments
                    ));
                }
            }
            // 原始 Responses item 只用于供应商原生重放，其中可能包含 opaque 的
            // encrypted_content；压缩提示使用相邻 Assistant 的可见语义即可。
            Message::ResponseOutputItems(_) => {}
            Message::ToolResult { call_id, content } => {
                output.push_str(&format!("\n[tool_result id={call_id}]\n{content}\n"));
            }
        }
    }
    output
}

fn render_compaction_input(
    normal_instructions: &str,
    initial: &Message,
    output_contract: Option<&ToolSpec>,
    transcript: &str,
) -> String {
    let mut output = String::from("# 当前执行契约\n\n## 正常执行说明\n");
    output.push_str(normal_instructions);
    output.push_str("\n\n## 初始任务与数据\n");
    output.push_str(&render_transcript(std::slice::from_ref(initial)));
    output.push_str("\n## 最终输出契约\n");
    if let Some(tool) = output_contract {
        output.push_str(&format!(
            "工具名：{}\n说明：{}\n参数 schema：\n{}\n",
            tool.name,
            tool.description,
            serde_json::to_string_pretty(&tool.parameters).expect("工具 schema 可以序列化")
        ));
    } else {
        output.push_str("最终结果按正常执行说明直接输出文本。\n");
    }
    output.push('\n');
    output.push_str(transcript);
    output
}

async fn request_compaction(
    llm: &LlmClient,
    mut history: Vec<Message>,
    tools: &[ToolSpec],
    attempts: u32,
    budget: u64,
    audit: &mut AuditLog,
    cancel: &CancellationToken,
) -> Result<(CompactionSubmission, Vec<CompactionCallUsage>), CompactionError> {
    let mut last_reason = String::new();
    let mut call_usage = Vec::new();
    for attempt in 1..=attempts {
        let estimated_input_tokens = llm.estimate_request_tokens(INSTRUCTIONS, &history, tools);
        if estimated_input_tokens > budget {
            return Err(CompactionError::CompactionRequestTooLarge);
        }
        let prepared = llm.prepare_call(INSTRUCTIONS, &history, tools);
        audit.push_compaction_request(attempt, prepared.body())?;
        let called = tokio::select! {
            _ = cancel.cancelled() => return Err(CompactionError::Cancelled),
            result = llm.send(prepared) => result,
        };
        let mut call = match called {
            Ok(call) => call,
            Err(error) if retryable(&error) && attempt < attempts => {
                call_usage.push(CompactionCallUsage {
                    estimated_input_tokens,
                    provider_usage: ProviderUsage::default(),
                });
                record_retry(
                    audit,
                    attempt,
                    u64::from(attempt).saturating_mul(1000),
                    &error.to_string(),
                )?;
                tokio::select! {
                    _ = cancel.cancelled() => return Err(CompactionError::Cancelled),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(attempt as u64)) => {}
                }
                continue;
            }
            Err(error) => return Err(CompactionError::Llm(error)),
        };
        let mut text = String::new();
        let mut calls = Vec::new();
        let mut finish = None;
        let mut stream_error = None;
        let mut provider_usage = ProviderUsage::default();
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    write_call_audit(audit, &mut call)?;
                    return Err(CompactionError::Cancelled);
                },
                result = call.next_event() => result,
            };
            match next {
                Ok(Some(LlmEvent::TextDelta(delta))) => text.push_str(&delta),
                Ok(Some(LlmEvent::ToolCall(tool_call))) => calls.push(tool_call),
                Ok(Some(LlmEvent::Usage(usage))) => provider_usage.merge(usage),
                Ok(Some(LlmEvent::Finished(reason))) => finish = Some(reason),
                Ok(None) => break,
                Err(error) => {
                    stream_error = Some(error);
                    break;
                }
            }
        }
        write_call_audit(audit, &mut call)?;
        let response_output_items = if finish.is_some() {
            call.response_output_items().to_vec()
        } else {
            Vec::new()
        };
        call_usage.push(CompactionCallUsage {
            estimated_input_tokens,
            provider_usage,
        });
        if let Some(error) = stream_error {
            if retryable(&error) && attempt < attempts {
                last_reason = error.to_string();
                record_retry(audit, attempt, 0, &last_reason)?;
                continue;
            }
            return Err(CompactionError::Llm(error));
        }
        let validation = validate_turn(&text, &calls, finish);
        match validation {
            Ok(submission) => return Ok((submission, call_usage)),
            Err(reason) => {
                last_reason = reason.clone();
                audit.push(&AuditEntry::ContextCompaction {
                    valid: false,
                    before_tokens: 0,
                    after_tokens: 0,
                    reason: reason.clone(),
                })?;
                if attempt < attempts {
                    append_compaction_correction(
                        &mut history,
                        text,
                        calls,
                        response_output_items,
                        &reason,
                    );
                }
            }
        }
    }
    Err(CompactionError::InvalidResult {
        attempts,
        reason: last_reason,
    })
}

fn record_retry(
    audit: &mut AuditLog,
    attempt: u32,
    delay_ms: u64,
    reason: &str,
) -> Result<(), CompactionError> {
    audit.push(&AuditEntry::Retry {
        attempt,
        next_attempt: attempt + 1,
        delay_ms,
        reason: format!("上下文压缩调用失败：{reason}"),
    })?;
    audit.push(&AuditEntry::State {
        state: WorkerState::RetryingModel,
        reason: format!("上下文压缩第 {attempt} 次调用失败，等待后重试：{reason}"),
    })?;
    Ok(())
}

fn validate_turn(
    text: &str,
    calls: &[ToolCallReq],
    finish: Option<Finish>,
) -> Result<CompactionSubmission, String> {
    match finish {
        Some(Finish::Refusal) => return Err("模型拒绝压缩".into()),
        Some(Finish::MaxTokens) => return Err("压缩输出达到长度上限".into()),
        Some(Finish::Stop) => {
            return Err("必须调用 formic_submit_compaction，不能用最终文本提交".into());
        }
        None => return Err("压缩响应没有完成事件".into()),
        Some(Finish::ToolUse) => {}
    }
    if calls.len() != 1 || calls[0].name != SUBMIT_COMPACTION_TOOL || !text.trim().is_empty() {
        return Err("formic_submit_compaction 必须单独调用且不能同时输出文本".into());
    }
    let submission: CompactionSubmission = serde_json::from_str(&calls[0].arguments)
        .map_err(|error| format!("压缩参数不符合固定结构：{error}"))?;
    if submission.summary.trim().is_empty() {
        return Err("压缩结果 summary 不能为空".into());
    }
    Ok(submission)
}

fn append_compaction_correction(
    history: &mut Vec<Message>,
    text: String,
    calls: Vec<ToolCallReq>,
    response_output_items: Vec<serde_json::Value>,
    reason: &str,
) {
    if calls.is_empty() {
        history.push(Message::Assistant {
            text,
            tool_calls: Vec::new(),
        });
        if !response_output_items.is_empty() {
            history.push(Message::ResponseOutputItems(response_output_items));
        }
        history.push(Message::User(format!("压缩结果无效：{reason}；请重新提交")));
        return;
    }
    let mut sanitized = calls;
    for call in &mut sanitized {
        if serde_json::from_str::<Value>(&call.arguments).is_err() {
            call.arguments = "{}".into();
        }
    }
    history.push(Message::Assistant {
        text,
        tool_calls: sanitized.clone(),
    });
    if !response_output_items.is_empty() {
        history.push(Message::ResponseOutputItems(response_output_items));
    }
    for call in sanitized {
        history.push(Message::ToolResult {
            call_id: call.call_id,
            content: format!("错误：{reason}；请重新提交"),
        });
    }
}

fn write_call_audit(audit: &mut AuditLog, call: &mut crate::llm::Call) -> io::Result<()> {
    audit.push_llm_event_stream(&call.take_audit_snapshot())
}

fn retryable(error: &LlmError) -> bool {
    match error {
        LlmError::Transport(_) | LlmError::Protocol { .. } | LlmError::Timeout { .. } => true,
        LlmError::Http { status, .. } => *status == 429 || *status >= 500,
        LlmError::ContextLimit { .. } | LlmError::StreamLimit { .. } => false,
    }
}

fn compaction_spec() -> ToolSpec {
    ToolSpec {
        name: SUBMIT_COMPACTION_TOOL.into(),
        description: "提交经过核对的历史压缩摘要；必须单独调用。".into(),
        parameters: serde_json::json!({
            "type":"object",
            "properties":{
                "summary":{"type":"string"},
                "verified_facts":{"type":"array","items":{"type":"string"}},
                "evidence":{"type":"array","items":{"type":"string"}},
                "remaining_work":{"type":"array","items":{"type":"string"}}
            },
            "required":["summary","verified_facts","evidence","remaining_work"],
            "additionalProperties":false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(id: &str) -> ToolCallReq {
        ToolCallReq {
            call_id: id.into(),
            name: "search".into(),
            arguments: "{}".into(),
        }
    }

    #[test]
    fn only_complete_prefix_groups_are_candidates() {
        let history = vec![
            Message::User("task".into()),
            Message::Assistant {
                text: String::new(),
                tool_calls: vec![call("a")],
            },
            Message::ToolResult {
                call_id: "a".into(),
                content: "one".into(),
            },
            Message::Assistant {
                text: String::new(),
                tool_calls: vec![call("b")],
            },
        ];
        assert_eq!(complete_prefix_ends(&history), [3]);
    }

    #[test]
    fn responses_output_items_stay_inside_complete_group() {
        let history = vec![
            Message::User("task".into()),
            Message::Assistant {
                text: String::new(),
                tool_calls: vec![call("a")],
            },
            Message::ResponseOutputItems(vec![serde_json::json!({
                "type":"reasoning","id":"reasoning-1"
            })]),
            Message::ToolResult {
                call_id: "a".into(),
                content: "one".into(),
            },
        ];
        assert_eq!(complete_prefix_ends(&history), [4]);
    }

    #[test]
    fn compaction_input_keeps_task_instructions_and_output_contract() {
        let contract = ToolSpec {
            name: SUBMIT_RESULT_TOOL.into(),
            description: "提交最终结果".into(),
            parameters: serde_json::json!({
                "type":"object",
                "required":["answer"]
            }),
        };
        let rendered = render_compaction_input(
            "必须依据证据回答",
            &Message::User("检查 data/a.txt".into()),
            Some(&contract),
            "# 待压缩的原始历史\n[assistant]\n已读取文件",
        );
        assert!(rendered.contains("必须依据证据回答"));
        assert!(rendered.contains("检查 data/a.txt"));
        assert!(rendered.contains(SUBMIT_RESULT_TOOL));
        assert!(rendered.contains("\"answer\""));
        assert!(rendered.contains("已读取文件"));
    }

    #[test]
    fn compaction_submission_is_strict() {
        let valid = ToolCallReq {
            call_id: "c".into(),
            name: SUBMIT_COMPACTION_TOOL.into(),
            arguments: serde_json::json!({
                "summary":"done","verified_facts":[],"evidence":[],"remaining_work":[]
            })
            .to_string(),
        };
        assert!(validate_turn("", std::slice::from_ref(&valid), Some(Finish::ToolUse)).is_ok());
        assert!(validate_turn("", &[valid], Some(Finish::Stop)).is_err());
        assert!(validate_turn("extra", &[], Some(Finish::Stop)).is_err());
    }
}
