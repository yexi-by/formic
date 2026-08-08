//! OpenAI Chat Completions 协议：历史 → 请求映射，SSE → 内部事件 transform。
//! 兼容讲同一形状的供应商（OpenAI、DeepSeek、Moonshot、OpenRouter、vLLM 等）。
//! 工具调用的流式增量按 index 缓冲组装，finish_reason=tool_calls 时一次性放出。

use super::{Finish, LlmConfig, LlmError, LlmEvent, Message, ProviderUsage, ToolCallReq, ToolSpec};

pub fn build_request(
    config: &LlmConfig,
    instructions: &str,
    history: &[Message],
    tools: &[ToolSpec],
) -> (String, String, Vec<(String, String)>) {
    let mut messages = vec![serde_json::json!({"role": "system", "content": instructions})];
    for message in history {
        match message {
            Message::User(text) => {
                messages.push(serde_json::json!({"role": "user", "content": text}));
            }
            Message::Compaction(text) => {
                messages.push(serde_json::json!({
                    "role": "user",
                    "content": format!("此前历史的已验证压缩摘要：\n{text}"),
                }));
            }
            Message::Assistant { text, tool_calls } => {
                let mut msg = serde_json::json!({"role": "assistant"});
                msg["content"] = if text.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(text)
                };
                if !tool_calls.is_empty() {
                    msg["tool_calls"] = tool_calls
                        .iter()
                        .map(|tc| {
                            serde_json::json!({
                                "id": tc.call_id,
                                "type": "function",
                                "function": {"name": tc.name, "arguments": tc.arguments},
                            })
                        })
                        .collect();
                }
                messages.push(msg);
            }
            Message::ToolResult { call_id, content } => {
                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": content,
                }));
            }
        }
    }
    let mut body = serde_json::json!({
        "model": config.model,
        "max_tokens": config.max_output_tokens,
        "stream": true,
        "messages": messages,
    });
    if !tools.is_empty() {
        body["tools"] = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {"name": t.name, "description": t.description, "parameters": t.parameters},
                })
            })
            .collect();
    }
    let headers = config
        .api_key
        .as_ref()
        .map(|key| ("authorization".to_string(), format!("Bearer {key}")))
        .into_iter()
        .collect();
    (
        format!("{}/chat/completions", config.base_url.trim_end_matches('/')),
        body.to_string(),
        headers,
    )
}

/// 每个 index 一条在组装的工具调用：(id, name, arguments)。
#[derive(Default)]
pub struct Transform {
    partial: Vec<(String, String, String)>,
    saw_refusal: bool,
    pending_usage: Option<ProviderUsage>,
}

impl Transform {
    pub fn new() -> Self {
        Self::default()
    }

    fn finish_with_usage(&mut self, finish: Finish) -> Vec<LlmEvent> {
        let mut events = self
            .pending_usage
            .take()
            .map(LlmEvent::Usage)
            .into_iter()
            .collect::<Vec<_>>();
        events.push(LlmEvent::Finished(finish));
        events
    }
}

impl super::Transform for Transform {
    fn push(&mut self, payload: &str) -> Result<Vec<LlmEvent>, LlmError> {
        if payload.trim() == "[DONE]" {
            return Ok(Vec::new());
        }
        let v: serde_json::Value = serde_json::from_str(payload)
            .map_err(|e| LlmError::protocol(format!("JSON 解析失败：{e}"), payload))?;
        if let Some(usage) = v.get("usage").filter(|usage| usage.is_object()) {
            let event = LlmEvent::Usage(ProviderUsage {
                input_tokens: usage
                    .get("prompt_tokens")
                    .and_then(serde_json::Value::as_u64),
                output_tokens: usage
                    .get("completion_tokens")
                    .and_then(serde_json::Value::as_u64),
                cache_read_tokens: usage
                    .pointer("/prompt_tokens_details/cached_tokens")
                    .and_then(serde_json::Value::as_u64),
                cache_creation_tokens: usage
                    .pointer("/prompt_tokens_details/cache_creation_tokens")
                    .and_then(serde_json::Value::as_u64),
            });
            if v.get("choices")
                .and_then(serde_json::Value::as_array)
                .is_some_and(Vec::is_empty)
            {
                return Ok(vec![event]);
            }
            let LlmEvent::Usage(usage) = event else {
                unreachable!()
            };
            self.pending_usage = Some(usage);
        }
        let choices = v["choices"]
            .as_array()
            .ok_or_else(|| LlmError::protocol("缺少 choices 数组", payload))?;
        let Some(choice) = choices.first() else {
            return Ok(Vec::new()); // 纯用量等无 choices 的帧，与产出无关
        };

        if let Some(delta) = choice.get("delta") {
            if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                for part in tool_calls {
                    let index = part["index"].as_u64().unwrap_or(0) as usize;
                    if self.partial.len() <= index {
                        self.partial.resize_with(index + 1, Default::default);
                    }
                    let slot = &mut self.partial[index];
                    if let Some(id) = part["id"].as_str() {
                        slot.0.push_str(id);
                    }
                    if let Some(name) = part["function"]["name"].as_str() {
                        slot.1.push_str(name);
                    }
                    if let Some(args) = part["function"]["arguments"].as_str() {
                        slot.2.push_str(args);
                    }
                }
                return Ok(Vec::new());
            }
            if let Some(content) = delta["content"].as_str()
                && !content.is_empty()
            {
                return Ok(vec![LlmEvent::TextDelta(content.to_string())]);
            }
            if delta.get("refusal").is_some_and(|value| !value.is_null()) {
                self.saw_refusal = true;
            }
        }

        match choice.get("finish_reason") {
            Some(serde_json::Value::String(reason)) => match reason.as_str() {
                "stop" if self.saw_refusal => Ok(self.finish_with_usage(Finish::Refusal)),
                "stop" => Ok(self.finish_with_usage(Finish::Stop)),
                "length" => Ok(self.finish_with_usage(Finish::MaxTokens)),
                "content_filter" | "refusal" => Ok(self.finish_with_usage(Finish::Refusal)),
                "tool_calls" | "function_call" => {
                    if self.partial.is_empty() {
                        return Err(LlmError::protocol(
                            "finish_reason 声称工具调用但没有工具调用内容",
                            payload,
                        ));
                    }
                    let mut events: Vec<LlmEvent> = self
                        .partial
                        .drain(..)
                        .map(|(call_id, name, arguments)| {
                            LlmEvent::ToolCall(ToolCallReq {
                                call_id,
                                name,
                                arguments,
                            })
                        })
                        .collect();
                    if let Some(usage) = self.pending_usage.take() {
                        events.push(LlmEvent::Usage(usage));
                    }
                    events.push(LlmEvent::Finished(Finish::ToolUse));
                    Ok(events)
                }
                other => Err(LlmError::protocol(
                    format!("未知 finish_reason {other:?}"),
                    payload,
                )),
            },
            _ => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::Transform as _;
    use crate::llm::events_of;

    const TEXT_SAMPLE: &str = concat!(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"世界\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    const TOOL_SAMPLE: &str = concat!(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"scope\\\":\\\"input\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    #[test]
    fn recorded_sample_yields_text_then_stop() {
        let events = events_of(Box::new(Transform::new()), TEXT_SAMPLE);
        assert_eq!(
            events,
            vec![
                LlmEvent::TextDelta("你好".into()),
                LlmEvent::TextDelta("世界".into()),
                LlmEvent::Finished(Finish::Stop),
            ]
        );
    }

    #[test]
    fn tool_call_assembled_across_deltas() {
        let events = events_of(Box::new(Transform::new()), TOOL_SAMPLE);
        assert_eq!(
            events,
            vec![
                LlmEvent::ToolCall(ToolCallReq {
                    call_id: "call_1".into(),
                    name: "search".into(),
                    arguments: "{\"pattern\":\"苹果\",\"scope\":\"input\"}".into(),
                }),
                LlmEvent::Finished(Finish::ToolUse),
            ]
        );
    }

    #[test]
    fn tool_calls_finish_without_content_is_error() {
        let mut t = Transform::new();
        let result =
            t.push("{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}");
        assert!(matches!(result, Err(LlmError::Protocol { .. })));
    }

    #[test]
    fn length_finish_is_max_tokens() {
        let mut t = Transform::new();
        let events = t
            .push("{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}")
            .unwrap();
        assert_eq!(events, vec![LlmEvent::Finished(Finish::MaxTokens)]);
    }

    #[test]
    fn usage_and_refusal_are_preserved_on_final_choice() {
        let mut usage = Transform::new();
        let events = usage
            .push("{\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":7,\"cache_creation_tokens\":1}},\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}")
            .unwrap();
        assert_eq!(
            events,
            vec![
                LlmEvent::Usage(ProviderUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    cache_read_tokens: Some(7),
                    cache_creation_tokens: Some(1),
                }),
                LlmEvent::Finished(Finish::Stop),
            ]
        );

        let mut refusal = Transform::new();
        let events = refusal
            .push("{\"choices\":[{\"delta\":{\"refusal\":\"no\"},\"finish_reason\":\"stop\"}]}")
            .unwrap();
        assert_eq!(events, vec![LlmEvent::Finished(Finish::Refusal)]);
    }

    #[test]
    fn history_maps_to_messages() {
        let config = LlmConfig {
            protocol: super::super::Protocol::Completions,
            base_url: "http://x/v1".into(),
            model: "m".into(),
            api_key: None,
            context_window_tokens: 131072,
            max_output_tokens: 16384,
        };
        let history = vec![
            Message::User("任务".into()),
            Message::Assistant {
                text: String::new(),
                tool_calls: vec![ToolCallReq {
                    call_id: "call_1".into(),
                    name: "search".into(),
                    arguments: "{\"pattern\":\"x\"}".into(),
                }],
            },
            Message::ToolResult {
                call_id: "call_1".into(),
                content: "结果".into(),
            },
        ];
        let (_url, body, _headers) = build_request(&config, "说明", &history, &[]);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["messages"][0]["role"], "system");
        assert_eq!(v["messages"][1]["role"], "user");
        assert_eq!(v["messages"][2]["role"], "assistant");
        assert_eq!(v["messages"][2]["tool_calls"][0]["id"], "call_1");
        assert_eq!(v["messages"][3]["role"], "tool");
        assert_eq!(v["messages"][3]["tool_call_id"], "call_1");
        assert_eq!(v["messages"][3]["content"], "结果");
        assert!(v.get("tools").is_none(), "无工具时不发 tools 字段");
    }
}
