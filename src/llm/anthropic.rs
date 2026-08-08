//! Anthropic Messages 协议：历史 → 请求映射，SSE → 内部事件 transform。
//! 工具调用的参数以 input_json_delta 增量到达，content_block_stop 时拼好放出。
//! max_tokens 是该协议的必填字段，使用已校验的模型配置。

use super::{Finish, LlmConfig, LlmError, LlmEvent, Message, ProviderUsage, ToolCallReq, ToolSpec};

const ANTHROPIC_VERSION: &str = "2023-06-01";

pub fn build_request(
    config: &LlmConfig,
    instructions: &str,
    history: &[Message],
    tools: &[ToolSpec],
) -> (String, String, Vec<(String, String)>) {
    let mut messages: Vec<serde_json::Value> = Vec::new();
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
                let mut content = Vec::new();
                if !text.is_empty() {
                    content.push(serde_json::json!({"type": "text", "text": text}));
                }
                for tc in tool_calls {
                    content.push(serde_json::json!({
                        "type": "tool_use",
                        "id": tc.call_id,
                        "name": tc.name,
                        // worker 在进入历史前已校验 arguments 为合法 JSON
                        "input": serde_json::from_str::<serde_json::Value>(&tc.arguments)
                            .expect("进入历史的工具参数必为合法 JSON"),
                    }));
                }
                messages.push(serde_json::json!({"role": "assistant", "content": content}));
            }
            Message::ToolResult { call_id, content } => {
                // 连续 ToolResult 折叠进同一条 user 消息（Anthropic 的交替约束）
                let block = serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": content,
                });
                let fold_into = messages.last_mut().and_then(|last| {
                    if last["role"] == "user" && last["content"].is_array() {
                        last["content"].as_array_mut()
                    } else {
                        None
                    }
                });
                match fold_into {
                    Some(blocks) => blocks.push(block),
                    None => messages.push(serde_json::json!({"role": "user", "content": [block]})),
                }
            }
        }
    }
    let mut body = serde_json::json!({
        "model": config.model,
        "max_tokens": config.max_output_tokens,
        "stream": true,
        "system": instructions,
        "messages": messages,
    });
    if !tools.is_empty() {
        body["tools"] = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect();
    }
    let mut headers = vec![(
        "anthropic-version".to_string(),
        ANTHROPIC_VERSION.to_string(),
    )];
    if let Some(key) = &config.api_key {
        headers.push(("x-api-key".to_string(), key.clone()));
    }
    (
        format!("{}/messages", config.base_url.trim_end_matches('/')),
        body.to_string(),
        headers,
    )
}

/// 在组装的工具调用块：(id, name, 已拼接的 partial_json)。
#[derive(Default)]
pub struct Transform {
    pending_tool: Option<(String, String, String)>,
}

impl Transform {
    pub fn new() -> Self {
        Self::default()
    }
}

impl super::Transform for Transform {
    fn push(&mut self, payload: &str) -> Result<Vec<LlmEvent>, LlmError> {
        let v: serde_json::Value = serde_json::from_str(payload)
            .map_err(|e| LlmError::protocol(format!("JSON 解析失败：{e}"), payload))?;
        let ty = v["type"]
            .as_str()
            .ok_or_else(|| LlmError::protocol("缺少 type 字段", payload))?;
        match ty {
            "message_start" => Ok(usage_at(&v["message"]["usage"])
                .map(LlmEvent::Usage)
                .into_iter()
                .collect()),
            "content_block_start" => {
                if v["content_block"]["type"].as_str() == Some("tool_use") {
                    self.pending_tool = Some((
                        v["content_block"]["id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        v["content_block"]["name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        String::new(),
                    ));
                }
                Ok(Vec::new())
            }
            "content_block_delta" => match v["delta"]["type"].as_str() {
                Some("text_delta") => {
                    let text = v["delta"]["text"].as_str().unwrap_or_default();
                    Ok((!text.is_empty())
                        .then(|| LlmEvent::TextDelta(text.to_string()))
                        .into_iter()
                        .collect())
                }
                Some("input_json_delta") => {
                    if let Some(pending) = &mut self.pending_tool {
                        pending
                            .2
                            .push_str(v["delta"]["partial_json"].as_str().unwrap_or_default());
                    }
                    Ok(Vec::new())
                }
                _ => Ok(Vec::new()),
            },
            "content_block_stop" => Ok(match self.pending_tool.take() {
                Some((call_id, name, arguments)) => {
                    vec![LlmEvent::ToolCall(ToolCallReq {
                        call_id,
                        name,
                        arguments,
                    })]
                }
                None => Vec::new(),
            }),
            "message_delta" => {
                let mut events = usage_at(&v["usage"])
                    .map(LlmEvent::Usage)
                    .into_iter()
                    .collect::<Vec<_>>();
                let finish = match v["delta"]["stop_reason"].as_str() {
                    Some("end_turn") => Some(Finish::Stop),
                    Some("max_tokens") => Some(Finish::MaxTokens),
                    Some("tool_use") => Some(Finish::ToolUse),
                    Some("refusal") => Some(Finish::Refusal),
                    _ => None,
                };
                events.extend(finish.map(LlmEvent::Finished));
                Ok(events)
            }
            "error" => Err(LlmError::protocol("错误事件", payload)),
            _ => Ok(Vec::new()), // message_start / message_stop / ping 等
        }
    }
}

fn usage_at(usage: &serde_json::Value) -> Option<ProviderUsage> {
    usage.is_object().then(|| ProviderUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
        cache_read_tokens: usage
            .get("cache_read_input_tokens")
            .and_then(serde_json::Value::as_u64),
        cache_creation_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(serde_json::Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::Transform as _;
    use crate::llm::events_of;

    const TEXT_SAMPLE: &str = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"世界\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":5}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    const TOOL_SAMPLE: &str = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    #[test]
    fn recorded_sample_yields_text_then_stop() {
        let events = events_of(Box::new(Transform::new()), TEXT_SAMPLE);
        assert_eq!(
            events,
            vec![
                LlmEvent::Usage(ProviderUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(1),
                    cache_read_tokens: None,
                    cache_creation_tokens: None,
                }),
                LlmEvent::TextDelta("你好".into()),
                LlmEvent::TextDelta("世界".into()),
                LlmEvent::Usage(ProviderUsage {
                    input_tokens: None,
                    output_tokens: Some(5),
                    cache_read_tokens: None,
                    cache_creation_tokens: None,
                }),
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
                    call_id: "toolu_1".into(),
                    name: "search".into(),
                    arguments: "{\"pattern\":\"苹果\",\"scope\":\"input\"}".into(),
                }),
                LlmEvent::Finished(Finish::ToolUse),
            ]
        );
    }

    #[test]
    fn max_tokens_stop_detected() {
        let mut t = Transform::new();
        let events = t
            .push("{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":9}}")
            .unwrap();
        assert_eq!(
            events,
            vec![
                LlmEvent::Usage(ProviderUsage {
                    input_tokens: None,
                    output_tokens: Some(9),
                    cache_read_tokens: None,
                    cache_creation_tokens: None,
                }),
                LlmEvent::Finished(Finish::MaxTokens)
            ]
        );
    }

    #[test]
    fn refusal_stop_is_not_reported_as_empty_output() {
        let mut transform = Transform::new();
        let events = transform
            .push("{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"}}")
            .unwrap();
        assert_eq!(events, vec![LlmEvent::Finished(Finish::Refusal)]);
    }

    #[test]
    fn history_folds_consecutive_tool_results() {
        let config = LlmConfig {
            protocol: super::super::Protocol::Anthropic,
            base_url: "http://x".into(),
            model: "m".into(),
            api_key: None,
            context_window_tokens: 131072,
            max_output_tokens: 16384,
        };
        let history = vec![
            Message::User("任务".into()),
            Message::Assistant {
                text: "查两个".into(),
                tool_calls: vec![
                    ToolCallReq {
                        call_id: "t1".into(),
                        name: "search".into(),
                        arguments: "{\"a\":1}".into(),
                    },
                    ToolCallReq {
                        call_id: "t2".into(),
                        name: "search".into(),
                        arguments: "{\"b\":2}".into(),
                    },
                ],
            },
            Message::ToolResult {
                call_id: "t1".into(),
                content: "甲".into(),
            },
            Message::ToolResult {
                call_id: "t2".into(),
                content: "乙".into(),
            },
        ];
        let (_url, body, _headers) = build_request(&config, "说明", &history, &[]);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["messages"][1]["role"], "assistant");
        assert_eq!(v["messages"][1]["content"][0]["type"], "text");
        assert_eq!(v["messages"][1]["content"][1]["type"], "tool_use");
        assert_eq!(v["messages"][1]["content"][1]["input"]["a"], 1);
        assert_eq!(v["messages"][2]["role"], "user");
        assert_eq!(
            v["messages"][2]["content"].as_array().unwrap().len(),
            2,
            "两个连续 ToolResult 折叠进一条 user 消息"
        );
        assert_eq!(v["messages"][2]["content"][0]["type"], "tool_result");
    }
}
