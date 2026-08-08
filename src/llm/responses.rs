//! OpenAI Responses 协议：历史 → 请求映射，SSE → 内部事件 transform。
//! 工具调用的完整参数在 response.output_item.done 事件上取，无需增量组装。
//! 事件类型是开放集合，未识别的一律忽略；失败类事件显式报错。

use super::{Finish, LlmConfig, LlmError, LlmEvent, Message, ProviderUsage, ToolCallReq, ToolSpec};

pub fn build_request(
    config: &LlmConfig,
    instructions: &str,
    history: &[Message],
    tools: &[ToolSpec],
) -> (String, String, Vec<(String, String)>) {
    let mut input = Vec::new();
    for message in history {
        match message {
            Message::User(text) => {
                input.push(serde_json::json!({
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text}],
                }));
            }
            Message::Compaction(text) => {
                input.push(serde_json::json!({
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": format!("此前历史的已验证压缩摘要：\n{text}")}],
                }));
            }
            Message::Assistant { text, tool_calls } => {
                if !text.is_empty() {
                    input.push(serde_json::json!({
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}],
                    }));
                }
                for tc in tool_calls {
                    input.push(serde_json::json!({
                        "type": "function_call",
                        "call_id": tc.call_id,
                        "name": tc.name,
                        "arguments": tc.arguments,
                    }));
                }
            }
            Message::ToolResult { call_id, content } => {
                input.push(serde_json::json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": content,
                }));
            }
        }
    }
    let mut body = serde_json::json!({
        "model": config.model,
        "max_output_tokens": config.max_output_tokens,
        "stream": true,
        "instructions": instructions,
        "input": input,
    });
    if !tools.is_empty() {
        body["tools"] = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
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
        format!("{}/responses", config.base_url.trim_end_matches('/')),
        body.to_string(),
        headers,
    )
}

#[derive(Default)]
pub struct Transform {
    saw_tool_call: bool,
    saw_refusal: bool,
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
            "response.output_text.delta" => {
                let delta = v["delta"].as_str().unwrap_or_default();
                Ok((!delta.is_empty())
                    .then(|| LlmEvent::TextDelta(delta.to_string()))
                    .into_iter()
                    .collect())
            }
            "response.refusal.delta" => {
                self.saw_refusal = true;
                Ok(Vec::new())
            }
            "response.output_item.done" => {
                if v["item"]["type"].as_str() == Some("function_call") {
                    self.saw_tool_call = true;
                    Ok(vec![LlmEvent::ToolCall(ToolCallReq {
                        call_id: v["item"]["call_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        name: v["item"]["name"].as_str().unwrap_or_default().to_string(),
                        arguments: v["item"]["arguments"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    })])
                } else {
                    if v["item"]["type"].as_str() == Some("refusal") {
                        self.saw_refusal = true;
                    }
                    Ok(Vec::new())
                }
            }
            "response.completed" => {
                let mut events = response_usage(&v)
                    .into_iter()
                    .map(LlmEvent::Usage)
                    .collect::<Vec<_>>();
                events.push(LlmEvent::Finished(if self.saw_refusal {
                    Finish::Refusal
                } else if self.saw_tool_call {
                    Finish::ToolUse
                } else {
                    Finish::Stop
                }));
                Ok(events)
            }
            "response.incomplete" => match v["response"]["incomplete_details"]["reason"].as_str() {
                Some("max_output_tokens") => Ok(vec![LlmEvent::Finished(Finish::MaxTokens)]),
                Some("content_filter") => Ok(vec![LlmEvent::Finished(Finish::Refusal)]),
                other => Err(LlmError::protocol(
                    format!("响应未完成，原因 {other:?}"),
                    payload,
                )),
            },
            "response.failed" | "error" => Err(LlmError::protocol("响应失败事件", payload)),
            _ => Ok(Vec::new()),
        }
    }
}

fn response_usage(value: &serde_json::Value) -> Option<ProviderUsage> {
    let usage = value.pointer("/response/usage")?;
    Some(ProviderUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
        cache_read_tokens: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(serde_json::Value::as_u64),
        cache_creation_tokens: usage
            .pointer("/input_tokens_details/cache_creation_tokens")
            .and_then(serde_json::Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::Transform as _;
    use crate::llm::events_of;

    const TEXT_SAMPLE: &str = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"你好\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"世界\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"你好世界\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"status\":\"completed\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
    );

    const TOOL_SAMPLE: &str = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc1\",\"output_index\":0,\"delta\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc1\",\"output_index\":0,\"delta\":\",\\\"scope\\\":\\\"input\\\"}\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
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
    fn tool_call_taken_from_done_item() {
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
    fn incomplete_max_output_tokens() {
        let mut t = Transform::new();
        let events = t
            .push("{\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}")
            .unwrap();
        assert_eq!(events, vec![LlmEvent::Finished(Finish::MaxTokens)]);
    }

    #[test]
    fn completed_usage_and_refusal_are_distinct() {
        let mut usage = Transform::new();
        let events = usage
            .push("{\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":7,\"cache_creation_tokens\":1}}}}")
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
        refusal
            .push("{\"type\":\"response.refusal.delta\",\"delta\":\"no\"}")
            .unwrap();
        let events = refusal
            .push("{\"type\":\"response.completed\",\"response\":{}}")
            .unwrap();
        assert_eq!(events, vec![LlmEvent::Finished(Finish::Refusal)]);
    }

    #[test]
    fn history_maps_to_input_items() {
        let config = LlmConfig {
            protocol: super::super::Protocol::Responses,
            base_url: "http://x/v1".into(),
            model: "m".into(),
            api_key: None,
            context_window_tokens: 131072,
            max_output_tokens: 16384,
        };
        let history = vec![
            Message::User("任务".into()),
            Message::Assistant {
                text: "看一下".into(),
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
        assert_eq!(v["instructions"], "说明");
        assert_eq!(v["input"][0]["role"], "user");
        assert_eq!(v["input"][1]["role"], "assistant");
        assert_eq!(v["input"][2]["type"], "function_call");
        assert_eq!(v["input"][2]["call_id"], "call_1");
        assert_eq!(v["input"][3]["type"], "function_call_output");
        assert_eq!(v["input"][3]["output"], "结果");
    }
}
