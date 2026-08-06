//! OpenAI Responses 协议：历史 → 请求映射，SSE → 内部事件 transform。
//! 工具调用的完整参数在 response.output_item.done 事件上取，无需增量组装。
//! 事件类型是开放集合，未识别的一律忽略；失败类事件显式报错。

use super::{Finish, LlmConfig, LlmError, LlmEvent, Message, ToolCallReq, ToolSpec};

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
                    Ok(Vec::new())
                }
            }
            "response.completed" => Ok(vec![LlmEvent::Finished(if self.saw_tool_call {
                Finish::ToolUse
            } else {
                Finish::Stop
            })]),
            "response.incomplete" => match v["response"]["incomplete_details"]["reason"].as_str() {
                Some("max_output_tokens") => Ok(vec![LlmEvent::Finished(Finish::MaxTokens)]),
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
    fn history_maps_to_input_items() {
        let config = LlmConfig {
            protocol: super::super::Protocol::Responses,
            base_url: "http://x/v1".into(),
            model: "m".into(),
            api_key: None,
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
