//! OpenAI Chat Completions 协议：请求构造与 SSE → 内部事件 transform。
//! 兼容讲同一形状的供应商（OpenAI、DeepSeek、Moonshot、OpenRouter、vLLM 等）。

use super::{CallInput, Finish, LlmConfig, LlmError, LlmEvent};

pub fn build_request(
    config: &LlmConfig,
    input: &CallInput<'_>,
) -> (String, String, Vec<(String, String)>) {
    let url = format!("{}/chat/completions", config.base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": config.model,
        "stream": true,
        "messages": [
            {"role": "system", "content": input.instructions},
            {"role": "user", "content": input.user},
        ],
    })
    .to_string();
    let headers = config
        .api_key
        .as_ref()
        .map(|key| ("authorization".to_string(), format!("Bearer {key}")))
        .into_iter()
        .collect();
    (url, body, headers)
}

pub fn transform(payload: &str) -> Result<Option<LlmEvent>, LlmError> {
    if payload.trim() == "[DONE]" {
        return Ok(None);
    }
    let v: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| LlmError::protocol(format!("JSON 解析失败：{e}"), payload))?;
    let choices = v["choices"]
        .as_array()
        .ok_or_else(|| LlmError::protocol("缺少 choices 数组", payload))?;
    let Some(choice) = choices.first() else {
        return Ok(None); // 纯用量等无 choices 的帧，与产出无关
    };
    if let Some(delta) = choice.get("delta") {
        if delta.get("tool_calls").is_some_and(|t| !t.is_null()) {
            return Ok(Some(LlmEvent::ToolCall));
        }
        if let Some(content) = delta["content"].as_str()
            && !content.is_empty()
        {
            return Ok(Some(LlmEvent::TextDelta(content.to_string())));
        }
    }
    match choice.get("finish_reason") {
        Some(serde_json::Value::String(reason)) => match reason.as_str() {
            "stop" => Ok(Some(LlmEvent::Finished(Finish::Stop))),
            "length" => Ok(Some(LlmEvent::Finished(Finish::MaxTokens))),
            "tool_calls" | "function_call" => Ok(Some(LlmEvent::ToolCall)),
            other => Err(LlmError::protocol(
                format!("未知 finish_reason {other:?}"),
                payload,
            )),
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = concat!(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"世界\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    fn events_of(sample: &str) -> Vec<LlmEvent> {
        let mut buffer = sample.as_bytes().to_vec();
        let mut events = Vec::new();
        while let Some(frame) = super::super::take_frame(&mut buffer) {
            for payload in super::super::frame_payloads(&frame) {
                if let Some(ev) = transform(&payload).unwrap() {
                    events.push(ev);
                }
            }
        }
        events
    }

    #[test]
    fn recorded_sample_yields_text_then_stop() {
        assert_eq!(
            events_of(SAMPLE),
            vec![
                LlmEvent::TextDelta("你好".into()),
                LlmEvent::TextDelta("世界".into()),
                LlmEvent::Finished(Finish::Stop),
            ]
        );
    }

    #[test]
    fn tool_call_delta_detected() {
        let payload = "{\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"search\"}}]},\"finish_reason\":null}]}";
        assert_eq!(transform(payload).unwrap(), Some(LlmEvent::ToolCall));
    }

    #[test]
    fn length_finish_is_max_tokens() {
        let payload = "{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}";
        assert_eq!(
            transform(payload).unwrap(),
            Some(LlmEvent::Finished(Finish::MaxTokens))
        );
    }

    #[test]
    fn garbage_payload_is_protocol_error() {
        assert!(matches!(transform("{oops"), Err(LlmError::Protocol { .. })));
    }
}
