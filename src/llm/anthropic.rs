//! Anthropic Messages 协议：请求构造与 SSE → 内部事件 transform。
//! max_tokens 是该协议的必填字段，属于内部参数，由代码固定（AGENTS.md §7）。

use super::{CallInput, Finish, LlmConfig, LlmError, LlmEvent};

/// 协议必填的输出上限；单元产出超过它即判截断失败，不作为调用方配置项。
const MAX_OUTPUT_TOKENS: u32 = 16384;

const ANTHROPIC_VERSION: &str = "2023-06-01";

pub fn build_request(
    config: &LlmConfig,
    input: &CallInput<'_>,
) -> (String, String, Vec<(String, String)>) {
    let url = format!("{}/messages", config.base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": config.model,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "stream": true,
        "system": input.instructions,
        "messages": [
            {"role": "user", "content": input.user},
        ],
    })
    .to_string();
    let mut headers = vec![(
        "anthropic-version".to_string(),
        ANTHROPIC_VERSION.to_string(),
    )];
    if let Some(key) = &config.api_key {
        headers.push(("x-api-key".to_string(), key.clone()));
    }
    (url, body, headers)
}

pub fn transform(payload: &str) -> Result<Option<LlmEvent>, LlmError> {
    let v: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| LlmError::protocol(format!("JSON 解析失败：{e}"), payload))?;
    let ty = v["type"]
        .as_str()
        .ok_or_else(|| LlmError::protocol("缺少 type 字段", payload))?;
    match ty {
        "content_block_start" => {
            Ok((v["content_block"]["type"].as_str() == Some("tool_use"))
                .then_some(LlmEvent::ToolCall))
        }
        "content_block_delta" => match v["delta"]["type"].as_str() {
            Some("text_delta") => {
                let text = v["delta"]["text"].as_str().unwrap_or_default();
                Ok((!text.is_empty()).then(|| LlmEvent::TextDelta(text.to_string())))
            }
            _ => Ok(None), // input_json_delta 等由 content_block_start 的 tool_use 先行判出
        },
        "message_delta" => match v["delta"]["stop_reason"].as_str() {
            Some("end_turn") => Ok(Some(LlmEvent::Finished(Finish::Stop))),
            Some("max_tokens") => Ok(Some(LlmEvent::Finished(Finish::MaxTokens))),
            Some("tool_use") => Ok(Some(LlmEvent::ToolCall)),
            _ => Ok(None),
        },
        "error" => Err(LlmError::protocol("错误事件", payload)),
        _ => Ok(None), // message_start / content_block_stop / message_stop / ping 等
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"世界\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":5}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
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
    fn tool_use_block_detected() {
        let payload = "{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"name\":\"search\"}}";
        assert_eq!(transform(payload).unwrap(), Some(LlmEvent::ToolCall));
    }

    #[test]
    fn max_tokens_stop_detected() {
        let payload = "{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":9}}";
        assert_eq!(
            transform(payload).unwrap(),
            Some(LlmEvent::Finished(Finish::MaxTokens))
        );
    }
}
