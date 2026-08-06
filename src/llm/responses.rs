//! OpenAI Responses 协议：请求构造与 SSE → 内部事件 transform。
//! 事件类型是开放集合，未识别的 response.* 事件一律忽略；失败类事件显式报错。

use super::{CallInput, Finish, LlmConfig, LlmError, LlmEvent};

pub fn build_request(
    config: &LlmConfig,
    input: &CallInput<'_>,
) -> (String, String, Vec<(String, String)>) {
    let url = format!("{}/responses", config.base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": config.model,
        "stream": true,
        "instructions": input.instructions,
        "input": input.user,
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
    let v: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| LlmError::protocol(format!("JSON 解析失败：{e}"), payload))?;
    let ty = v["type"]
        .as_str()
        .ok_or_else(|| LlmError::protocol("缺少 type 字段", payload))?;
    match ty {
        "response.output_text.delta" => {
            let delta = v["delta"].as_str().unwrap_or_default();
            Ok((!delta.is_empty()).then(|| LlmEvent::TextDelta(delta.to_string())))
        }
        "response.completed" => Ok(Some(LlmEvent::Finished(Finish::Stop))),
        "response.incomplete" => match v["response"]["incomplete_details"]["reason"].as_str() {
            Some("max_output_tokens") => Ok(Some(LlmEvent::Finished(Finish::MaxTokens))),
            other => Err(LlmError::protocol(
                format!("响应未完成，原因 {other:?}"),
                payload,
            )),
        },
        "response.output_item.done" => {
            Ok((v["item"]["type"].as_str() == Some("function_call")).then_some(LlmEvent::ToolCall))
        }
        "response.failed" | "error" => Err(LlmError::protocol("响应失败事件", payload)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.content_part.added\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"你好\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"世界\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"你好世界\"}\n\n",
        "data: {\"type\":\"response.content_part.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"你好世界\"}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"status\":\"completed\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
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
    fn function_call_item_detected() {
        let payload = "{\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"name\":\"search\"}}";
        assert_eq!(transform(payload).unwrap(), Some(LlmEvent::ToolCall));
    }

    #[test]
    fn incomplete_max_output_tokens() {
        let payload = "{\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}";
        assert_eq!(
            transform(payload).unwrap(),
            Some(LlmEvent::Finished(Finish::MaxTokens))
        );
    }
}
