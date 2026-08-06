//! LLM 调用层：三种 API 协议（Chat Completions / Responses / Anthropic）各自
//! 一个请求构造 + SSE transform，统一输出内部事件枚举；worker 主循环只消费
//! 内部事件，不感知后端差异。每次调用的请求体与原始 SSE 负载完整留痕（审计）。

pub mod anthropic;
pub mod completions;
pub mod responses;

use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};

/// 协议形状，由环境变量 FORMIC_LLM_PROTOCOL 选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Completions,
    Responses,
    Anthropic,
}

impl Protocol {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "completions" => Ok(Self::Completions),
            "responses" => Ok(Self::Responses),
            "anthropic" => Ok(Self::Anthropic),
            other => Err(format!(
                "未知协议 {other:?}，FORMIC_LLM_PROTOCOL 可选值：completions / responses / anthropic"
            )),
        }
    }
}

/// 统一内部事件：worker 只消费它。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmEvent {
    /// 产出一个文本增量。
    TextDelta(String),
    /// 模型请求工具调用（原型未配置工具，worker 据此判单元失败）。
    ToolCall,
    /// 流正常收尾。
    Finished(Finish),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finish {
    Stop,
    MaxTokens,
}

/// 一次调用的输入：instructions 顶层字段 + 用户消息。
pub struct CallInput<'a> {
    pub instructions: &'a str,
    pub user: &'a str,
}

/// LLM 调用配置，来源是环境变量（部署选择），缺失必填项在启动时明确失败。
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub protocol: Protocol,
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
}

/// 调用层错误：保留控制流所需事实，呈现由入口完成。
#[derive(thiserror::Error, Debug)]
pub enum LlmError {
    #[error("HTTP 请求失败：{0}")]
    Transport(#[from] reqwest::Error),
    #[error("LLM 返回 HTTP {status}：{body}")]
    Http { status: u16, body: String },
    #[error("协议事件无法解析：{reason}；原始负载：{payload}")]
    Protocol { reason: String, payload: String },
}

impl LlmError {
    pub(crate) fn protocol(reason: impl Into<String>, payload: &str) -> Self {
        LlmError::Protocol {
            reason: reason.into(),
            payload: payload.to_string(),
        }
    }
}

/// HTTP 错误响应体的留痕上限，避免把整页错误倒进诊断。
const HTTP_ERROR_BODY_LIMIT: usize = 1024;

pub struct LlmClient {
    http: reqwest::Client,
    config: LlmConfig,
}

impl LlmClient {
    pub fn new(config: LlmConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    /// 发起一次流式调用。SSE 解析在调用方（worker）循环内驱动，不另起任务。
    pub async fn call(&self, input: &CallInput<'_>) -> Result<Call, LlmError> {
        let (url, body, headers) = match self.config.protocol {
            Protocol::Completions => completions::build_request(&self.config, input),
            Protocol::Responses => responses::build_request(&self.config, input),
            Protocol::Anthropic => anthropic::build_request(&self.config, input),
        };
        let mut req = self
            .http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone());
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            let snippet: String = text.chars().take(HTTP_ERROR_BODY_LIMIT).collect();
            return Err(LlmError::Http {
                status: status.as_u16(),
                body: snippet,
            });
        }
        Ok(Call {
            request_body: body,
            stream: EventStream::new(Box::pin(resp.bytes_stream()), self.config.protocol),
        })
    }
}

/// 一次进行中的调用：事件流 + 审计留痕（请求体与全部原始 SSE 负载）。
pub struct Call {
    /// 发出的请求体原文。
    pub request_body: String,
    stream: EventStream,
}

impl Call {
    /// 取下一个内部事件；流结束返回 Ok(None)。
    pub async fn next_event(&mut self) -> Result<Option<LlmEvent>, LlmError> {
        self.stream.next_event().await
    }

    /// 已收到的全部原始 SSE data 负载，按到达顺序。
    pub fn raw_log(&self) -> &[String] {
        &self.stream.raw_log
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct EventStream {
    bytes: ByteStream,
    protocol: Protocol,
    buffer: Vec<u8>,
    raw_log: Vec<String>,
    eof: bool,
}

impl EventStream {
    fn new(bytes: ByteStream, protocol: Protocol) -> Self {
        Self {
            bytes,
            protocol,
            buffer: Vec::new(),
            raw_log: Vec::new(),
            eof: false,
        }
    }

    async fn next_event(&mut self) -> Result<Option<LlmEvent>, LlmError> {
        loop {
            while let Some(frame) = take_frame(&mut self.buffer) {
                for payload in frame_payloads(&frame) {
                    self.raw_log.push(payload.clone());
                    if let Some(event) = transform(self.protocol, &payload)? {
                        return Ok(Some(event));
                    }
                }
            }
            if self.eof {
                return Ok(None);
            }
            match self.bytes.next().await {
                Some(Ok(chunk)) => self.buffer.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(LlmError::Transport(e)),
                None => {
                    self.eof = true;
                    // 流末尾可能有没有空行收尾的残帧，补一个分帧符再收一遍。
                    if !self.buffer.is_empty() {
                        self.buffer.extend_from_slice(b"\n\n");
                    }
                }
            }
        }
    }
}

fn transform(protocol: Protocol, payload: &str) -> Result<Option<LlmEvent>, LlmError> {
    match protocol {
        Protocol::Completions => completions::transform(payload),
        Protocol::Responses => responses::transform(payload),
        Protocol::Anthropic => anthropic::transform(payload),
    }
}

/// 从字节缓冲取出下一个完整 SSE 帧（空行分隔，兼容 LF 与 CRLF）。
pub(crate) fn take_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let lf = find_subslice(buffer, b"\n\n").map(|i| (i, 2));
    let crlf = find_subslice(buffer, b"\r\n\r\n").map(|i| (i, 4));
    let (idx, sep) = match (lf, crlf) {
        (Some(a), Some(b)) => {
            if a.0 <= b.0 {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => return None,
    };
    Some(buffer.drain(..idx + sep).take(idx).collect())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// 从一帧中提取 data 负载：多行 data 以 \n 拼接；忽略 event:/注释等其他行。
pub(crate) fn frame_payloads(frame: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(frame);
    let mut data_lines: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.strip_prefix(' ').unwrap_or(data));
        }
    }
    if !data_lines.is_empty() {
        out.push(data_lines.join("\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_split_across_chunks() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice("data: 你".as_bytes());
        assert!(take_frame(&mut buffer).is_none(), "半个帧不产生输出");
        buffer.extend_from_slice("好\n\n".as_bytes());
        assert_eq!(take_frame(&mut buffer).unwrap(), "data: 你好".as_bytes());
    }

    #[test]
    fn crlf_frames_split() {
        let mut buffer = b"data: a\r\n\r\ndata: b\r\n\r\n".to_vec();
        assert_eq!(take_frame(&mut buffer).unwrap(), b"data: a");
        assert_eq!(take_frame(&mut buffer).unwrap(), b"data: b");
        assert!(take_frame(&mut buffer).is_none());
    }

    #[test]
    fn multiline_data_joined_and_comments_ignored() {
        let frame = ": comment\nevent: message\ndata: 第一\ndata: 第二\n".as_bytes();
        assert_eq!(frame_payloads(frame), vec!["第一\n第二".to_string()]);
    }

    #[test]
    fn non_data_frame_yields_nothing() {
        assert!(frame_payloads(b"event: message_start\n").is_empty());
    }
}
