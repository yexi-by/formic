//! LLM 调用层：三种 API 协议（Chat Completions / Responses / Anthropic）各自负责
//! 双向翻译——请求侧把内部对话历史映射为协议格式，响应侧把 SSE 流组装成统一
//! 内部事件；worker 主循环只消费内部事件，不感知后端差异。每次调用的请求体与
//! 原始 SSE 负载完整留痕（审计）。

pub mod anthropic;
pub mod completions;
pub mod responses;

use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
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

/// 一条完整组装的工具调用请求。arguments 是模型给出的原始 JSON 文本；
/// JSON 合法性由 worker 在进入历史前校验（唯一一次，在 LLM 边界上）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallReq {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

/// 内部对话历史：协议无关，三个协议各自翻译成自己的请求格式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    User(String),
    /// 经内部压缩工具验证的历史摘要；不是模型可调用的普通工具结果。
    Compaction(String),
    Assistant {
        text: String,
        tool_calls: Vec<ToolCallReq>,
    },
    /// Responses 返回的完整 output item，按供应商顺序原样保存。该项紧跟对应的
    /// Assistant；Responses 重放它，其他协议继续使用协议无关的 Assistant。
    ResponseOutputItems(Vec<serde_json::Value>),
    ToolResult {
        call_id: String,
        content: String,
    },
}

/// 工具规格：走请求的 tools 字段，不进提示词文字。实例的唯一来源是调度器。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// 统一内部事件：worker 只消费它。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmEvent {
    /// 产出一个文本增量。
    TextDelta(String),
    /// 一条完整组装的工具调用（流式增量已拼好）。
    ToolCall(ToolCallReq),
    /// 供应商明确报告的用量；缺失字段保持 None，不用估算值冒充。
    Usage(ProviderUsage),
    /// 流正常收尾。
    Finished(Finish),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_creation_tokens: Option<u64>,
}

impl ProviderUsage {
    pub fn merge(&mut self, newer: Self) {
        if newer.input_tokens.is_some() {
            self.input_tokens = newer.input_tokens;
        }
        if newer.output_tokens.is_some() {
            self.output_tokens = newer.output_tokens;
        }
        if newer.cache_read_tokens.is_some() {
            self.cache_read_tokens = newer.cache_read_tokens;
        }
        if newer.cache_creation_tokens.is_some() {
            self.cache_creation_tokens = newer.cache_creation_tokens;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.input_tokens.is_none()
            && self.output_tokens.is_none()
            && self.cache_read_tokens.is_none()
            && self.cache_creation_tokens.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finish {
    Stop,
    MaxTokens,
    Refusal,
    /// 模型以工具调用结束本回合；transform 保证此时确有 ToolCall 事件发出。
    ToolUse,
}

/// 调用层错误：保留控制流所需事实，呈现由入口完成。
#[derive(thiserror::Error, Debug)]
pub enum LlmError {
    #[error("HTTP 请求失败：{0}")]
    Transport(#[from] reqwest::Error),
    #[error("LLM 返回 HTTP {status}：{body}")]
    Http { status: u16, body: String },
    #[error("LLM 明确报告上下文超过限制（HTTP {status}）：{body}")]
    ContextLimit { status: u16, body: String },
    #[error("协议事件无法解析：{reason}；原始负载：{payload}")]
    Protocol { reason: String, payload: String },
    #[error("{phase}超过 {timeout:?}")]
    Timeout {
        phase: &'static str,
        timeout: Duration,
    },
    #[error("LLM 流超过本地安全上限 {limit} 字节")]
    StreamLimit { limit: usize },
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
/// 只约束建立连接并取得响应头，不限制正常长生成的总时长。
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
/// 连续十分钟没有收到任何正文数据才中止；每次收到数据后重新计时。
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
/// 单次供应商响应的本地硬边界，同时约束 SSE 缓冲、原始审计和文本增量总量。
const MAX_STREAM_BYTES: usize = 64 * 1024 * 1024;
/// 流量上限已经触发后，仅额外保存该网络块的这个精确前缀。它让常见网络块完整
/// 留痕，同时避免供应商用一个超大块迫使审计再分配无界内存。
const STREAM_LIMIT_AUDIT_CAPTURE_BYTES: usize = 64 * 1024;

pub struct LlmClient {
    http: reqwest::Client,
    config: LlmConfig,
}

/// 已按当前协议完整构造、但尚未发送的请求。调用方先保存 `body`，再把对象交回
/// LlmClient 发送，确保传输失败和取消也有精确输入证据。headers 保持私有，密钥
/// 不会进入 worker 档案。
pub struct PreparedLlmCall {
    url: String,
    body: String,
    headers: Vec<(String, String)>,
}

impl PreparedLlmCall {
    pub fn body(&self) -> &str {
        &self.body
    }
}

/// LLM 调用配置，由启动边界合并环境变量和配置文件，缺失必填项时明确失败。
#[derive(Clone)]
pub struct LlmConfig {
    pub protocol: Protocol,
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    /// 模型声明的完整上下文窗口，用于调用前预算。
    pub context_window_tokens: u64,
    /// Anthropic Messages 协议要求的 max_tokens。其他协议必须为 None，也不会发送
    /// 任何生成控制参数。
    pub anthropic_max_tokens: Option<u64>,
}

impl LlmClient {
    pub fn new(config: LlmConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(RESPONSE_HEADER_TIMEOUT)
                .build()
                .expect("固定的 LLM HTTP client 配置必须有效"),
            config,
        }
    }

    pub fn prepare_call(
        &self,
        instructions: &str,
        history: &[Message],
        tools: &[ToolSpec],
    ) -> PreparedLlmCall {
        let (url, body, headers) = self.build_request(instructions, history, tools);
        PreparedLlmCall { url, body, headers }
    }

    /// 发送一个已经留痕的流式调用。SSE 解析在调用方（worker）循环内驱动，
    /// 不另起任务。
    pub async fn send(&self, prepared: PreparedLlmCall) -> Result<Call, LlmError> {
        // 在途计数覆盖「请求发送 → 响应头 → 流式正文」全程（规模观测）
        let in_flight = LlmInFlight::new();

        let mut req = self
            .http
            .post(&prepared.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(prepared.body);
        for (name, value) in prepared.headers {
            req = req.header(name, value);
        }
        let resp = tokio::time::timeout(RESPONSE_HEADER_TIMEOUT, req.send())
            .await
            .map_err(|_| LlmError::Timeout {
                phase: "等待 LLM 响应头",
                timeout: RESPONSE_HEADER_TIMEOUT,
            })??;
        let status = resp.status();
        if !status.is_success() {
            let snippet = read_error_body(resp).await?;
            if is_structured_context_limit(&snippet) {
                return Err(LlmError::ContextLimit {
                    status: status.as_u16(),
                    body: snippet,
                });
            }
            return Err(LlmError::Http {
                status: status.as_u16(),
                body: snippet,
            });
        }
        let transform: Box<dyn Transform + Send> = match self.config.protocol {
            Protocol::Completions => Box::new(completions::Transform::new()),
            Protocol::Responses => Box::new(responses::Transform::new()),
            Protocol::Anthropic => Box::new(anthropic::Transform::new()),
        };
        Ok(Call {
            stream: EventStream::new(Box::pin(resp.bytes_stream()), transform),
            _in_flight: in_flight,
        })
    }

    pub fn build_request(
        &self,
        instructions: &str,
        history: &[Message],
        tools: &[ToolSpec],
    ) -> (String, String, Vec<(String, String)>) {
        match self.config.protocol {
            Protocol::Completions => {
                completions::build_request(&self.config, instructions, history, tools)
            }
            Protocol::Responses => {
                responses::build_request(&self.config, instructions, history, tools)
            }
            Protocol::Anthropic => {
                anthropic::build_request(&self.config, instructions, history, tools)
            }
        }
    }

    pub fn estimate_request_tokens(
        &self,
        instructions: &str,
        history: &[Message],
        tools: &[ToolSpec],
    ) -> u64 {
        let (_, body, _) = self.build_request(instructions, history, tools);
        crate::tokenize::count(&body)
    }

    pub fn input_budget(&self, safety_tokens: u64) -> u64 {
        self.config
            .context_window_tokens
            .saturating_sub(self.config.anthropic_max_tokens.unwrap_or(0))
            .saturating_sub(safety_tokens)
    }
}

async fn read_error_body(resp: reqwest::Response) -> Result<String, LlmError> {
    let mut bytes = resp.bytes_stream();
    let mut body = Vec::new();
    let mut idle_deadline = tokio::time::Instant::now() + STREAM_IDLE_TIMEOUT;
    while body.len() < HTTP_ERROR_BODY_LIMIT {
        let next = tokio::time::timeout_at(idle_deadline, bytes.next())
            .await
            .map_err(|_| LlmError::Timeout {
                phase: "等待 LLM 错误响应正文",
                timeout: STREAM_IDLE_TIMEOUT,
            })?;
        match next {
            Some(Ok(chunk)) if chunk.is_empty() => continue,
            Some(Ok(chunk)) => {
                idle_deadline = tokio::time::Instant::now() + STREAM_IDLE_TIMEOUT;
                let remaining = HTTP_ERROR_BODY_LIMIT - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Some(Err(error)) => return Err(LlmError::Transport(error)),
            None => break,
        }
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn is_structured_context_limit(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let known = [
        "context_length_exceeded",
        "context_window_exceeded",
        "prompt_too_long",
        "request_too_large",
    ];
    [
        value
            .pointer("/error/code")
            .and_then(serde_json::Value::as_str),
        value
            .pointer("/error/type")
            .and_then(serde_json::Value::as_str),
        value.get("code").and_then(serde_json::Value::as_str),
        value.get("type").and_then(serde_json::Value::as_str),
    ]
    .into_iter()
    .flatten()
    .any(|code| known.contains(&code))
}

/// LLM 在途调用的计数守卫：创建即 +1，销毁即 -1（规模观测）。
struct LlmInFlight;

impl LlmInFlight {
    fn new() -> Self {
        crate::metrics::gauge_add(&crate::metrics::LLM_IN_FLIGHT, 1);
        Self
    }
}

impl Drop for LlmInFlight {
    fn drop(&mut self) {
        crate::metrics::gauge_add(&crate::metrics::LLM_IN_FLIGHT, -1);
    }
}

/// 一次进行中的调用：事件流及全部原始 SSE 负载。
/// 在途计数随请求开始与调用结束更新（规模观测）。
pub struct Call {
    stream: EventStream,
    _in_flight: LlmInFlight,
}

impl Call {
    /// 取下一个内部事件；流结束返回 Ok(None)。
    pub async fn next_event(&mut self) -> Result<Option<LlmEvent>, LlmError> {
        self.stream.next_event().await
    }

    /// 取走当前可审计的流快照，不复制已经积累的正文。完整 SSE data 负载按到达
    /// 顺序保存；若传输或取消发生在一帧中间，末尾先写残帧标记，再写精确残余内容。
    pub fn take_audit_snapshot(&mut self) -> Vec<String> {
        self.stream.take_audit_snapshot()
    }

    /// Responses 本回合已经完成的全部 output item。只有收到供应商完成事件并接受
    /// 本回合后，worker 才把它们写入历史。
    pub fn response_output_items(&self) -> &[serde_json::Value] {
        self.stream.transform.response_output_items()
    }
}

/// 协议 transform：把 SSE data 负载翻译成 0..n 个内部事件。
/// 有状态——流式工具调用的增量在同一调用的连续帧中组装。
pub(crate) trait Transform {
    fn push(&mut self, payload: &str) -> Result<Vec<LlmEvent>, LlmError>;

    fn response_output_items(&self) -> &[serde_json::Value] {
        &[]
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

/// SSE 字节缓冲。`scan_from` 记录上次已经确认不可能出现分隔符的位置，避免一个
/// 大帧被拆成许多小网络块时反复扫描已有字节。
#[derive(Default)]
pub(crate) struct SseBuffer {
    bytes: BytesMut,
    scan_from: usize,
}

impl SseBuffer {
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn take_remaining(&mut self) -> BytesMut {
        self.scan_from = 0;
        std::mem::take(&mut self.bytes)
    }

    #[cfg(test)]
    fn remaining(&self) -> &[u8] {
        &self.bytes
    }
}

struct StreamLimitAudit {
    stream_limit_bytes: usize,
    received_before_chunk: usize,
    chunk_byte_length: usize,
    captured_prefix: Vec<u8>,
}

impl StreamLimitAudit {
    fn capture(stream_limit_bytes: usize, received_before_chunk: usize, chunk: &[u8]) -> Self {
        let captured_bytes = chunk.len().min(STREAM_LIMIT_AUDIT_CAPTURE_BYTES);
        Self {
            stream_limit_bytes,
            received_before_chunk,
            chunk_byte_length: chunk.len(),
            captured_prefix: chunk[..captured_bytes].to_vec(),
        }
    }

    fn append_to(self, snapshot: &mut Vec<String>) {
        let captured_prefix_bytes = self.captured_prefix.len();
        let omitted_suffix_bytes = self.chunk_byte_length.saturating_sub(captured_prefix_bytes);
        let exceeded_by_bytes = self.chunk_byte_length.saturating_sub(
            self.stream_limit_bytes
                .saturating_sub(self.received_before_chunk),
        );
        let (encoding, captured_prefix) = match String::from_utf8(self.captured_prefix) {
            Ok(text) => ("utf-8", text),
            Err(error) => ("hex", encode_hex(&error.into_bytes())),
        };
        snapshot.push(
            serde_json::json!({
                "formic_audit_kind": "stream_limit_exceeded_chunk",
                "stream_limit_bytes": self.stream_limit_bytes,
                "received_before_chunk": self.received_before_chunk,
                "chunk_byte_length": self.chunk_byte_length,
                "exceeded_by_bytes": exceeded_by_bytes,
                "captured_prefix_bytes": captured_prefix_bytes,
                "omitted_suffix_bytes": omitted_suffix_bytes,
                "capture_limit_bytes": STREAM_LIMIT_AUDIT_CAPTURE_BYTES,
                "encoding": encoding,
                "next_record_is_captured_prefix": true,
            })
            .to_string(),
        );
        snapshot.push(captured_prefix);
    }
}

struct EventStream {
    bytes: ByteStream,
    transform: Box<dyn Transform + Send>,
    pending: VecDeque<LlmEvent>,
    buffer: SseBuffer,
    raw_log: Vec<String>,
    received_bytes: usize,
    max_stream_bytes: usize,
    idle_timeout: Duration,
    eof: bool,
    finished: bool,
    stream_limit_audit: Option<StreamLimitAudit>,
}

impl EventStream {
    fn new(bytes: ByteStream, transform: Box<dyn Transform + Send>) -> Self {
        Self::with_policy(bytes, transform, MAX_STREAM_BYTES, STREAM_IDLE_TIMEOUT)
    }

    fn with_policy(
        bytes: ByteStream,
        transform: Box<dyn Transform + Send>,
        max_stream_bytes: usize,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            bytes,
            transform,
            pending: VecDeque::new(),
            buffer: SseBuffer::default(),
            raw_log: Vec::new(),
            received_bytes: 0,
            max_stream_bytes,
            idle_timeout,
            eof: false,
            finished: false,
            stream_limit_audit: None,
        }
    }

    fn take_audit_snapshot(&mut self) -> Vec<String> {
        let mut snapshot = std::mem::take(&mut self.raw_log);
        let residual = self.buffer.take_remaining();
        if !residual.is_empty() {
            let byte_length = residual.len();
            match String::from_utf8(Vec::from(residual)) {
                Ok(residual) => {
                    snapshot.push(
                        serde_json::json!({
                            "formic_audit_kind": "incomplete_sse_frame",
                            "encoding": "utf-8",
                            "byte_length": byte_length,
                            "next_record_is_residual": true,
                        })
                        .to_string(),
                    );
                    snapshot.push(residual);
                }
                Err(error) => {
                    snapshot.push(
                        serde_json::json!({
                            "formic_audit_kind": "incomplete_sse_frame",
                            "encoding": "hex",
                            "byte_length": byte_length,
                            "next_record_is_residual": true,
                        })
                        .to_string(),
                    );
                    snapshot.push(encode_hex(&error.into_bytes()));
                }
            }
        }
        if let Some(limit_audit) = self.stream_limit_audit.take() {
            limit_audit.append_to(&mut snapshot);
        }
        snapshot
    }

    fn accept_event(&mut self, event: LlmEvent) -> Result<LlmEvent, LlmError> {
        if self.finished && !matches!(event, LlmEvent::Usage(_)) {
            return Err(LlmError::protocol("首个完成事件之后又收到协议事件", ""));
        }
        if matches!(event, LlmEvent::Finished(_)) {
            self.finished = true;
        }
        Ok(event)
    }

    async fn next_event(&mut self) -> Result<Option<LlmEvent>, LlmError> {
        let mut idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
        loop {
            if let Some(event) = self.pending.pop_front() {
                return self.accept_event(event).map(Some);
            }
            while let Some(frame) = take_frame(&mut self.buffer) {
                let payloads = match frame_payloads(&frame) {
                    Ok(payloads) => payloads,
                    Err(_) => {
                        let marker = serde_json::json!({
                            "formic_audit_kind": "invalid_utf8_sse_frame",
                            "encoding": "hex",
                            "byte_length": frame.len(),
                            "next_record_is_frame": true,
                        })
                        .to_string();
                        self.raw_log.push(marker.clone());
                        self.raw_log.push(encode_hex(&frame));
                        return Err(LlmError::protocol(
                            "完整 SSE 帧不是合法 UTF-8，精确字节已按十六进制写入审计",
                            &marker,
                        ));
                    }
                };
                for payload in payloads {
                    self.raw_log.push(payload.clone());
                    self.pending.extend(self.transform.push(&payload)?);
                }
                if let Some(event) = self.pending.pop_front() {
                    return self.accept_event(event).map(Some);
                }
            }
            if self.eof {
                return Ok(None);
            }
            let next = tokio::time::timeout_at(idle_deadline, self.bytes.next())
                .await
                .map_err(|_| LlmError::Timeout {
                    phase: "等待 LLM 流数据",
                    timeout: self.idle_timeout,
                })?;
            match next {
                Some(Ok(chunk)) if chunk.is_empty() => continue,
                Some(Ok(chunk)) => {
                    idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
                    let remaining = self.max_stream_bytes.saturating_sub(self.received_bytes);
                    if chunk.len() > remaining {
                        self.stream_limit_audit = Some(StreamLimitAudit::capture(
                            self.max_stream_bytes,
                            self.received_bytes,
                            &chunk,
                        ));
                        return Err(LlmError::StreamLimit {
                            limit: self.max_stream_bytes,
                        });
                    }
                    self.received_bytes += chunk.len();
                    self.buffer.extend_from_slice(&chunk);
                }
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

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
    }
    encoded
}

/// 从字节缓冲取出下一个完整 SSE 帧（空行分隔，兼容 LF 与 CRLF）。已消费的
/// 前缀通过 `BytesMut::split_to` 分离，不随剩余流长度搬移；每个输入字节只会被
/// 扫描常数次。
pub(crate) fn take_frame(buffer: &mut SseBuffer) -> Option<BytesMut> {
    let bytes = &buffer.bytes;
    let mut index = buffer.scan_from.min(bytes.len());
    let separator = loop {
        if bytes.get(index..index.saturating_add(2)) == Some(b"\n\n") {
            break Some((index, 2));
        }
        if bytes.get(index..index.saturating_add(4)) == Some(b"\r\n\r\n") {
            break Some((index, 4));
        }
        if index == bytes.len() {
            break None;
        }
        index += 1;
    };

    let Some((index, separator_len)) = separator else {
        // 最长分隔符有四字节。只保留末尾三个候选起点供下一批字节到达后复查。
        buffer.scan_from = buffer.bytes.len().saturating_sub(3);
        return None;
    };

    let mut frame = buffer.bytes.split_to(index + separator_len);
    frame.truncate(index);
    buffer.scan_from = 0;
    Some(frame)
}

/// 从一帧中提取 data 负载：多行 data 以 \n 拼接；忽略 event:/注释等其他行。
pub(crate) fn frame_payloads(frame: &[u8]) -> Result<Vec<String>, std::str::Utf8Error> {
    let text = std::str::from_utf8(frame)?;
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
    Ok(out)
}

#[cfg(test)]
pub(crate) fn events_of(mut t: Box<dyn Transform + Send>, sample: &str) -> Vec<LlmEvent> {
    let mut buffer = SseBuffer::default();
    buffer.extend_from_slice(sample.as_bytes());
    let mut events = Vec::new();
    while let Some(frame) = take_frame(&mut buffer) {
        for payload in frame_payloads(&frame).expect("测试样本必须是合法 UTF-8") {
            events.extend(t.push(&payload).unwrap());
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_split_across_chunks() {
        let mut buffer = SseBuffer::default();
        buffer.extend_from_slice("data: 你".as_bytes());
        assert!(take_frame(&mut buffer).is_none(), "半个帧不产生输出");
        buffer.extend_from_slice("好\n\n".as_bytes());
        assert_eq!(take_frame(&mut buffer).unwrap(), "data: 你好".as_bytes());
    }

    #[test]
    fn crlf_frames_split() {
        let mut buffer = SseBuffer::default();
        buffer.extend_from_slice(b"data: a\r\n\r\ndata: b\r\n\r\n");
        assert_eq!(take_frame(&mut buffer).unwrap().as_ref(), b"data: a");
        assert_eq!(take_frame(&mut buffer).unwrap().as_ref(), b"data: b");
        assert!(take_frame(&mut buffer).is_none());
    }

    #[test]
    fn many_small_frames_are_consumed_in_order_and_leave_residual_unchanged() {
        const FRAME_COUNT: usize = 25_000;
        const RESIDUAL: &[u8] = b"data: unfinished";

        let mut source = Vec::with_capacity(FRAME_COUNT * 10 + RESIDUAL.len());
        for index in 0..FRAME_COUNT {
            source.extend_from_slice(format!("data: {index}\n\n").as_bytes());
        }
        source.extend_from_slice(RESIDUAL);

        let mut buffer = SseBuffer::default();
        buffer.extend_from_slice(&source);
        for index in 0..FRAME_COUNT {
            let frame = take_frame(&mut buffer).expect("每个完整帧都应被取出");
            assert_eq!(frame, format!("data: {index}").as_bytes());
        }
        assert!(take_frame(&mut buffer).is_none());
        assert_eq!(buffer.remaining(), RESIDUAL);
    }

    #[test]
    fn long_frame_split_into_single_bytes_is_scanned_incrementally() {
        const PAYLOAD_LEN: usize = 32 * 1024;

        let mut expected = b"data: ".to_vec();
        expected.extend(std::iter::repeat_n(b'x', PAYLOAD_LEN));
        let mut buffer = SseBuffer::default();
        for byte in &expected {
            buffer.extend_from_slice(std::slice::from_ref(byte));
            assert!(take_frame(&mut buffer).is_none());
        }
        buffer.extend_from_slice(b"\r");
        assert!(take_frame(&mut buffer).is_none());
        buffer.extend_from_slice(b"\n");
        assert!(take_frame(&mut buffer).is_none());
        buffer.extend_from_slice(b"\r");
        assert!(take_frame(&mut buffer).is_none());
        buffer.extend_from_slice(b"\n");

        assert_eq!(take_frame(&mut buffer).unwrap(), expected);
        assert!(buffer.is_empty());
    }

    #[test]
    fn multiline_data_joined_and_comments_ignored() {
        let frame = ": comment\nevent: message\ndata: 第一\ndata: 第二\n".as_bytes();
        assert_eq!(
            frame_payloads(frame).unwrap(),
            vec!["第一\n第二".to_string()]
        );
    }

    #[test]
    fn non_data_frame_yields_nothing() {
        assert!(
            frame_payloads(b"event: message_start\n")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn input_budget_only_reserves_anthropic_required_output() {
        let common = LlmConfig {
            protocol: Protocol::Responses,
            base_url: "http://localhost".into(),
            model: "m".into(),
            api_key: None,
            context_window_tokens: 100_000,
            anthropic_max_tokens: None,
        };
        assert_eq!(LlmClient::new(common.clone()).input_budget(2_000), 98_000);

        let anthropic = LlmConfig {
            protocol: Protocol::Anthropic,
            anthropic_max_tokens: Some(8_000),
            ..common
        };
        assert_eq!(LlmClient::new(anthropic).input_budget(2_000), 90_000);
    }

    #[tokio::test]
    async fn stream_rejects_received_bytes_over_local_limit() {
        const CHUNK: &[u8] = b"data: 12345\n\n";
        let bytes =
            futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(Bytes::from_static(CHUNK))]);
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(completions::Transform::new()),
            4,
            Duration::from_secs(1),
        );

        let error = stream.next_event().await.unwrap_err();
        assert!(matches!(error, LlmError::StreamLimit { limit: 4 }));
        let snapshot = stream.take_audit_snapshot();
        assert_eq!(snapshot.len(), 2);
        let marker: serde_json::Value = serde_json::from_str(&snapshot[0]).unwrap();
        assert_eq!(marker["formic_audit_kind"], "stream_limit_exceeded_chunk");
        assert_eq!(marker["stream_limit_bytes"], 4);
        assert_eq!(marker["received_before_chunk"], 0);
        assert_eq!(marker["chunk_byte_length"], CHUNK.len());
        assert_eq!(marker["captured_prefix_bytes"], CHUNK.len());
        assert_eq!(marker["omitted_suffix_bytes"], 0);
        assert_eq!(marker["encoding"], "utf-8");
        assert_eq!(snapshot[1].as_bytes(), CHUNK);
    }

    #[tokio::test]
    async fn stream_limit_audit_capture_is_explicitly_bounded() {
        let chunk = vec![b'x'; STREAM_LIMIT_AUDIT_CAPTURE_BYTES + 17];
        let chunk_length = chunk.len();
        let bytes = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(Bytes::from(chunk))]);
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(completions::Transform::new()),
            4,
            Duration::from_secs(1),
        );

        assert!(matches!(
            stream.next_event().await,
            Err(LlmError::StreamLimit { limit: 4 })
        ));
        let snapshot = stream.take_audit_snapshot();
        assert_eq!(snapshot.len(), 2);
        let marker: serde_json::Value = serde_json::from_str(&snapshot[0]).unwrap();
        assert_eq!(marker["chunk_byte_length"], chunk_length);
        assert_eq!(
            marker["captured_prefix_bytes"],
            STREAM_LIMIT_AUDIT_CAPTURE_BYTES
        );
        assert_eq!(marker["omitted_suffix_bytes"], 17);
        assert_eq!(
            marker["capture_limit_bytes"],
            STREAM_LIMIT_AUDIT_CAPTURE_BYTES
        );
        assert_eq!(snapshot[1].len(), STREAM_LIMIT_AUDIT_CAPTURE_BYTES);
        assert!(snapshot[1].bytes().all(|byte| byte == b'x'));
    }

    #[tokio::test]
    async fn stream_idle_timeout_is_per_read() {
        let bytes = futures_util::stream::pending::<Result<Bytes, reqwest::Error>>();
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(completions::Transform::new()),
            1024,
            Duration::from_millis(1),
        );

        let error = stream.next_event().await.unwrap_err();
        assert!(matches!(
            error,
            LlmError::Timeout {
                phase: "等待 LLM 流数据",
                timeout
            } if timeout == Duration::from_millis(1)
        ));
    }

    #[tokio::test]
    async fn usage_is_allowed_after_finish_but_a_second_terminal_event_is_not() {
        let bytes = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(
            Bytes::from_static(concat!(
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1},\"choices\":[]}\n\n",
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
            ).as_bytes()),
        )]);
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(completions::Transform::new()),
            1024,
            Duration::from_secs(1),
        );

        assert_eq!(
            stream.next_event().await.unwrap(),
            Some(LlmEvent::Finished(Finish::Stop))
        );
        assert_eq!(
            stream.next_event().await.unwrap(),
            Some(LlmEvent::Usage(ProviderUsage {
                input_tokens: Some(1),
                output_tokens: Some(1),
                cache_read_tokens: None,
                cache_creation_tokens: None,
            }))
        );
        assert!(matches!(
            stream.next_event().await,
            Err(LlmError::Protocol { reason, .. })
                if reason.contains("首个完成事件之后")
        ));
        assert_eq!(stream.raw_log.len(), 3, "违规终态后帧仍须保留审计证据");
    }

    #[tokio::test]
    async fn audit_snapshot_marks_bytes_from_an_incomplete_sse_frame() {
        const RESIDUAL: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"半帧";
        let body = format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"完整\"}},\"finish_reason\":null}}]}}\n\n{RESIDUAL}"
        );
        let bytes = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(Bytes::from(body))]);
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(completions::Transform::new()),
            4096,
            Duration::from_secs(1),
        );

        assert_eq!(
            stream.next_event().await.unwrap(),
            Some(LlmEvent::TextDelta("完整".into()))
        );
        let snapshot = stream.take_audit_snapshot();
        assert_eq!(snapshot.len(), 3);
        let marker: serde_json::Value = serde_json::from_str(&snapshot[1]).unwrap();
        assert_eq!(marker["formic_audit_kind"], "incomplete_sse_frame");
        assert_eq!(marker["encoding"], "utf-8");
        assert_eq!(marker["byte_length"], RESIDUAL.len());
        assert_eq!(snapshot[2], RESIDUAL);
        assert!(stream.raw_log.is_empty());
        assert!(stream.buffer.is_empty());
    }

    #[tokio::test]
    async fn invalid_utf8_frame_is_audited_losslessly_before_transform() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingTransform(Arc<AtomicUsize>);

        impl Transform for CountingTransform {
            fn push(&mut self, _payload: &str) -> Result<Vec<LlmEvent>, LlmError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(Vec::new())
            }
        }

        const INVALID_FRAME: &[u8] = b"data: invalid-\xff";
        let mut body = b"data: valid\n\n".to_vec();
        body.extend_from_slice(INVALID_FRAME);
        body.extend_from_slice(b"\n\n");
        let bytes = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(Bytes::from(body))]);
        let transform_calls = Arc::new(AtomicUsize::new(0));
        let mut stream = EventStream::with_policy(
            Box::pin(bytes),
            Box::new(CountingTransform(Arc::clone(&transform_calls))),
            4096,
            Duration::from_secs(1),
        );

        assert!(matches!(
            stream.next_event().await,
            Err(LlmError::Protocol { reason, .. }) if reason.contains("不是合法 UTF-8")
        ));
        assert_eq!(
            transform_calls.load(Ordering::Relaxed),
            1,
            "非法帧不得进入协议 transform"
        );

        let snapshot = stream.take_audit_snapshot();
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot[0], "valid", "合法帧继续使用原有审计格式");
        let marker: serde_json::Value = serde_json::from_str(&snapshot[1]).unwrap();
        assert_eq!(marker["formic_audit_kind"], "invalid_utf8_sse_frame");
        assert_eq!(marker["encoding"], "hex");
        assert_eq!(marker["byte_length"], INVALID_FRAME.len());
        assert_eq!(marker["next_record_is_frame"], true);
        assert_eq!(snapshot[2], encode_hex(INVALID_FRAME));
    }
}
