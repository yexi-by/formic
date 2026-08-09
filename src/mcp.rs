//! 通用 MCP 客户端：启动时发现并冻结允许的工具目录，运行时按 job/unit 管理会话。
//! 传输失败和超时从工具结果中分离；原调用从不自动重放。

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, Stream, StreamExt, future::join_all, stream::BoxStream};
use http::{HeaderName, HeaderValue};
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotification,
    CancelledNotificationParam, ClientCapabilities, ClientInfo, ClientJsonRpcMessage,
    ClientRequest, ContentBlock, Implementation, JsonRpcMessage, RequestId, ServerJsonRpcMessage,
    ServerResult, Tool,
};
use rmcp::service::{
    NotificationContext, Peer, PeerRequestOptions, RoleClient, RunningService, RxJsonRpcMessage,
    ServiceError, TxJsonRpcMessage,
};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::Transport;
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use rmcp::transport::streamable_http_client::{
    AuthRequiredError, InsufficientScopeError, StreamableHttpClient,
    StreamableHttpClientTransportConfig, StreamableHttpError, StreamableHttpPostResponse,
};
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use sse_stream::{Error as SseError, Sse, SseStream};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::config::{McpServerConfig, McpTransportConfig, SessionScope};
use crate::llm::ToolSpec;
use crate::tools::ToolOutput;

const MODEL_TOOL_NAME_LIMIT: usize = 64;
const SESSION_CLOSE_TIMEOUT_SEC: u64 = 5;
const CHILD_EXIT_TIMEOUT_SEC: u64 = 3;
const CANCELLATION_DISPATCH_GRACE_MS: u64 = 100;
const MCP_JSON_ESCAPE_EXPANSION: usize = 6;
const MCP_PROTOCOL_OVERHEAD_BYTES: usize = 64 * 1024;
const MCP_STDERR_LINE_LIMIT: usize = 64 * 1024;
const MCP_SESSION_HEADER: &str = "mcp-session-id";
const MCP_LAST_EVENT_ID_HEADER: &str = "last-event-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const JSON_MIME_TYPE: &str = "application/json";

type ClientService = RunningService<RoleClient, FormicClientHandler>;
type StdioWriter = FramedWrite<ChildStdin, JsonRpcMessageCodec<TxJsonRpcMessage<RoleClient>>>;

fn bounded_stderr_lines<R: tokio::io::AsyncRead>(reader: R) -> FramedRead<R, LinesCodec> {
    FramedRead::new(
        reader,
        LinesCodec::new_with_max_length(MCP_STDERR_LINE_LIMIT),
    )
}

#[derive(Clone)]
pub struct McpManager {
    servers: Arc<BTreeMap<String, Arc<McpServer>>>,
}

#[derive(Clone)]
pub struct McpTool {
    server: Arc<McpServer>,
    remote_name: String,
}

pub struct McpRegistration {
    pub model_name: String,
    pub remote_name: String,
    pub server_name: String,
    pub spec: ToolSpec,
    pub tool: McpTool,
    pub server_max_in_flight: usize,
    pub tool_max_in_flight: usize,
    pub max_result_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum McpStartupError {
    #[error("MCP server {server} 启动失败：{reason}")]
    Server { server: String, reason: String },
    #[error("MCP 工具目录无效：{0}")]
    Catalog(String),
}

#[derive(Debug, thiserror::Error)]
pub enum McpCallError {
    #[error("MCP server {server} 调用超时（未自动重放）")]
    Timeout { server: String },
    #[error("MCP server {server} 会话不可用：{reason}（未自动重放）")]
    Session { server: String, reason: String },
    #[error(
        "MCP server {server} 已完成工具调用，但本地结果处理失败：{reason}（远端调用已经完成，不得重放）"
    )]
    CompletedResult { server: String, reason: String },
}

struct McpServer {
    name: String,
    config: McpServerConfig,
    frozen: BTreeMap<String, FrozenTool>,
    job: Arc<SessionSlot>,
    units: Mutex<HashMap<u64, Arc<SessionSlot>>>,
    retired_sessions: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug, Clone, PartialEq)]
struct FrozenTool {
    description: String,
    input_schema: Value,
}

struct SessionSlot {
    state: Mutex<SessionState>,
}

enum SessionState {
    Empty,
    Active(Arc<ActiveSession>),
    Broken(String),
}

struct ActiveSession {
    peer: Peer<RoleClient>,
    service: Mutex<Option<ClientService>>,
    service_cancel: std::sync::Mutex<Option<rmcp::service::RunningServiceCancellationToken>>,
    stderr_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    http_client: Option<BoundedHttpClient>,
    force_stdio_kill: Option<tokio_util::sync::CancellationToken>,
    broken: std::sync::atomic::AtomicBool,
}

struct CallCancellationGuard {
    server: Arc<McpServer>,
    slot: Arc<SessionSlot>,
    session: Arc<ActiveSession>,
    request_id: Option<RequestId>,
    armed: bool,
}

enum AwaitCallError {
    Closed,
    Timeout,
}

/// rmcp 的标准 stdio transport 会先把整行 JSON 读入内存再解析，且没有公开行长配置。
/// 此 transport 在 JSON 解码前限制单条消息，同时保留原有的进程组或 Job Object 清理语义。
struct BoundedChildProcess {
    server: String,
    child: Option<Box<dyn ChildWrapper>>,
    read: FramedRead<ChildStdout, JsonRpcMessageCodec<RxJsonRpcMessage<RoleClient>>>,
    write: Arc<Mutex<Option<StdioWriter>>>,
    force_kill: tokio_util::sync::CancellationToken,
}

#[derive(Clone)]
struct BoundedHttpClient {
    inner: reqwest_mcp::Client,
    max_message_bytes: usize,
    in_flight: Arc<std::sync::Mutex<HashMap<RequestId, HttpCancelRoute>>>,
    transport_cancel: tokio_util::sync::CancellationToken,
}

#[derive(Clone)]
struct HttpCancelRoute {
    uri: Arc<str>,
    session_id: Option<Arc<str>>,
    auth_header: Option<String>,
    custom_headers: HashMap<HeaderName, HeaderValue>,
}

#[derive(Debug, thiserror::Error)]
enum BoundedSseStreamError {
    #[error(transparent)]
    Source(#[from] reqwest_mcp::Error),
    #[error("SSE event 超过 {max_bytes} 字节传输上限")]
    EventTooLarge { max_bytes: usize },
}

struct BoundedSseByteStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest_mcp::Error>> + Send>>,
    limiter: SseEventSizeLimiter,
    failed: bool,
}

#[derive(Debug)]
struct SseEventSizeLimiter {
    max_bytes: usize,
    retained_bytes: usize,
    line_bytes: usize,
    line_is_comment: bool,
    previous_was_cr: bool,
}

#[derive(Clone)]
struct FormicClientHandler {
    server: String,
}

impl ClientHandler for FormicClientHandler {
    fn on_tool_list_changed(
        &self,
        _context: NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        eprintln!(
            "MCP server {} 报告工具目录变化；当前作业继续使用启动时冻结的目录",
            self.server
        );
        std::future::ready(())
    }

    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("formic", env!("CARGO_PKG_VERSION")),
        )
    }
}

impl BoundedChildProcess {
    fn spawn(
        server: &str,
        mut command: CommandWrap,
        max_message_bytes: usize,
    ) -> io::Result<(Self, Option<ChildStderr>)> {
        command
            .command_mut()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child
            .inner_mut()
            .stdin()
            .take()
            .ok_or_else(|| io::Error::other("MCP stdio stdin 不可用"))?;
        let stdout = child
            .inner_mut()
            .stdout()
            .take()
            .ok_or_else(|| io::Error::other("MCP stdio stdout 不可用"))?;
        let stderr = child.inner_mut().stderr().take();
        let force_kill = tokio_util::sync::CancellationToken::new();
        Ok((
            Self {
                server: server.to_string(),
                child: Some(child),
                read: FramedRead::new(
                    stdout,
                    JsonRpcMessageCodec::new_with_max_length(max_message_bytes),
                ),
                write: Arc::new(Mutex::new(Some(FramedWrite::new(
                    stdin,
                    JsonRpcMessageCodec::new(),
                )))),
                force_kill,
            },
            stderr,
        ))
    }

    async fn close_child(&mut self) -> io::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if self.force_kill.is_cancelled() {
            return Box::into_pin(child.kill()).await;
        }
        tokio::select! {
            result = child.wait() => result.map(|_| ()),
            _ = self.force_kill.cancelled() => Box::into_pin(child.kill()).await,
            _ = tokio::time::sleep(std::time::Duration::from_secs(CHILD_EXIT_TIMEOUT_SEC)) => {
                Box::into_pin(child.kill()).await
            }
        }
    }
}

impl Transport<RoleClient> for BoundedChildProcess {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let write = Arc::clone(&self.write);
        async move {
            let mut write = write.lock().await;
            let Some(write) = write.as_mut() else {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "MCP stdio transport 已关闭",
                ));
            };
            write.send(item).await.map_err(Into::into)
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        let next = tokio::select! {
            _ = self.force_kill.cancelled() => {
                if let Err(error) = self.close_child().await {
                    eprintln!("MCP server {} 的 stdio 子进程终止失败：{error}", self.server);
                }
                return None;
            }
            next = self.read.next() => next,
        };
        match next {
            Some(Ok(message)) => Some(message),
            Some(Err(error)) => {
                eprintln!(
                    "MCP server {} 的 stdio 消息无效或超过传输上限：{error}",
                    self.server
                );
                None
            }
            None => None,
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        if let Some(mut write) = self.write.lock().await.take() {
            write.close().await.map_err(io::Error::from)?;
        }
        self.close_child().await
    }
}

impl Drop for BoundedChildProcess {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        // 发出同步 kill 信号，不能依赖即将关闭的 Tokio runtime 再调度一个任务。
        let _ = child.start_kill();
    }
}

impl SseEventSizeLimiter {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            retained_bytes: 0,
            line_bytes: 0,
            line_is_comment: false,
            previous_was_cr: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for &byte in chunk {
            if self.previous_was_cr {
                self.previous_was_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.finish_line()?;
                    self.previous_was_cr = true;
                }
                b'\n' => self.finish_line()?,
                _ => {
                    if self.line_bytes == 0 {
                        self.line_is_comment = byte == b':';
                    }
                    self.line_bytes = self.line_bytes.saturating_add(1);
                    self.check_limit()?;
                }
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), ()> {
        if self.line_bytes == 0 {
            self.retained_bytes = 0;
        } else if !self.line_is_comment {
            self.retained_bytes = self
                .retained_bytes
                .saturating_add(self.line_bytes)
                .saturating_add(1);
        }
        self.line_bytes = 0;
        self.line_is_comment = false;
        self.check_limit()
    }

    fn check_limit(&self) -> Result<(), ()> {
        (self.retained_bytes.saturating_add(self.line_bytes) <= self.max_bytes)
            .then_some(())
            .ok_or(())
    }
}

impl Stream for BoundedSseByteStream {
    type Item = Result<Bytes, BoundedSseStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failed {
            return Poll::Ready(None);
        }
        match ready!(self.inner.as_mut().poll_next(context)) {
            Some(Ok(chunk)) if self.limiter.observe(&chunk).is_ok() => Poll::Ready(Some(Ok(chunk))),
            Some(Ok(_)) => {
                self.failed = true;
                Poll::Ready(Some(Err(BoundedSseStreamError::EventTooLarge {
                    max_bytes: self.limiter.max_bytes,
                })))
            }
            Some(Err(error)) => {
                self.failed = true;
                Poll::Ready(Some(Err(error.into())))
            }
            None => Poll::Ready(None),
        }
    }
}

impl BoundedHttpClient {
    fn new(max_message_bytes: usize) -> Result<Self, reqwest_mcp::Error> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Ok(Self {
            inner: reqwest_mcp::Client::builder().build()?,
            max_message_bytes,
            in_flight: Arc::new(std::sync::Mutex::new(HashMap::new())),
            transport_cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    async fn send_cancel_request(
        &self,
        request_id: RequestId,
        route: HttpCancelRoute,
        reason: String,
    ) {
        let notification = CancelledNotification::new(CancelledNotificationParam::new(
            Some(request_id),
            Some(reason),
        ));
        let mut request = self
            .inner
            .post(route.uri.as_ref())
            .header(
                reqwest_mcp::header::ACCEPT,
                [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "),
            )
            .timeout(std::time::Duration::from_secs(SESSION_CLOSE_TIMEOUT_SEC));
        if let Some(session_id) = route.session_id {
            request = request.header(MCP_SESSION_HEADER, session_id.as_ref());
        }
        if let Some(auth_header) = route.auth_header {
            request = request.bearer_auth(auth_header);
        }
        let Ok(request) = apply_http_custom_headers(request, route.custom_headers) else {
            return;
        };
        let _ = request
            .json(&ClientJsonRpcMessage::notification(notification.into()))
            .send()
            .await;
    }

    /// 立即中止本地 transport，并在后台向所有仍在途的请求发送协议取消。
    /// 返回的任务只供会话后台清理等待，调用超时路径不得同步等待网络。
    fn start_cancel_in_flight(&self, reason: &str) -> Option<tokio::task::JoinHandle<()>> {
        // 必须先发布取消事实，再取走登记项。这样尚未进入 HTTP send 的请求在取得
        // map 锁后会看到取消，不会在本轮取走之后补登记成永远无法取消的请求。
        self.transport_cancel.cancel();
        let pending = {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *in_flight)
        };
        if pending.is_empty() {
            return None;
        }
        let client = self.clone();
        let reason = reason.to_string();
        Some(tokio::spawn(async move {
            join_all(pending.into_iter().map(|(request_id, route)| {
                client.send_cancel_request(request_id, route, reason.clone())
            }))
            .await;
        }))
    }

    /// 只有收到与请求对应的明确 JSON-RPC 终态后才能调用；HTTP 接受态和传输失败
    /// 都不能删除登记项，因为远端工具仍可能在执行。
    fn complete_in_flight(&self, request_id: &RequestId) {
        self.in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(request_id);
    }

    async fn post_message_bounded(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest_mcp::Error>> {
        let cancellable = match &message {
            ClientJsonRpcMessage::Request(request)
                if matches!(&request.request, ClientRequest::CallToolRequest(_)) =>
            {
                Some((
                    request.id.clone(),
                    HttpCancelRoute {
                        uri: Arc::clone(&uri),
                        session_id: session_id.clone(),
                        auth_header: auth_header.clone(),
                        custom_headers: custom_headers.clone(),
                    },
                ))
            }
            _ => None,
        };
        let mut request = self.inner.post(uri.as_ref()).header(
            reqwest_mcp::header::ACCEPT,
            [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "),
        );
        if let Some(auth_header) = auth_header {
            request = request.bearer_auth(auth_header);
        }
        request = apply_http_custom_headers(request, custom_headers)?;
        let had_session = session_id.is_some();
        if let Some(session_id) = session_id {
            request = request.header(MCP_SESSION_HEADER, session_id.as_ref());
        }
        if let Some((request_id, route)) = cancellable {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.transport_cancel.is_cancelled() {
                return Err(StreamableHttpError::UnexpectedServerResponse(
                    Cow::Borrowed("MCP HTTP session 已取消"),
                ));
            }
            in_flight.insert(request_id, route);
        }
        let response = tokio::select! {
            response = request.json(&message).send() => response?,
            _ = self.transport_cancel.cancelled() => {
                return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                    "MCP HTTP session 已取消",
                )));
            }
        };
        check_http_authentication(&response)?;

        let status = response.status();
        if matches!(
            status,
            reqwest_mcp::StatusCode::ACCEPTED | reqwest_mcp::StatusCode::NO_CONTENT
        ) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == reqwest_mcp::StatusCode::NOT_FOUND && had_session {
            return Err(StreamableHttpError::SessionExpired);
        }

        let content_type = response
            .headers()
            .get(reqwest_mcp::header::CONTENT_TYPE)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).to_string());
        let content_length = response.content_length();
        let response_session = response
            .headers()
            .get(MCP_SESSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        if status.is_success()
            && content_length == Some(0)
            && matches!(
                message,
                ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_)
            )
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }

        if !status.is_success() {
            let body = read_bounded_http_body(response, self.max_message_bytes).await?;
            let body = String::from_utf8_lossy(&body);
            if content_type
                .as_deref()
                .is_some_and(|value| value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()))
                && let Some(message) = parse_json_rpc_error(&body)
            {
                return Ok(StreamableHttpPostResponse::Json(message, response_session));
            }
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!("HTTP {status}: {body}"),
            )));
        }

        match content_type.as_deref() {
            Some(value)
                if value
                    .as_bytes()
                    .starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) =>
            {
                let limit = max_sse_event_size.min(self.max_message_bytes);
                let stream = bounded_sse_stream(response.bytes_stream(), limit);
                Ok(StreamableHttpPostResponse::Sse(stream, response_session))
            }
            Some(value) if value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) => {
                let body = read_bounded_http_body(response, self.max_message_bytes).await?;
                let message = parse_http_json_response(&body)?;
                Ok(StreamableHttpPostResponse::Json(message, response_session))
            }
            _ => Err(StreamableHttpError::UnexpectedContentType(content_type)),
        }
    }
}

impl StreamableHttpClient for BoundedHttpClient {
    type Error = reqwest_mcp::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.post_message_bounded(
            uri,
            message,
            session_id,
            auth_header,
            custom_headers,
            self.max_message_bytes,
        )
        .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.post_message_bounded(
            uri,
            message,
            session_id,
            auth_header,
            custom_headers,
            max_sse_event_size,
        )
        .await
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        <reqwest_mcp::Client as StreamableHttpClient>::delete_session(
            &self.inner,
            uri,
            session_id,
            auth_header,
            custom_headers,
        )
        .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session_id,
            last_event_id,
            auth_header,
            custom_headers,
            self.max_message_bytes,
        )
        .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        <reqwest_mcp::Client as StreamableHttpClient>::get_stream_with_max_sse_event_size(
            &self.inner,
            uri,
            session_id,
            last_event_id,
            auth_header,
            custom_headers,
            max_sse_event_size.min(self.max_message_bytes),
        )
        .await
    }
}

fn apply_http_custom_headers(
    mut request: reqwest_mcp::RequestBuilder,
    headers: HashMap<HeaderName, HeaderValue>,
) -> Result<reqwest_mcp::RequestBuilder, StreamableHttpError<reqwest_mcp::Error>> {
    for (name, value) in headers {
        let name_text = name.as_str();
        let reserved = name_text.eq_ignore_ascii_case(reqwest_mcp::header::ACCEPT.as_str())
            || name_text.eq_ignore_ascii_case(MCP_SESSION_HEADER)
            || name_text.eq_ignore_ascii_case(MCP_LAST_EVENT_ID_HEADER);
        if reserved && !name_text.eq_ignore_ascii_case(MCP_PROTOCOL_VERSION_HEADER) {
            return Err(StreamableHttpError::ReservedHeaderConflict(
                name.to_string(),
            ));
        }
        request = request.header(name, value);
    }
    Ok(request)
}

fn check_http_authentication(
    response: &reqwest_mcp::Response,
) -> Result<(), StreamableHttpError<reqwest_mcp::Error>> {
    let Some(value) = response.headers().get(http::header::WWW_AUTHENTICATE) else {
        return Ok(());
    };
    let header = value.to_str().map_err(|_| {
        StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
            "WWW-Authenticate header 不是有效文本",
        ))
    })?;
    match response.status() {
        reqwest_mcp::StatusCode::UNAUTHORIZED => Err(StreamableHttpError::AuthRequired(
            AuthRequiredError::new(header.to_string()),
        )),
        reqwest_mcp::StatusCode::FORBIDDEN => Err(StreamableHttpError::InsufficientScope(
            InsufficientScopeError::new(header.to_string(), extract_auth_scope(header)),
        )),
        _ => Ok(()),
    }
}

fn extract_auth_scope(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let start = lower.find("scope=")? + "scope=".len();
    let value = &header[start..];
    if let Some(value) = value.strip_prefix('"') {
        return value.split_once('"').map(|(scope, _)| scope.to_string());
    }
    let end = value
        .find(|character: char| character == ',' || character == ';' || character.is_whitespace())
        .unwrap_or(value.len());
    (end > 0).then(|| value[..end].to_string())
}

fn parse_json_rpc_error(body: &str) -> Option<ServerJsonRpcMessage> {
    match serde_json::from_str::<ServerJsonRpcMessage>(body) {
        Ok(message @ JsonRpcMessage::Error(_)) => Some(message),
        _ => None,
    }
}

fn parse_http_json_response(
    body: &[u8],
) -> Result<ServerJsonRpcMessage, StreamableHttpError<reqwest_mcp::Error>> {
    serde_json::from_slice(body).map_err(|error| {
        StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
            "MCP HTTP JSON 响应无效：{error}"
        )))
    })
}

async fn read_bounded_http_body(
    response: reqwest_mcp::Response,
    max_bytes: usize,
) -> Result<Bytes, StreamableHttpError<reqwest_mcp::Error>> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(http_body_too_large(max_bytes));
    }
    let mut body = BytesMut::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(max_bytes),
    );
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(http_body_too_large(max_bytes));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn http_body_too_large(max_bytes: usize) -> StreamableHttpError<reqwest_mcp::Error> {
    StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
        "MCP HTTP 响应超过 {max_bytes} 字节传输上限"
    )))
}

fn bounded_sse_stream(
    stream: impl Stream<Item = Result<Bytes, reqwest_mcp::Error>> + Send + 'static,
    max_bytes: usize,
) -> BoxStream<'static, Result<Sse, SseError>> {
    let stream = BoundedSseByteStream {
        inner: Box::pin(stream),
        limiter: SseEventSizeLimiter::new(max_bytes),
        failed: false,
    };
    SseStream::from_bytes_stream(stream).boxed()
}

impl SessionSlot {
    fn empty() -> Self {
        Self {
            state: Mutex::new(SessionState::Empty),
        }
    }

    fn active(session: Arc<ActiveSession>) -> Self {
        Self {
            state: Mutex::new(SessionState::Active(session)),
        }
    }
}

impl McpManager {
    pub async fn initialize(
        configs: &BTreeMap<String, McpServerConfig>,
    ) -> Result<Self, McpStartupError> {
        let mut servers = BTreeMap::new();
        for (name, config) in configs {
            let (session, frozen) = tokio::time::timeout(config.startup_timeout, async {
                let session = connect(name, config).await?;
                let tools = session
                    .peer
                    .list_all_tools()
                    .await
                    .map_err(|error| error.to_string())?;
                let frozen = select_tools(name, config, tools)?;
                Ok::<_, String>((session, frozen))
            })
            .await
            .map_err(|_| McpStartupError::Server {
                server: name.clone(),
                reason: format!(
                    "初始化和 tools/list 超过 {} 秒",
                    config.startup_timeout.as_secs()
                ),
            })?
            .map_err(|reason| McpStartupError::Server {
                server: name.clone(),
                reason,
            })?;

            let job = if config.session_scope == SessionScope::Job {
                Arc::new(SessionSlot::active(session))
            } else {
                session.close().await;
                Arc::new(SessionSlot::empty())
            };
            servers.insert(
                name.clone(),
                Arc::new(McpServer {
                    name: name.clone(),
                    config: config.clone(),
                    frozen,
                    job,
                    units: Mutex::new(HashMap::new()),
                    retired_sessions: std::sync::Mutex::new(Vec::new()),
                }),
            );
        }
        Ok(Self {
            servers: Arc::new(servers),
        })
    }

    pub fn registrations(&self) -> Result<Vec<McpRegistration>, McpStartupError> {
        let mut registrations = Vec::new();
        for (server_name, server) in self.servers.iter() {
            for (remote_name, frozen) in &server.frozen {
                let visible = server
                    .config
                    .tool_aliases
                    .get(remote_name)
                    .unwrap_or(remote_name);
                let model_name = format!("{server_name}__{visible}");
                validate_model_name(&model_name).map_err(McpStartupError::Catalog)?;
                let limit = server.config.tool_limits.get(remote_name);
                registrations.push(McpRegistration {
                    model_name: model_name.clone(),
                    remote_name: remote_name.clone(),
                    server_name: server_name.clone(),
                    spec: ToolSpec {
                        name: model_name,
                        description: frozen.description.clone(),
                        parameters: frozen.input_schema.clone(),
                    },
                    tool: McpTool {
                        server: Arc::clone(server),
                        remote_name: remote_name.clone(),
                    },
                    server_max_in_flight: server.config.max_in_flight,
                    tool_max_in_flight: limit
                        .map(|limit| limit.max_in_flight)
                        .unwrap_or(server.config.max_in_flight),
                    max_result_bytes: server.config.max_result_bytes,
                });
            }
        }
        registrations.sort_by(|left, right| left.model_name.cmp(&right.model_name));
        for pair in registrations.windows(2) {
            if pair[0].model_name == pair[1].model_name {
                return Err(McpStartupError::Catalog(format!(
                    "模型可见名称 {} 发生碰撞",
                    pair[0].model_name
                )));
            }
        }
        Ok(registrations)
    }

    pub async fn finish_unit(&self, unit: u64) {
        for server in self.servers.values() {
            if let Some(slot) = server.units.lock().await.remove(&unit) {
                close_slot(&slot).await;
            }
        }
    }

    pub async fn shutdown(&self) {
        join_all(self.servers.values().map(|server| async move {
            let mut slots: Vec<Arc<SessionSlot>> = server
                .units
                .lock()
                .await
                .drain()
                .map(|(_, slot)| slot)
                .collect();
            slots.push(Arc::clone(&server.job));
            join_all(slots.iter().map(close_slot)).await;

            let retired: Vec<_> = server
                .retired_sessions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .drain(..)
                .collect();
            join_all(retired).await;
        }))
        .await;
    }
}

impl CallCancellationGuard {
    fn new(
        server: Arc<McpServer>,
        slot: Arc<SessionSlot>,
        session: Arc<ActiveSession>,
    ) -> Option<Self> {
        if session.is_broken() {
            return None;
        }
        Some(Self {
            server,
            slot,
            session,
            request_id: None,
            armed: true,
        })
    }

    fn set_request_id(&mut self, request_id: RequestId) {
        self.request_id = Some(request_id);
    }

    fn disarm(&mut self) {
        if let Some(request_id) = self.request_id.take()
            && let Some(client) = self.session.http_client.as_ref()
        {
            client.complete_in_flight(&request_id);
        }
        self.armed = false;
    }

    fn retire(&mut self, reason: &str) {
        if !self.armed {
            return;
        }
        self.server
            .schedule_retirement(&self.slot, &self.session, reason, self.request_id.take());
        self.armed = false;
    }
}

impl Drop for CallCancellationGuard {
    fn drop(&mut self) {
        self.retire("工具调用被取消");
    }
}

async fn await_call_result(
    deadline: tokio::time::Instant,
    receiver: &mut tokio::sync::oneshot::Receiver<Result<ServerResult, ServiceError>>,
) -> Result<Result<ServerResult, ServiceError>, AwaitCallError> {
    match tokio::time::timeout_at(deadline, receiver).await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(_)) => Err(AwaitCallError::Closed),
        Err(_) => Err(AwaitCallError::Timeout),
    }
}

fn service_error_is_terminal(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::McpError(_) | ServiceError::Cancelled { .. }
    )
}

async fn convert_completed_result(
    server: &str,
    result: CallToolResult,
    max_result_bytes: usize,
) -> Result<ToolOutput, McpCallError> {
    // CallToolResult 已经是远端请求的明确终态。这里的本地、定长处理不得继续使用
    // tool_timeout；否则会把已经发生的副作用重新标成可重跑的远端超时。
    tokio::task::spawn_blocking(move || convert_result(result, max_result_bytes))
        .await
        .map_err(|error| McpCallError::CompletedResult {
            server: server.to_string(),
            reason: error.to_string(),
        })
}

impl McpTool {
    pub async fn call(
        &self,
        unit: u64,
        arguments: Value,
        max_result_bytes: usize,
    ) -> Result<ToolOutput, McpCallError> {
        let deadline = tokio::time::Instant::now() + self.server.config.tool_timeout;
        let Value::Object(arguments) = arguments else {
            return Ok(ToolOutput {
                content: "错误：MCP 工具参数必须是 JSON object".into(),
                cacheable: false,
            });
        };
        let slot = tokio::time::timeout_at(deadline, self.server.slot(unit))
            .await
            .map_err(|_| McpCallError::Timeout {
                server: self.server.name.clone(),
            })?;
        let session = tokio::time::timeout_at(deadline, self.server.ensure_session(&slot))
            .await
            .map_err(|_| McpCallError::Timeout {
                server: self.server.name.clone(),
            })??;
        let mut cancellation_guard = CallCancellationGuard::new(
            Arc::clone(&self.server),
            Arc::clone(&slot),
            Arc::clone(&session),
        )
        .ok_or_else(|| McpCallError::Session {
            server: self.server.name.clone(),
            reason: "会话已取消".to_string(),
        })?;
        let params = CallToolRequestParams::new(self.remote_name.clone()).with_arguments(arguments);
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(params));
        let handle = tokio::time::timeout_at(
            deadline,
            session
                .peer
                .send_cancellable_request(request, PeerRequestOptions::no_options()),
        )
        .await;
        let mut handle = match handle {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => {
                let reason = error.to_string();
                cancellation_guard.retire(&reason);
                return Err(McpCallError::Session {
                    server: self.server.name.clone(),
                    reason,
                });
            }
            Err(_) => {
                cancellation_guard.retire("发送工具调用超时");
                return Err(McpCallError::Timeout {
                    server: self.server.name.clone(),
                });
            }
        };
        cancellation_guard.set_request_id(handle.id.clone());
        match await_call_result(deadline, &mut handle.rx).await {
            Ok(Ok(ServerResult::CallToolResult(result))) => {
                cancellation_guard.disarm();
                convert_completed_result(&self.server.name, result, max_result_bytes).await
            }
            Ok(Ok(_)) => {
                cancellation_guard.disarm();
                Err(McpCallError::Session {
                    server: self.server.name.clone(),
                    reason: ServiceError::UnexpectedResponse.to_string(),
                })
            }
            Err(AwaitCallError::Timeout) => {
                cancellation_guard.retire("工具调用超时");
                Err(McpCallError::Timeout {
                    server: self.server.name.clone(),
                })
            }
            Ok(Err(error)) => {
                let reason = error.to_string();
                if service_error_is_terminal(&error) {
                    cancellation_guard.disarm();
                } else {
                    cancellation_guard.retire(&reason);
                }
                Err(McpCallError::Session {
                    server: self.server.name.clone(),
                    reason,
                })
            }
            Err(AwaitCallError::Closed) => {
                let reason = ServiceError::TransportClosed.to_string();
                cancellation_guard.retire(&reason);
                Err(McpCallError::Session {
                    server: self.server.name.clone(),
                    reason,
                })
            }
        }
    }
}

impl McpServer {
    async fn slot(&self, unit: u64) -> Arc<SessionSlot> {
        match self.config.session_scope {
            SessionScope::Job => Arc::clone(&self.job),
            SessionScope::Unit => {
                let mut units = self.units.lock().await;
                Arc::clone(
                    units
                        .entry(unit)
                        .or_insert_with(|| Arc::new(SessionSlot::empty())),
                )
            }
        }
    }

    async fn ensure_session(
        &self,
        slot: &Arc<SessionSlot>,
    ) -> Result<Arc<ActiveSession>, McpCallError> {
        let mut state = slot.state.lock().await;
        if let SessionState::Active(session) = &*state {
            if !session.is_broken() && !session.peer.is_transport_closed() {
                return Ok(Arc::clone(session));
            }
            let reason = if session.is_broken() {
                "会话已取消"
            } else {
                "传输已关闭"
            };
            *state = SessionState::Broken(reason.into());
        }
        if let SessionState::Broken(reason) = &*state
            && !self.config.reconnect
        {
            return Err(McpCallError::Session {
                server: self.name.clone(),
                reason: reason.clone(),
            });
        }

        let connected = tokio::time::timeout(self.config.startup_timeout, async {
            let session = connect(&self.name, &self.config).await?;
            let tools = session
                .peer
                .list_all_tools()
                .await
                .map_err(|error| error.to_string())?;
            let frozen = select_tools(&self.name, &self.config, tools)?;
            if frozen != self.frozen {
                session.close().await;
                return Err("重新连接后的允许工具 schema 与作业启动时不同".into());
            }
            Ok::<_, String>(session)
        })
        .await
        .map_err(|_| McpCallError::Session {
            server: self.name.clone(),
            reason: format!("建立会话超过 {} 秒", self.config.startup_timeout.as_secs()),
        })?
        .map_err(|reason| McpCallError::Session {
            server: self.name.clone(),
            reason,
        })?;
        *state = SessionState::Active(Arc::clone(&connected));
        Ok(connected)
    }

    /// 同步标记会话不可复用并中止 transport；协议取消与资源关闭在登记过的后台任务中完成。
    fn schedule_retirement(
        &self,
        slot: &Arc<SessionSlot>,
        session: &Arc<ActiveSession>,
        reason: &str,
        request_id: Option<RequestId>,
    ) {
        let Some(notifications) = session.begin_retirement(reason, request_id) else {
            return;
        };
        let slot = Arc::clone(slot);
        let session = Arc::clone(session);
        let reason = reason.to_string();
        let task = tokio::spawn(async move {
            {
                let mut state = slot.state.lock().await;
                if matches!(&*state, SessionState::Active(current) if Arc::ptr_eq(current, &session))
                {
                    *state = SessionState::Broken(reason);
                }
            }
            if !notifications.is_empty() {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(CANCELLATION_DISPATCH_GRACE_MS),
                    join_all(notifications),
                )
                .await;
            }
            session.close().await;
        });
        let mut tasks = self
            .retired_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }
}

impl ActiveSession {
    fn is_broken(&self) -> bool {
        self.broken.load(std::sync::atomic::Ordering::Acquire)
    }

    fn begin_retirement(
        &self,
        reason: &str,
        request_id: Option<RequestId>,
    ) -> Option<Vec<tokio::task::JoinHandle<()>>> {
        if self.broken.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return None;
        }
        let mut notifications = Vec::new();
        if let Some(client) = self.http_client.as_ref() {
            if let Some(task) = client.start_cancel_in_flight(reason) {
                notifications.push(task);
            }
        } else if let Some(request_id) = request_id {
            let peer = self.peer.clone();
            let reason = reason.to_string();
            notifications.push(tokio::spawn(async move {
                let notification = CancelledNotification::new(CancelledNotificationParam::new(
                    Some(request_id),
                    Some(reason),
                ));
                let _ = peer.send_notification(notification.into()).await;
            }));
        }
        self.abort_timed_out_transport();
        Some(notifications)
    }

    fn abort_timed_out_transport(&self) {
        if let Some(force_kill) = &self.force_stdio_kill {
            force_kill.cancel();
        } else {
            // HTTP transport 没有可直接关闭的子进程，通过 rmcp service token 停止本地会话。
            // 慢 initialize 的底层 TCP 回收限制见 design.md §6。
            if let Some(service_cancel) = self
                .service_cancel
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                service_cancel.cancel();
            }
        }
    }

    async fn close(&self) {
        if let Some(mut service) = self.service.lock().await.take() {
            let _ = service
                .close_with_timeout(std::time::Duration::from_secs(SESSION_CLOSE_TIMEOUT_SEC))
                .await;
        }
        if let Some(task) = self.stderr_task.lock().await.take() {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(SESSION_CLOSE_TIMEOUT_SEC),
                task,
            )
            .await;
        }
    }
}

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.broken
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(client) = self.http_client.as_ref() {
            client.transport_cancel.cancel();
        }
        self.abort_timed_out_transport();
    }
}

async fn close_slot(slot: &Arc<SessionSlot>) {
    let session = {
        let mut state = slot.state.lock().await;
        match std::mem::replace(&mut *state, SessionState::Empty) {
            SessionState::Active(session) => Some(session),
            SessionState::Empty | SessionState::Broken(_) => None,
        }
    };
    if let Some(session) = session {
        session.close().await;
    }
}

fn mcp_transport_message_limit(config: &McpServerConfig) -> usize {
    mcp_result_message_limit(config.max_result_bytes)
}

fn mcp_result_message_limit(max_result_bytes: usize) -> usize {
    max_result_bytes
        .saturating_mul(MCP_JSON_ESCAPE_EXPANSION)
        .saturating_add(MCP_PROTOCOL_OVERHEAD_BYTES)
}

fn stdio_system_environment(
    parent: impl IntoIterator<Item = (OsString, OsString)>,
) -> Vec<(OsString, OsString)> {
    parent
        .into_iter()
        .filter(|(name, _)| is_stdio_system_variable(name))
        .collect()
}

fn configure_stdio_process(
    process: &mut tokio::process::Command,
    args: &[String],
    env: &BTreeMap<String, String>,
    parent: impl IntoIterator<Item = (OsString, OsString)>,
) {
    process.args(args);
    process.env_clear();
    process.envs(stdio_system_environment(parent));
    process.envs(env);
}

#[cfg(windows)]
fn is_stdio_system_variable(name: &OsStr) -> bool {
    const NAMES: &[&str] = &[
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "SYSTEMDRIVE",
        "TEMP",
        "TMP",
    ];
    name.to_str().is_some_and(|name| {
        NAMES
            .iter()
            .any(|allowed| name.eq_ignore_ascii_case(allowed))
    })
}

#[cfg(unix)]
fn is_stdio_system_variable(name: &OsStr) -> bool {
    const NAMES: &[&str] = &["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE"];
    name.to_str().is_some_and(|name| NAMES.contains(&name))
}

async fn connect(name: &str, config: &McpServerConfig) -> Result<Arc<ActiveSession>, String> {
    let handler = FormicClientHandler {
        server: name.to_string(),
    };
    let (service, stderr_task, http_client, force_stdio_kill) = match &config.transport {
        McpTransportConfig::Stdio { command, args, env } => {
            let mut command = CommandWrap::with_new(command, |process| {
                configure_stdio_process(process, args, env, std::env::vars_os());
            });
            #[cfg(windows)]
            command.wrap(JobObject);
            #[cfg(unix)]
            command.wrap(ProcessGroup::leader());
            let (transport, stderr) =
                BoundedChildProcess::spawn(name, command, mcp_transport_message_limit(config))
                    .map_err(|error| format!("无法启动 stdio 子进程：{error}"))?;
            let force_kill = transport.force_kill.clone();
            let attempt_guard = force_kill.clone().drop_guard();
            let task = stderr.map(|stderr| {
                let server = name.to_string();
                tokio::spawn(async move {
                    let mut lines = bounded_stderr_lines(stderr);
                    while let Some(line) = lines.next().await {
                        match line {
                            Ok(line) => eprintln!("MCP server {server} stderr：{line}"),
                            Err(error) => {
                                eprintln!(
                                    "MCP server {server} stderr 读取失败（单行上限 {MCP_STDERR_LINE_LIMIT} 字节）：{error}"
                                );
                                break;
                            }
                        }
                    }
                })
            });
            let service = handler
                .serve(transport)
                .await
                .map_err(|error| format!("initialize 失败：{error}"))?;
            let _ = attempt_guard.disarm();
            (service, task, None, Some(force_kill))
        }
        McpTransportConfig::Http {
            url,
            bearer_token,
            headers,
        } => {
            let mut custom_headers = HashMap::new();
            for (header, value) in headers {
                custom_headers.insert(
                    header
                        .parse::<HeaderName>()
                        .map_err(|error| format!("HTTP header 名无效：{error}"))?,
                    HeaderValue::from_str(value)
                        .map_err(|error| format!("HTTP header 值无效：{error}"))?,
                );
            }
            let max_message_bytes = mcp_transport_message_limit(config);
            let mut transport_config = StreamableHttpClientTransportConfig::with_uri(url.clone())
                .custom_headers(custom_headers)
                .max_sse_event_size(max_message_bytes)
                .reinit_on_expired_session(false);
            if let Some(token) = bearer_token {
                transport_config = transport_config.auth_header(token.clone());
            }
            let client = BoundedHttpClient::new(max_message_bytes)
                .map_err(|error| format!("无法创建 HTTP client：{error}"))?;
            // 取消本地 initialize future 不保证 Hyper 立即关闭已经写完请求的 TCP
            // 连接；调用方期限与这条连接的最终回收语义见 design.md §6。
            let attempt_guard = client.transport_cancel.clone().drop_guard();
            let transport =
                StreamableHttpClientTransport::with_client(client.clone(), transport_config);
            let service = handler
                .serve(transport)
                .await
                .map_err(|error| format!("initialize 失败：{error}"))?;
            let _ = attempt_guard.disarm();
            (service, None, Some(client), None)
        }
    };
    let peer = service.peer().clone();
    let service_cancel = service.cancellation_token();
    Ok(Arc::new(ActiveSession {
        peer,
        service: Mutex::new(Some(service)),
        service_cancel: std::sync::Mutex::new(Some(service_cancel)),
        stderr_task: Mutex::new(stderr_task),
        http_client,
        force_stdio_kill,
        broken: std::sync::atomic::AtomicBool::new(false),
    }))
}

fn select_tools(
    server: &str,
    config: &McpServerConfig,
    tools: Vec<Tool>,
) -> Result<BTreeMap<String, FrozenTool>, String> {
    let mut discovered = BTreeMap::new();
    for tool in tools {
        let remote_name = tool.name.to_string();
        if discovered.insert(remote_name.clone(), tool).is_some() {
            return Err(format!(
                "server {server} 的 tools/list 重复返回工具 {remote_name:?}"
            ));
        }
    }
    for name in config.tool_aliases.keys().chain(config.tool_limits.keys()) {
        if !discovered.contains_key(name) {
            return Err(format!(
                "配置引用的工具 {name:?} 不在 server {server} 的 tools/list 结果中"
            ));
        }
    }
    let selected_names: Vec<String> = match &config.enabled_tools {
        Some(names) => names.clone(),
        None => discovered.keys().cloned().collect(),
    };
    let mut selected = BTreeMap::new();
    for name in &selected_names {
        let tool = discovered.get(name).ok_or_else(|| {
            format!("enabled_tools 中的 {name:?} 不在 server {server} 的 tools/list 结果中")
        })?;
        selected.insert(
            name.clone(),
            FrozenTool {
                description: tool
                    .description
                    .as_deref()
                    .or(tool.title.as_deref())
                    .unwrap_or("外部 MCP 工具")
                    .to_string(),
                input_schema: Value::Object((*tool.input_schema).clone()),
            },
        );
    }
    Ok(selected)
}

fn validate_model_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > MODEL_TOOL_NAME_LIMIT
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(format!(
            "MCP 模型可见工具名 {name:?} 必须为 1-{MODEL_TOOL_NAME_LIMIT} 个 ASCII 字母、数字、_ 或 -；请调整 server 名或 tool_aliases"
        ));
    }
    Ok(())
}

fn convert_result(result: CallToolResult, max_bytes: usize) -> ToolOutput {
    let mut text = String::new();
    let text_buffer_limit = max_bytes.saturating_add(4);
    for block in result.content {
        match block {
            ContentBlock::Text(content) => {
                push_utf8_prefix(&mut text, &content.text, text_buffer_limit)
            }
            ContentBlock::Image(_) => return unsupported_result("image", max_bytes),
            ContentBlock::Audio(_) => return unsupported_result("audio", max_bytes),
            ContentBlock::Resource(_) => return unsupported_result("resource", max_bytes),
            ContentBlock::ResourceLink(_) => {
                return unsupported_result("resource_link", max_bytes);
            }
            _ => return unsupported_result("unknown", max_bytes),
        }
    }
    let structured = result.structured_content;
    let (content, _truncated) = match structured {
        None => truncate_text(text, max_bytes),
        Some(structured) if text.is_empty() => {
            let serialized_len = serialized_value_len(&structured);
            if serialized_len > max_bytes {
                return external_error(
                    format!(
                        "MCP structuredContent 为 {} 字节，超过 {max_bytes} 字节上限",
                        serialized_len
                    ),
                    max_bytes,
                );
            }
            let serialized = serde_json::to_string(&structured).expect("Value 可序列化");
            (serialized, false)
        }
        Some(structured) => match wrap_text_and_structured(&text, structured, max_bytes) {
            Some(value) => value,
            None => {
                return external_error(
                    "MCP structuredContent 连同固定包装本身已超过结果上限".to_string(),
                    max_bytes,
                );
            }
        },
    };
    if result.is_error == Some(true) {
        return external_error(format!("MCP 工具报告失败：{content}"), max_bytes);
    }
    ToolOutput {
        content,
        cacheable: false,
    }
}

fn unsupported_result(kind: &str, max_bytes: usize) -> ToolOutput {
    external_error(format!("MCP 返回不支持的结果类型 {kind}"), max_bytes)
}

fn external_error(message: String, max_bytes: usize) -> ToolOutput {
    const PREFIX: &str = "错误：";
    const COMPACT: &str = "[cut]";
    let full = format!("{PREFIX}{message}");
    let content = if full.len() <= max_bytes {
        full
    } else if PREFIX.len().saturating_add(COMPACT.len()) <= max_bytes {
        format!("{PREFIX}{COMPACT}")
    } else {
        let prefix_len = utf8_prefix_len(PREFIX, max_bytes);
        let mut compact = PREFIX[..prefix_len].to_string();
        let remaining = max_bytes.saturating_sub(compact.len());
        compact.push_str(&COMPACT[..remaining.min(COMPACT.len())]);
        compact
    };
    ToolOutput {
        content,
        cacheable: false,
    }
}

fn truncate_text(text: String, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text, false);
    }
    let marker = format!("\n[已截断：MCP 文本结果达到 {max_bytes} 字节上限]");
    if marker.len() >= max_bytes {
        let compact = "[cut]";
        return (compact[..max_bytes.min(compact.len())].to_string(), true);
    }
    let keep = utf8_prefix_len(&text, max_bytes - marker.len());
    (format!("{}{marker}", &text[..keep]), true)
}

fn wrap_text_and_structured(
    text: &str,
    structured: Value,
    max_bytes: usize,
) -> Option<(String, bool)> {
    const PREFIX: &str = "{\"structuredContent\":";
    const BETWEEN: &str = ",\"text\":";
    const SUFFIX: &str = "}";

    let structured_len = serialized_value_len(&structured);
    let minimum = PREFIX
        .len()
        .saturating_add(structured_len)
        .saturating_add(BETWEEN.len())
        .saturating_add(2)
        .saturating_add(SUFFIX.len());
    if minimum > max_bytes {
        return None;
    }
    let structured = serde_json::to_string(&structured).expect("Value 可序列化");
    let render = |text: &str| {
        let text = serde_json::to_string(text).expect("str 可序列化");
        format!("{PREFIX}{structured}{BETWEEN}{text}{SUFFIX}")
    };
    let full = render(text);
    if full.len() <= max_bytes {
        return Some((full, false));
    }
    let marker = format!("\n[已截断：MCP 文本结果达到 {max_bytes} 字节上限]");
    let mut low = 0usize;
    let mut high = text.len();
    let mut best = None;
    while low <= high {
        let middle = utf8_prefix_len(text, low + (high - low) / 2);
        let candidate_text = format!("{}{marker}", &text[..middle]);
        let candidate = render(&candidate_text);
        if candidate.len() <= max_bytes {
            best = Some(candidate);
            if middle == text.len() {
                break;
            }
            low = next_utf8_boundary(text, middle)?;
        } else {
            high = previous_utf8_boundary(text, middle)?;
        }
    }
    best.map(|value| (value, true))
}

fn push_utf8_prefix(target: &mut String, value: &str, max_bytes: usize) {
    let remaining = max_bytes.saturating_sub(target.len());
    if remaining == 0 {
        return;
    }
    let keep = utf8_prefix_len(value, remaining);
    target.push_str(&value[..keep]);
}

fn serialized_value_len(value: &Value) -> usize {
    struct Counter(usize);

    impl io::Write for Counter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(buffer.len());
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).expect("Value 可序列化");
    counter.0
}

fn next_utf8_boundary(text: &str, current: usize) -> Option<usize> {
    text.get(current..)?
        .chars()
        .next()
        .map(|character| current + character.len_utf8())
}

fn previous_utf8_boundary(text: &str, current: usize) -> Option<usize> {
    let mut previous = current.checked_sub(1)?;
    while !text.is_char_boundary(previous) {
        previous = previous.checked_sub(1)?;
    }
    Some(previous)
}

fn utf8_prefix_len(text: &str, max_bytes: usize) -> usize {
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Default)]
    enum MockCallResponse {
        #[default]
        SlowThenSuccess,
        AcceptedPending,
        InvalidJsonAfterDispatch,
    }

    #[derive(Default)]
    struct ReconnectMockState {
        sessions: usize,
        calls: Vec<String>,
        cancellations: usize,
        side_effects: usize,
        cancellation_tokens: HashMap<String, tokio_util::sync::CancellationToken>,
        request_tokens: HashMap<String, tokio_util::sync::CancellationToken>,
        call_response: MockCallResponse,
    }

    struct MockHttpRequest {
        method: String,
        headers: HashMap<String, String>,
        body: Vec<u8>,
    }

    async fn read_mock_http_request(
        socket: &mut tokio::net::TcpStream,
    ) -> io::Result<MockHttpRequest> {
        use tokio::io::AsyncReadExt;

        let mut received = Vec::new();
        let header_end = loop {
            if let Some(position) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break position + 4;
            }
            let mut buffer = [0u8; 4096];
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "HTTP request headers 不完整",
                ));
            }
            received.extend_from_slice(&buffer[..count]);
        };
        let headers_text = std::str::from_utf8(&received[..header_end])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut lines = headers_text.split("\r\n");
        let method = lines
            .next()
            .and_then(|line| line.split_whitespace().next())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP method 缺失"))?
            .to_string();
        let headers: HashMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while received.len().saturating_sub(header_end) < content_length {
            let mut buffer = [0u8; 4096];
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "HTTP request body 不完整",
                ));
            }
            received.extend_from_slice(&buffer[..count]);
        }
        Ok(MockHttpRequest {
            method,
            headers,
            body: received[header_end..header_end + content_length].to_vec(),
        })
    }

    async fn write_mock_http_response(
        socket: &mut tokio::net::TcpStream,
        status: &str,
        session: Option<&str>,
        body: &str,
    ) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let session_header = session
            .map(|session| format!("Mcp-Session-Id: {session}\r\n"))
            .unwrap_or_default();
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{session_header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await
    }

    async fn handle_reconnect_mock_request(
        mut socket: tokio::net::TcpStream,
        state: Arc<std::sync::Mutex<ReconnectMockState>>,
    ) -> io::Result<()> {
        let request = read_mock_http_request(&mut socket).await?;
        if request.method == "GET" {
            return write_mock_http_response(&mut socket, "405 Method Not Allowed", None, "").await;
        }
        let request_session = request.headers.get(MCP_SESSION_HEADER).cloned();
        if request.method == "DELETE" {
            if let Some(token) = request_session.as_ref().and_then(|session| {
                state
                    .lock()
                    .unwrap()
                    .cancellation_tokens
                    .get(session)
                    .cloned()
            }) {
                token.cancel();
            }
            return write_mock_http_response(&mut socket, "204 No Content", None, "").await;
        }

        let body: Value = serde_json::from_slice(&request.body)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        if method == "initialize" {
            let (session_number, session, token) = {
                let mut state = state.lock().unwrap();
                state.sessions += 1;
                let session_number = state.sessions;
                let session = format!("session-{session_number}");
                let token = tokio_util::sync::CancellationToken::new();
                state
                    .cancellation_tokens
                    .insert(session.clone(), token.clone());
                (session_number, session, token)
            };
            drop(token);
            debug_assert_eq!(session, format!("session-{session_number}"));
            let id = body.get("id").cloned().unwrap_or(Value::Null);
            let version = body
                .pointer("/params/protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("2025-06-18");
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "reconnect-test", "version": "1"}
                }
            })
            .to_string();
            return write_mock_http_response(&mut socket, "200 OK", Some(&session), &response)
                .await;
        }
        if method == "notifications/initialized" {
            return write_mock_http_response(
                &mut socket,
                "202 Accepted",
                request_session.as_deref(),
                "",
            )
            .await;
        }
        if method == "notifications/cancelled" {
            let request_id = body
                .pointer("/params/requestId")
                .map(Value::to_string)
                .unwrap_or_default();
            if let Some(token) = {
                let mut state = state.lock().unwrap();
                state.cancellations += 1;
                state.request_tokens.get(&request_id).cloned().or_else(|| {
                    request_session
                        .as_ref()
                        .and_then(|session| state.cancellation_tokens.get(session).cloned())
                })
            } {
                token.cancel();
            }
            // 故意延迟 HTTP 响应：工具超时的返回期限不能被取消通知的网络耗时拉长。
            tokio::time::sleep(std::time::Duration::from_millis(750)).await;
            return write_mock_http_response(
                &mut socket,
                "202 Accepted",
                request_session.as_deref(),
                "",
            )
            .await;
        }

        let id = body.get("id").cloned().unwrap_or(Value::Null);
        if method == "tools/list" {
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "tools": [{
                        "name": "slow",
                        "description": "测试取消",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": false
                        }
                    }]
                }
            })
            .to_string();
            return write_mock_http_response(
                &mut socket,
                "200 OK",
                request_session.as_deref(),
                &response,
            )
            .await;
        }
        if method == "tools/call" {
            let session = request_session.unwrap_or_default();
            let (token, call_response) = {
                let mut state = state.lock().unwrap();
                state.calls.push(session.clone());
                let token = state.cancellation_tokens[&session].clone();
                state.request_tokens.insert(id.to_string(), token.clone());
                (token, state.call_response)
            };
            match call_response {
                MockCallResponse::SlowThenSuccess => {
                    if session == "session-1" {
                        tokio::select! {
                            _ = token.cancelled() => {}
                            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                                state.lock().unwrap().side_effects += 1;
                            }
                        }
                    } else {
                        state.lock().unwrap().side_effects += 1;
                    }
                }
                MockCallResponse::AcceptedPending | MockCallResponse::InvalidJsonAfterDispatch => {
                    let side_effect_state = Arc::clone(&state);
                    let side_effect_token = token.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = side_effect_token.cancelled() => {}
                            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                                side_effect_state.lock().unwrap().side_effects += 1;
                            }
                        }
                    });
                    return match call_response {
                        MockCallResponse::AcceptedPending => {
                            write_mock_http_response(
                                &mut socket,
                                "202 Accepted",
                                Some(&session),
                                "",
                            )
                            .await
                        }
                        MockCallResponse::InvalidJsonAfterDispatch => {
                            write_mock_http_response(
                                &mut socket,
                                "200 OK",
                                Some(&session),
                                "{not-json}",
                            )
                            .await
                        }
                        MockCallResponse::SlowThenSuccess => unreachable!(),
                    };
                }
            }
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{"type": "text", "text": "ok"}],
                    "isError": false
                }
            })
            .to_string();
            return write_mock_http_response(&mut socket, "200 OK", Some(&session), &response)
                .await;
        }
        write_mock_http_response(
            &mut socket,
            "400 Bad Request",
            request_session.as_deref(),
            "",
        )
        .await
    }

    async fn start_reconnect_mcp_mock() -> (
        String,
        Arc<std::sync::Mutex<ReconnectMockState>>,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        start_reconnect_mcp_mock_with_response(MockCallResponse::SlowThenSuccess).await
    }

    async fn start_reconnect_mcp_mock_with_response(
        call_response: MockCallResponse,
    ) -> (
        String,
        Arc<std::sync::Mutex<ReconnectMockState>>,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(std::sync::Mutex::new(ReconnectMockState {
            call_response,
            ..ReconnectMockState::default()
        }));
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task_state = Arc::clone(&state);
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let state = Arc::clone(&task_state);
                        tokio::spawn(async move {
                            let _ = handle_reconnect_mock_request(socket, state).await;
                        });
                    }
                }
            }
        });
        (format!("http://{address}/mcp"), state, shutdown, task)
    }

    fn reconnect_mock_config(url: String, tool_timeout: std::time::Duration) -> McpServerConfig {
        McpServerConfig {
            enabled_tools: Some(vec!["slow".to_string()]),
            tool_aliases: BTreeMap::new(),
            session_scope: SessionScope::Job,
            max_in_flight: 1,
            startup_timeout: std::time::Duration::from_secs(2),
            tool_timeout,
            max_result_bytes: 1024,
            reconnect: true,
            tool_limits: BTreeMap::new(),
            transport: McpTransportConfig::Http {
                url,
                bearer_token: None,
                headers: BTreeMap::new(),
            },
        }
    }

    async fn wait_for_mock_cancellation(state: &Arc<std::sync::Mutex<ReconnectMockState>>) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if state.lock().unwrap().cancellations > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("已发送但未终结的 HTTP 工具请求必须发出 cancelled 通知");
    }

    #[test]
    fn model_names_require_explicit_readable_aliases() {
        assert!(validate_model_name("web__search").is_ok());
        assert!(validate_model_name("网页__搜索").is_err());
        assert!(validate_model_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn text_and_structured_results_have_fixed_valid_json_wrapper() {
        let mut result = CallToolResult::structured(serde_json::json!({"b":2,"a":1}));
        result.content = vec![ContentBlock::text("hello")];
        let output = convert_result(result, 1024);
        let value: Value = serde_json::from_str(&output.content).unwrap();
        assert_eq!(value["text"], "hello");
        assert_eq!(value["structuredContent"]["a"], 1);
        assert!(!output.cacheable);
    }

    #[test]
    fn text_truncation_keeps_utf8_and_marker() {
        let result = CallToolResult::success(vec![ContentBlock::text("苹果".repeat(100))]);
        let output = convert_result(result, 80);
        assert!(output.content.contains("已截断"));
        assert!(output.content.is_char_boundary(output.content.len()));
        assert!(output.content.len() <= 80);
    }

    #[tokio::test]
    async fn received_result_wins_when_receiver_and_deadline_are_both_ready() {
        let result = CallToolResult::success(vec![ContentBlock::text("already completed")]);
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        sender
            .send(Ok(ServerResult::CallToolResult(result)))
            .expect("测试结果必须进入 receiver");
        let expired = tokio::time::Instant::now() - std::time::Duration::from_millis(1);

        let Ok(Ok(ServerResult::CallToolResult(result))) =
            await_call_result(expired, &mut receiver).await
        else {
            panic!("已经完整收到的 CallToolResult 必须胜过同时就绪的 deadline");
        };
        let output = convert_completed_result("test", result, 1024)
            .await
            .expect("远端结果完成后的本地转换不再受远端 deadline 限制");
        assert_eq!(output.content, "already completed");
    }

    #[test]
    fn structured_wrapper_binary_search_progresses_on_multibyte_text() {
        let (output, truncated) =
            wrap_text_and_structured(&"苹".repeat(19), serde_json::json!({"data": ""}), 97)
                .expect("固定包装和截断标记应能放入上限");
        assert!(truncated);
        assert!(output.len() <= 97);
        assert!(output.contains("已截断"));
        serde_json::from_str::<Value>(&output).expect("截断后的包装必须仍是有效 JSON");
    }

    #[tokio::test]
    async fn stdio_environment_does_not_inherit_unapproved_secrets() {
        #[cfg(windows)]
        let (program, args, unapproved) = (
            std::env::var_os("COMSPEC").unwrap_or_else(|| OsString::from("cmd.exe")),
            vec![
                "/D".to_string(),
                "/C".to_string(),
                "if defined USERPROFILE exit /b 7 & if not \"%FORMIC_EXPLICIT_ENV%\"==\"visible\" exit /b 8 & exit /b 0".to_string(),
            ],
            "USERPROFILE",
        );
        #[cfg(unix)]
        let (program, args, unapproved) = (
            OsString::from("/bin/sh"),
            vec![
                "-c".to_string(),
                "test -z \"${USER+x}\" && test \"$FORMIC_EXPLICIT_ENV\" = visible".to_string(),
            ],
            "USER",
        );
        assert!(
            std::env::var_os(unapproved).is_some(),
            "测试父进程必须实际包含未授权变量 {unapproved}"
        );

        let mut process = tokio::process::Command::new(program);
        configure_stdio_process(
            &mut process,
            &args,
            &BTreeMap::from([("FORMIC_EXPLICIT_ENV".to_string(), "visible".to_string())]),
            std::env::vars_os(),
        );
        let output = process.output().await.unwrap();
        assert!(
            output.status.success(),
            "stdio 子进程看到了未授权父环境变量，或没有收到显式 env：{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn stdio_stderr_rejects_an_unbounded_line() {
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::io::duplex(MCP_STDERR_LINE_LIMIT + 2);
        let write = tokio::spawn(async move {
            writer
                .write_all(&vec![b'x'; MCP_STDERR_LINE_LIMIT + 1])
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
        });
        let mut lines = bounded_stderr_lines(reader);
        assert!(matches!(lines.next().await, Some(Err(_))));
        write.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_http_json_response_is_a_protocol_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mock_http_request(&mut socket).await.unwrap();
            write_mock_http_response(&mut socket, "200 OK", None, "{not-json}")
                .await
                .unwrap();
        });
        let params = CallToolRequestParams::new("test");
        let message = ClientJsonRpcMessage::request(
            ClientRequest::CallToolRequest(CallToolRequest::new(params)),
            RequestId::Number(1),
        );
        let error = BoundedHttpClient::new(1024)
            .unwrap()
            .post_message(
                Arc::from(format!("http://{address}/mcp")),
                message,
                None,
                None,
                HashMap::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            StreamableHttpError::UnexpectedServerResponse(_)
        ));
        assert!(error.to_string().contains("JSON 响应无效"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sessionless_http_request_still_sends_cancelled_notification() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut pending_socket, _) = listener.accept().await.unwrap();
            let pending = read_mock_http_request(&mut pending_socket).await.unwrap();
            assert!(!pending.headers.contains_key(MCP_SESSION_HEADER));
            request_seen_tx.send(()).unwrap();

            let (mut cancel_socket, _) = listener.accept().await.unwrap();
            let cancellation = read_mock_http_request(&mut cancel_socket).await.unwrap();
            assert!(!cancellation.headers.contains_key(MCP_SESSION_HEADER));
            let body: Value = serde_json::from_slice(&cancellation.body).unwrap();
            assert_eq!(body["method"], "notifications/cancelled");
            assert_eq!(body["params"]["requestId"], 77);
            write_mock_http_response(&mut cancel_socket, "202 Accepted", None, "")
                .await
                .unwrap();
            drop(pending_socket);
        });

        let client = BoundedHttpClient::new(1024).unwrap();
        let request_client = client.clone();
        let uri: Arc<str> = Arc::from(format!("http://{address}/mcp"));
        let request = ClientJsonRpcMessage::request(
            ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(
                "slow",
            ))),
            RequestId::Number(77),
        );
        let pending = tokio::spawn(async move {
            request_client
                .post_message(uri, request, None, None, HashMap::new())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), request_seen_rx)
            .await
            .expect("server 应收到无 session 的原请求")
            .unwrap();

        let cancellation = client
            .start_cancel_in_flight("测试取消")
            .expect("无 session 请求也必须登记为在途请求");
        tokio::time::timeout(std::time::Duration::from_secs(1), cancellation)
            .await
            .expect("取消通知不应阻塞")
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), pending)
                .await
                .expect("本地 transport 取消后原请求必须结束")
                .unwrap()
                .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_content_length_is_rejected_before_body_is_read() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        });

        let response = BoundedHttpClient::new(32)
            .unwrap()
            .inner
            .get(format!("http://{address}/mcp"))
            .send()
            .await
            .unwrap();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_bounded_http_body(response, 32),
        )
        .await
        .expect("Content-Length 超限必须在等待正文前返回")
        .unwrap_err();
        assert!(error.to_string().contains("超过 32 字节传输上限"));
        server.abort();
    }

    #[tokio::test]
    async fn tool_timeout_includes_waiting_for_the_session_slot() {
        let (url, _state, shutdown, server_task) = start_reconnect_mcp_mock().await;
        let config = McpServerConfig {
            enabled_tools: Some(vec!["slow".to_string()]),
            tool_aliases: BTreeMap::new(),
            session_scope: SessionScope::Job,
            max_in_flight: 1,
            startup_timeout: std::time::Duration::from_secs(2),
            tool_timeout: std::time::Duration::from_millis(100),
            max_result_bytes: 1024,
            reconnect: true,
            tool_limits: BTreeMap::new(),
            transport: McpTransportConfig::Http {
                url,
                bearer_token: None,
                headers: BTreeMap::new(),
            },
        };
        let manager = McpManager::initialize(&BTreeMap::from([("test".to_string(), config)]))
            .await
            .unwrap();
        let tool = manager
            .registrations()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .tool;
        let state_guard = manager.servers["test"].job.state.lock().await;

        let started = std::time::Instant::now();
        assert!(matches!(
            tool.call(1, serde_json::json!({}), 1024).await,
            Err(McpCallError::Timeout { .. })
        ));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "等待会话锁也必须计入 tool_timeout，实际耗时 {:?}",
            started.elapsed()
        );

        drop(state_guard);
        manager.shutdown().await;
        shutdown.cancel();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn accepted_http_tool_request_remains_cancellable_until_terminal_response() {
        let (url, state, shutdown, server) =
            start_reconnect_mcp_mock_with_response(MockCallResponse::AcceptedPending).await;
        let config = reconnect_mock_config(url, std::time::Duration::from_millis(100));
        let manager = McpManager::initialize(&BTreeMap::from([("test".to_string(), config)]))
            .await
            .unwrap();
        let tool = manager
            .registrations()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .tool;

        assert!(matches!(
            tool.call(1, serde_json::json!({}), 1024).await,
            Err(McpCallError::Timeout { .. })
        ));
        wait_for_mock_cancellation(&state).await;
        manager.shutdown().await;

        {
            let state = state.lock().unwrap();
            assert_eq!(state.calls.len(), 1);
            assert_eq!(state.cancellations, 1, "202 只表示接受，不能清除取消路由");
            assert_eq!(state.side_effects, 0, "202 后仍在执行的工具必须被取消");
        }
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_http_response_retires_and_cancels_dispatched_tool_request() {
        let (url, state, shutdown, server) =
            start_reconnect_mcp_mock_with_response(MockCallResponse::InvalidJsonAfterDispatch)
                .await;
        let config = reconnect_mock_config(url, std::time::Duration::from_secs(2));
        let manager = McpManager::initialize(&BTreeMap::from([("test".to_string(), config)]))
            .await
            .unwrap();
        let tool = manager
            .registrations()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .tool;

        assert!(matches!(
            tool.call(1, serde_json::json!({}), 1024).await,
            Err(McpCallError::Session { .. })
        ));
        wait_for_mock_cancellation(&state).await;
        manager.shutdown().await;

        {
            let state = state.lock().unwrap();
            assert_eq!(state.calls.len(), 1);
            assert_eq!(
                state.cancellations, 1,
                "请求发出后的响应解析失败仍须保留取消路由"
            );
            assert_eq!(
                state.side_effects, 0,
                "响应异常不能让未知状态的工具继续执行"
            );
        }
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timed_out_call_is_cancelled_and_next_call_reconnects() {
        let (url, state, shutdown, server) = start_reconnect_mcp_mock().await;
        let config = McpServerConfig {
            enabled_tools: Some(vec!["slow".to_string()]),
            tool_aliases: BTreeMap::new(),
            session_scope: SessionScope::Job,
            max_in_flight: 1,
            startup_timeout: std::time::Duration::from_secs(2),
            tool_timeout: std::time::Duration::from_millis(100),
            max_result_bytes: 1024,
            reconnect: true,
            tool_limits: BTreeMap::new(),
            transport: McpTransportConfig::Http {
                url,
                bearer_token: None,
                headers: BTreeMap::new(),
            },
        };
        let manager = McpManager::initialize(&BTreeMap::from([("test".to_string(), config)]))
            .await
            .unwrap();
        let tool = manager
            .registrations()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .tool;

        let started = std::time::Instant::now();
        assert!(matches!(
            tool.call(1, serde_json::json!({}), 1024).await,
            Err(McpCallError::Timeout { .. })
        ));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "tool_timeout 到达后不应等待远端取消响应，实际耗时 {:?}",
            started.elapsed()
        );
        let second = tool.call(1, serde_json::json!({}), 1024).await.unwrap();
        assert_eq!(second.content, "ok");
        manager.shutdown().await;

        {
            let state = state.lock().unwrap();
            assert_eq!(state.sessions, 2, "超时后的新调用必须建立新 session");
            assert_eq!(state.calls, ["session-1", "session-2"]);
            assert_eq!(state.cancellations, 1, "超时必须发送 MCP cancelled 通知");
            assert_eq!(
                state.side_effects, 1,
                "已取消的旧调用不得在新 session 建立后继续产生副作用"
            );
        }
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dropping_a_sent_call_cancels_and_retires_the_session() {
        let (url, state, shutdown, server) = start_reconnect_mcp_mock().await;
        let config = McpServerConfig {
            enabled_tools: Some(vec!["slow".to_string()]),
            tool_aliases: BTreeMap::new(),
            session_scope: SessionScope::Job,
            max_in_flight: 1,
            startup_timeout: std::time::Duration::from_secs(2),
            tool_timeout: std::time::Duration::from_secs(5),
            max_result_bytes: 1024,
            reconnect: true,
            tool_limits: BTreeMap::new(),
            transport: McpTransportConfig::Http {
                url,
                bearer_token: None,
                headers: BTreeMap::new(),
            },
        };
        let manager = McpManager::initialize(&BTreeMap::from([("test".to_string(), config)]))
            .await
            .unwrap();
        let tool = manager
            .registrations()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .tool;

        let running_tool = tool.clone();
        let running =
            tokio::spawn(async move { running_tool.call(1, serde_json::json!({}), 1024).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if state.lock().unwrap().calls.len() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("旧 tools/call 必须先到达 server");
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());

        {
            let slot = manager.servers["test"].job.state.lock().await;
            assert!(
                matches!(&*slot, SessionState::Broken(_))
                    || matches!(&*slot, SessionState::Active(session) if session.is_broken()),
                "future Drop 返回前必须同步使旧 session 不可复用"
            );
        }
        let second = tool.call(1, serde_json::json!({}), 1024).await.unwrap();
        assert_eq!(second.content, "ok");
        manager.shutdown().await;

        {
            let state = state.lock().unwrap();
            assert_eq!(state.sessions, 2, "取消后必须建立新 session");
            assert_eq!(state.calls, ["session-1", "session-2"]);
            assert_eq!(state.cancellations, 1, "已发送请求必须显式取消一次");
            assert_eq!(
                state.side_effects, 1,
                "被 scheduler 丢弃的旧调用不得继续产生副作用"
            );
        }
        shutdown.cancel();
        server.await.unwrap();
    }

    #[test]
    fn structured_over_limit_is_tool_error_not_invalid_json() {
        let mut result = CallToolResult::structured(serde_json::json!({"data":"x".repeat(200)}));
        result.content.clear();
        let output = convert_result(result, 20);
        assert!(output.content.starts_with("错误："));
        assert!(!output.content.contains("[已截断"));
        assert!(output.content.len() <= 20);
    }

    #[test]
    fn multimedia_is_explicitly_rejected() {
        let result = CallToolResult::success(vec![ContentBlock::image("AA==", "image/png")]);
        let output = convert_result(result, 1024);
        assert!(output.content.contains("不支持的结果类型 image"));
    }

    #[test]
    fn every_mcp_result_branch_obeys_the_final_byte_limit() {
        let mut reported_error =
            CallToolResult::success(vec![ContentBlock::text("失败详情".repeat(100))]);
        reported_error.is_error = Some(true);
        let mut oversized_structured =
            CallToolResult::structured(serde_json::json!({"data":"x".repeat(200)}));
        oversized_structured.content.clear();
        let unsupported = CallToolResult::success(vec![ContentBlock::image("AA==", "image/png")]);

        for result in [reported_error, oversized_structured, unsupported] {
            let output = convert_result(result, 20);
            assert!(output.content.len() <= 20, "{}", output.content);
            assert!(output.content.is_char_boundary(output.content.len()));
            assert!(!output.cacheable);
        }
        assert!(external_error("x".repeat(100), 4).content.len() <= 4);
    }
}
