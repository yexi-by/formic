//! 通用 MCP 客户端：启动时发现并冻结允许的工具目录，运行时按 job/unit 管理会话。
//! 传输失败和超时从工具结果中分离；原调用从不自动重放。

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use http::{HeaderName, HeaderValue};
use process_wrap::tokio::CommandWrap;
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, ContentBlock,
    Implementation, Tool,
};
use rmcp::service::{NotificationContext, Peer, RoleClient, RunningService};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::{StreamableHttpClientTransport, TokioChildProcess};
use rmcp::{ClientHandler, ServiceExt};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

use crate::config::{McpServerConfig, McpTransportConfig, SessionScope};
use crate::llm::ToolSpec;
use crate::tools::ToolOutput;

const MODEL_TOOL_NAME_LIMIT: usize = 64;
const SESSION_CLOSE_TIMEOUT_SEC: u64 = 5;

type ClientService = RunningService<RoleClient, FormicClientHandler>;

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
}

struct McpServer {
    name: String,
    config: McpServerConfig,
    frozen: BTreeMap<String, FrozenTool>,
    job: Arc<SessionSlot>,
    units: Mutex<HashMap<u64, Arc<SessionSlot>>>,
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
    stderr_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
                    max_result_bytes: limit
                        .map(|limit| limit.max_result_bytes)
                        .unwrap_or(server.config.max_result_bytes),
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
        for server in self.servers.values() {
            let units: Vec<Arc<SessionSlot>> = server
                .units
                .lock()
                .await
                .drain()
                .map(|(_, slot)| slot)
                .collect();
            for slot in units {
                close_slot(&slot).await;
            }
            close_slot(&server.job).await;
        }
    }
}

impl McpTool {
    pub async fn call(
        &self,
        unit: u64,
        arguments: Value,
        max_result_bytes: usize,
    ) -> Result<ToolOutput, McpCallError> {
        let Some(arguments) = arguments.as_object().cloned() else {
            return Ok(ToolOutput {
                content: "错误：MCP 工具参数必须是 JSON object".into(),
                cacheable: false,
            });
        };
        let slot = self.server.slot(unit).await;
        let session = self.server.ensure_session(&slot).await?;
        let params = CallToolRequestParams::new(self.remote_name.clone()).with_arguments(arguments);
        let result = tokio::time::timeout(
            self.server.config.tool_timeout,
            session.peer.call_tool(params),
        )
        .await
        .map_err(|_| McpCallError::Timeout {
            server: self.server.name.clone(),
        })?;
        match result {
            Ok(result) => Ok(convert_result(result, max_result_bytes)),
            Err(error) => {
                let reason = error.to_string();
                if session.peer.is_transport_closed() {
                    self.server.mark_broken(&slot, &session, &reason).await;
                }
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
            if !session.peer.is_transport_closed() {
                return Ok(Arc::clone(session));
            }
            *state = SessionState::Broken("传输已关闭".into());
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

    async fn mark_broken(
        &self,
        slot: &Arc<SessionSlot>,
        session: &Arc<ActiveSession>,
        reason: &str,
    ) {
        let mut state = slot.state.lock().await;
        if matches!(&*state, SessionState::Active(current) if Arc::ptr_eq(current, session)) {
            *state = SessionState::Broken(reason.to_string());
        }
    }
}

impl ActiveSession {
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

async fn connect(name: &str, config: &McpServerConfig) -> Result<Arc<ActiveSession>, String> {
    let handler = FormicClientHandler {
        server: name.to_string(),
    };
    let (service, stderr_task) = match &config.transport {
        McpTransportConfig::Stdio { command, args, env } => {
            let mut command = CommandWrap::with_new(command, |process| {
                process.args(args);
                process.envs(env);
            });
            #[cfg(windows)]
            command.wrap(JobObject);
            #[cfg(unix)]
            command.wrap(ProcessGroup::leader());
            let (transport, stderr) = TokioChildProcess::builder(command)
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|error| format!("无法启动 stdio 子进程：{error}"))?;
            let task = stderr.map(|stderr| {
                let server = name.to_string();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        eprintln!("MCP server {server} stderr：{line}");
                    }
                })
            });
            let service = handler
                .serve(transport)
                .await
                .map_err(|error| format!("initialize 失败：{error}"))?;
            (service, task)
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
            let mut transport_config = StreamableHttpClientTransportConfig::with_uri(url.clone())
                .custom_headers(custom_headers)
                .reinit_on_expired_session(false);
            if let Some(token) = bearer_token {
                transport_config = transport_config.auth_header(token.clone());
            }
            let transport = StreamableHttpClientTransport::from_config(transport_config);
            let service = handler
                .serve(transport)
                .await
                .map_err(|error| format!("initialize 失败：{error}"))?;
            (service, None)
        }
    };
    let peer = service.peer().clone();
    Ok(Arc::new(ActiveSession {
        peer,
        service: Mutex::new(Some(service)),
        stderr_task: Mutex::new(stderr_task),
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
    for block in result.content {
        match block {
            ContentBlock::Text(content) => text.push_str(&content.text),
            ContentBlock::Image(_) => return unsupported_result("image"),
            ContentBlock::Audio(_) => return unsupported_result("audio"),
            ContentBlock::Resource(_) => return unsupported_result("resource"),
            ContentBlock::ResourceLink(_) => return unsupported_result("resource_link"),
            _ => return unsupported_result("unknown"),
        }
    }
    let structured = result.structured_content;
    let (mut content, _truncated) = match structured {
        None => truncate_text(text, max_bytes),
        Some(structured) if text.is_empty() => {
            let serialized = serde_json::to_string(&structured).expect("Value 可序列化");
            if serialized.len() > max_bytes {
                return external_error(format!(
                    "MCP structuredContent 为 {} 字节，超过 {max_bytes} 字节上限",
                    serialized.len()
                ));
            }
            (serialized, false)
        }
        Some(structured) => match wrap_text_and_structured(&text, structured, max_bytes) {
            Some(value) => value,
            None => {
                return external_error(
                    "MCP structuredContent 连同固定包装本身已超过结果上限".to_string(),
                );
            }
        },
    };
    if result.is_error == Some(true) {
        content = format!("错误：MCP 工具报告失败：{content}");
    }
    ToolOutput {
        content,
        cacheable: false,
    }
}

fn unsupported_result(kind: &str) -> ToolOutput {
    external_error(format!("MCP 返回不支持的结果类型 {kind}"))
}

fn external_error(message: String) -> ToolOutput {
    ToolOutput {
        content: format!("错误：{message}"),
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
    let full = serde_json::json!({"structuredContent": structured, "text": text}).to_string();
    if full.len() <= max_bytes {
        return Some((full, false));
    }
    let marker = format!("\n[已截断：MCP 文本结果达到 {max_bytes} 字节上限]");
    let mut low = 0usize;
    let mut high = text.len();
    let mut best = None;
    while low <= high {
        let middle = utf8_prefix_len(text, low + (high - low) / 2);
        let candidate = serde_json::json!({
            "structuredContent": structured,
            "text": format!("{}{marker}", &text[..middle]),
        })
        .to_string();
        if candidate.len() <= max_bytes {
            best = Some(candidate);
            if middle == text.len() {
                break;
            }
            low = middle.saturating_add(1);
        } else {
            if middle == 0 {
                break;
            }
            high = middle - 1;
        }
    }
    best.map(|value| (value, true))
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

    #[test]
    fn structured_over_limit_is_tool_error_not_invalid_json() {
        let mut result = CallToolResult::structured(serde_json::json!({"data":"x".repeat(200)}));
        result.content.clear();
        let output = convert_result(result, 20);
        assert!(output.content.starts_with("错误："));
        assert!(!output.content.contains("[已截断"));
    }

    #[test]
    fn multimedia_is_explicitly_rejected() {
        let result = CallToolResult::success(vec![ContentBlock::image("AA==", "image/png")]);
        let output = convert_result(result, 1024);
        assert!(output.content.contains("不支持的结果类型 image"));
    }
}
