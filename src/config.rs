//! 作业启动配置：读取调用方明确指定的 `config.toml`，并在边界上完成默认值、
//! 环境变量覆盖、外部服务密钥解析和全部资源参数校验。

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;

use crate::llm::{LlmConfig, Protocol};

const CONFIG_FILE: &str = "config.toml";
const DEFAULT_LLM_ATTEMPTS: u32 = 5;
const DEFAULT_MAX_CONCURRENT_UNITS: usize = 64;
const DEFAULT_IDENTICAL_TOOL_CALL_LIMIT: u32 = 16;
const DEFAULT_CONTEXT_SAFETY_TOKENS: u64 = 2048;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_READ_TIMEOUT_MS: u64 = 600_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 1_800_000;
const DEFAULT_RETRY_DELAYS_MS: &[u64] = &[1_000, 2_000, 5_000];
const DEFAULT_MAX_RETRY_AFTER_MS: u64 = 60_000;
const DEFAULT_MAX_RESULT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT: usize = 64;
const DEFAULT_MAX_MATCHES: usize = 1000;
const DEFAULT_MAX_CONTEXT_LINES: usize = 100;
const DEFAULT_CACHE_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_MCP_MAX_IN_FLIGHT: usize = 64;
const DEFAULT_MCP_STARTUP_TIMEOUT_SEC: u64 = 60;
const DEFAULT_MCP_TOOL_TIMEOUT_SEC: u64 = 600;

#[derive(Clone)]
pub struct AppConfig {
    pub llm: LlmConfig,
    pub execution: ExecutionConfig,
    pub tools: ToolsConfig,
    pub cache: CacheConfig,
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub llm_attempts: u32,
    pub max_concurrent_units: usize,
    pub identical_tool_call_limit: u32,
    pub context_safety_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct ToolsConfig {
    pub max_in_flight: usize,
    pub search: SearchToolConfig,
    pub read: ReadToolConfig,
}

#[derive(Debug, Clone)]
pub struct SearchToolConfig {
    pub enabled: bool,
    pub max_result_bytes: usize,
    pub max_in_flight: usize,
    pub max_matches: usize,
    pub max_context_lines: usize,
}

#[derive(Debug, Clone)]
pub struct ReadToolConfig {
    pub enabled: bool,
    pub max_result_bytes: usize,
    pub max_in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_bytes: usize,
}

#[derive(Clone)]
pub struct McpServerConfig {
    pub enabled_tools: Option<Vec<String>>,
    pub tool_aliases: BTreeMap<String, String>,
    pub session_scope: SessionScope,
    pub max_in_flight: usize,
    pub startup_timeout: Duration,
    pub tool_timeout: Duration,
    pub max_result_bytes: usize,
    pub reconnect: bool,
    pub tool_limits: BTreeMap<String, McpToolLimit>,
    pub transport: McpTransportConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionScope {
    Job,
    Unit,
}

#[derive(Debug, Clone)]
pub struct McpToolLimit {
    pub max_in_flight: usize,
}

#[derive(Clone)]
pub enum McpTransportConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
        bearer_token: Option<String>,
        headers: BTreeMap<String, String>,
    },
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    context_window_tokens: Option<u64>,
    /// Anthropic Messages 协议要求的必填参数；其他协议不得配置。
    anthropic_max_tokens: Option<u64>,
    connect_timeout_ms: Option<u64>,
    read_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    retry_delays_ms: Option<Vec<u64>>,
    max_retry_after_ms: Option<u64>,
    requests_per_minute: Option<u32>,
    execution: FileExecutionConfig,
    tools: FileToolsConfig,
    cache: FileCacheConfig,
    mcp_servers: BTreeMap<String, FileMcpServerConfig>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileExecutionConfig {
    llm_attempts: Option<u32>,
    max_concurrent_units: Option<usize>,
    identical_tool_call_limit: Option<u32>,
    context_safety_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileToolsConfig {
    max_result_bytes: Option<usize>,
    max_in_flight: Option<usize>,
    search: FileSearchToolConfig,
    read: FileReadToolConfig,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileSearchToolConfig {
    enabled: bool,
    max_result_bytes: Option<usize>,
    max_in_flight: Option<usize>,
    max_matches: Option<usize>,
    max_context_lines: Option<usize>,
}

impl Default for FileSearchToolConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_result_bytes: None,
            max_in_flight: None,
            max_matches: None,
            max_context_lines: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileReadToolConfig {
    enabled: bool,
    max_result_bytes: Option<usize>,
    max_in_flight: Option<usize>,
}

impl Default for FileReadToolConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_result_bytes: None,
            max_in_flight: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileCacheConfig {
    enabled: bool,
    max_bytes: Option<usize>,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMcpServerConfig {
    enabled: bool,
    command: Option<String>,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    env_vars: BTreeMap<String, String>,
    url: Option<String>,
    bearer_token: Option<String>,
    bearer_token_env: Option<String>,
    headers: BTreeMap<String, String>,
    header_env: BTreeMap<String, String>,
    enabled_tools: Option<Vec<String>>,
    tool_aliases: BTreeMap<String, String>,
    session_scope: Option<String>,
    max_in_flight: Option<usize>,
    startup_timeout_sec: Option<u64>,
    tool_timeout_sec: Option<u64>,
    max_result_bytes: Option<usize>,
    reconnect: Option<bool>,
    tool_limits: BTreeMap<String, FileMcpToolLimit>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMcpToolLimit {
    max_in_flight: Option<usize>,
    max_result_bytes: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("无法读取配置文件 {path}：{source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("指定的配置文件 {0} 不存在")]
    MissingFile(PathBuf),
    #[error("配置文件 {path} 不是有效配置：请检查 TOML 语法、字段名和字段类型")]
    Parse { path: PathBuf },
    #[error(
        "缺少环境变量 FORMIC_LLM_PROTOCOL：指定 API 协议形状：completions / responses / anthropic"
    )]
    MissingProtocol,
    #[error("{0}")]
    InvalidProtocol(String),
    #[error("缺少 LLM URL：请设置 FORMIC_LLM_BASE_URL，或填写 config.toml 的 url")]
    MissingUrl,
    #[error("缺少模型名：请设置 FORMIC_LLM_MODEL，或填写 config.toml 的 model")]
    MissingModel,
    #[error(
        "缺少模型上下文大小：请设置 FORMIC_LLM_CONTEXT_WINDOW_TOKENS，或填写 config.toml 的 context_window_tokens"
    )]
    MissingContextWindow,
    #[error(
        "Anthropic Messages 缺少 max_tokens：请设置 FORMIC_ANTHROPIC_MAX_TOKENS，或填写 config.toml 的 anthropic_max_tokens"
    )]
    MissingAnthropicMaxTokens,
    #[error("配置无效：{0}")]
    Invalid(String),
}

/// 显式配置路径必须存在；省略路径时读取当前目录的 `config.toml`，默认文件不存在则
/// 允许完全由环境变量提供 LLM 身份。环境变量只覆盖 LLM 身份与模型容量字段。
pub fn load(path: Option<&Path>) -> Result<AppConfig, ConfigError> {
    let (path, required) = path.map_or((Path::new(CONFIG_FILE), false), |path| (path, true));
    load_from_with(path, required, |name| env::var(name).ok())
}

fn load_from_with(
    path: &Path,
    required: bool,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<AppConfig, ConfigError> {
    let file = match fs::read_to_string(path) {
        Ok(contents) => toml::from_str(&contents).map_err(|_| ConfigError::Parse {
            path: path.to_path_buf(),
        })?,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound && required => {
            return Err(ConfigError::MissingFile(path.to_path_buf()));
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => FileConfig::default(),
        Err(source) => {
            return Err(ConfigError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    resolve(file, get_env)
}

fn resolve(
    file: FileConfig,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<AppConfig, ConfigError> {
    let env_value = |name: &str| get_env(name).filter(|value| !value.is_empty());
    let protocol_name = env_value("FORMIC_LLM_PROTOCOL").ok_or(ConfigError::MissingProtocol)?;
    let protocol = Protocol::parse(&protocol_name).map_err(ConfigError::InvalidProtocol)?;
    let context_window_tokens = parse_env_u64(
        env_value("FORMIC_LLM_CONTEXT_WINDOW_TOKENS"),
        "FORMIC_LLM_CONTEXT_WINDOW_TOKENS",
    )?
    .or(file.context_window_tokens)
    .ok_or(ConfigError::MissingContextWindow)?;
    require_positive(context_window_tokens, "context_window_tokens")?;

    let configured_anthropic_max_tokens = parse_env_u64(
        env_value("FORMIC_ANTHROPIC_MAX_TOKENS"),
        "FORMIC_ANTHROPIC_MAX_TOKENS",
    )?
    .or(file.anthropic_max_tokens);
    let anthropic_max_tokens = match (protocol, configured_anthropic_max_tokens) {
        (Protocol::Anthropic, Some(value)) => {
            require_positive(value, "anthropic_max_tokens")?;
            Some(value)
        }
        (Protocol::Anthropic, None) => return Err(ConfigError::MissingAnthropicMaxTokens),
        (_, Some(_)) => {
            return Err(ConfigError::Invalid(
                "anthropic_max_tokens 只允许用于 anthropic 协议".into(),
            ));
        }
        (_, None) => None,
    };

    let execution = ExecutionConfig {
        llm_attempts: positive_or(
            file.execution.llm_attempts,
            DEFAULT_LLM_ATTEMPTS,
            "execution.llm_attempts",
        )?,
        max_concurrent_units: positive_or(
            file.execution.max_concurrent_units,
            DEFAULT_MAX_CONCURRENT_UNITS,
            "execution.max_concurrent_units",
        )?,
        identical_tool_call_limit: positive_or(
            file.execution.identical_tool_call_limit,
            DEFAULT_IDENTICAL_TOOL_CALL_LIMIT,
            "execution.identical_tool_call_limit",
        )?,
        context_safety_tokens: positive_or(
            file.execution.context_safety_tokens,
            DEFAULT_CONTEXT_SAFETY_TOKENS,
            "execution.context_safety_tokens",
        )?,
    };
    let reserved = anthropic_max_tokens
        .unwrap_or(0)
        .checked_add(execution.context_safety_tokens)
        .ok_or_else(|| ConfigError::Invalid("上下文保留配置发生整数溢出".into()))?;
    if reserved >= context_window_tokens {
        let components = if anthropic_max_tokens.is_some() {
            "anthropic_max_tokens 与 execution.context_safety_tokens 之和"
        } else {
            "execution.context_safety_tokens"
        };
        return Err(ConfigError::Invalid(format!(
            "context_window_tokens 必须大于 {components}（当前保留 {reserved}）"
        )));
    }

    let global_result = positive_or(
        file.tools.max_result_bytes,
        DEFAULT_MAX_RESULT_BYTES,
        "tools.max_result_bytes",
    )?;
    let global_in_flight = positive_or(
        file.tools.max_in_flight,
        DEFAULT_MAX_IN_FLIGHT,
        "tools.max_in_flight",
    )?;
    let tools = ToolsConfig {
        max_in_flight: global_in_flight,
        search: SearchToolConfig {
            enabled: file.tools.search.enabled,
            max_result_bytes: positive_or(
                file.tools.search.max_result_bytes,
                global_result,
                "tools.search.max_result_bytes",
            )?,
            max_in_flight: positive_or(
                file.tools.search.max_in_flight,
                global_in_flight,
                "tools.search.max_in_flight",
            )?,
            max_matches: positive_or(
                file.tools.search.max_matches,
                DEFAULT_MAX_MATCHES,
                "tools.search.max_matches",
            )?,
            max_context_lines: positive_or(
                file.tools.search.max_context_lines,
                DEFAULT_MAX_CONTEXT_LINES,
                "tools.search.max_context_lines",
            )?,
        },
        read: ReadToolConfig {
            enabled: file.tools.read.enabled,
            max_result_bytes: positive_or(
                file.tools.read.max_result_bytes,
                global_result,
                "tools.read.max_result_bytes",
            )?,
            max_in_flight: positive_or(
                file.tools.read.max_in_flight,
                global_in_flight,
                "tools.read.max_in_flight",
            )?,
        },
    };
    let cache = CacheConfig {
        enabled: file.cache.enabled,
        max_bytes: positive_or(file.cache.max_bytes, DEFAULT_CACHE_BYTES, "cache.max_bytes")?,
    };

    let mut mcp_servers = BTreeMap::new();
    for (name, server) in file.mcp_servers {
        if !server.enabled {
            continue;
        }
        let resolved = resolve_mcp_server(&name, server, &env_value, global_result)?;
        mcp_servers.insert(name, resolved);
    }

    Ok(AppConfig {
        llm: LlmConfig {
            protocol,
            base_url: env_value("FORMIC_LLM_BASE_URL")
                .or_else(|| non_empty(file.url))
                .ok_or(ConfigError::MissingUrl)?,
            model: env_value("FORMIC_LLM_MODEL")
                .or_else(|| non_empty(file.model))
                .ok_or(ConfigError::MissingModel)?,
            api_key: env_value("FORMIC_LLM_API_KEY").or_else(|| non_empty(file.api_key)),
            context_window_tokens,
            anthropic_max_tokens,
            connect_timeout: Duration::from_millis(positive_or(
                file.connect_timeout_ms,
                DEFAULT_CONNECT_TIMEOUT_MS,
                "connect_timeout_ms",
            )?),
            read_timeout: Duration::from_millis(positive_or(
                file.read_timeout_ms,
                DEFAULT_READ_TIMEOUT_MS,
                "read_timeout_ms",
            )?),
            request_timeout: Duration::from_millis(positive_or(
                file.request_timeout_ms,
                DEFAULT_REQUEST_TIMEOUT_MS,
                "request_timeout_ms",
            )?),
            retry_delays: retry_delays(file.retry_delays_ms)?,
            max_retry_after: Duration::from_millis(positive_or(
                file.max_retry_after_ms,
                DEFAULT_MAX_RETRY_AFTER_MS,
                "max_retry_after_ms",
            )?),
            requests_per_minute: optional_positive(
                file.requests_per_minute,
                "requests_per_minute",
            )?,
        },
        execution,
        tools,
        cache,
        mcp_servers,
    })
}

fn resolve_mcp_server(
    name: &str,
    file: FileMcpServerConfig,
    env_value: &impl Fn(&str) -> Option<String>,
    global_result: usize,
) -> Result<McpServerConfig, ConfigError> {
    let enabled_tools = file.enabled_tools;
    let mut unique = BTreeSet::new();
    if let Some(enabled_tools) = &enabled_tools {
        if enabled_tools.is_empty() {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.enabled_tools 若配置则不能为空"
            )));
        }
        for tool in enabled_tools {
            if tool.is_empty() || !unique.insert(tool.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "mcp_servers.{name}.enabled_tools 含空名称或重复名称 {tool:?}"
                )));
            }
        }
    }
    let mut aliases = BTreeSet::new();
    for (remote, alias) in &file.tool_aliases {
        if enabled_tools.is_some() && !unique.contains(remote) {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.tool_aliases.{remote} 不在 enabled_tools 中"
            )));
        }
        if alias.is_empty() || !aliases.insert(alias) {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.tool_aliases 的别名必须非空且互不重复"
            )));
        }
    }
    for remote in file.tool_limits.keys() {
        if enabled_tools.is_some() && !unique.contains(remote) {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.tool_limits.{remote} 不在 enabled_tools 中"
            )));
        }
    }

    let server_in_flight = positive_or(
        file.max_in_flight,
        DEFAULT_MCP_MAX_IN_FLIGHT,
        &format!("mcp_servers.{name}.max_in_flight"),
    )?;
    let server_result = positive_or(
        file.max_result_bytes,
        global_result,
        &format!("mcp_servers.{name}.max_result_bytes"),
    )?;
    let mut tool_limits = BTreeMap::new();
    for (tool, limit) in file.tool_limits {
        if limit.max_result_bytes.is_some() {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.tool_limits.{tool}.max_result_bytes 无效；结果字节上限必须统一配置在 mcp_servers.{name}.max_result_bytes"
            )));
        }
        tool_limits.insert(
            tool.clone(),
            McpToolLimit {
                max_in_flight: positive_or(
                    limit.max_in_flight,
                    server_in_flight,
                    &format!("mcp_servers.{name}.tool_limits.{tool}.max_in_flight"),
                )?,
            },
        );
    }

    let has_stdio_extras =
        !file.args.is_empty() || !file.env.is_empty() || !file.env_vars.is_empty();
    let has_http_extras = file.bearer_token.is_some()
        || file.bearer_token_env.is_some()
        || !file.headers.is_empty()
        || !file.header_env.is_empty();
    let transport = match (non_empty(file.command), non_empty(file.url)) {
        (Some(command), None) if !has_http_extras => {
            let mut child_env = file.env;
            for (child_name, source_name) in file.env_vars {
                let value = env_value(&source_name).ok_or_else(|| {
                    ConfigError::Invalid(format!(
                        "mcp_servers.{name}.env_vars.{child_name} 引用的环境变量 {source_name} 缺失或为空"
                    ))
                })?;
                child_env.insert(child_name, value);
            }
            McpTransportConfig::Stdio {
                command,
                args: file.args,
                env: child_env,
            }
        }
        (None, Some(url)) if !has_stdio_extras => {
            let mut headers = file.headers;
            for (header, source_name) in file.header_env {
                let value = env_value(&source_name).ok_or_else(|| {
                    ConfigError::Invalid(format!(
                        "mcp_servers.{name}.header_env.{header} 引用的环境变量 {source_name} 缺失或为空"
                    ))
                })?;
                headers.insert(header, value);
            }
            validate_headers(name, &headers)?;
            let bearer_token = match non_empty(file.bearer_token_env) {
                Some(source_name) => {
                    env_value(&source_name).or_else(|| non_empty(file.bearer_token))
                }
                None => non_empty(file.bearer_token),
            };
            McpTransportConfig::Http {
                url,
                bearer_token,
                headers,
            }
        }
        (Some(_), Some(_)) => {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name} 的 command 与 url 互斥"
            )));
        }
        (Some(_), None) => {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name} 使用 stdio 时不能配置 HTTP 字段"
            )));
        }
        (None, Some(_)) => {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name} 使用 HTTP 时不能配置 stdio 字段"
            )));
        }
        (None, None) => {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name} 必须且只能配置 command 或 url"
            )));
        }
    };

    let session_scope = match file.session_scope.as_deref().unwrap_or("job") {
        "job" => SessionScope::Job,
        "unit" => SessionScope::Unit,
        other => {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{name}.session_scope 必须是 job 或 unit，当前为 {other:?}"
            )));
        }
    };

    Ok(McpServerConfig {
        enabled_tools,
        tool_aliases: file.tool_aliases,
        session_scope,
        max_in_flight: server_in_flight,
        startup_timeout: Duration::from_secs(positive_or(
            file.startup_timeout_sec,
            DEFAULT_MCP_STARTUP_TIMEOUT_SEC,
            &format!("mcp_servers.{name}.startup_timeout_sec"),
        )?),
        tool_timeout: Duration::from_secs(positive_or(
            file.tool_timeout_sec,
            DEFAULT_MCP_TOOL_TIMEOUT_SEC,
            &format!("mcp_servers.{name}.tool_timeout_sec"),
        )?),
        max_result_bytes: server_result,
        reconnect: file.reconnect.unwrap_or(true),
        tool_limits,
        transport,
    })
}

fn validate_headers(server: &str, headers: &BTreeMap<String, String>) -> Result<(), ConfigError> {
    for (name, value) in headers {
        if name.parse::<HeaderName>().is_err() || HeaderValue::from_str(value).is_err() {
            return Err(ConfigError::Invalid(format!(
                "mcp_servers.{server}.headers 含无效 HTTP header {name:?}"
            )));
        }
    }
    Ok(())
}

fn parse_env_u64(value: Option<String>, name: &str) -> Result<Option<u64>, ConfigError> {
    value
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ConfigError::Invalid(format!("环境变量 {name} 必须是正整数")))
        })
        .transpose()
}

fn positive_or<T>(value: Option<T>, default: T, name: &str) -> Result<T, ConfigError>
where
    T: Copy + PartialEq + From<u8>,
{
    let value = value.unwrap_or(default);
    if value == T::from(0) {
        return Err(ConfigError::Invalid(format!("{name} 必须是正整数")));
    }
    Ok(value)
}

fn optional_positive<T>(value: Option<T>, name: &str) -> Result<Option<T>, ConfigError>
where
    T: Copy + PartialEq + From<u8>,
{
    if value == Some(T::from(0)) {
        return Err(ConfigError::Invalid(format!("{name} 必须是正整数")));
    }
    Ok(value)
}

fn retry_delays(values: Option<Vec<u64>>) -> Result<Vec<Duration>, ConfigError> {
    let values = values.unwrap_or_else(|| DEFAULT_RETRY_DELAYS_MS.to_vec());
    if values.contains(&0) {
        return Err(ConfigError::Invalid(
            "retry_delays_ms 的每个等待时间都必须是正整数".into(),
        ));
    }
    Ok(values.into_iter().map(Duration::from_millis).collect())
}

fn require_positive<T>(value: T, name: &str) -> Result<(), ConfigError>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        return Err(ConfigError::Invalid(format!("{name} 必须是正整数")));
    }
    Ok(())
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn load_fixture(
        contents: Option<&str>,
        values: &[(&str, &str)],
    ) -> Result<AppConfig, ConfigError> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        if let Some(contents) = contents {
            fs::write(&path, contents).unwrap();
        }
        let values: HashMap<&str, &str> = values.iter().copied().collect();
        load_from_with(&path, false, |name| {
            values.get(name).map(|value| (*value).to_string())
        })
    }

    #[test]
    fn explicitly_selected_config_must_exist() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.toml");
        let error = load_from_with(&missing, true, |_| None)
            .err()
            .expect("显式缺失配置必须失败");
        assert!(matches!(error, ConfigError::MissingFile(path) if path == missing));
    }

    const BASE_FILE: &str = r#"
url = "https://file.example/v1"
api_key = "file-key"
model = "file-model"
context_window_tokens = 131072
"#;

    #[test]
    fn file_supplies_llm_and_defaults() {
        let config =
            load_fixture(Some(BASE_FILE), &[("FORMIC_LLM_PROTOCOL", "responses")]).unwrap();

        assert_eq!(config.llm.protocol, Protocol::Responses);
        assert_eq!(config.llm.base_url, "https://file.example/v1");
        assert_eq!(config.llm.api_key.as_deref(), Some("file-key"));
        assert_eq!(config.llm.model, "file-model");
        assert_eq!(config.llm.context_window_tokens, 131072);
        assert_eq!(config.llm.anthropic_max_tokens, None);
        assert_eq!(config.llm.connect_timeout, Duration::from_millis(30_000));
        assert_eq!(config.llm.read_timeout, Duration::from_millis(600_000));
        assert_eq!(config.llm.request_timeout, Duration::from_millis(1_800_000));
        assert_eq!(
            config.llm.retry_delays,
            [1_000, 2_000, 5_000].map(Duration::from_millis)
        );
        assert_eq!(config.llm.max_retry_after, Duration::from_millis(60_000));
        assert_eq!(config.llm.requests_per_minute, None);
        assert_eq!(config.execution.llm_attempts, 5);
        assert_eq!(config.execution.max_concurrent_units, 64);
        assert_eq!(config.execution.identical_tool_call_limit, 16);
        assert_eq!(config.execution.context_safety_tokens, 2048);
        assert_eq!(config.tools.max_in_flight, 64);
        assert!(config.tools.search.enabled);
        assert_eq!(config.tools.search.max_result_bytes, 1024 * 1024);
        assert_eq!(config.tools.search.max_in_flight, 64);
        assert_eq!(config.tools.search.max_matches, 1000);
        assert_eq!(config.tools.search.max_context_lines, 100);
        assert!(config.tools.read.enabled);
        assert_eq!(config.tools.read.max_result_bytes, 1024 * 1024);
        assert_eq!(config.tools.read.max_in_flight, 64);
        assert_eq!(config.cache.max_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn request_policy_is_configurable_and_empty_retry_list_disables_retries() {
        let file = format!(
            "{BASE_FILE}\nconnect_timeout_ms=11\nread_timeout_ms=22\nrequest_timeout_ms=33\nretry_delays_ms=[]\nmax_retry_after_ms=44\nrequests_per_minute=55\n[execution]\nmax_concurrent_units=7\n"
        );
        let config = load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")]).unwrap();
        assert_eq!(config.llm.connect_timeout, Duration::from_millis(11));
        assert_eq!(config.llm.read_timeout, Duration::from_millis(22));
        assert_eq!(config.llm.request_timeout, Duration::from_millis(33));
        assert!(config.llm.retry_delays.is_empty());
        assert_eq!(config.llm.max_retry_after, Duration::from_millis(44));
        assert_eq!(config.llm.requests_per_minute, Some(55));
        assert_eq!(config.execution.max_concurrent_units, 7);
    }

    #[test]
    fn request_policy_rejects_zero_values_inside_nonempty_settings() {
        for field in [
            "connect_timeout_ms=0",
            "read_timeout_ms=0",
            "request_timeout_ms=0",
            "max_retry_after_ms=0",
            "requests_per_minute=0",
            "retry_delays_ms=[1,0]",
        ] {
            let file = format!("{BASE_FILE}\n{field}\n");
            assert!(
                matches!(
                    load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")]),
                    Err(ConfigError::Invalid(_))
                ),
                "应拒绝 {field}"
            );
        }
    }

    #[test]
    fn environment_overrides_every_llm_file_value() {
        let config = load_fixture(
            Some(BASE_FILE),
            &[
                ("FORMIC_LLM_PROTOCOL", "completions"),
                ("FORMIC_LLM_BASE_URL", "https://env.example/v1"),
                ("FORMIC_LLM_API_KEY", "env-key"),
                ("FORMIC_LLM_MODEL", "env-model"),
                ("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "200000"),
            ],
        )
        .unwrap();

        assert_eq!(config.llm.base_url, "https://env.example/v1");
        assert_eq!(config.llm.api_key.as_deref(), Some("env-key"));
        assert_eq!(config.llm.model, "env-model");
        assert_eq!(config.llm.context_window_tokens, 200000);
        assert_eq!(config.llm.anthropic_max_tokens, None);
    }

    #[test]
    fn missing_context_names_both_sources() {
        let error = load_fixture(
            Some("url='x'\nmodel='m'\n"),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .err()
        .expect("应拒绝无效配置");
        let message = error.to_string();
        assert!(
            message.contains("FORMIC_LLM_CONTEXT_WINDOW_TOKENS"),
            "{message}"
        );
        assert!(message.contains("context_window_tokens"), "{message}");
    }

    #[test]
    fn unknown_fields_and_zero_values_are_rejected() {
        let unknown = format!("{BASE_FILE}\nunknown = 1\n");
        assert!(matches!(
            load_fixture(Some(&unknown), &[("FORMIC_LLM_PROTOCOL", "responses")]),
            Err(ConfigError::Parse { .. })
        ));
        let zero = format!("{BASE_FILE}\n[tools]\nmax_result_bytes = 0\n");
        let error = load_fixture(Some(&zero), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("应拒绝无效配置");
        assert!(error.to_string().contains("tools.max_result_bytes"));
    }

    #[test]
    fn context_reserve_must_leave_input_room() {
        let file = "url='x'\nmodel='m'\ncontext_window_tokens=100\n[execution]\ncontext_safety_tokens=100\n";
        let error = load_fixture(Some(file), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("应拒绝无效配置");
        assert!(error.to_string().contains("必须大于"));
    }

    #[test]
    fn anthropic_max_tokens_is_required_and_protocol_specific() {
        let missing = load_fixture(Some(BASE_FILE), &[("FORMIC_LLM_PROTOCOL", "anthropic")])
            .err()
            .expect("Anthropic 必须显式配置协议必填参数");
        assert!(
            missing.to_string().contains("FORMIC_ANTHROPIC_MAX_TOKENS"),
            "{missing}"
        );

        let config = load_fixture(
            Some(BASE_FILE),
            &[
                ("FORMIC_LLM_PROTOCOL", "anthropic"),
                ("FORMIC_ANTHROPIC_MAX_TOKENS", "20000"),
            ],
        )
        .unwrap();
        assert_eq!(config.llm.anthropic_max_tokens, Some(20000));

        let file = format!("{BASE_FILE}\nanthropic_max_tokens=1000\n");
        let error = load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("其他协议不得携带 Anthropic 专属参数");
        assert!(error.to_string().contains("只允许用于 anthropic"));
    }

    #[test]
    fn anthropic_reserve_must_leave_input_room() {
        let file = "url='x'\nmodel='m'\ncontext_window_tokens=100\nanthropic_max_tokens=90\n[execution]\ncontext_safety_tokens=10\n";
        let error = load_fixture(Some(file), &[("FORMIC_LLM_PROTOCOL", "anthropic")])
            .err()
            .expect("应拒绝无效配置");
        assert!(error.to_string().contains("必须大于"));
    }

    #[test]
    fn enabled_mcp_auto_discovers_tools_and_requires_one_transport() {
        let automatic =
            format!("{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\nurl='http://localhost/mcp'\n");
        let config =
            load_fixture(Some(&automatic), &[("FORMIC_LLM_PROTOCOL", "responses")]).unwrap();
        let automatic = &config.mcp_servers["demo"];
        assert!(automatic.enabled_tools.is_none());
        assert_eq!(automatic.session_scope, SessionScope::Job);
        assert_eq!(automatic.max_in_flight, 64);
        assert_eq!(automatic.max_result_bytes, 1024 * 1024);
        assert_eq!(automatic.startup_timeout, Duration::from_secs(60));
        assert_eq!(automatic.tool_timeout, Duration::from_secs(600));
        assert!(automatic.reconnect);

        let explicit_no_reconnect = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\nurl='http://localhost/mcp'\nreconnect=false\n"
        );
        let config = load_fixture(
            Some(&explicit_no_reconnect),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .unwrap();
        assert!(!config.mcp_servers["demo"].reconnect);

        let conflict = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\nurl='http://localhost/mcp'\ncommand='server'\n"
        );
        let error = load_fixture(Some(&conflict), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("应拒绝无效配置");
        assert!(error.to_string().contains("互斥"));

        let empty = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\nurl='http://localhost/mcp'\nenabled_tools=[]\n"
        );
        let error = load_fixture(Some(&empty), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("显式空允许列表没有可执行含义");
        assert!(error.to_string().contains("若配置则不能为空"));
    }

    #[test]
    fn mcp_environment_secrets_override_plaintext() {
        let file = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\nurl='http://localhost/mcp'\nbearer_token='plain'\nbearer_token_env='TOKEN'\nenabled_tools=['search']\n[mcp_servers.demo.header_env]\nx-key='HEADER_TOKEN'\n"
        );
        let config = load_fixture(
            Some(&file),
            &[
                ("FORMIC_LLM_PROTOCOL", "responses"),
                ("TOKEN", "environment"),
                ("HEADER_TOKEN", "header-secret"),
            ],
        )
        .unwrap();
        let server = &config.mcp_servers["demo"];
        let McpTransportConfig::Http {
            bearer_token,
            headers,
            ..
        } = &server.transport
        else {
            panic!("应解析为 HTTP")
        };
        assert_eq!(bearer_token.as_deref(), Some("environment"));
        assert_eq!(headers["x-key"], "header-secret");
    }

    #[test]
    fn tool_concurrency_limit_inherits_server_value() {
        let file = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\ncommand='server'\nenabled_tools=['search']\nmax_in_flight=7\nmax_result_bytes=4567\n[mcp_servers.demo.tool_limits.search]\n"
        );
        let config = load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")]).unwrap();
        let limit = &config.mcp_servers["demo"].tool_limits["search"];
        assert_eq!(limit.max_in_flight, 7);
    }

    #[test]
    fn result_limit_cannot_be_configured_per_tool() {
        let file = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\ncommand='server'\nenabled_tools=['search']\nmax_result_bytes=1024\n[mcp_servers.demo.tool_limits.search]\nmax_result_bytes=512\n"
        );
        let error = load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")])
            .err()
            .expect("公共 MCP 结果流无法提供互不相同的解码前上限");
        assert!(
            error
                .to_string()
                .contains("结果字节上限必须统一配置在 mcp_servers.demo.max_result_bytes")
        );
    }

    #[test]
    fn parse_error_does_not_expose_plaintext_api_key() {
        let error = load_fixture(
            Some("api_key = \"secret-value\"\nmodel = [\n"),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .err()
        .expect("应拒绝无效配置");

        assert!(!error.to_string().contains("secret-value"));
        assert!(!format!("{error:?}").contains("secret-value"));
    }
}
