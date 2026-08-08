//! 作业启动配置：只读取当前工作目录的 `config.toml`，并在边界上完成默认值、
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
const DEFAULT_LLM_ATTEMPTS: u32 = 3;
const DEFAULT_IDENTICAL_TOOL_CALL_LIMIT: u32 = 3;
const DEFAULT_CONTEXT_SAFETY_TOKENS: u64 = 4096;
const DEFAULT_MAX_RESULT_BYTES: usize = 32 * 1024;
const DEFAULT_MAX_MATCHES: usize = 100;
const DEFAULT_MAX_CONTEXT_LINES: usize = 20;
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MCP_MAX_IN_FLIGHT: usize = 1;
const DEFAULT_MCP_STARTUP_TIMEOUT_SEC: u64 = 30;
const DEFAULT_MCP_TOOL_TIMEOUT_SEC: u64 = 300;

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
    pub max_result_bytes: usize,
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
    max_output_tokens: Option<u64>,
    execution: FileExecutionConfig,
    tools: FileToolsConfig,
    cache: FileCacheConfig,
    mcp_servers: BTreeMap<String, FileMcpServerConfig>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileExecutionConfig {
    llm_attempts: Option<u32>,
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
    reconnect: bool,
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
        "缺少模型最大输出：请设置 FORMIC_LLM_MAX_OUTPUT_TOKENS，或填写 config.toml 的 max_output_tokens"
    )]
    MissingMaxOutput,
    #[error("配置无效：{0}")]
    Invalid(String),
}

/// 只读取当前工作目录下的固定文件名，不接受外部路径。
pub fn load() -> Result<AppConfig, ConfigError> {
    load_from_with(Path::new(CONFIG_FILE), |name| env::var(name).ok())
}

fn load_from_with(
    path: &Path,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<AppConfig, ConfigError> {
    let file = match fs::read_to_string(path) {
        Ok(contents) => toml::from_str(&contents).map_err(|_| ConfigError::Parse {
            path: path.to_path_buf(),
        })?,
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
    let protocol = env_value("FORMIC_LLM_PROTOCOL").ok_or(ConfigError::MissingProtocol)?;
    let context_window_tokens = parse_env_u64(
        env_value("FORMIC_LLM_CONTEXT_WINDOW_TOKENS"),
        "FORMIC_LLM_CONTEXT_WINDOW_TOKENS",
    )?
    .or(file.context_window_tokens)
    .ok_or(ConfigError::MissingContextWindow)?;
    let max_output_tokens = parse_env_u64(
        env_value("FORMIC_LLM_MAX_OUTPUT_TOKENS"),
        "FORMIC_LLM_MAX_OUTPUT_TOKENS",
    )?
    .or(file.max_output_tokens)
    .ok_or(ConfigError::MissingMaxOutput)?;
    require_positive(context_window_tokens, "context_window_tokens")?;
    require_positive(max_output_tokens, "max_output_tokens")?;

    let execution = ExecutionConfig {
        llm_attempts: positive_or(
            file.execution.llm_attempts,
            DEFAULT_LLM_ATTEMPTS,
            "execution.llm_attempts",
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
    let reserved = max_output_tokens
        .checked_add(execution.context_safety_tokens)
        .ok_or_else(|| ConfigError::Invalid("模型 token 配置发生整数溢出".into()))?;
    if reserved >= context_window_tokens {
        return Err(ConfigError::Invalid(format!(
            "context_window_tokens 必须大于 max_output_tokens 与 execution.context_safety_tokens 之和（当前保留 {reserved}）"
        )));
    }

    let cpu_default = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let global_result = positive_or(
        file.tools.max_result_bytes,
        DEFAULT_MAX_RESULT_BYTES,
        "tools.max_result_bytes",
    )?;
    let global_in_flight =
        positive_or(file.tools.max_in_flight, cpu_default, "tools.max_in_flight")?;
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
            protocol: Protocol::parse(&protocol).map_err(ConfigError::InvalidProtocol)?,
            base_url: env_value("FORMIC_LLM_BASE_URL")
                .or_else(|| non_empty(file.url))
                .ok_or(ConfigError::MissingUrl)?,
            model: env_value("FORMIC_LLM_MODEL")
                .or_else(|| non_empty(file.model))
                .ok_or(ConfigError::MissingModel)?,
            api_key: env_value("FORMIC_LLM_API_KEY").or_else(|| non_empty(file.api_key)),
            context_window_tokens,
            max_output_tokens,
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
        tool_limits.insert(
            tool.clone(),
            McpToolLimit {
                max_in_flight: positive_or(
                    limit.max_in_flight,
                    server_in_flight,
                    &format!("mcp_servers.{name}.tool_limits.{tool}.max_in_flight"),
                )?,
                max_result_bytes: positive_or(
                    limit.max_result_bytes,
                    server_result,
                    &format!("mcp_servers.{name}.tool_limits.{tool}.max_result_bytes"),
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
        reconnect: file.reconnect,
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
        load_from_with(&path, |name| {
            values.get(name).map(|value| (*value).to_string())
        })
    }

    const BASE_FILE: &str = r#"
url = "https://file.example/v1"
api_key = "file-key"
model = "file-model"
context_window_tokens = 131072
max_output_tokens = 16384
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
        assert_eq!(config.execution.llm_attempts, 3);
        assert!(config.tools.search.enabled);
        assert!(config.tools.read.enabled);
        assert_eq!(config.cache.max_bytes, 64 * 1024 * 1024);
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
                ("FORMIC_LLM_MAX_OUTPUT_TOKENS", "20000"),
            ],
        )
        .unwrap();

        assert_eq!(config.llm.base_url, "https://env.example/v1");
        assert_eq!(config.llm.api_key.as_deref(), Some("env-key"));
        assert_eq!(config.llm.model, "env-model");
        assert_eq!(config.llm.context_window_tokens, 200000);
        assert_eq!(config.llm.max_output_tokens, 20000);
    }

    #[test]
    fn missing_context_names_both_sources() {
        let error = load_fixture(
            Some("url='x'\nmodel='m'\nmax_output_tokens=100\n"),
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
        let file = "url='x'\nmodel='m'\ncontext_window_tokens=100\nmax_output_tokens=90\n[execution]\ncontext_safety_tokens=10\n";
        let error = load_fixture(Some(file), &[("FORMIC_LLM_PROTOCOL", "responses")])
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
        assert!(config.mcp_servers["demo"].enabled_tools.is_none());

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
    fn tool_limit_inherits_server_values() {
        let file = format!(
            "{BASE_FILE}\n[mcp_servers.demo]\nenabled=true\ncommand='server'\nenabled_tools=['search']\nmax_in_flight=7\nmax_result_bytes=4567\n[mcp_servers.demo.tool_limits.search]\n"
        );
        let config = load_fixture(Some(&file), &[("FORMIC_LLM_PROTOCOL", "responses")]).unwrap();
        let limit = &config.mcp_servers["demo"].tool_limits["search"];
        assert_eq!(limit.max_in_flight, 7);
        assert_eq!(limit.max_result_bytes, 4567);
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
