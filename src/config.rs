//! LLM 启动配置：读取当前工作目录的 config.toml，并让非空环境变量逐项覆盖文件值。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::llm::{LlmConfig, Protocol};

const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("无法读取配置文件 {path}：{source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "配置文件 {path} 不是有效配置：请检查 TOML 语法，并且只使用字符串字段 url、api_key 和 model"
    )]
    Parse { path: PathBuf },
    #[error(
        "缺少环境变量 FORMIC_LLM_PROTOCOL：指定 API 协议形状：completions / responses / anthropic"
    )]
    MissingProtocol,
    #[error("{0}")]
    InvalidProtocol(String),
    #[error("缺少 LLM URL：请设置环境变量 FORMIC_LLM_BASE_URL，或填写 config.toml 的 url")]
    MissingUrl,
    #[error("缺少模型名：请设置环境变量 FORMIC_LLM_MODEL，或填写 config.toml 的 model")]
    MissingModel,
}

pub fn load() -> Result<LlmConfig, ConfigError> {
    load_from_with(Path::new(CONFIG_FILE), |name| env::var(name).ok())
}

fn load_from_with(
    path: &Path,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<LlmConfig, ConfigError> {
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
) -> Result<LlmConfig, ConfigError> {
    let env_value = |name| get_env(name).filter(|value| !value.is_empty());
    let protocol = env_value("FORMIC_LLM_PROTOCOL").ok_or(ConfigError::MissingProtocol)?;

    Ok(LlmConfig {
        protocol: Protocol::parse(&protocol).map_err(ConfigError::InvalidProtocol)?,
        base_url: env_value("FORMIC_LLM_BASE_URL")
            .or_else(|| non_empty(file.url))
            .ok_or(ConfigError::MissingUrl)?,
        model: env_value("FORMIC_LLM_MODEL")
            .or_else(|| non_empty(file.model))
            .ok_or(ConfigError::MissingModel)?,
        api_key: env_value("FORMIC_LLM_API_KEY").or_else(|| non_empty(file.api_key)),
    })
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
    ) -> Result<LlmConfig, ConfigError> {
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

    #[test]
    fn file_supplies_url_api_key_and_model() {
        let config = load_fixture(
            Some(
                r#"
url = "https://file.example/v1"
api_key = "file-key"
model = "file-model"
"#,
            ),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .unwrap();

        assert_eq!(config.protocol, Protocol::Responses);
        assert_eq!(config.base_url, "https://file.example/v1");
        assert_eq!(config.api_key.as_deref(), Some("file-key"));
        assert_eq!(config.model, "file-model");
    }

    #[test]
    fn environment_overrides_each_file_value() {
        let config = load_fixture(
            Some(
                r#"
url = "https://file.example/v1"
api_key = "file-key"
model = "file-model"
"#,
            ),
            &[
                ("FORMIC_LLM_PROTOCOL", "completions"),
                ("FORMIC_LLM_BASE_URL", "https://env.example/v1"),
                ("FORMIC_LLM_API_KEY", "env-key"),
                ("FORMIC_LLM_MODEL", "env-model"),
            ],
        )
        .unwrap();

        assert_eq!(config.base_url, "https://env.example/v1");
        assert_eq!(config.api_key.as_deref(), Some("env-key"));
        assert_eq!(config.model, "env-model");
    }

    #[test]
    fn existing_environment_only_configuration_still_works() {
        let config = load_fixture(
            None,
            &[
                ("FORMIC_LLM_PROTOCOL", "anthropic"),
                ("FORMIC_LLM_BASE_URL", "https://env.example"),
                ("FORMIC_LLM_MODEL", "env-model"),
            ],
        )
        .unwrap();

        assert_eq!(config.protocol, Protocol::Anthropic);
        assert_eq!(config.base_url, "https://env.example");
        assert_eq!(config.api_key, None);
        assert_eq!(config.model, "env-model");
    }

    #[test]
    fn empty_environment_value_falls_back_to_file() {
        let config = load_fixture(
            Some(
                r#"
url = "https://file.example/v1"
model = "file-model"
"#,
            ),
            &[
                ("FORMIC_LLM_PROTOCOL", "responses"),
                ("FORMIC_LLM_BASE_URL", ""),
                ("FORMIC_LLM_MODEL", ""),
            ],
        )
        .unwrap();

        assert_eq!(config.base_url, "https://file.example/v1");
        assert_eq!(config.model, "file-model");
    }

    #[test]
    fn missing_required_value_names_both_sources() {
        let error = load_fixture(
            Some("model = \"file-model\"\n"),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("FORMIC_LLM_BASE_URL"), "{message}");
        assert!(message.contains("config.toml"), "{message}");
        assert!(message.contains("url"), "{message}");
    }

    #[test]
    fn parse_error_does_not_expose_plaintext_api_key() {
        let error = load_fixture(
            Some("api_key = \"secret-value\"\nmodel = [\n"),
            &[("FORMIC_LLM_PROTOCOL", "responses")],
        )
        .unwrap_err();

        assert!(!error.to_string().contains("secret-value"));
        assert!(!format!("{error:?}").contains("secret-value"));
    }
}
