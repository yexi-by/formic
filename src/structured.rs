//! 作业输出契约：文本模式保持原行为；结构化模式编译受限 JSON Schema，
//! 提供内部提交工具并负责输出目录只能存在一种完成事实。

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::llm::ToolSpec;
use crate::output::{OutputRoot, RecordFormat};

pub const SUBMIT_RESULT_TOOL: &str = "formic_submit_result";
const SCHEMA_RECORD: &str = "output-schema.json";

#[derive(Clone)]
pub enum OutputContract {
    Text,
    Structured(Arc<StructuredOutput>),
}

pub struct StructuredOutput {
    schema: Value,
    validator: jsonschema::Validator,
    source: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub instance_path: String,
    pub schema_path: String,
    pub reason: String,
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "结果位置 {} 不符合 schema {}：{}",
            display_pointer(&self.instance_path),
            display_pointer(&self.schema_path),
            self.reason
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OutputContractError {
    #[error("无法读取输出 schema {path}：{source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("输出 schema {0} 不是合法 JSON")]
    Json(PathBuf),
    #[error("输出 schema {path} 不受支持：{reason}")]
    Unsupported { path: PathBuf, reason: String },
    #[error("输出 schema {path} 无法编译：{reason}")]
    Compile { path: PathBuf, reason: String },
    #[error("输出目录 {path} 与当前输出模式冲突：{reason}")]
    Directory { path: PathBuf, reason: String },
    #[error("无法写入输出 schema 记录 {path}：{source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl OutputContract {
    pub fn prepare(
        schema_path: Option<&Path>,
        out_root: &OutputRoot,
    ) -> Result<Self, OutputContractError> {
        match schema_path {
            None => {
                enforce_text_directory(out_root)?;
                Ok(Self::Text)
            }
            Some(path) => {
                let bytes = fs::read(path).map_err(|source| OutputContractError::Read {
                    path: path.to_path_buf(),
                    source,
                })?;
                let schema: Value = serde_json::from_slice(&bytes)
                    .map_err(|_| OutputContractError::Json(path.to_path_buf()))?;
                validate_subset(&schema).map_err(|reason| OutputContractError::Unsupported {
                    path: path.to_path_buf(),
                    reason,
                })?;
                let validator = jsonschema::validator_for(&schema).map_err(|error| {
                    OutputContractError::Compile {
                        path: path.to_path_buf(),
                        reason: error.to_string(),
                    }
                })?;
                validate_structured_directory(out_root, &schema)?;
                Ok(Self::Structured(Arc::new(StructuredOutput {
                    schema,
                    validator,
                    source: bytes,
                })))
            }
        }
    }

    /// 作业身份与已有结果均已通过校验后，才发布结构化 schema 记录。
    /// `prepare` 本身只读，错误的 `--resume` 因而不会改变输出树。
    pub fn publish_schema_record(&self, out_root: &OutputRoot) -> Result<(), OutputContractError> {
        let Self::Structured(contract) = self else {
            return Ok(());
        };
        let pretty_schema = format!(
            "{}\n",
            serde_json::to_string_pretty(&contract.schema).expect("JSON Value 可序列化")
        );
        publish_structured_schema(out_root, &contract.schema, &pretty_schema)
    }

    pub fn source_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Text => None,
            Self::Structured(contract) => Some(&contract.source),
        }
    }

    pub fn format(&self) -> RecordFormat {
        match self {
            Self::Text => RecordFormat::Markdown,
            Self::Structured(_) => RecordFormat::Json,
        }
    }

    pub fn submit_spec(&self) -> Option<ToolSpec> {
        let Self::Structured(contract) = self else {
            return None;
        };
        Some(ToolSpec {
            name: SUBMIT_RESULT_TOOL.into(),
            description: "提交本单元的最终结构化结果；必须在一个不含其他工具调用的回合中单独调用。"
                .into(),
            parameters: contract.schema.clone(),
        })
    }

    pub fn validate_submission(&self, value: &Value) -> Result<String, ValidationIssue> {
        let Self::Structured(contract) = self else {
            unreachable!("文本模式没有结构化提交")
        };
        if let Some(error) = contract.validator.iter_errors(value).next() {
            return Err(ValidationIssue {
                instance_path: error.instance_path().to_string(),
                schema_path: error.schema_path().to_string(),
                reason: error.to_string(),
            });
        }
        Ok(format!(
            "{}\n",
            serde_json::to_string_pretty(value).expect("已验证 JSON Value 可序列化")
        ))
    }

    pub fn is_structured(&self) -> bool {
        matches!(self, Self::Structured(_))
    }

    pub fn validate_published_record(&self, bytes: &[u8]) -> Result<(), String> {
        match self {
            Self::Text => {
                let text =
                    std::str::from_utf8(bytes).map_err(|_| "完成记录不是合法 UTF-8".to_string())?;
                if text.trim().is_empty() {
                    return Err("完成记录为空".into());
                }
                Ok(())
            }
            Self::Structured(contract) => {
                let value: Value = serde_json::from_slice(bytes)
                    .map_err(|_| "完成记录不是合法 JSON".to_string())?;
                if let Some(error) = contract.validator.iter_errors(&value).next() {
                    return Err(format!("完成记录不符合当前 schema：{error}"));
                }
                Ok(())
            }
        }
    }
}

fn validate_subset(schema: &Value) -> Result<(), String> {
    let root = schema
        .as_object()
        .ok_or_else(|| "根 schema 必须是 object".to_string())?;
    if root.get("type").and_then(Value::as_str) != Some("object") {
        return Err("根 schema 的 type 必须是 object".into());
    }
    validate_node(schema, "#")
}

fn validate_node(schema: &Value, pointer: &str) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{pointer} 必须是 schema object"))?;
    let allowed = [
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "description",
        "title",
    ];
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !allowed.contains(&keyword.as_str()))
    {
        return Err(format!("{pointer} 含不支持的关键字 {keyword:?}"));
    }
    for keyword in ["description", "title"] {
        if object.get(keyword).is_some_and(|value| !value.is_string()) {
            return Err(format!("{pointer}/{keyword} 必须是字符串"));
        }
    }
    let schema_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{pointer}/type 必须是基础类型字符串"))?;
    if ![
        "object", "array", "string", "number", "integer", "boolean", "null",
    ]
    .contains(&schema_type)
    {
        return Err(format!("{pointer}/type 的值 {schema_type:?} 不受支持"));
    }
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| format!("{pointer}/enum 必须是非空数组"))?;
        if values
            .iter()
            .any(|value| value.is_array() || value.is_object())
        {
            return Err(format!("{pointer}/enum 只能包含基础值"));
        }
    }

    match schema_type {
        "object" => {
            if object.get("additionalProperties") != Some(&Value::Bool(false)) {
                return Err(format!("{pointer}/additionalProperties 必须显式为 false"));
            }
            let properties = object
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("{pointer}/properties 必须是 object"))?;
            let required = object
                .get("required")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("{pointer}/required 必须是字符串数组"))?;
            let mut seen = BTreeSet::new();
            for value in required {
                let name = value
                    .as_str()
                    .ok_or_else(|| format!("{pointer}/required 必须是字符串数组"))?;
                if !properties.contains_key(name) || !seen.insert(name) {
                    return Err(format!(
                        "{pointer}/required 的字段 {name:?} 必须存在于 properties 且不能重复"
                    ));
                }
            }
            for (name, child) in properties {
                validate_node(
                    child,
                    &format!("{pointer}/properties/{}", escape_pointer(name)),
                )?;
            }
            if object.contains_key("items") {
                return Err(format!("{pointer} 的 object 不能含 items"));
            }
        }
        "array" => {
            let items = object
                .get("items")
                .ok_or_else(|| format!("{pointer}/items 对 array 是必填项"))?;
            validate_node(items, &format!("{pointer}/items"))?;
            reject_object_keywords(object, pointer)?;
        }
        _ => {
            if object.contains_key("items") {
                return Err(format!("{pointer} 的 {schema_type} 不能含 items"));
            }
            reject_object_keywords(object, pointer)?;
        }
    }
    Ok(())
}

fn reject_object_keywords(
    object: &serde_json::Map<String, Value>,
    pointer: &str,
) -> Result<(), String> {
    for keyword in ["properties", "required", "additionalProperties"] {
        if object.contains_key(keyword) {
            return Err(format!("{pointer} 的当前类型不能含 {keyword}"));
        }
    }
    Ok(())
}

fn enforce_text_directory(out_root: &OutputRoot) -> Result<(), OutputContractError> {
    if out_root.exists(Path::new(SCHEMA_RECORD)) {
        return Err(directory_error(
            out_root.path(),
            "存在 output-schema.json，不能以文本模式继续",
        ));
    }
    if let Some(record) = numbered_record(out_root, "json")? {
        return Err(directory_error(
            out_root.path(),
            &format!("存在结构化完成记录 {}", record.display()),
        ));
    }
    Ok(())
}

fn validate_structured_directory(
    out_root: &OutputRoot,
    schema: &Value,
) -> Result<(), OutputContractError> {
    if let Some(record) = numbered_record(out_root, "md")? {
        return Err(directory_error(
            out_root.path(),
            &format!("存在文本完成记录 {}", record.display()),
        ));
    }
    let record = Path::new(SCHEMA_RECORD);
    let display_record = out_root.display(record);
    if out_root.exists(record) {
        let existing = out_root
            .read(record)
            .map_err(|source| OutputContractError::Read {
                path: display_record.clone(),
                source,
            })?;
        let existing: Value =
            serde_json::from_slice(&existing).map_err(|_| OutputContractError::Directory {
                path: out_root.path().to_path_buf(),
                reason: "现有 output-schema.json 不是合法 JSON".into(),
            })?;
        if &existing != schema {
            return Err(directory_error(
                out_root.path(),
                "现有 output-schema.json 与本次 schema 不同",
            ));
        }
        return Ok(());
    }
    if numbered_record(out_root, "json")?.is_some() {
        return Err(directory_error(
            out_root.path(),
            "已有结构化完成记录但缺少 output-schema.json，无法确认其契约",
        ));
    }
    Ok(())
}

fn publish_structured_schema(
    out_root: &OutputRoot,
    schema: &Value,
    pretty_schema: &str,
) -> Result<(), OutputContractError> {
    // 发布前重新确认目录仍与已准备的契约一致，避免校验后并发变化被覆盖。
    validate_structured_directory(out_root, schema)?;
    let record = Path::new(SCHEMA_RECORD);
    if out_root.exists(record) {
        return Ok(());
    }
    let temporary = Path::new(".tmp-output-schema");
    out_root
        .write(temporary, pretty_schema)
        .map_err(|source| OutputContractError::Write {
            path: out_root.display(temporary),
            source,
        })?;
    out_root
        .rename(temporary, record)
        .map_err(|source| OutputContractError::Write {
            path: out_root.display(record),
            source,
        })
}

fn numbered_record(
    out_root: &OutputRoot,
    extension: &str,
) -> Result<Option<PathBuf>, OutputContractError> {
    let entries =
        out_root
            .read_dir(Path::new("."))
            .map_err(|source| OutputContractError::Read {
                path: out_root.path().to_path_buf(),
                source,
            })?;
    for entry in entries {
        let entry = entry.map_err(|source| OutputContractError::Read {
            path: out_root.path().to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        if entry.file_type().is_ok_and(|kind| kind.is_file())
            && Path::new(&name)
                .extension()
                .and_then(|value| value.to_str())
                == Some(extension)
            && Path::new(&name)
                .file_stem()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.parse::<u64>().is_ok_and(|unit| unit > 0))
        {
            return Ok(Some(out_root.display(Path::new(&name))));
        }
    }
    Ok(None)
}

fn directory_error(out_dir: &Path, reason: &str) -> OutputContractError {
    OutputContractError::Directory {
        path: out_dir.to_path_buf(),
        reason: reason.into(),
    }
}

fn escape_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn display_pointer(pointer: &str) -> &str {
    if pointer.is_empty() { "/" } else { pointer }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_schema() -> Value {
        serde_json::json!({
            "type":"object",
            "properties":{
                "answer":{"type":"string"},
                "facts":{"type":"array","items":{"type":"string"}}
            },
            "required":["answer","facts"],
            "additionalProperties":false
        })
    }

    #[test]
    fn common_subset_accepts_nested_objects_and_rejects_refs() {
        assert!(validate_subset(&valid_schema()).is_ok());
        let mut invalid = valid_schema();
        invalid["properties"]["answer"] = serde_json::json!({"$ref":"other.json"});
        assert!(validate_subset(&invalid).unwrap_err().contains("$ref"));
    }

    #[test]
    fn validator_reports_both_paths() {
        let directory = tempfile::tempdir().unwrap();
        let schema_path = directory.path().join("schema.json");
        fs::write(&schema_path, serde_json::to_vec(&valid_schema()).unwrap()).unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let out_root = OutputRoot::open(out).unwrap();
        let contract = OutputContract::prepare(Some(&schema_path), &out_root).unwrap();
        let issue = contract
            .validate_submission(&serde_json::json!({"answer":1,"facts":[]}))
            .unwrap_err();
        assert!(issue.instance_path.contains("answer"));
        assert!(issue.schema_path.contains("type"));
    }

    #[test]
    fn compiled_contract_and_job_identity_share_one_schema_read() {
        let directory = tempfile::tempdir().unwrap();
        let schema_path = directory.path().join("schema.json");
        let source = serde_json::to_vec(&valid_schema()).unwrap();
        fs::write(&schema_path, &source).unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let out_root = OutputRoot::open(out.clone()).unwrap();

        let contract = OutputContract::prepare(Some(&schema_path), &out_root).unwrap();
        fs::write(
            &schema_path,
            r#"{"type":"object","properties":{},"required":[],"additionalProperties":false}"#,
        )
        .unwrap();

        assert_eq!(contract.source_bytes(), Some(source.as_slice()));
        contract.publish_schema_record(&out_root).unwrap();
        let published: Value =
            serde_json::from_slice(&fs::read(out.join(SCHEMA_RECORD)).unwrap()).unwrap();
        assert_eq!(published, valid_schema());
    }

    #[test]
    fn directory_cannot_mix_modes_or_schemas() {
        let directory = tempfile::tempdir().unwrap();
        let schema_path = directory.path().join("schema.json");
        fs::write(&schema_path, serde_json::to_vec(&valid_schema()).unwrap()).unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        fs::write(out.join("1.md"), "text").unwrap();
        let out_root = OutputRoot::open(out.clone()).unwrap();
        assert!(OutputContract::prepare(Some(&schema_path), &out_root).is_err());
        fs::remove_file(out.join("1.md")).unwrap();
        let contract = OutputContract::prepare(Some(&schema_path), &out_root).unwrap();
        assert!(!out.join(SCHEMA_RECORD).exists());
        contract.publish_schema_record(&out_root).unwrap();
        assert!(OutputContract::prepare(None, &out_root).is_err());
        let other = serde_json::json!({
            "type":"object","properties":{},"required":[],"additionalProperties":false
        });
        fs::write(&schema_path, serde_json::to_vec(&other).unwrap()).unwrap();
        assert!(OutputContract::prepare(Some(&schema_path), &out_root).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn schema_record_stays_with_opened_output_root_after_path_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let schema_path = directory.path().join("schema.json");
        fs::write(&schema_path, serde_json::to_vec(&valid_schema()).unwrap()).unwrap();
        let ambient = directory.path().join("out");
        let opened_directory = directory.path().join("opened-out");
        fs::create_dir(&ambient).unwrap();
        let out_root = OutputRoot::open(ambient.clone()).unwrap();

        fs::rename(&ambient, &opened_directory).unwrap();
        fs::create_dir(&ambient).unwrap();
        let contract = OutputContract::prepare(Some(&schema_path), &out_root).unwrap();
        assert!(!opened_directory.join(SCHEMA_RECORD).exists());
        contract.publish_schema_record(&out_root).unwrap();

        assert!(opened_directory.join(SCHEMA_RECORD).exists());
        assert!(!ambient.join(SCHEMA_RECORD).exists());
    }
}
