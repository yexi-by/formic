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

#[derive(Debug, Clone)]
pub struct ValidationIssues {
    issues: Vec<ValidationIssue>,
}

impl ValidationIssues {
    pub fn issues(&self) -> &[ValidationIssue] {
        &self.issues
    }
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

impl std::fmt::Display for ValidationIssues {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.issues.len() == 1 {
            return self.issues[0].fmt(formatter);
        }
        write!(formatter, "结果需要修正 {} 个格式问题：", self.issues.len())?;
        for (index, issue) in self.issues.iter().enumerate() {
            write!(formatter, "\n{}. {issue}", index + 1)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationIssues {}

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

    pub fn validate_submission(&self, value: &Value) -> Result<String, ValidationIssues> {
        let Self::Structured(contract) = self else {
            unreachable!("文本模式没有结构化提交")
        };
        if let Some(issues) = validation_issues(&contract.validator, value) {
            return Err(issues);
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
                if let Some(issues) = validation_issues(&contract.validator, &value) {
                    return Err(format!("完成记录不符合当前 schema：{issues}"));
                }
                Ok(())
            }
        }
    }
}

fn validation_issues(validator: &jsonschema::Validator, value: &Value) -> Option<ValidationIssues> {
    let mut issues: Vec<_> = validator
        .iter_errors(value)
        .map(|error| ValidationIssue {
            instance_path: error.instance_path().to_string(),
            schema_path: error.schema_path().to_string(),
            reason: error.to_string(),
        })
        .collect();
    issues.sort_by(|left, right| {
        (&left.instance_path, &left.schema_path, &left.reason).cmp(&(
            &right.instance_path,
            &right.schema_path,
            &right.reason,
        ))
    });
    (!issues.is_empty()).then_some(ValidationIssues { issues })
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

#[derive(Clone, Copy)]
enum SchemaType {
    Object,
    Array,
    String,
    Number,
    Integer,
    Boolean,
    Null,
}

impl SchemaType {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "object" => Some(Self::Object),
            "array" => Some(Self::Array),
            "string" => Some(Self::String),
            "number" => Some(Self::Number),
            "integer" => Some(Self::Integer),
            "boolean" => Some(Self::Boolean),
            "null" => Some(Self::Null),
            _ => None,
        }
    }

    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
            Self::String => value.is_string(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_number().is_some_and(|number| {
                number.is_i64()
                    || number.is_u64()
                    || number.as_f64().is_some_and(|number| number.fract() == 0.0)
            }),
            Self::Boolean => value.is_boolean(),
            Self::Null => value.is_null(),
        }
    }
}

#[derive(Clone, Copy)]
struct DeclaredType {
    base: SchemaType,
    nullable: bool,
}

impl DeclaredType {
    fn accepts(self, value: &Value) -> bool {
        (self.nullable && value.is_null()) || self.base.accepts(value)
    }
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
        "const",
        "description",
        "title",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minimum",
        "maximum",
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
    let declared_type = parse_declared_type(
        object
            .get("type")
            .ok_or_else(|| format!("{pointer}/type 是必填项"))?,
        pointer,
    )?;
    let enum_values = if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| format!("{pointer}/enum 必须是非空数组"))?;
        for value in values {
            validate_fixed_value(value, declared_type, &format!("{pointer}/enum"))?;
        }
        Some(values.as_slice())
    } else {
        None
    };
    let constant = object.get("const");
    if let Some(value) = constant {
        validate_fixed_value(value, declared_type, &format!("{pointer}/const"))?;
    }

    match declared_type.base {
        SchemaType::Object => {
            reject_keywords(
                object,
                pointer,
                &[
                    "items",
                    "minItems",
                    "maxItems",
                    "minLength",
                    "maxLength",
                    "minimum",
                    "maximum",
                ],
            )?;
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
        }
        SchemaType::Array => {
            reject_keywords(
                object,
                pointer,
                &[
                    "properties",
                    "required",
                    "additionalProperties",
                    "minLength",
                    "maxLength",
                    "minimum",
                    "maximum",
                ],
            )?;
            let items = object
                .get("items")
                .ok_or_else(|| format!("{pointer}/items 对 array 是必填项"))?;
            validate_node(items, &format!("{pointer}/items"))?;
            validate_ordered_nonnegative_integer_keywords(object, pointer, "minItems", "maxItems")?;
        }
        SchemaType::String => {
            reject_keywords(
                object,
                pointer,
                &[
                    "properties",
                    "required",
                    "additionalProperties",
                    "items",
                    "minItems",
                    "maxItems",
                    "minimum",
                    "maximum",
                ],
            )?;
            validate_ordered_nonnegative_integer_keywords(
                object,
                pointer,
                "minLength",
                "maxLength",
            )?;
        }
        SchemaType::Number | SchemaType::Integer => {
            reject_keywords(
                object,
                pointer,
                &[
                    "properties",
                    "required",
                    "additionalProperties",
                    "items",
                    "minItems",
                    "maxItems",
                    "minLength",
                    "maxLength",
                ],
            )?;
            validate_number_bounds(object, pointer)?;
        }
        SchemaType::Boolean | SchemaType::Null => {
            reject_keywords(
                object,
                pointer,
                &[
                    "properties",
                    "required",
                    "additionalProperties",
                    "items",
                    "minItems",
                    "maxItems",
                    "minLength",
                    "maxLength",
                    "minimum",
                    "maximum",
                ],
            )?;
        }
    }
    validate_fixed_value_constraints(schema, pointer, enum_values, constant)?;
    Ok(())
}

fn parse_declared_type(value: &Value, pointer: &str) -> Result<DeclaredType, String> {
    if let Some(name) = value.as_str() {
        let base = SchemaType::parse(name)
            .ok_or_else(|| format!("{pointer}/type 的值 {name:?} 不受支持"))?;
        return Ok(DeclaredType {
            base,
            nullable: false,
        });
    }
    let Some(names) = value.as_array() else {
        return Err(format!(
            "{pointer}/type 必须是基础类型字符串，或一个非 null 类型与 null 组成的二元数组"
        ));
    };
    if names.len() != 2 || names.iter().any(|name| !name.is_string()) {
        return Err(format!(
            "{pointer}/type 必须由一个非 null 类型与 null 组成二元字符串数组"
        ));
    }
    let first = names[0].as_str().expect("已确认 type 数组元素是字符串");
    let second = names[1].as_str().expect("已确认 type 数组元素是字符串");
    let base_name = match (first, second) {
        ("null", base) if base != "null" => base,
        (base, "null") if base != "null" => base,
        _ => {
            return Err(format!(
                "{pointer}/type 必须恰好包含一个非 null 类型和 null"
            ));
        }
    };
    let base = SchemaType::parse(base_name)
        .filter(|schema_type| !matches!(schema_type, SchemaType::Null))
        .ok_or_else(|| format!("{pointer}/type 的值 {base_name:?} 不受支持"))?;
    Ok(DeclaredType {
        base,
        nullable: true,
    })
}

fn validate_fixed_value(
    value: &Value,
    declared_type: DeclaredType,
    pointer: &str,
) -> Result<(), String> {
    if value.is_array() || value.is_object() {
        return Err(format!("{pointer} 只能使用基础 JSON 值"));
    }
    if !declared_type.accepts(value) {
        return Err(format!("{pointer} 的值 {value} 与声明的 type 不相容"));
    }
    Ok(())
}

fn reject_keywords(
    object: &serde_json::Map<String, Value>,
    pointer: &str,
    keywords: &[&str],
) -> Result<(), String> {
    for keyword in keywords {
        if object.contains_key(*keyword) {
            return Err(format!("{pointer}/{} 只适用于对应的声明类型", keyword));
        }
    }
    Ok(())
}

fn validate_ordered_nonnegative_integer_keywords(
    object: &serde_json::Map<String, Value>,
    pointer: &str,
    minimum_keyword: &str,
    maximum_keyword: &str,
) -> Result<(), String> {
    let minimum = nonnegative_integer_keyword(object, pointer, minimum_keyword)?;
    let maximum = nonnegative_integer_keyword(object, pointer, maximum_keyword)?;
    if minimum
        .zip(maximum)
        .is_some_and(|(minimum, maximum)| number_is_greater(minimum, maximum))
    {
        return Err(format!(
            "{pointer}/{minimum_keyword} 必须小于或等于 {maximum_keyword}"
        ));
    }
    Ok(())
}

fn nonnegative_integer_keyword<'a>(
    object: &'a serde_json::Map<String, Value>,
    pointer: &str,
    keyword: &str,
) -> Result<Option<&'a serde_json::Number>, String> {
    let Some(value) = object.get(keyword) else {
        return Ok(None);
    };
    let number = value.as_number().filter(|number| {
        number.as_i64().is_some_and(|number| number >= 0)
            || number.as_u64().is_some()
            || number
                .as_f64()
                .is_some_and(|number| number >= 0.0 && number.fract() == 0.0)
    });
    number
        .map(Some)
        .ok_or_else(|| format!("{pointer}/{keyword} 必须是非负整数"))
}

fn validate_number_bounds(
    object: &serde_json::Map<String, Value>,
    pointer: &str,
) -> Result<(), String> {
    let minimum = number_keyword(object, pointer, "minimum")?;
    let maximum = number_keyword(object, pointer, "maximum")?;
    if minimum
        .zip(maximum)
        .is_some_and(|(minimum, maximum)| number_is_greater(minimum, maximum))
    {
        return Err(format!("{pointer}/minimum 必须小于或等于 maximum"));
    }
    Ok(())
}

fn number_keyword<'a>(
    object: &'a serde_json::Map<String, Value>,
    pointer: &str,
    keyword: &str,
) -> Result<Option<&'a serde_json::Number>, String> {
    let Some(value) = object.get(keyword) else {
        return Ok(None);
    };
    value
        .as_number()
        .map(Some)
        .ok_or_else(|| format!("{pointer}/{keyword} 必须是数字"))
}

fn number_is_greater(left: &serde_json::Number, right: &serde_json::Number) -> bool {
    match (integer_value(left), integer_value(right)) {
        (Some(left), Some(right)) => left > right,
        _ => {
            left.as_f64().expect("JSON 数字可表示为有限 f64")
                > right.as_f64().expect("JSON 数字可表示为有限 f64")
        }
    }
}

fn integer_value(number: &serde_json::Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
        .or_else(|| {
            let value = number.as_f64()?;
            (value.fract() == 0.0 && value >= i128::MIN as f64 && value < i128::MAX as f64)
                .then_some(value as i128)
        })
}

fn validate_fixed_value_constraints(
    schema: &Value,
    pointer: &str,
    enum_values: Option<&[Value]>,
    constant: Option<&Value>,
) -> Result<(), String> {
    if enum_values.is_none() && constant.is_none() {
        return Ok(());
    }
    let validator = jsonschema::validator_for(schema)
        .map_err(|error| format!("{pointer} 的固定值约束无法编译：{error}"))?;
    if constant.is_some_and(|value| !validator.is_valid(value)) {
        return Err(format!(
            "{pointer}/const 必须同时满足当前节点的 type、enum 和范围约束"
        ));
    }
    if enum_values.is_some_and(|values| !values.iter().any(|value| validator.is_valid(value))) {
        return Err(format!(
            "{pointer}/enum 至少需要一个能满足当前节点其他约束的值"
        ));
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

    fn schema_with_property(property: Value) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {"value": property},
            "required": ["value"],
            "additionalProperties": false
        })
    }

    fn contract_for_schema(schema: &Value) -> OutputContract {
        let directory = tempfile::tempdir().unwrap();
        let schema_path = directory.path().join("schema.json");
        fs::write(&schema_path, serde_json::to_vec(schema).unwrap()).unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let out_root = OutputRoot::open(out).unwrap();
        OutputContract::prepare(Some(&schema_path), &out_root).unwrap()
    }

    #[test]
    fn common_subset_accepts_nested_objects_and_rejects_refs() {
        assert!(validate_subset(&valid_schema()).is_ok());
        let mut invalid = valid_schema();
        invalid["properties"]["answer"] = serde_json::json!({"$ref":"other.json"});
        assert!(validate_subset(&invalid).unwrap_err().contains("$ref"));
    }

    #[test]
    fn validator_reports_every_submission_issue_and_published_record_issue() {
        let contract = contract_for_schema(&valid_schema());
        let invalid = serde_json::json!({"answer":1,"extra":true});
        let issues = contract.validate_submission(&invalid).unwrap_err();
        assert_eq!(issues.issues().len(), 3);
        assert!(issues.issues().iter().any(|issue| {
            issue.instance_path.contains("answer") && issue.schema_path.contains("type")
        }));
        assert!(
            issues
                .issues()
                .iter()
                .any(|issue| issue.schema_path.contains("required"))
        );
        assert!(
            issues
                .issues()
                .iter()
                .any(|issue| { issue.schema_path.contains("additionalProperties") })
        );
        let display = issues.to_string();
        assert!(display.contains("3 个格式问题"), "{display}");

        let published_error = contract
            .validate_published_record(&serde_json::to_vec(&invalid).unwrap())
            .unwrap_err();
        for issue in issues.issues() {
            assert!(
                published_error.contains(&issue.to_string()),
                "{published_error}"
            );
        }
    }

    #[test]
    fn subset_accepts_nullable_types_const_and_common_ranges() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "review"},
                "title": {
                    "type": ["null", "string"],
                    "minLength": 1,
                    "maxLength": 120
                },
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "evidence": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "maxItems": 10
                }
            },
            "required": ["kind", "title", "confidence", "evidence"],
            "additionalProperties": false
        });
        assert!(validate_subset(&schema).is_ok());
        let contract = contract_for_schema(&schema);
        assert!(
            contract
                .validate_submission(&serde_json::json!({
                    "kind": "review",
                    "title": null,
                    "confidence": 0.8,
                    "evidence": ["source"]
                }))
                .is_ok()
        );

        let issues = contract
            .validate_submission(&serde_json::json!({
                "kind": "draft",
                "title": "",
                "confidence": 2,
                "evidence": []
            }))
            .unwrap_err();
        for keyword in ["const", "minLength", "maximum", "minItems"] {
            assert!(
                issues
                    .issues()
                    .iter()
                    .any(|issue| issue.schema_path.contains(keyword)),
                "缺少 {keyword} 校验问题：{issues}"
            );
        }
    }

    #[test]
    fn nullable_type_requires_one_supported_non_null_type_and_null() {
        let nullable_nodes = [
            serde_json::json!({
                "type": ["object", "null"],
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            serde_json::json!({
                "type": ["null", "array"],
                "items": {"type": "string"}
            }),
            serde_json::json!({"type": ["string", "null"]}),
            serde_json::json!({"type": ["null", "number"]}),
            serde_json::json!({"type": ["integer", "null"]}),
            serde_json::json!({"type": ["null", "boolean"]}),
        ];
        for node in nullable_nodes {
            assert!(validate_subset(&schema_with_property(node)).is_ok());
        }

        for node in [
            serde_json::json!({"type": ["string"]}),
            serde_json::json!({"type": ["null", "null"]}),
            serde_json::json!({"type": ["string", "number"]}),
            serde_json::json!({"type": ["string", "null", "number"]}),
            serde_json::json!({"type": ["unknown", "null"]}),
            serde_json::json!({"type": ["string", 1]}),
        ] {
            let error = validate_subset(&schema_with_property(node)).unwrap_err();
            assert!(error.contains("/type"), "{error}");
        }

        let nullable_root = serde_json::json!({
            "type": ["object", "null"],
            "properties": {},
            "required": [],
            "additionalProperties": false
        });
        assert!(
            validate_subset(&nullable_root)
                .unwrap_err()
                .contains("根 schema")
        );
    }

    #[test]
    fn subset_checks_range_keyword_placement_types_and_order() {
        let invalid_nodes = [
            (
                serde_json::json!({"type": "string", "minItems": 1}),
                "minItems",
            ),
            (
                serde_json::json!({
                    "type": "array",
                    "items": {"type": "string"},
                    "minLength": 1
                }),
                "minLength",
            ),
            (
                serde_json::json!({"type": "number", "minimum": "0"}),
                "minimum",
            ),
            (
                serde_json::json!({"type": "string", "minLength": -1}),
                "minLength",
            ),
            (
                serde_json::json!({
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 1.5
                }),
                "maxItems",
            ),
            (
                serde_json::json!({"type": "string", "minLength": 2, "maxLength": 1}),
                "minLength",
            ),
            (
                serde_json::json!({
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 2,
                    "maxItems": 1
                }),
                "minItems",
            ),
            (
                serde_json::json!({"type": "integer", "minimum": 2, "maximum": 1}),
                "minimum",
            ),
            (
                serde_json::json!({
                    "type": "number",
                    "minimum": 9_007_199_254_740_993_u64,
                    "maximum": 9_007_199_254_740_992_u64
                }),
                "minimum",
            ),
            (
                serde_json::json!({
                    "type": "number",
                    "minimum": 9_007_199_254_740_993_u64,
                    "maximum": 9_007_199_254_740_992.0
                }),
                "minimum",
            ),
        ];
        for (node, keyword) in invalid_nodes {
            let error = validate_subset(&schema_with_property(node)).unwrap_err();
            assert!(error.contains(keyword), "{error}");
        }
    }

    #[test]
    fn subset_checks_enum_and_const_against_type_and_other_constraints() {
        for valid in [
            serde_json::json!({
                "type": ["string", "null"],
                "enum": ["ready", null],
                "minLength": 2
            }),
            serde_json::json!({"type": "integer", "const": 1.0}),
            serde_json::json!({"type": "number", "enum": [-1, 2], "minimum": 0}),
            serde_json::json!({
                "type": ["object", "null"],
                "properties": {},
                "required": [],
                "additionalProperties": false,
                "const": null
            }),
        ] {
            assert!(validate_subset(&schema_with_property(valid)).is_ok());
        }

        for invalid in [
            serde_json::json!({"type": "string", "enum": ["ready", 1]}),
            serde_json::json!({"type": "integer", "const": 1.5}),
            serde_json::json!({"type": "string", "const": []}),
            serde_json::json!({"type": "string", "enum": ["draft"], "const": "review"}),
            serde_json::json!({"type": "string", "const": "x", "minLength": 2}),
            serde_json::json!({"type": "number", "enum": [-2, -1], "minimum": 0}),
        ] {
            assert!(validate_subset(&schema_with_property(invalid)).is_err());
        }
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
