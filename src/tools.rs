//! Formic 内置只读工具。这里唯一拥有参数语义、路径边界、结果截断和缓存键规范化。

use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, SearcherBuilder, Sink, SinkMatch};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::config::{ReadToolConfig, SearchToolConfig, ToolsConfig};
use crate::llm::ToolSpec;
use crate::output::RecordFormat;

/// 两棵只读根：input 是输入数据集；output 是已完成单元记录所在目录。
#[derive(Clone)]
pub struct Roots {
    pub input: PathBuf,
    pub output: PathBuf,
    pub output_format: RecordFormat,
}

#[derive(Debug, Clone)]
pub enum BuiltinTool {
    Search(SearchToolConfig),
    Read(ReadToolConfig),
}

pub struct BuiltinRegistration {
    pub spec: ToolSpec,
    pub executor: BuiltinTool,
    pub max_in_flight: usize,
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub content: String,
    pub cacheable: bool,
}

impl ToolOutput {
    fn success(content: String, truncated: bool, cacheable: bool) -> Self {
        Self {
            content,
            cacheable: cacheable && !truncated,
        }
    }

    fn error(message: impl std::fmt::Display) -> Self {
        Self {
            content: format!("错误：{message}"),
            cacheable: false,
        }
    }
}

pub fn registrations(config: &ToolsConfig) -> Vec<BuiltinRegistration> {
    let mut registrations = Vec::new();
    if config.read.enabled {
        registrations.push(BuiltinRegistration {
            spec: read_spec(),
            executor: BuiltinTool::Read(config.read.clone()),
            max_in_flight: config.read.max_in_flight,
        });
    }
    if config.search.enabled {
        registrations.push(BuiltinRegistration {
            spec: search_spec(config.search.max_context_lines),
            executor: BuiltinTool::Search(config.search.clone()),
            max_in_flight: config.search.max_in_flight,
        });
    }
    registrations.sort_by(|left, right| left.spec.name.cmp(&right.spec.name));
    registrations
}

fn search_spec(max_context_lines: usize) -> ToolSpec {
    ToolSpec {
        name: "search".into(),
        description:
            "在只读 input 数据集或当前模式的 output 完成记录中搜索文本；结果截断时有明确标记。"
                .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "正则表达式；literal 为 true 时按字面量匹配"},
                "scope": {"type": "string", "enum": ["input", "output"]},
                "glob": {"type": "string", "description": "可选，根内 glob 过滤，如 **/*.txt"},
                "context": {"type": "integer", "minimum": 0, "maximum": max_context_lines},
                "literal": {"type": "boolean"}
            },
            "required": ["pattern", "scope"],
            "additionalProperties": false
        }),
    }
}

fn read_spec() -> ToolSpec {
    ToolSpec {
        name: "read".into(),
        description: "按根内相对路径读取 UTF-8 文本；行号从 1 开始，区间为闭区间。output 只允许读取当前模式的数字编号完成记录。".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "scope": {"type": "string", "enum": ["input", "output"]},
                "path": {"type": "string", "description": "根内相对路径"},
                "start_line": {"type": "integer", "minimum": 1},
                "end_line": {"type": "integer", "minimum": 1}
            },
            "required": ["scope", "path"],
            "additionalProperties": false
        }),
    }
}

impl BuiltinTool {
    pub fn canonical_cache_key(&self, arguments: &Value) -> Option<String> {
        match self {
            Self::Search(config) => parse_search(arguments, config)
                .ok()
                .filter(|args| args.scope == Scope::Input)
                .map(|args| serde_json::to_string(&args.canonical()).expect("规范参数可序列化")),
            Self::Read(_) => parse_read(arguments)
                .ok()
                .filter(|args| args.scope == Scope::Input)
                .map(|args| serde_json::to_string(&args.canonical()).expect("规范参数可序列化")),
        }
    }

    #[cfg(test)]
    pub fn execute(&self, roots: &Roots, arguments: &Value) -> ToolOutput {
        self.execute_cancellable(roots, arguments, &CancellationToken::new())
    }

    pub fn execute_cancellable(
        &self,
        roots: &Roots,
        arguments: &Value,
        cancel: &CancellationToken,
    ) -> ToolOutput {
        match self {
            Self::Search(config) => match parse_search(arguments, config) {
                Ok(args) => search(roots, config, &args, cancel)
                    .unwrap_or_else(|message| ToolOutput::error(message)),
                Err(message) => ToolOutput::error(message),
            },
            Self::Read(config) => match parse_read(arguments) {
                Ok(args) => read(roots, config, &args, cancel)
                    .unwrap_or_else(|message| ToolOutput::error(message)),
                Err(message) => ToolOutput::error(message),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Input,
    Output,
}

impl Scope {
    fn parse(value: &Value) -> Result<Self, String> {
        match value.as_str() {
            Some("input") => Ok(Self::Input),
            Some("output") => Ok(Self::Output),
            _ => Err("scope 必须是 input 或 output".into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

struct SearchArgs {
    pattern: String,
    scope: Scope,
    glob: Option<String>,
    context: usize,
    literal: bool,
}

impl SearchArgs {
    fn canonical(&self) -> Value {
        serde_json::json!({
            "context": self.context,
            "glob": self.glob,
            "literal": self.literal,
            "pattern": self.pattern,
            "scope": self.scope.name(),
        })
    }
}

fn parse_search(arguments: &Value, config: &SearchToolConfig) -> Result<SearchArgs, String> {
    let object = arguments
        .as_object()
        .ok_or_else(|| "工具参数必须是 JSON object".to_string())?;
    reject_unknown(object, &["pattern", "scope", "glob", "context", "literal"])?;
    let pattern = object
        .get("pattern")
        .and_then(Value::as_str)
        .ok_or_else(|| "缺少字符串参数 pattern".to_string())?;
    let scope = Scope::parse(object.get("scope").unwrap_or(&Value::Null))?;
    let glob = object
        .get("glob")
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "glob 必须是字符串".to_string())
        })
        .transpose()?;
    let context = object
        .get("context")
        .map(Value::as_u64)
        .transpose_option("context 必须是非负整数")?
        .unwrap_or(0) as usize;
    if context > config.max_context_lines {
        return Err(format!(
            "context 不能超过配置的 {} 行",
            config.max_context_lines
        ));
    }
    let literal = object
        .get("literal")
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| "literal 必须是布尔值".to_string())
        })
        .transpose()?
        .unwrap_or(false);
    Ok(SearchArgs {
        pattern: pattern.to_string(),
        scope,
        glob,
        context,
        literal,
    })
}

struct ReadArgs {
    scope: Scope,
    path: PathBuf,
    start_line: u64,
    end_line: Option<u64>,
}

impl ReadArgs {
    fn canonical(&self) -> Value {
        serde_json::json!({
            "end_line": self.end_line,
            "path": crate::prompt::slash_path(&self.path),
            "scope": self.scope.name(),
            "start_line": self.start_line,
        })
    }
}

fn parse_read(arguments: &Value) -> Result<ReadArgs, String> {
    let object = arguments
        .as_object()
        .ok_or_else(|| "工具参数必须是 JSON object".to_string())?;
    reject_unknown(object, &["scope", "path", "start_line", "end_line"])?;
    let scope = Scope::parse(object.get("scope").unwrap_or(&Value::Null))?;
    let raw_path = object
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| "缺少字符串参数 path".to_string())?;
    let path = safe_relative_path(raw_path)?;
    let start_line = positive_integer(object.get("start_line"), "start_line")?.unwrap_or(1);
    let end_line = positive_integer(object.get("end_line"), "end_line")?;
    if end_line.is_some_and(|end| end < start_line) {
        return Err("end_line 不能小于 start_line".into());
    }
    Ok(ReadArgs {
        scope,
        path,
        start_line,
        end_line,
    })
}

trait OptionalU64 {
    fn transpose_option(self, message: &str) -> Result<Option<u64>, String>;
}

impl OptionalU64 for Option<Option<u64>> {
    fn transpose_option(self, message: &str) -> Result<Option<u64>, String> {
        match self {
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Err(message.into()),
            None => Ok(None),
        }
    }
}

fn positive_integer(value: Option<&Value>, name: &str) -> Result<Option<u64>, String> {
    value
        .map(|value| {
            value
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or_else(|| format!("{name} 必须是正整数"))
        })
        .transpose()
}

fn reject_unknown(object: &serde_json::Map<String, Value>, allowed: &[&str]) -> Result<(), String> {
    if let Some(name) = object.keys().find(|name| !allowed.contains(&name.as_str())) {
        return Err(format!("未知参数 {name}"));
    }
    Ok(())
}

fn safe_relative_path(raw: &str) -> Result<PathBuf, String> {
    if raw.is_empty() {
        return Err("path 不能为空".into());
    }
    let path = Path::new(raw);
    if path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err("path 必须是根内相对路径，且不能包含 . 或 ..".into());
    }
    Ok(path.to_path_buf())
}

fn search(
    roots: &Roots,
    config: &SearchToolConfig,
    args: &SearchArgs,
    cancel: &CancellationToken,
) -> Result<ToolOutput, String> {
    let mut builder = RegexMatcherBuilder::new();
    if args.literal {
        builder.fixed_strings(true);
    }
    let matcher = builder
        .build(&args.pattern)
        .map_err(|error| format!("pattern 不是合法的模式：{error}"))?;
    let glob_matcher = args
        .glob
        .as_deref()
        .map(|glob| {
            globset::GlobBuilder::new(glob)
                .build()
                .map(|glob| glob.compile_matcher())
                .map_err(|error| format!("glob 不合法：{error}"))
        })
        .transpose()?;
    let root = scope_root(roots, args.scope);
    let files = scope_files(root, args.scope, roots.output_format, Some(cancel))
        .map_err(|error| format!("无法遍历根目录：{error}"))?;

    let mut out = String::new();
    let mut matches = 0usize;
    let mut truncated = None;
    'files: for relative in &files {
        if cancel.is_cancelled() {
            return Err("工具调用已取消".into());
        }
        if glob_matcher
            .as_ref()
            .is_some_and(|matcher| !matcher.is_match(relative))
        {
            continue;
        }
        let Ok(bytes) = fs::read(root.join(relative)) else {
            continue;
        };
        let mut sink = MatchSink {
            path: crate::prompt::slash_path(relative),
            context: args.context,
            max_matches: config.max_matches,
            max_bytes: config.max_result_bytes,
            header_written: false,
            out: &mut out,
            matches: &mut matches,
            truncated: &mut truncated,
            cancel,
        };
        let mut searcher = SearcherBuilder::new()
            .line_number(true)
            .before_context(args.context)
            .after_context(args.context)
            .binary_detection(BinaryDetection::quit(b'\0'))
            .build();
        let _ = searcher.search_reader(&matcher, &bytes[..], &mut sink);
        if truncated.is_some() {
            break 'files;
        }
    }

    if cancel.is_cancelled() {
        return Err("工具调用已取消".into());
    }

    if matches == 0 && truncated.is_none() {
        return Ok(ToolOutput::success(
            "无匹配。".into(),
            false,
            args.scope == Scope::Input,
        ));
    }
    if let Some(reason) = &truncated {
        apply_truncation_marker(&mut out, reason, config.max_result_bytes);
    }
    Ok(ToolOutput::success(
        out,
        truncated.is_some(),
        args.scope == Scope::Input,
    ))
}

fn read(
    roots: &Roots,
    config: &ReadToolConfig,
    args: &ReadArgs,
    cancel: &CancellationToken,
) -> Result<ToolOutput, String> {
    if args.scope == Scope::Output {
        validate_output_record(&args.path, roots.output_format)?;
    }
    let root = scope_root(roots, args.scope);
    let target = resolve_no_symlink(root, &args.path)?;
    let file = fs::File::open(&target).map_err(|error| {
        format!(
            "无法读取 {}：{error}",
            crate::prompt::slash_path(&args.path)
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut line_number = 0u64;
    let mut out = String::new();
    let mut truncated = false;
    loop {
        if cancel.is_cancelled() {
            return Err("工具调用已取消".into());
        }
        line.clear();
        let bytes = reader.read_line(&mut line).map_err(|error| {
            if error.kind() == io::ErrorKind::InvalidData {
                format!("{} 不是合法 UTF-8", crate::prompt::slash_path(&args.path))
            } else {
                format!(
                    "读取 {} 失败：{error}",
                    crate::prompt::slash_path(&args.path)
                )
            }
        })?;
        if bytes == 0 {
            break;
        }
        line_number += 1;
        let selected =
            line_number >= args.start_line && args.end_line.is_none_or(|end| line_number <= end);
        if selected && !truncated {
            let text = line.trim_end_matches(['\n', '\r']);
            let rendered = format!("{line_number}: {text}\n");
            if out.len() + rendered.len() > config.max_result_bytes {
                let remaining = config.max_result_bytes.saturating_sub(out.len());
                if remaining > 0 {
                    out.push_str(truncate_utf8(&rendered, remaining));
                }
                truncated = true;
            } else {
                out.push_str(&rendered);
            }
        }
    }
    if out.is_empty() && !truncated {
        out.push_str("无内容。");
    }
    if truncated {
        apply_truncation_marker(
            &mut out,
            &format!("结果达到 {} 字节上限", config.max_result_bytes),
            config.max_result_bytes,
        );
    }
    Ok(ToolOutput::success(
        out,
        truncated,
        args.scope == Scope::Input,
    ))
}

fn scope_root(roots: &Roots, scope: Scope) -> &Path {
    match scope {
        Scope::Input => &roots.input,
        Scope::Output => &roots.output,
    }
}

fn validate_output_record(path: &Path, format: RecordFormat) -> Result<(), String> {
    if path.components().count() != 1
        || path.extension().and_then(|value| value.to_str()) != Some(format.extension())
        || path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<u64>().ok())
            .is_none_or(|unit| unit == 0)
    {
        return Err(format!(
            "output 只允许读取顶层数字编号的 .{} 完成记录",
            format.extension()
        ));
    }
    Ok(())
}

fn resolve_no_symlink(root: &Path, relative: &Path) -> Result<PathBuf, String> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            format!(
                "路径 {} 不可用：{error}",
                crate::prompt::slash_path(relative)
            )
        })?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "路径 {} 包含符号链接",
                crate::prompt::slash_path(relative)
            ));
        }
    }
    if !current.is_file() {
        return Err(format!(
            "路径 {} 不是文件",
            crate::prompt::slash_path(relative)
        ));
    }
    let canonical_root =
        fs::canonicalize(root).map_err(|error| format!("无法解析根目录：{error}"))?;
    let canonical_target =
        fs::canonicalize(&current).map_err(|error| format!("无法解析文件路径：{error}"))?;
    if !canonical_target.starts_with(canonical_root) {
        return Err("路径逃逸出只读根目录".into());
    }
    Ok(current)
}

fn truncate_utf8(text: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn apply_truncation_marker(output: &mut String, reason: &str, max_bytes: usize) {
    let full = format!("\n[已截断：{reason}]\n");
    let marker = if full.len() <= max_bytes {
        full.as_str()
    } else if max_bytes >= "[cut]".len() {
        "[cut]"
    } else {
        "!"
    };
    let keep = max_bytes.saturating_sub(marker.len()).min(output.len());
    let keep = truncate_utf8(output, keep).len();
    output.truncate(keep);
    output.push_str(marker);
}

fn scope_files(
    root: &Path,
    scope: Scope,
    format: RecordFormat,
    cancel: Option<&CancellationToken>,
) -> io::Result<Vec<PathBuf>> {
    match scope {
        Scope::Input => walk_files_cancellable(root, cancel),
        Scope::Output => {
            let mut files = Vec::new();
            for entry in fs::read_dir(root)? {
                if cancel.is_some_and(CancellationToken::is_cancelled) {
                    break;
                }
                let entry = entry?;
                if entry.file_type()?.is_symlink() {
                    continue;
                }
                let path = entry.path();
                let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
                    continue;
                };
                if path.is_file()
                    && path.extension().and_then(|value| value.to_str()) == Some(format.extension())
                    && name.parse::<u64>().is_ok_and(|unit| unit > 0)
                {
                    files.push(path.strip_prefix(root).expect("目录项在根内").to_path_buf());
                }
            }
            files.sort();
            Ok(files)
        }
    }
}

/// 递归列出根内普通文件；符号链接不跟随。
pub fn walk_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    walk_files_cancellable(root, None)
}

fn walk_files_cancellable(
    root: &Path,
    cancel: Option<&CancellationToken>,
) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(directory)? {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Ok(files);
            }
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                continue;
            }
            let path = entry.path();
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() {
                files.push(
                    path.strip_prefix(root)
                        .expect("遍历结果在根内")
                        .to_path_buf(),
                );
            }
        }
    }
    files.sort();
    Ok(files)
}

struct MatchSink<'a> {
    path: String,
    context: usize,
    max_matches: usize,
    max_bytes: usize,
    header_written: bool,
    out: &'a mut String,
    matches: &'a mut usize,
    truncated: &'a mut Option<String>,
    cancel: &'a CancellationToken,
}

impl MatchSink<'_> {
    fn push_line(&mut self, line: u64, text: &str, matched: bool) -> bool {
        if self.cancel.is_cancelled() || self.truncated.is_some() {
            return false;
        }
        if matched {
            *self.matches += 1;
            if *self.matches > self.max_matches {
                *self.truncated = Some(format!("匹配数达到 {} 上限", self.max_matches));
                return false;
            }
        }
        let separator = if matched { ':' } else { '-' };
        let line_text = format!("{line}{separator} {text}\n");
        let header_bytes = (!self.header_written)
            .then_some(self.path.len() + 7)
            .unwrap_or(0);
        if self.out.len() + header_bytes + line_text.len() > self.max_bytes {
            *self.truncated = Some(format!("结果达到 {} 字节上限", self.max_bytes));
            return false;
        }
        if !self.header_written {
            self.out.push_str(&format!("== {} ==\n", self.path));
            self.header_written = true;
        }
        self.out.push_str(&line_text);
        true
    }
}

impl Sink for MatchSink<'_> {
    type Error = io::Error;

    fn matched(
        &mut self,
        _searcher: &grep_searcher::Searcher,
        matched: &SinkMatch,
    ) -> Result<bool, Self::Error> {
        Ok(self.push_line(
            matched.line_number().unwrap_or(0),
            String::from_utf8_lossy(matched.bytes()).trim_end_matches(['\n', '\r']),
            true,
        ))
    }

    fn context(
        &mut self,
        _searcher: &grep_searcher::Searcher,
        context: &grep_searcher::SinkContext,
    ) -> Result<bool, Self::Error> {
        Ok(self.push_line(
            context.line_number().unwrap_or(0),
            String::from_utf8_lossy(context.bytes()).trim_end_matches(['\n', '\r']),
            false,
        ))
    }

    fn context_break(&mut self, _searcher: &grep_searcher::Searcher) -> Result<bool, Self::Error> {
        if self.context > 0 && self.truncated.is_none() {
            if self.out.len() + 3 > self.max_bytes {
                *self.truncated = Some(format!("结果达到 {} 字节上限", self.max_bytes));
            } else {
                self.out.push_str("--\n");
            }
        }
        Ok(self.truncated.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _directory: tempfile::TempDir,
        roots: Roots,
        config: ToolsConfig,
    }

    fn fixture(format: RecordFormat) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("data");
        let output = directory.path().join("out");
        fs::create_dir_all(input.join("sub")).unwrap();
        fs::create_dir_all(output.join("workers").join("20260808T120000.000Z")).unwrap();
        fs::write(
            input.join("a.txt"),
            "苹果是水果。\n香蕉也是。\n橙子第三个。\n",
        )
        .unwrap();
        fs::write(input.join("sub").join("b.txt"), "这里有苹果树。\n").unwrap();
        fs::write(
            output.join(format!("1.{}", format.extension())),
            "完成记录提到苹果。\n",
        )
        .unwrap();
        fs::write(
            output
                .join("workers")
                .join("20260808T120000.000Z")
                .join("1.md"),
            "worker 档案提到苹果。\n",
        )
        .unwrap();
        let search = SearchToolConfig {
            enabled: true,
            max_result_bytes: 32 * 1024,
            max_in_flight: 4,
            max_matches: 100,
            max_context_lines: 20,
        };
        let read = ReadToolConfig {
            enabled: true,
            max_result_bytes: 32 * 1024,
            max_in_flight: 4,
        };
        Fixture {
            _directory: directory,
            roots: Roots {
                input,
                output,
                output_format: format,
            },
            config: ToolsConfig {
                max_in_flight: 4,
                search,
                read,
            },
        }
    }

    fn execute(fixture: &Fixture, name: &str, json: &str) -> ToolOutput {
        let value = serde_json::from_str(json).unwrap();
        registrations(&fixture.config)
            .into_iter()
            .find(|entry| entry.spec.name == name)
            .unwrap()
            .executor
            .execute(&fixture.roots, &value)
    }

    #[test]
    fn search_and_read_input() {
        let fixture = fixture(RecordFormat::Markdown);
        let found = execute(&fixture, "search", r#"{"pattern":"苹果","scope":"input"}"#);
        assert!(found.content.contains("== a.txt =="), "{}", found.content);
        assert!(found.cacheable);
        let read = execute(
            &fixture,
            "read",
            r#"{"scope":"input","path":"a.txt","start_line":2,"end_line":3}"#,
        );
        assert_eq!(read.content, "2: 香蕉也是。\n3: 橙子第三个。\n");
        assert!(read.cacheable);
    }

    #[test]
    fn output_sees_only_current_numbered_records() {
        let fixture = fixture(RecordFormat::Json);
        let found = execute(&fixture, "search", r#"{"pattern":"苹果","scope":"output"}"#);
        assert!(found.content.contains("1.json"));
        assert!(!found.content.contains("workers"));
        assert!(!found.cacheable);
        let denied = execute(
            &fixture,
            "read",
            r#"{"scope":"output","path":"workers/20260808T120000.000Z/1.md"}"#,
        );
        assert!(denied.content.starts_with("错误："));
    }

    #[test]
    fn read_rejects_escape_absolute_and_non_utf8() {
        let fixture = fixture(RecordFormat::Markdown);
        for path in ["../x", ".\\a.txt", "C:\\Windows\\win.ini"] {
            let output = execute(
                &fixture,
                "read",
                &serde_json::json!({"scope":"input","path":path}).to_string(),
            );
            assert!(
                output.content.starts_with("错误："),
                "{path}: {}",
                output.content
            );
        }
        fs::write(fixture.roots.input.join("bad.txt"), [0xff, 0xfe]).unwrap();
        let output = execute(&fixture, "read", r#"{"scope":"input","path":"bad.txt"}"#);
        assert!(output.content.contains("UTF-8"), "{}", output.content);
    }

    #[cfg(unix)]
    #[test]
    fn read_rejects_symlink() {
        use std::os::unix::fs::symlink;
        let fixture = fixture(RecordFormat::Markdown);
        symlink(
            fixture.roots.input.join("a.txt"),
            fixture.roots.input.join("link.txt"),
        )
        .unwrap();
        let output = execute(&fixture, "read", r#"{"scope":"input","path":"link.txt"}"#);
        assert!(output.content.contains("符号链接"));
    }

    #[test]
    fn configured_limits_are_enforced_and_marked() {
        let mut fixture = fixture(RecordFormat::Markdown);
        fixture.config.search.max_matches = 1;
        let found = execute(&fixture, "search", r#"{"pattern":"苹果","scope":"input"}"#);
        assert!(found.content.contains("匹配数达到 1 上限"));
        assert!(!found.cacheable);
        fixture.config.read.max_result_bytes = 8;
        let read = execute(&fixture, "read", r#"{"scope":"input","path":"a.txt"}"#);
        assert!(read.content.contains("[cut]"));
        assert!(read.content.len() <= 8);
        assert!(!read.cacheable);
    }

    #[test]
    fn canonical_keys_merge_omitted_defaults() {
        let fixture = fixture(RecordFormat::Markdown);
        let tool = registrations(&fixture.config)
            .into_iter()
            .find(|entry| entry.spec.name == "search")
            .unwrap()
            .executor;
        let short = serde_json::json!({"pattern":"x","scope":"input"});
        let explicit =
            serde_json::json!({"literal":false,"scope":"input","context":0,"pattern":"x"});
        assert_eq!(
            tool.canonical_cache_key(&short),
            tool.canonical_cache_key(&explicit)
        );
    }
}
