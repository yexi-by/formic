//! 内置工具：search（原型唯一一个，rg 是灵感不是契约，引擎是 grep crates 进程内嵌）。
//! 这里是工具语义的唯一所有者：入参形状、两棵只读根、遍历边界、出参硬边界与
//! 截断标记。工具级错误（非法正则、缺参数等）返回 `错误：...` 文本回注模型。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, SearcherBuilder, Sink, SinkMatch};
use serde_json::Value;

use crate::llm::ToolSpec;

/// 两棵只读根：input = 输入数据集；output = 已完成单元记录所在目录。
pub struct Roots {
    pub input: PathBuf,
    pub output: PathBuf,
}

/// 出参硬边界（内部参数，非配置项；第 5 轮规模实验可据实测修订）。
const MAX_MATCHES: usize = 100;
const MAX_TOTAL_BYTES: usize = 32 * 1024;
const MAX_CONTEXT_LINES: usize = 20;

/// search 的工具规格，走请求的 tools 字段。
pub fn search_spec() -> ToolSpec {
    ToolSpec {
        name: "search",
        description: "在两棵只读根内搜索文本：input 是整个输入数据集，output 是已完成单元的产出记录。\
            结果有大小上限，截断时显式标记。",
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "正则表达式；literal 为 true 时按字面量匹配"},
                "scope": {"type": "string", "enum": ["input", "output"], "description": "input=输入数据集；output=已完成单元记录"},
                "glob": {"type": "string", "description": "可选，根内 glob 过滤，如 **/*.txt"},
                "context": {"type": "integer", "description": "可选，每个匹配的前后上下文行数，0-20"},
                "literal": {"type": "boolean", "description": "可选，true 时 pattern 按字面量匹配"}
            },
            "required": ["pattern", "scope"],
            "additionalProperties": false
        }),
    }
}

/// 已注册工具的规格集：调度器是工具命名空间的唯一来源（只读 enforcement 由构造保证）。
pub fn registered_specs() -> Vec<ToolSpec> {
    vec![search_spec()]
}

/// 执行一个工具调用，返回模型可读的结果文本；工具级错误以 `错误：` 文本返回。
pub fn execute(roots: &Roots, name: &str, arguments: &Value) -> String {
    match name {
        "search" => search(roots, arguments).unwrap_or_else(|e| format!("错误：{e}")),
        other => format!("错误：未知工具 {other}，可用工具只有 search"),
    }
}

enum Scope {
    Input,
    Output,
}

fn search(roots: &Roots, args: &Value) -> Result<String, String> {
    let pattern = args["pattern"]
        .as_str()
        .ok_or_else(|| "缺少字符串参数 pattern".to_string())?;
    let scope = match args["scope"].as_str() {
        Some("input") => Scope::Input,
        Some("output") => Scope::Output,
        _ => return Err("scope 必须是 input 或 output".to_string()),
    };
    let literal = args["literal"].as_bool().unwrap_or(false);
    let context = args["context"]
        .as_u64()
        .unwrap_or(0)
        .min(MAX_CONTEXT_LINES as u64) as usize;
    let glob = args["glob"].as_str();

    let mut builder = RegexMatcherBuilder::new();
    if literal {
        builder.fixed_strings(true);
    }
    let matcher = builder
        .build(pattern)
        .map_err(|e| format!("pattern 不是合法的模式：{e}"))?;
    let glob_matcher = glob
        .map(|g| {
            globset::GlobBuilder::new(g)
                .build()
                .map(|g| g.compile_matcher())
                .map_err(|e| format!("glob 不合法：{e}"))
        })
        .transpose()?;

    let root = match scope {
        Scope::Input => &roots.input,
        Scope::Output => &roots.output,
    };
    let files = scope_files(root, &scope).map_err(|e| format!("无法遍历根目录：{e}"))?;

    let mut out = String::new();
    let mut matches = 0usize;
    let mut truncated: Option<String> = None;
    'files: for rel in &files {
        if let Some(g) = &glob_matcher
            && !g.is_match(rel)
        {
            continue;
        }
        let Ok(bytes) = fs::read(root.join(rel)) else {
            continue; // 读取失败的文件不参与本次搜索（根内容在搜索期间由运行时保证稳定）
        };
        let mut sink = MatchSink {
            path: crate::prompt::slash_path(rel),
            context,
            header_written: false,
            out: &mut out,
            matches: &mut matches,
            truncated: &mut truncated,
        };
        let mut searcher = SearcherBuilder::new()
            .line_number(true)
            .before_context(context)
            .after_context(context)
            .binary_detection(BinaryDetection::quit(b'\x00'))
            .build();
        let _ = searcher.search_reader(&matcher, &bytes[..], &mut sink);
        if truncated.is_some() {
            break 'files;
        }
    }

    if matches == 0 && truncated.is_none() {
        return Ok("无匹配。".to_string());
    }
    if let Some(reason) = truncated {
        out.push_str(&format!("[已截断：{reason}]\n"));
    }
    Ok(out)
}

/// 列出某棵根参与搜索的文件（根内相对路径）。
/// input：递归遍历，符号链接一律跳过；output：只取顶层 `<单元号>.md` 记录。
fn scope_files(root: &Path, scope: &Scope) -> io::Result<Vec<PathBuf>> {
    match scope {
        Scope::Input => walk_files(root),
        Scope::Output => {
            let mut out = Vec::new();
            for entry in fs::read_dir(root)? {
                let entry = entry?;
                if entry.file_type()?.is_symlink() {
                    continue;
                }
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|e| e == "md") {
                    if let Ok(rel) = path.strip_prefix(root) {
                        out.push(rel.to_path_buf());
                    }
                }
            }
            out.sort();
            Ok(out)
        }
    }
}

/// 递归列出根内全部文件（相对路径，排序）；符号链接不跟随——可见性边界由构造保证。
pub fn walk_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                continue;
            }
            let path = entry.path();
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() {
                out.push(
                    path.strip_prefix(root)
                        .expect("遍历结果必在根内")
                        .to_path_buf(),
                );
            }
        }
    }
    out.sort();
    Ok(out)
}

/// grep-searcher 的收集汇：格式化匹配行与上下文行，执行硬边界并记录截断。
/// 一个文件一个实例，文件头在首行写出。
struct MatchSink<'a> {
    path: String,
    context: usize,
    header_written: bool,
    out: &'a mut String,
    matches: &'a mut usize,
    truncated: &'a mut Option<String>,
}

impl MatchSink<'_> {
    /// 追加一行，返回是否应继续（触及硬边界时记截断并停止）。
    fn push_line(&mut self, line: u64, text: &str, matched: bool) -> bool {
        if self.truncated.is_some() {
            return false;
        }
        if matched {
            *self.matches += 1;
            if *self.matches > MAX_MATCHES {
                *self.truncated = Some(format!("匹配数达到 {MAX_MATCHES} 上限"));
                return false;
            }
        }
        let sep = if matched { ':' } else { '-' };
        let line_text = format!("{line}{sep} {text}\n");
        if self.out.len() + self.path.len() + line_text.len() > MAX_TOTAL_BYTES {
            *self.truncated = Some(format!("结果达到 {} KiB 上限", MAX_TOTAL_BYTES / 1024));
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
        mat: &SinkMatch,
    ) -> Result<bool, Self::Error> {
        let line = mat.line_number().unwrap_or(0);
        let text = String::from_utf8_lossy(mat.bytes());
        Ok(self.push_line(line, text.trim_end_matches(['\n', '\r']), true))
    }

    fn context(
        &mut self,
        _searcher: &grep_searcher::Searcher,
        ctx: &grep_searcher::SinkContext,
    ) -> Result<bool, Self::Error> {
        let line = ctx.line_number().unwrap_or(0);
        let text = String::from_utf8_lossy(ctx.bytes());
        Ok(self.push_line(line, text.trim_end_matches(['\n', '\r']), false))
    }

    fn context_break(&mut self, _searcher: &grep_searcher::Searcher) -> Result<bool, Self::Error> {
        if self.context > 0 && self.truncated.is_none() {
            self.out.push_str("--\n");
        }
        Ok(self.truncated.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _dir: tempfile::TempDir,
        roots: Roots,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("data");
        let output = dir.path().join("out");
        fs::create_dir_all(input.join("sub")).unwrap();
        fs::create_dir_all(output.join("audit")).unwrap();
        fs::write(
            input.join("a.txt"),
            "苹果是水果。\n香蕉也是。\n橙子第三个。\n",
        )
        .unwrap();
        fs::write(input.join("sub").join("b.txt"), "这里有苹果树。\n").unwrap();
        fs::write(output.join("1.md"), "已完成的产出提到苹果。\n").unwrap();
        fs::write(
            output.join("audit").join("1.jsonl"),
            "{\"direction\":\"request\"}\n",
        )
        .unwrap();
        Fixture {
            _dir: dir,
            roots: Roots { input, output },
        }
    }

    fn args(v: &str) -> Value {
        serde_json::from_str(v).unwrap()
    }

    #[test]
    fn regex_search_across_root() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"苹果\",\"scope\":\"input\"}"),
        );
        assert!(r.contains("== a.txt =="), "{r}");
        assert!(r.contains("1: 苹果是水果。"), "{r}");
        assert!(r.contains("== sub/b.txt =="), "{r}");
        assert!(r.contains("这里有苹果树。"), "{r}");
        assert!(!r.contains("已截断"), "{r}");
    }

    #[test]
    fn literal_mode_treats_pattern_verbatim() {
        let f = fixture();
        fs::write(f.roots.input.join("re.txt"), "价格 a.b\n价格 axb\n").unwrap();
        let regex = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"a.b\",\"scope\":\"input\"}"),
        );
        assert!(regex.contains("价格 a.b"), "{regex}");
        assert!(
            regex.contains("价格 axb"),
            "regex 模式 a.b 应同时命中 axb：{regex}"
        );
        let literal = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"a.b\",\"scope\":\"input\",\"literal\":true}"),
        );
        assert!(literal.contains("价格 a.b"), "{literal}");
        assert!(
            !literal.contains("价格 axb"),
            "字面量模式不应命中 axb：{literal}"
        );
    }

    #[test]
    fn glob_filters_files() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"苹果\",\"scope\":\"input\",\"glob\":\"sub/**\"}"),
        );
        assert!(r.contains("sub/b.txt"), "{r}");
        assert!(!r.contains("== a.txt =="), "{r}");
    }

    #[test]
    fn context_lines_around_match() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"香蕉\",\"scope\":\"input\",\"context\":1}"),
        );
        assert!(r.contains("1- 苹果是水果。"), "{r}");
        assert!(r.contains("2: 香蕉也是。"), "{r}");
        assert!(r.contains("3- 橙子第三个。"), "{r}");
    }

    #[test]
    fn output_scope_sees_only_records() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"苹果\",\"scope\":\"output\"}"),
        );
        assert!(r.contains("== 1.md =="), "{r}");
        assert!(r.contains("已完成的产出提到苹果。"), "{r}");
        assert!(!r.contains("1.jsonl"), "audit 子目录不参与：{r}");
    }

    #[test]
    fn match_count_truncation_is_marked() {
        let f = fixture();
        let many: String = (1..=150).map(|i| format!("第 {i} 个苹果\n")).collect();
        fs::write(f.roots.input.join("many.txt"), many).unwrap();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"苹果\",\"scope\":\"input\"}"),
        );
        assert!(r.contains("[已截断：匹配数达到 100 上限]"), "{r}");
    }

    #[test]
    fn invalid_regex_returns_error_text() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"([\",\"scope\":\"input\"}"),
        );
        assert!(r.starts_with("错误："), "{r}");
    }

    #[test]
    fn unknown_tool_error_text() {
        let f = fixture();
        let r = execute(&f.roots, "write_file", &args("{}"));
        assert!(r.starts_with("错误：未知工具"), "{r}");
    }

    #[test]
    fn missing_scope_is_error() {
        let f = fixture();
        let r = execute(&f.roots, "search", &args("{\"pattern\":\"x\"}"));
        assert!(r.starts_with("错误："), "{r}");
    }

    #[test]
    fn no_match_text() {
        let f = fixture();
        let r = execute(
            &f.roots,
            "search",
            &args("{\"pattern\":\"不存在的东西\",\"scope\":\"input\"}"),
        );
        assert_eq!(r, "无匹配。");
    }
}
