//! 由端到端测试临时用 rustc 编译的最小 MCP stdio server。
//! 只实现 initialize、分页 tools/list 和 tools/call，不依赖项目 crate。

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::thread;
use std::time::Duration;

fn main() {
    append_log("FAKE_MCP_START_LOG", "start\n");
    eprintln!("fake MCP stdio ready");
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Some(id) = json_id(&line) else {
            continue;
        };
        let result = if line.contains("\"method\":\"initialize\"") {
            let version = string_field(&line, "protocolVersion")
                .unwrap_or_else(|| "2025-06-18".to_string());
            format!(
                "{{\"protocolVersion\":\"{version}\",\"capabilities\":{{\"tools\":{{\"listChanged\":true}}}},\"serverInfo\":{{\"name\":\"formic-test-mcp\",\"version\":\"1\"}}}}"
            )
        } else if line.contains("\"method\":\"tools/list\"") {
            if line.contains("\"cursor\":\"page-2\"") {
                "{\"tools\":[{\"name\":\"slow\",\"description\":\"延迟返回\",\"inputSchema\":{\"type\":\"object\",\"properties\":{},\"additionalProperties\":false}}]}".to_string()
            } else {
                "{\"tools\":[{\"name\":\"echo\",\"description\":\"返回固定文本和结构数据\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"text\":{\"type\":\"string\"}},\"required\":[\"text\"],\"additionalProperties\":false}}],\"nextCursor\":\"page-2\"}".to_string()
            }
        } else if line.contains("\"method\":\"tools/call\"") {
            append_log("FAKE_MCP_CALL_LOG", "call\n");
            if line.contains("\"name\":\"slow\"") {
                thread::sleep(Duration::from_secs(5));
                "{\"content\":[{\"type\":\"text\",\"text\":\"late\"}],\"isError\":false}".to_string()
            } else {
                "{\"content\":[{\"type\":\"text\",\"text\":\"echo:hello\"}],\"structuredContent\":{\"value\":\"hello\"},\"isError\":false}".to_string()
            }
        } else {
            "{}".to_string()
        };
        writeln!(stdout, "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{result}}}")
            .unwrap();
        stdout.flush().unwrap();
    }
}

fn append_log(variable: &str, text: &str) {
    let Some(path) = env::var_os(variable) else {
        return;
    };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = file.write_all(text.as_bytes());
    }
}

fn json_id(line: &str) -> Option<String> {
    let rest = line.split_once("\"id\":")?.1.trim_start();
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(format!("\"{}\"", &rest[..end]));
    }
    let end = rest
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(rest.len());
    (end > 0).then(|| rest[..end].to_string())
}

fn string_field(line: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let rest = line.split_once(&marker)?.1;
    Some(rest.split_once('"')?.0.to_string())
}
