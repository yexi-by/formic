//! 输出区：单元记录的原子发布、审计落盘与作业汇总。
//! 不变量：任何时刻读到的都是完整记录——先写同目录临时文件，rename 一次性可见；
//! 失败单元没有记录文件，完成记录就是完成事实的权威表示（供调用方算续跑差集）。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// 原子发布单元产出，返回记录路径。同一单元重复发布会以新记录替换。
pub fn publish(out_dir: &Path, unit: u64, content: &str) -> io::Result<PathBuf> {
    let tmp = out_dir.join(format!(".tmp-unit-{unit}"));
    fs::write(&tmp, content)?;
    let target = out_dir.join(format!("{unit}.md"));
    fs::rename(&tmp, &target)?;
    Ok(target)
}

/// 落盘一次 LLM 调用的完整证据：请求体原文 + 按到达顺序的全部原始 SSE 负载。
pub fn write_audit(
    out_dir: &Path,
    unit: u64,
    request_body: &str,
    raw_events: &[String],
) -> io::Result<PathBuf> {
    let audit_dir = out_dir.join("audit");
    fs::create_dir_all(&audit_dir)?;
    let mut text = String::new();
    text.push_str(&audit_line("request", request_body));
    for event in raw_events {
        text.push_str(&audit_line("event", event));
    }
    let path = audit_dir.join(format!("{unit}.jsonl"));
    fs::write(&path, text)?;
    Ok(path)
}

fn audit_line(direction: &str, data: &str) -> String {
    // data 原样作为 JSON 字符串留痕，不重新解析，保证逐字可查。
    serde_json::json!({"direction": direction, "data": data}).to_string() + "\n"
}

/// 作业汇总：完成数、失败单元号；失败原因已在各单元完成时即时报告。
pub struct Summary {
    pub completed: u64,
    pub failed: Vec<u64>,
}

impl Summary {
    pub fn render(&self) -> String {
        let mut line = format!("完成 {}，失败 {}", self.completed, self.failed.len());
        if !self.failed.is_empty() {
            let ids: Vec<String> = self.failed.iter().map(u64::to_string).collect();
            line.push_str(&format!("；失败单元：{}", ids.join(", ")));
        }
        line
    }

    pub fn exit_code(&self) -> u8 {
        if self.failed.is_empty() { 0 } else { 1 }
    }
}
