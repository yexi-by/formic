//! 输出区：单元记录的原子发布、审计落盘与作业汇总。
//! 不变量：任何时刻读到的都是完整记录——先写同目录临时文件，rename 一次性可见；
//! 失败单元没有记录文件，完成记录就是完成事实的权威表示（供调用方算续跑差集）。
//! 审计的语义所有者也是本模块：每次 LLM 调用与工具调用的输入输出完整留痕。

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

/// 一条审计留痕：data 逐字保留原始内容，不重新解析。
pub enum AuditEntry {
    /// LLM 请求体原文。
    LlmRequest(String),
    /// 原始 SSE data 负载。
    LlmEvent(String),
    /// 组装完毕的工具调用（名称 + 模型给出的原始参数文本）。
    ToolCall { name: String, arguments: String },
    /// 工具结果文本（含 `错误：` 与截断标记）。
    ToolResult(String),
}

impl AuditEntry {
    fn to_line(&self) -> String {
        match self {
            AuditEntry::LlmRequest(data) => {
                serde_json::json!({"direction": "request", "data": data}).to_string()
            }
            AuditEntry::LlmEvent(data) => {
                serde_json::json!({"direction": "event", "data": data}).to_string()
            }
            AuditEntry::ToolCall { name, arguments } => {
                serde_json::json!({"direction": "tool_call", "name": name, "data": arguments})
                    .to_string()
            }
            AuditEntry::ToolResult(data) => {
                serde_json::json!({"direction": "tool_result", "data": data}).to_string()
            }
        }
    }
}

/// 落盘一个单元的完整证据：每次 LLM 调用的请求体与原始响应、每次工具调用的
/// 输入与输出，按发生顺序排列。
pub fn write_audit(out_dir: &Path, unit: u64, entries: &[AuditEntry]) -> io::Result<PathBuf> {
    let audit_dir = out_dir.join("audit");
    fs::create_dir_all(&audit_dir)?;
    let mut text = String::new();
    for entry in entries {
        text.push_str(&entry.to_line());
        text.push('\n');
    }
    let path = audit_dir.join(format!("{unit}.jsonl"));
    fs::write(&path, text)?;
    Ok(path)
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
