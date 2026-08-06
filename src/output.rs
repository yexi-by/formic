//! 输出区：单元记录的原子发布、审计落盘与作业汇总。
//! 不变量：任何时刻读到的都是完整记录——先写同目录临时文件，rename 一次性可见；
//! 失败单元没有记录文件，完成记录就是完成事实的权威表示（供调用方算续跑差集）。
//! 审计的语义所有者也是本模块：每次 LLM 调用与工具调用的输入输出完整留痕。

use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// 原子发布单元产出，返回记录路径。同一单元重复发布会以新记录替换。
pub fn publish(out_dir: &Path, unit: u64, content: &str) -> io::Result<PathBuf> {
    let tmp = out_dir.join(format!(".tmp-unit-{unit}"));
    fs::write(&tmp, content)?;
    let target = out_dir.join(format!("{unit}.md"));
    fs::rename(&tmp, &target)?;
    Ok(target)
}

/// 单元审计日志：流式逐条落盘，不在内存累积（规模验证证明内存放大主要来自
/// 审计——每回合请求体含全量历史，在内存累积 ≈ Σ回合大小）。
/// 文件自创建起存在，空文件表示单元在首次调用前结束。
pub struct AuditLog {
    writer: BufWriter<fs::File>,
    path: PathBuf,
}

impl AuditLog {
    pub fn create(out_dir: &Path, unit: u64) -> io::Result<Self> {
        let audit_dir = out_dir.join("audit");
        fs::create_dir_all(&audit_dir)?;
        let path = audit_dir.join(format!("{unit}.jsonl"));
        let file = fs::File::create(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    pub fn push(&mut self, entry: &AuditEntry) -> io::Result<()> {
        self.writer.write_all(entry.to_line().as_bytes())?;
        self.writer.write_all(b"\n")
    }

    /// 冲刷并关闭。证据完整是契约要求：失败使单元不成立。
    pub fn finish(self) -> io::Result<PathBuf> {
        let mut writer = self.writer;
        writer.flush()?;
        Ok(self.path)
    }
}

/// 一条审计留痕：data 逐字保留原始内容，不重新解析。
pub enum AuditEntry {
    /// LLM 请求体原文；attempt 是本次调用的尝试序号（1 起始，重试递增）。
    LlmRequest { attempt: u32, body: String },
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
            AuditEntry::LlmRequest { attempt, body } => {
                serde_json::json!({"direction": "request", "attempt": attempt, "data": body})
                    .to_string()
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

/// 单元的运行统计：输出区的派生视图（权威事实在审计里，stats 由运行时
/// 在结束时聚合，供调用方做指标分析；token 为内部估算值，非计费依据）。
#[derive(Debug, Default)]
pub struct UnitStats {
    pub turns: u32,
    pub llm_calls: u32,
    pub retries: u32,
    pub tool_calls: std::collections::HashMap<String, u32>,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// 追加一行单元统计到 out/stats.jsonl。附属证据：写失败只产生诊断，
/// 不改写单元的业务结果（§9）。
pub fn append_stats(out_dir: &Path, unit: u64, outcome: &str, stats: &UnitStats) -> io::Result<()> {
    let line = serde_json::json!({
        "unit": unit,
        "outcome": outcome,
        "turns": stats.turns,
        "llm_calls": stats.llm_calls,
        "retries": stats.retries,
        "tool_calls": stats.tool_calls,
        "input_tokens_est": stats.input_tokens,
        "output_tokens_est": stats.output_tokens,
    });
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out_dir.join("stats.jsonl"))?;
    writeln!(file, "{line}")
}

/// 作业汇总：完成数、失败单元号、取消数；失败原因已在各单元完成时即时报告。
pub struct Summary {
    pub completed: u64,
    pub failed: Vec<u64>,
    pub cancelled: u64,
}

impl Summary {
    pub fn render(&self) -> String {
        let mut line = format!("完成 {}，失败 {}", self.completed, self.failed.len());
        if !self.failed.is_empty() {
            let ids: Vec<String> = self.failed.iter().map(u64::to_string).collect();
            line.push_str(&format!("；失败单元：{}", ids.join(", ")));
        }
        if self.cancelled > 0 {
            line.push_str(&format!("；取消 {}（作业已被终止）", self.cancelled));
        }
        line
    }

    pub fn exit_code(&self) -> u8 {
        if self.failed.is_empty() { 0 } else { 1 }
    }
}
