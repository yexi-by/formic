//! 输出区：单元记录的原子发布、worker 运行档案与作业汇总。
//! 不变量：任何时刻读到的都是完整记录——先写同目录临时文件，rename 一次性可见；
//! 失败单元没有记录文件，完成记录就是完成事实的权威表示（供调用方算续跑差集）。
//! 审计的语义所有者也是本模块：每次 LLM 调用与工具调用的输入输出完整留痕。

use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFormat {
    Markdown,
    Json,
}

impl RecordFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Markdown => "md",
            Self::Json => "json",
        }
    }
}

/// 任务级运行事实。只保存排查行为所需且不含密钥的稳定配置。
#[derive(Debug, Clone)]
pub struct JobReportFacts {
    pub protocol: String,
    pub model: String,
    pub context_window_tokens: u64,
    pub max_output_tokens: u64,
    pub context_safety_tokens: u64,
    pub concurrency: usize,
    pub output_format: RecordFormat,
    pub tools: Vec<String>,
}

/// 一次任务的 worker 观测目录。时间戳在 worker 启动前确定，原始审计和
/// Markdown 视图共享该目录，避免复用输出区时把旧任务的证据指向新审计。
pub struct WorkerRun {
    directory: PathBuf,
    started_at: DateTime<Utc>,
    facts: JobReportFacts,
}

impl WorkerRun {
    pub fn create(out_dir: &Path, facts: JobReportFacts) -> io::Result<Self> {
        let root = out_dir.join("workers");
        fs::create_dir_all(&root)?;
        let started_at = Utc::now();
        let timestamp = started_at.format("%Y%m%dT%H%M%S%.3fZ").to_string();
        let mut sequence = 1u64;
        let directory = loop {
            let name = if sequence == 1 {
                timestamp.clone()
            } else {
                format!("{timestamp}-{sequence}")
            };
            let candidate = root.join(name);
            match fs::create_dir(&candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => sequence += 1,
                Err(error) => return Err(error),
            }
        };
        Ok(Self {
            directory,
            started_at,
            facts,
        })
    }

    pub(crate) fn audit_path(&self, unit: u64) -> PathBuf {
        self.directory.join(format!(".tmp-worker-{unit}.jsonl"))
    }

    fn request_base_path(&self, unit: u64, kind: RequestKind) -> PathBuf {
        self.directory
            .join(format!(".tmp-worker-{unit}-{}-request", kind.key()))
    }

    pub fn report_path(&self, unit: u64) -> PathBuf {
        self.directory.join(format!("{unit}.md"))
    }
}

/// 原子发布单元产出，返回记录路径。同一单元重复发布会以新记录替换。
pub fn publish(
    out_dir: &Path,
    unit: u64,
    content: &str,
    format: RecordFormat,
) -> io::Result<PathBuf> {
    let tmp = out_dir.join(format!(".tmp-unit-{unit}"));
    fs::write(&tmp, content)?;
    let target = out_dir.join(format!("{unit}.{}", format.extension()));
    atomic_replace(&tmp, &target)?;
    Ok(target)
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }
    let existing: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let replacement: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY：两个缓冲区均由有效 Windows 路径编码并以 NUL 结尾，调用期间保持存活。
    let moved = unsafe {
        MoveFileExW(
            existing.as_ptr(),
            replacement.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// 单元审计日志：流式逐条落盘，不在内存累积。请求正文以磁盘上的上一份同类请求
/// 计算可逆增量，避免最终档案和 worker 内存随“每轮完整历史之和”增长。
/// 文件自创建起存在，空文件表示单元在首次调用前结束。
pub struct AuditLog {
    writer: BufWriter<fs::File>,
    path: PathBuf,
    llm_request_base: PathBuf,
    compaction_request_base: PathBuf,
    started: Instant,
    sequence: u64,
    request_bases_cleaned: bool,
}

impl AuditLog {
    pub fn create(run: &WorkerRun, unit: u64) -> io::Result<Self> {
        let path = run.audit_path(unit);
        let file = fs::File::create(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
            llm_request_base: run.request_base_path(unit, RequestKind::Llm),
            compaction_request_base: run.request_base_path(unit, RequestKind::Compaction),
            started: Instant::now(),
            sequence: 0,
            request_bases_cleaned: false,
        })
    }

    pub fn push(&mut self, entry: &AuditEntry) -> io::Result<()> {
        let mut value = entry.to_value();
        let object = value
            .as_object_mut()
            .expect("AuditEntry 必须序列化为 JSON object");
        let (sequence, elapsed_ms) = self.next_stamp();
        object.insert("sequence".into(), sequence.into());
        object.insert("elapsed_ms".into(), elapsed_ms.into());
        serde_json::to_writer(&mut self.writer, &value)?;
        self.writer.write_all(b"\n")
    }

    /// 保存一次普通模型请求。首个请求保存完整正文，后续请求保存相对上一份普通请求的
    /// 可逆字节增量。基准正文只在 worker 运行期间保留在临时文件中，避免用内存保存
    /// 随历史增长的大字符串。
    pub fn push_llm_request(&mut self, attempt: u32, body: &str) -> io::Result<()> {
        self.push_request(RequestKind::Llm, attempt, body)
    }

    /// 压缩请求使用独立基准，避免它和普通请求的不同 instructions、工具目录互相打断
    /// 增量序列。
    pub fn push_compaction_request(&mut self, attempt: u32, body: &str) -> io::Result<()> {
        self.push_request(RequestKind::Compaction, attempt, body)
    }

    /// 一次 LLM 调用的全部原始 SSE data 负载按到达顺序保存为一个批次。事件边界和
    /// 原文仍完整保留，但 Markdown 不再为每个 token 片段生成标题。
    pub fn push_llm_event_stream(&mut self, events: &[String]) -> io::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let (sequence, elapsed_ms) = self.next_stamp();
        let record = LlmEventStreamRecord {
            direction: "llm_event_stream",
            sequence,
            elapsed_ms,
            event_count: events.len(),
            total_bytes: events.iter().map(String::len).sum(),
            max_backtick_run: events
                .iter()
                .map(|event| longest_backtick_run(event))
                .max()
                .unwrap_or(0),
        };
        serde_json::to_writer(&mut self.writer, &record)?;
        self.writer.write_all(b"\n")?;
        for (index, event) in events.iter().enumerate() {
            let record = LlmEventDataRecord {
                direction: "llm_event_data",
                stream_sequence: sequence,
                index,
                data: event,
            };
            serde_json::to_writer(&mut self.writer, &record)?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn push_request(&mut self, kind: RequestKind, attempt: u32, body: &str) -> io::Result<()> {
        let base_path = match kind {
            RequestKind::Llm => self.llm_request_base.clone(),
            RequestKind::Compaction => self.compaction_request_base.clone(),
        };
        let delta = request_delta(&base_path, body)?;
        let (sequence, elapsed_ms) = self.next_stamp();
        match delta.filter(|delta| delta.is_smaller_than(body)) {
            Some(delta) => {
                let record = RequestDeltaRecord {
                    direction: kind.direction(),
                    sequence,
                    elapsed_ms,
                    attempt,
                    request_encoding: "delta",
                    base_bytes: delta.base_bytes,
                    prefix_bytes: delta.prefix_bytes,
                    removed_bytes: delta.removed_bytes,
                    inserted: delta.inserted,
                    full_bytes: body.len(),
                };
                serde_json::to_writer(&mut self.writer, &record)?;
            }
            None => {
                let record = FullRequestRecord {
                    direction: kind.direction(),
                    sequence,
                    elapsed_ms,
                    attempt,
                    request_encoding: "full",
                    full_bytes: body.len(),
                    data: body,
                };
                serde_json::to_writer(&mut self.writer, &record)?;
            }
        }
        self.writer.write_all(b"\n")?;
        fs::write(base_path, body)
    }

    fn next_stamp(&mut self) -> (u64, u64) {
        self.sequence += 1;
        let elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        (self.sequence, elapsed_ms)
    }

    fn cleanup_request_bases(&self) -> io::Result<()> {
        for path in [&self.llm_request_base, &self.compaction_request_base] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// 冲刷并关闭。证据完整是契约要求：失败使单元不成立。
    pub fn finish(mut self) -> io::Result<PathBuf> {
        self.writer.flush()?;
        if let Err(error) = self.cleanup_request_bases() {
            eprintln!("worker 请求基准临时文件清理失败：{error}");
        }
        self.request_bases_cleaned = true;
        Ok(self.path.clone())
    }
}

impl Drop for AuditLog {
    fn drop(&mut self) {
        if self.request_bases_cleaned {
            return;
        }
        if let Err(error) = self.cleanup_request_bases() {
            eprintln!("worker 请求基准临时文件清理失败：{error}");
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RequestKind {
    Llm,
    Compaction,
}

impl RequestKind {
    fn key(self) -> &'static str {
        match self {
            Self::Llm => "llm",
            Self::Compaction => "compaction",
        }
    }

    fn direction(self) -> &'static str {
        match self {
            Self::Llm => "request",
            Self::Compaction => "compaction_request",
        }
    }
}

#[derive(serde::Serialize)]
struct FullRequestRecord<'a> {
    direction: &'static str,
    sequence: u64,
    elapsed_ms: u64,
    attempt: u32,
    request_encoding: &'static str,
    full_bytes: usize,
    data: &'a str,
}

#[derive(serde::Serialize)]
struct RequestDeltaRecord<'a> {
    direction: &'static str,
    sequence: u64,
    elapsed_ms: u64,
    attempt: u32,
    request_encoding: &'static str,
    base_bytes: usize,
    prefix_bytes: usize,
    removed_bytes: usize,
    inserted: &'a str,
    full_bytes: usize,
}

#[derive(serde::Serialize)]
struct LlmEventStreamRecord {
    direction: &'static str,
    sequence: u64,
    elapsed_ms: u64,
    event_count: usize,
    total_bytes: usize,
    max_backtick_run: usize,
}

#[derive(serde::Serialize)]
struct LlmEventDataRecord<'a> {
    direction: &'static str,
    stream_sequence: u64,
    index: usize,
    data: &'a str,
}

struct RequestDelta<'a> {
    base_bytes: usize,
    prefix_bytes: usize,
    removed_bytes: usize,
    inserted: &'a str,
}

impl RequestDelta<'_> {
    fn is_smaller_than(&self, body: &str) -> bool {
        // 数值字段和 JSON 键的固定开销约 160 bytes；只有确实减少档案体积时才用增量。
        self.inserted.len().saturating_add(160) < body.len()
    }
}

fn request_delta<'a>(base_path: &Path, body: &'a str) -> io::Result<Option<RequestDelta<'a>>> {
    let mut base = match fs::File::open(base_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let base_bytes = usize::try_from(base.metadata()?.len())
        .map_err(|_| io::Error::other("请求基准文件大小超出当前平台可表示范围"))?;
    let prefix_bytes = common_prefix_bytes(&mut base, base_bytes, body)?;
    let suffix_bytes = common_suffix_bytes(&mut base, base_bytes, body, prefix_bytes)?;
    let removed_bytes = base_bytes - prefix_bytes - suffix_bytes;
    let inserted_end = body.len() - suffix_bytes;
    Ok(Some(RequestDelta {
        base_bytes,
        prefix_bytes,
        removed_bytes,
        inserted: &body[prefix_bytes..inserted_end],
    }))
}

fn common_prefix_bytes(base: &mut fs::File, base_len: usize, body: &str) -> io::Result<usize> {
    const BUFFER_BYTES: usize = 64 * 1024;
    base.seek(SeekFrom::Start(0))?;
    let body_bytes = body.as_bytes();
    let limit = base_len.min(body_bytes.len());
    let mut buffer = [0u8; BUFFER_BYTES];
    let mut matched = 0usize;
    while matched < limit {
        let count = BUFFER_BYTES.min(limit - matched);
        base.read_exact(&mut buffer[..count])?;
        match buffer[..count]
            .iter()
            .zip(&body_bytes[matched..matched + count])
            .position(|(left, right)| left != right)
        {
            Some(offset) => {
                matched += offset;
                break;
            }
            None => matched += count,
        }
    }
    while matched > 0 && !body.is_char_boundary(matched) {
        matched -= 1;
    }
    Ok(matched)
}

fn common_suffix_bytes(
    base: &mut fs::File,
    base_len: usize,
    body: &str,
    prefix_bytes: usize,
) -> io::Result<usize> {
    const BUFFER_BYTES: usize = 64 * 1024;
    let body_bytes = body.as_bytes();
    let limit = (base_len - prefix_bytes).min(body_bytes.len() - prefix_bytes);
    let mut buffer = [0u8; BUFFER_BYTES];
    let mut matched = 0usize;
    while matched < limit {
        let count = BUFFER_BYTES.min(limit - matched);
        let base_start = base_len - matched - count;
        let base_start = u64::try_from(base_start)
            .map_err(|_| io::Error::other("请求基准偏移超出文件 API 可表示范围"))?;
        base.seek(SeekFrom::Start(base_start))?;
        base.read_exact(&mut buffer[..count])?;
        let body_start = body_bytes.len() - matched - count;
        let equal_from_end = buffer[..count]
            .iter()
            .rev()
            .zip(body_bytes[body_start..body_start + count].iter().rev())
            .take_while(|(left, right)| left == right)
            .count();
        matched += equal_from_end;
        if equal_from_end != count {
            break;
        }
    }
    while matched > 0 && !body.is_char_boundary(body.len() - matched) {
        matched -= 1;
    }
    Ok(matched)
}

/// worker 主循环对外可观察的逻辑状态。状态名进入结构化审计，中文名称只用于
/// Markdown 展示，避免用日志文字反推控制流。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Preparing,
    Ready,
    RequestingModel,
    RetryingModel,
    InterpretingModel,
    CompactingContext,
    WaitingForTool,
    CorrectingToolCall,
    CorrectingOutput,
    ReadyToPublish,
    Cancelled,
    Failed,
}

impl WorkerState {
    fn key(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Ready => "ready",
            Self::RequestingModel => "requesting_model",
            Self::RetryingModel => "retrying_model",
            Self::InterpretingModel => "interpreting_model",
            Self::CompactingContext => "compacting_context",
            Self::WaitingForTool => "waiting_for_tool",
            Self::CorrectingToolCall => "correcting_tool_call",
            Self::CorrectingOutput => "correcting_output",
            Self::ReadyToPublish => "ready_to_publish",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Preparing => "准备输入",
            Self::Ready => "准备执行",
            Self::RequestingModel => "请求模型",
            Self::RetryingModel => "等待重试",
            Self::InterpretingModel => "解释模型响应",
            Self::CompactingContext => "压缩上下文",
            Self::WaitingForTool => "等待工具",
            Self::CorrectingToolCall => "修正工具参数",
            Self::CorrectingOutput => "修正输出",
            Self::ReadyToPublish => "等待发布",
            Self::Cancelled => "已取消",
            Self::Failed => "失败",
        }
    }

    fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "preparing" => Self::Preparing,
            "ready" => Self::Ready,
            "requesting_model" => Self::RequestingModel,
            "retrying_model" => Self::RetryingModel,
            "interpreting_model" => Self::InterpretingModel,
            "compacting_context" => Self::CompactingContext,
            "waiting_for_tool" => Self::WaitingForTool,
            "correcting_tool_call" => Self::CorrectingToolCall,
            "correcting_output" => Self::CorrectingOutput,
            "ready_to_publish" => Self::ReadyToPublish,
            "cancelled" => Self::Cancelled,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}

/// 除模型请求和原始事件流之外的一条审计留痕；大正文由 AuditLog 的专用方法写入，
/// 避免在 serde_json::Value 中再次复制。
pub enum AuditEntry {
    /// worker 状态变化及其直接原因。
    State { state: WorkerState, reason: String },
    /// 每次模型调用前的完整输入预算判断。
    ContextBudget {
        estimated_tokens: u64,
        input_budget: u64,
        force: bool,
        action: String,
    },
    /// 组装完毕的工具调用（名称 + 模型给出的原始参数文本）。
    ToolCall {
        name: String,
        source: Option<String>,
        arguments: String,
    },
    /// 工具结果文本（含 `错误：` 与截断标记）。
    ToolResult(String),
    /// 调度器对一次普通工具调用给出的资源与缓存事实。
    ToolExecution {
        name: String,
        cache: String,
        wait_ms: u64,
        execution_ms: u64,
        result_bytes: usize,
        mcp_server: Option<String>,
    },
    /// 可重试模型错误与下一次尝试之间的等待决定。
    Retry {
        attempt: u32,
        next_attempt: u32,
        delay_ms: u64,
        reason: String,
    },
    /// 结构化结果的本地校验，不重复保存已在原始事件中的结果正文。
    OutputValidation {
        valid: bool,
        instance_path: Option<String>,
        schema_path: Option<String>,
        reason: String,
    },
    ContextCompaction {
        valid: bool,
        before_tokens: u64,
        after_tokens: u64,
        reason: String,
    },
}

impl AuditEntry {
    fn to_value(&self) -> serde_json::Value {
        match self {
            AuditEntry::State { state, reason } => serde_json::json!({
                "direction": "state",
                "state": state.key(),
                "reason": reason,
            }),
            AuditEntry::ContextBudget {
                estimated_tokens,
                input_budget,
                force,
                action,
            } => serde_json::json!({
                "direction": "context_budget",
                "estimated_tokens": estimated_tokens,
                "input_budget": input_budget,
                "force": force,
                "action": action,
            }),
            AuditEntry::ToolCall {
                name,
                source,
                arguments,
            } => serde_json::json!({
                "direction": "tool_call",
                "name": name,
                "source": source,
                "data": arguments,
            }),
            AuditEntry::ToolResult(data) => {
                serde_json::json!({"direction": "tool_result", "data": data})
            }
            AuditEntry::ToolExecution {
                name,
                cache,
                wait_ms,
                execution_ms,
                result_bytes,
                mcp_server,
            } => serde_json::json!({
                "direction": "tool_execution",
                "name": name,
                "cache": cache,
                "wait_ms": wait_ms,
                "execution_ms": execution_ms,
                "result_bytes": result_bytes,
                "mcp_server": mcp_server,
            }),
            AuditEntry::Retry {
                attempt,
                next_attempt,
                delay_ms,
                reason,
            } => serde_json::json!({
                "direction": "retry",
                "attempt": attempt,
                "next_attempt": next_attempt,
                "delay_ms": delay_ms,
                "reason": reason,
            }),
            AuditEntry::OutputValidation {
                valid,
                instance_path,
                schema_path,
                reason,
            } => serde_json::json!({
                "direction": "output_validation",
                "valid": valid,
                "instance_path": instance_path,
                "schema_path": schema_path,
                "reason": reason,
            }),
            AuditEntry::ContextCompaction {
                valid,
                before_tokens,
                after_tokens,
                reason,
            } => serde_json::json!({
                "direction": "context_compaction",
                "valid": valid,
                "before_tokens": before_tokens,
                "after_tokens": after_tokens,
                "reason": reason,
            }),
        }
    }
}

/// 单元的运行统计：输出区的派生视图（权威事实最终进入 worker 档案，stats 由运行时
/// 在结束时聚合，供调用方做指标分析；token 为内部估算值，非计费依据）。
#[derive(Debug, Default)]
pub struct UnitStats {
    pub turns: u32,
    pub llm_calls: u32,
    pub retries: u32,
    pub tool_calls: std::collections::HashMap<String, u32>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub structured_corrections: u32,
    pub compactions: u32,
    pub compaction_before_tokens: u64,
    pub compaction_after_tokens: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_in_flight_joins: u64,
    pub cache_evictions: u64,
    pub tool_wait_ms: std::collections::HashMap<String, u64>,
    pub tool_execution_ms: std::collections::HashMap<String, u64>,
    pub mcp_current_in_flight: std::collections::HashMap<String, u64>,
    pub mcp_peak_in_flight: std::collections::HashMap<String, u64>,
    pub provider_input_tokens: u64,
    pub provider_output_tokens: u64,
    pub provider_cache_read_tokens: u64,
    pub provider_cache_creation_tokens: u64,
    pub provider_usage_reports: u64,
    pub provider_usage_missing_calls: u64,
}

impl UnitStats {
    pub fn record_tool_response(&mut self, name: &str, response: &crate::scheduler::ToolResponse) {
        use crate::scheduler::CacheDisposition;
        match response.cache {
            CacheDisposition::Hit => self.cache_hits += 1,
            CacheDisposition::Miss => self.cache_misses += 1,
            CacheDisposition::Joined => self.cache_in_flight_joins += 1,
            CacheDisposition::Disabled | CacheDisposition::Bypassed => {}
        }
        self.cache_evictions += response.cache_evictions;
        *self.tool_wait_ms.entry(name.to_string()).or_default() += response.wait_ms;
        *self.tool_execution_ms.entry(name.to_string()).or_default() += response.execution_ms;
        if let Some(server) = &response.mcp_server {
            self.mcp_current_in_flight
                .insert(server.clone(), response.mcp_current_in_flight.unwrap_or(0));
            let peak = self.mcp_peak_in_flight.entry(server.clone()).or_default();
            *peak = (*peak).max(response.mcp_peak_in_flight.unwrap_or(0));
        }
    }

    pub fn record_provider_usage(&mut self, usage: &crate::llm::ProviderUsage) {
        if usage.is_empty() {
            self.provider_usage_missing_calls += 1;
            return;
        }
        self.provider_usage_reports += 1;
        self.provider_input_tokens += usage.input_tokens.unwrap_or(0);
        self.provider_output_tokens += usage.output_tokens.unwrap_or(0);
        self.provider_cache_read_tokens += usage.cache_read_tokens.unwrap_or(0);
        self.provider_cache_creation_tokens += usage.cache_creation_tokens.unwrap_or(0);
    }
}

/// 追加一行单元统计到 out/stats.jsonl。附属证据：写失败只产生诊断，
/// 不改写单元的业务结果（§9）。
pub fn append_stats(out_dir: &Path, unit: u64, outcome: &str, stats: &UnitStats) -> io::Result<()> {
    let line = stats_value(unit, outcome, stats);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out_dir.join("stats.jsonl"))?;
    writeln!(file, "{line}")
}

fn stats_value(unit: u64, outcome: &str, stats: &UnitStats) -> serde_json::Value {
    use std::collections::BTreeMap;

    let tool_calls: BTreeMap<_, _> = stats.tool_calls.iter().collect();
    let tool_wait_ms: BTreeMap<_, _> = stats.tool_wait_ms.iter().collect();
    let tool_execution_ms: BTreeMap<_, _> = stats.tool_execution_ms.iter().collect();
    let mcp_current_in_flight: BTreeMap<_, _> = stats.mcp_current_in_flight.iter().collect();
    let mcp_peak_in_flight: BTreeMap<_, _> = stats.mcp_peak_in_flight.iter().collect();
    serde_json::json!({
        "unit": unit,
        "outcome": outcome,
        "turns": stats.turns,
        "llm_calls": stats.llm_calls,
        "retries": stats.retries,
        "tool_calls": tool_calls,
        "input_tokens_est": stats.input_tokens,
        "output_tokens_est": stats.output_tokens,
        "structured_corrections": stats.structured_corrections,
        "compactions": stats.compactions,
        "compaction_before_tokens": stats.compaction_before_tokens,
        "compaction_after_tokens": stats.compaction_after_tokens,
        "cache_hits": stats.cache_hits,
        "cache_misses": stats.cache_misses,
        "cache_in_flight_joins": stats.cache_in_flight_joins,
        "cache_evictions": stats.cache_evictions,
        "tool_wait_ms": tool_wait_ms,
        "tool_execution_ms": tool_execution_ms,
        "mcp_current_in_flight": mcp_current_in_flight,
        "mcp_peak_in_flight": mcp_peak_in_flight,
        "provider_input_tokens": stats.provider_input_tokens,
        "provider_output_tokens": stats.provider_output_tokens,
        "provider_cache_read_tokens": stats.provider_cache_read_tokens,
        "provider_cache_creation_tokens": stats.provider_cache_creation_tokens,
        "provider_usage_reports": stats.provider_usage_reports,
        "provider_usage_missing_calls": stats.provider_usage_missing_calls,
    })
}

/// worker 结束时用于生成 Markdown 视图的事实。运行中的结构化 JSONL 是完整
/// 中间证据，最终 Markdown 是合并后的唯一档案，不参与 worker 的业务结局。
pub struct WorkerReport<'a> {
    pub unit: u64,
    pub shard: &'a str,
    pub outcome: &'a str,
    pub failure_reason: Option<&'a str>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration: Duration,
    pub stats: &'a UnitStats,
    pub record_format: Option<RecordFormat>,
}

/// 将一个 worker 的结构化审计流式渲染为 Markdown。内存中一次只保留一行原始
/// 事件，避免报告生成重新引入审计历史的内存放大。Markdown 成功原子发布后
/// 删除临时 JSONL，最终每个 worker 只保留一份完整证据。
pub fn render_worker_report(run: &WorkerRun, report: &WorkerReport<'_>) -> io::Result<PathBuf> {
    let target = run.report_path(report.unit);
    let temporary = run
        .directory
        .join(format!(".tmp-worker-{}.md", report.unit));
    let rendered = (|| -> io::Result<()> {
        let file = fs::File::create(&temporary)?;
        let mut writer = BufWriter::new(file);
        write_report_header(&mut writer, run, report)?;
        render_audit_timeline(&mut writer, &run.audit_path(report.unit))?;
        writer.flush()
    })();
    if let Err(error) = rendered {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    atomic_replace(&temporary, &target)?;
    let audit = run.audit_path(report.unit);
    if audit.exists() {
        fs::remove_file(audit)?;
    }
    Ok(target)
}

fn write_report_header(
    writer: &mut impl Write,
    run: &WorkerRun,
    report: &WorkerReport<'_>,
) -> io::Result<()> {
    writeln!(writer, "# Worker {} 运行档案\n", report.unit)?;
    writeln!(
        writer,
        "> 本文档在 worker 结束后由结构化审计确定性生成。它描述 Formic 可见的状态和决定，不推测模型不可见的内部思维。\n"
    )?;
    writeln!(writer, "## 任务与结局\n")?;
    writeln!(
        writer,
        "- 任务时间：`{}`",
        run.started_at.to_rfc3339_opts(SecondsFormat::Millis, true)
    )?;
    writeln!(writer, "- Worker ID：`{}`", report.unit)?;
    writeln!(writer, "- 分片：{}", markdown_inline(report.shard))?;
    writeln!(
        writer,
        "- 开始时间：`{}`",
        report
            .started_at
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    )?;
    writeln!(
        writer,
        "- 结束时间：`{}`",
        report
            .finished_at
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    )?;
    writeln!(writer, "- 运行时长：`{} ms`", report.duration.as_millis())?;
    writeln!(
        writer,
        "- 结局：`{}`（{}）",
        report.outcome,
        outcome_label(report.outcome)
    )?;
    if let Some(reason) = report.failure_reason {
        writeln!(writer, "- 失败原因：{}", markdown_inline(reason))?;
    }
    writeln!(writer, "- 审计证据：已完整合并到本文件的状态时间线")?;
    writeln!(
        writer,
        "- 审计编码：同类请求首份保存完整正文，后续保存可逆字节增量；每次 SSE 流合并为一个折叠块"
    )?;
    if let Some(format) = report.record_format {
        writeln!(
            writer,
            "- 完成记录：[{}.{}](../../{}.{})",
            report.unit,
            format.extension(),
            report.unit,
            format.extension()
        )?;
    }

    writeln!(writer, "\n## 冻结配置\n")?;
    writeln!(writer, "- 协议：`{}`", markdown_inline(&run.facts.protocol))?;
    writeln!(writer, "- 模型：`{}`", markdown_inline(&run.facts.model))?;
    writeln!(
        writer,
        "- 上下文窗口：`{}` token",
        run.facts.context_window_tokens
    )?;
    writeln!(
        writer,
        "- 最大输出：`{}` token",
        run.facts.max_output_tokens
    )?;
    writeln!(
        writer,
        "- 上下文安全余量：`{}` token",
        run.facts.context_safety_tokens
    )?;
    writeln!(writer, "- Worker 并发窗口：`{}`", run.facts.concurrency)?;
    writeln!(
        writer,
        "- 输出模式：`{}`",
        run.facts.output_format.extension()
    )?;
    writeln!(writer, "- 冻结工具：`{}`", run.facts.tools.join("`, `"))?;

    writeln!(writer, "\n## 结束统计\n")?;
    let stats =
        serde_json::to_string_pretty(&stats_value(report.unit, report.outcome, report.stats))
            .expect("UnitStats 可序列化");
    write_code_block(writer, "json", &stats)?;
    writeln!(writer, "\n## 状态时间线\n")?;
    Ok(())
}

fn render_audit_timeline(writer: &mut impl Write, path: &Path) -> io::Result<()> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            writeln!(
                writer,
                "没有生成结构化审计；worker 可能在审计文件创建前失败。"
            )?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let mut lines = BufReader::new(file).lines().enumerate();
    while let Some((index, line)) = lines.next() {
        let line = line?;
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                writeln!(writer, "### {}. 审计行无法解析\n", index + 1)?;
                writeln!(writer, "{}\n", markdown_inline(&error.to_string()))?;
                continue;
            }
        };
        if value.get("direction").and_then(serde_json::Value::as_str) == Some("llm_event_stream") {
            render_event_stream(writer, &value, index + 1, &mut lines)?;
        } else {
            render_audit_entry(writer, &value, index + 1)?;
        }
    }
    Ok(())
}

fn render_audit_entry(
    writer: &mut impl Write,
    value: &serde_json::Value,
    fallback_sequence: usize,
) -> io::Result<()> {
    let sequence = value
        .get("sequence")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(fallback_sequence as u64);
    let elapsed_ms = value
        .get("elapsed_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let direction = value
        .get("direction")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let title = audit_title(direction, value);
    writeln!(writer, "### {sequence}. +{elapsed_ms} ms · {title}\n")?;

    match direction {
        "state" => {
            write_reason(
                writer,
                value.get("reason").and_then(serde_json::Value::as_str),
            )?;
        }
        "context_budget" => {
            writeln!(
                writer,
                "预计输入 `{}` token，安全输入预算 `{}` token；强制检查：`{}`；决定：`{}`。\n",
                value
                    .get("estimated_tokens")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("input_budget")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("force")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                value
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown"),
            )?;
        }
        "request" | "compaction_request" => {
            let attempt = value
                .get("attempt")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let encoding = value
                .get("request_encoding")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            if encoding == "full" {
                let data = value
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                writeln!(
                    writer,
                    "第 `{attempt}` 次尝试，请求体 `{}` bytes；这是该类请求的完整基准。\n",
                    data.len()
                )?;
                write_details_code(writer, "完整请求体（基准）", "json", data)?;
            } else if encoding == "delta" {
                render_request_delta(writer, value, attempt)?;
            } else {
                writeln!(writer, "第 `{attempt}` 次尝试的请求编码无效。\n")?;
                let data = serde_json::to_string_pretty(value).expect("JSON value 可序列化");
                write_details_code(writer, "完整请求审计项", "json", &data)?;
            }
        }
        "tool_call" => {
            let name = value
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let source = value
                .get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            writeln!(
                writer,
                "工具：`{}`；来源：`{}`。\n",
                markdown_inline(name),
                markdown_inline(source)
            )?;
            let data = value
                .get("data")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            write_code_block(writer, "json", data)?;
        }
        "tool_execution" => {
            writeln!(
                writer,
                "工具：`{}`；缓存：`{}`；排队 `{}` ms；执行 `{}` ms；结果 `{}` bytes；MCP server：`{}`。\n",
                markdown_inline(
                    value
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                ),
                markdown_inline(
                    value
                        .get("cache")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                ),
                value
                    .get("wait_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("execution_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("result_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                markdown_inline(
                    value
                        .get("mcp_server")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("无")
                ),
            )?;
        }
        "tool_result" => {
            let data = value
                .get("data")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            writeln!(writer, "工具结果 `{}` bytes。\n", data.len())?;
            write_details_code(writer, "完整工具结果", "text", data)?;
        }
        "retry" => {
            writeln!(
                writer,
                "第 `{}` 次尝试失败，等待 `{}` ms 后执行第 `{}` 次尝试。\n",
                value
                    .get("attempt")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("delay_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("next_attempt")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            )?;
            write_reason(
                writer,
                value.get("reason").and_then(serde_json::Value::as_str),
            )?;
        }
        "output_validation" => {
            writeln!(
                writer,
                "校验通过：`{}`；实例位置：`{}`；schema 位置：`{}`。\n",
                value
                    .get("valid")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                markdown_inline(
                    value
                        .get("instance_path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("无")
                ),
                markdown_inline(
                    value
                        .get("schema_path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("无")
                ),
            )?;
            write_reason(
                writer,
                value.get("reason").and_then(serde_json::Value::as_str),
            )?;
        }
        "context_compaction" => {
            writeln!(
                writer,
                "校验通过：`{}`；压缩前 `{}` token；压缩后 `{}` token。\n",
                value
                    .get("valid")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                value
                    .get("before_tokens")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                value
                    .get("after_tokens")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            )?;
            write_reason(
                writer,
                value.get("reason").and_then(serde_json::Value::as_str),
            )?;
        }
        _ => {
            writeln!(writer, "未知审计类型；完整事件折叠在下方。\n")?;
            let data = serde_json::to_string_pretty(value).expect("JSON value 可序列化");
            write_details_code(writer, "完整未知事件", "json", &data)?;
        }
    }
    Ok(())
}

fn audit_title(direction: &str, value: &serde_json::Value) -> String {
    match direction {
        "state" => value
            .get("state")
            .and_then(serde_json::Value::as_str)
            .and_then(|key| WorkerState::from_key(key).map(|state| (key, state)))
            .map(|(key, state)| format!("状态：{} (`{key}`)", state.label()))
            .unwrap_or_else(|| "状态变化".into()),
        "context_budget" => "上下文预算判断".into(),
        "request" => "LLM 请求".into(),
        "llm_event_stream" => "LLM 原始事件流".into(),
        "tool_call" => "模型请求工具".into(),
        "tool_execution" => "工具执行事实".into(),
        "tool_result" => "工具结果".into(),
        "retry" => "模型调用重试".into(),
        "output_validation" => "结构化结果校验".into(),
        "compaction_request" => "上下文压缩请求".into(),
        "context_compaction" => "上下文压缩结果".into(),
        other => format!("审计事件 `{}`", markdown_inline(other)),
    }
}

fn outcome_label(outcome: &str) -> &'static str {
    match outcome {
        "published" => "已发布",
        "cancelled" => "已取消",
        "failed" => "失败",
        _ => "未知",
    }
}

fn event_summary(data: &str) -> String {
    if data == "[DONE]" {
        return "SSE 结束标记".into();
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
        return "非 JSON SSE 负载".into();
    };
    if let Some(kind) = value.get("type").and_then(serde_json::Value::as_str) {
        return format!("事件类型 `{}`", markdown_inline(kind));
    }
    if let Some(reason) = value
        .pointer("/choices/0/finish_reason")
        .and_then(serde_json::Value::as_str)
    {
        return format!("Chat Completions 完成原因 `{}`", markdown_inline(reason));
    }
    if let Some(reason) = value.get("stop_reason").and_then(serde_json::Value::as_str) {
        return format!("Anthropic 完成原因 `{}`", markdown_inline(reason));
    }
    if value.pointer("/choices/0/delta/tool_calls").is_some() {
        return "Chat Completions 工具调用增量".into();
    }
    if value
        .pointer("/choices/0/delta/content")
        .and_then(serde_json::Value::as_str)
        .is_some()
    {
        return "Chat Completions 文本增量".into();
    }
    if value.get("usage").is_some() {
        return "Chat Completions 用量".into();
    }
    "JSON SSE 负载".into()
}

fn render_request_delta(
    writer: &mut impl Write,
    value: &serde_json::Value,
    attempt: u64,
) -> io::Result<()> {
    let base_bytes = value
        .get("base_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let prefix_bytes = value
        .get("prefix_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let removed_bytes = value
        .get("removed_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let inserted = value
        .get("inserted")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let full_bytes = value
        .get("full_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let suffix_bytes = base_bytes.saturating_sub(prefix_bytes.saturating_add(removed_bytes));
    writeln!(
        writer,
        "第 `{attempt}` 次尝试，请求体 `{full_bytes}` bytes；相对上一份同类请求保留前 `{prefix_bytes}` bytes 和后 `{suffix_bytes}` bytes，删除 `{removed_bytes}` bytes，插入 `{}` bytes。\n",
        inserted.len()
    )?;
    writeln!(
        writer,
        "> 重建规则：上一份请求的前 `{prefix_bytes}` bytes + 下方插入正文 + 从第 `{}` byte 起的剩余正文。\n",
        prefix_bytes.saturating_add(removed_bytes)
    )?;
    write_details_code(writer, "本轮请求变化（逐字保留）", "text", inserted)
}

fn render_event_stream<I>(
    writer: &mut impl Write,
    value: &serde_json::Value,
    fallback_sequence: usize,
    lines: &mut I,
) -> io::Result<()>
where
    I: Iterator<Item = (usize, io::Result<String>)>,
{
    use std::collections::BTreeMap;

    const DISPLAYED_KINDS: usize = 8;
    let sequence = value
        .get("sequence")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(fallback_sequence as u64);
    let elapsed_ms = value
        .get("elapsed_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let expected_count = value
        .get("event_count")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);
    let expected_bytes = value
        .get("total_bytes")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);
    let max_backtick_run = value
        .get("max_backtick_run")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);

    writeln!(
        writer,
        "### {sequence}. +{elapsed_ms} ms · LLM 原始事件流\n"
    )?;
    writeln!(
        writer,
        "本次响应包含 `{expected_count}` 个原始 SSE data 负载，共 `{expected_bytes}` bytes；已合并显示，不按 token 片段展开标题。\n"
    )?;

    writeln!(writer, "<details>")?;
    writeln!(writer, "<summary>完整原始事件流（按到达顺序）</summary>\n")?;
    let fence = "`".repeat(max_backtick_run.saturating_add(1).max(3));
    writeln!(writer, "{fence}json")?;
    writeln!(writer, "[")?;

    let mut written = 0usize;
    let mut actual_bytes = 0usize;
    let mut counts = BTreeMap::<String, usize>::new();
    let mut other_events = 0usize;
    let mut issue = None;
    for expected_index in 0..expected_count {
        let Some((line_index, line)) = lines.next() else {
            issue = Some(format!(
                "事件流提前结束：应有 {expected_count} 个负载，实际只有 {written} 个"
            ));
            break;
        };
        let line = line?;
        let child: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                issue.get_or_insert_with(|| {
                    format!("审计第 {} 行无法解析：{error}", line_index + 1)
                });
                continue;
            }
        };
        let valid_child = child.get("direction").and_then(serde_json::Value::as_str)
            == Some("llm_event_data")
            && child
                .get("stream_sequence")
                .and_then(serde_json::Value::as_u64)
                == Some(sequence)
            && child
                .get("index")
                .and_then(serde_json::Value::as_u64)
                .and_then(|index| usize::try_from(index).ok())
                == Some(expected_index);
        if !valid_child {
            issue.get_or_insert_with(|| {
                format!(
                    "审计第 {} 行不是本响应流的第 {expected_index} 个负载",
                    line_index + 1
                )
            });
            continue;
        }
        let Some(data) = child.get("data").and_then(serde_json::Value::as_str) else {
            issue.get_or_insert_with(|| format!("审计第 {} 行缺少原始负载", line_index + 1));
            continue;
        };
        if written > 0 {
            writeln!(writer, ",")?;
        }
        let encoded = serde_json::to_string(data).expect("SSE 原始负载可序列化为 JSON string");
        write!(writer, "  {encoded}")?;
        written += 1;
        actual_bytes = actual_bytes.saturating_add(data.len());
        let summary = event_summary(data);
        if let Some(count) = counts.get_mut(&summary) {
            *count += 1;
        } else if counts.len() < DISPLAYED_KINDS {
            counts.insert(summary, 1);
        } else {
            other_events += 1;
        }
    }
    if written > 0 {
        writeln!(writer)?;
    }
    writeln!(writer, "]")?;
    writeln!(writer, "{fence}\n")?;
    writeln!(writer, "</details>\n")?;

    let mut parts: Vec<String> = counts
        .into_iter()
        .map(|(kind, count)| format!("{kind} × `{count}`"))
        .collect();
    if other_events > 0 {
        parts.push(format!("其他事件 × `{other_events}`"));
    }
    if !parts.is_empty() {
        writeln!(writer, "事件概况：{}。\n", parts.join("；"))?;
    }
    if written != expected_count || actual_bytes != expected_bytes {
        issue.get_or_insert_with(|| {
            format!(
                "事件流计数不一致：记录为 {expected_count} 个/{expected_bytes} bytes，实际读到 {written} 个/{actual_bytes} bytes"
            )
        });
    }
    if let Some(issue) = issue {
        writeln!(writer, "> 审计异常：{}。\n", markdown_inline(&issue))?;
    }
    Ok(())
}

fn write_reason(writer: &mut impl Write, reason: Option<&str>) -> io::Result<()> {
    writeln!(
        writer,
        "原因：{}\n",
        markdown_inline(reason.unwrap_or("未记录"))
    )
}

fn write_details_code(
    writer: &mut impl Write,
    summary: &str,
    language: &str,
    data: &str,
) -> io::Result<()> {
    writeln!(writer, "<details>")?;
    writeln!(writer, "<summary>{}</summary>\n", markdown_inline(summary))?;
    write_code_block(writer, language, data)?;
    writeln!(writer, "</details>\n")
}

fn write_code_block(writer: &mut impl Write, language: &str, data: &str) -> io::Result<()> {
    let fence = "`".repeat(longest_backtick_run(data).saturating_add(1).max(3));
    writeln!(writer, "{fence}{language}")?;
    writeln!(writer, "{data}")?;
    writeln!(writer, "{fence}\n")
}

fn longest_backtick_run(data: &str) -> usize {
    data.split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0)
}

fn markdown_inline(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\r' | '\n' => escaped.push(' '),
            '\\' | '`' | '*' | '_' | '[' | ']' | '<' | '>' | '|' => {
                escaped.push('\\');
                escaped.push(character);
            }
            _ => escaped.push(character),
        }
    }
    escaped
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_atomically_replaces_existing_record() {
        let directory = tempfile::tempdir().unwrap();
        publish(directory.path(), 1, "old", RecordFormat::Markdown).unwrap();
        publish(directory.path(), 1, "new", RecordFormat::Markdown).unwrap();
        assert_eq!(
            fs::read_to_string(directory.path().join("1.md")).unwrap(),
            "new"
        );
        assert!(!directory.path().join(".tmp-unit-1").exists());
    }

    #[test]
    fn request_delta_is_reversible_across_large_utf8_prefix_and_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let base_path = directory.path().join("request-base");
        let prefix = "前".repeat(30_000);
        let suffix = "后".repeat(30_000);
        let previous = format!("{prefix}旧值{suffix}");
        let current = format!("{prefix}新的完整值{suffix}");
        fs::write(&base_path, &previous).unwrap();

        let delta = request_delta(&base_path, &current).unwrap().unwrap();
        assert!(current.is_char_boundary(delta.prefix_bytes));
        assert!(previous.is_char_boundary(delta.prefix_bytes + delta.removed_bytes));
        let mut reconstructed = String::new();
        reconstructed.push_str(&previous[..delta.prefix_bytes]);
        reconstructed.push_str(delta.inserted);
        reconstructed.push_str(&previous[delta.prefix_bytes + delta.removed_bytes..]);
        assert_eq!(reconstructed, current);
    }

    #[test]
    fn worker_report_contains_full_evidence_and_removes_temporary_audit() {
        let directory = tempfile::tempdir().unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let run = WorkerRun::create(
            &out,
            JobReportFacts {
                protocol: "responses".into(),
                model: "model-a".into(),
                context_window_tokens: 100_000,
                max_output_tokens: 10_000,
                context_safety_tokens: 4096,
                concurrency: 8,
                output_format: RecordFormat::Markdown,
                tools: vec!["read".into(), "search".into()],
            },
        )
        .unwrap();
        assert_eq!(run.directory.parent(), Some(out.join("workers").as_path()));
        let name = run.directory.file_name().unwrap().to_string_lossy();
        assert!(name.ends_with('Z'), "任务目录应使用 UTC 时间戳：{name}");

        let mut audit = AuditLog::create(&run, 7).unwrap();
        audit
            .push(&AuditEntry::State {
                state: WorkerState::Preparing,
                reason: "构造输入".into(),
            })
            .unwrap();
        let static_tools = "工具定义".repeat(128);
        let first_request =
            format!(r#"{{"model":"model-a","input":"完整输入","tools":"{static_tools}"}}"#);
        let second_request = format!(
            r#"{{"model":"model-a","input":"完整输入，追加查证结果","tools":"{static_tools}"}}"#
        );
        audit.push_llm_request(1, &first_request).unwrap();
        audit
            .push_llm_event_stream(&[
                r#"{"type":"response.output_text.delta","delta":"甲"}"#.into(),
                r#"{"type":"response.output_text.delta","delta":"乙"}"#.into(),
                r#"{"type":"response.completed"}"#.into(),
            ])
            .unwrap();
        audit.push_llm_request(1, &second_request).unwrap();
        audit.finish().unwrap();

        let audit_lines: Vec<serde_json::Value> = fs::read_to_string(run.audit_path(7))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let requests: Vec<&serde_json::Value> = audit_lines
            .iter()
            .filter(|entry| entry["direction"] == "request")
            .collect();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["request_encoding"], "full");
        assert_eq!(requests[0]["data"], first_request);
        assert_eq!(requests[1]["request_encoding"], "delta");
        assert!(
            requests[1].to_string().len() < second_request.len(),
            "共享前后文较大时，增量审计必须小于重复保存完整请求"
        );
        let prefix = requests[1]["prefix_bytes"].as_u64().unwrap() as usize;
        let removed = requests[1]["removed_bytes"].as_u64().unwrap() as usize;
        let inserted = requests[1]["inserted"].as_str().unwrap();
        let mut reconstructed = String::new();
        reconstructed.push_str(&first_request[..prefix]);
        reconstructed.push_str(inserted);
        reconstructed.push_str(&first_request[prefix + removed..]);
        assert_eq!(reconstructed, second_request, "UTF-8 增量必须逐字可逆");
        let streams: Vec<&serde_json::Value> = audit_lines
            .iter()
            .filter(|entry| entry["direction"] == "llm_event_stream")
            .collect();
        assert_eq!(streams.len(), 1, "一次响应流只应产生一个审计项");
        assert_eq!(streams[0]["event_count"], 3);
        let event_data: Vec<&str> = audit_lines
            .iter()
            .filter(|entry| entry["direction"] == "llm_event_data")
            .map(|entry| entry["data"].as_str().unwrap())
            .collect();
        assert_eq!(
            event_data,
            [
                r#"{"type":"response.output_text.delta","delta":"甲"}"#,
                r#"{"type":"response.output_text.delta","delta":"乙"}"#,
                r#"{"type":"response.completed"}"#,
            ],
            "合并后必须保留每个原始负载的文字、边界和顺序"
        );

        let mut stats = UnitStats::default();
        stats.llm_calls = 1;
        let now = Utc::now();
        let report = render_worker_report(
            &run,
            &WorkerReport {
                unit: 7,
                shard: "文件 input.txt",
                outcome: "failed",
                failure_reason: Some("模型拒绝执行"),
                started_at: now,
                finished_at: now,
                duration: Duration::from_millis(12),
                stats: &stats,
                record_format: None,
            },
        )
        .unwrap();
        let markdown = fs::read_to_string(report).unwrap();
        assert!(markdown.contains("Worker 7 运行档案"), "{markdown}");
        assert!(markdown.contains("状态：准备输入"), "{markdown}");
        assert!(markdown.contains("完整输入"), "{markdown}");
        assert_eq!(
            markdown.matches("完整请求体（基准）").count(),
            1,
            "后续请求不得重复展开完整历史：{markdown}"
        );
        assert!(markdown.contains("本轮请求变化（逐字保留）"), "{markdown}");
        assert_eq!(
            markdown.matches("LLM 原始事件流").count(),
            1,
            "SSE 片段不得各自生成标题：{markdown}"
        );
        assert!(markdown.contains("response.completed"), "{markdown}");
        assert!(markdown.contains("结局：`failed`"), "{markdown}");
        assert!(markdown.contains("模型拒绝执行"), "{markdown}");
        assert!(!run.audit_path(7).exists(), "成功渲染后不得保留重复 JSONL");
        assert!(
            fs::read_dir(&run.directory).unwrap().all(|entry| entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "md")),
            "成功结束后不得残留请求基准或其他临时文件"
        );
    }
}
