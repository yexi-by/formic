//! 输出区：单元记录的原子发布、worker 运行档案与作业汇总。
//! 不变量：任何时刻读到的都是完整记录——先写同目录临时文件，rename 一次性可见；
//! 失败单元没有结果文件。续跑只把结果文件与追加式状态记录一致的单元视为已发布；
//! 任一侧缺失都会在发起请求前报告状态不一致。
//! 审计的语义所有者也是本模块：完整记录协议无关的模型输入、验收后的模型事实和
//! 工具调用事实；HTTP/SSE envelope、错误 payload 与传输库原文不进入档案。

use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[cfg(test)]
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;

const OUTPUT_LOCK_FILE: &str = ".formic-job.lock";

/// 启动时绑定的输出目录能力。所有运行期输出都相对此句柄访问；`path` 只用于
/// 用户可见诊断，输出目录的环境路径被改名或替换不会改变实际写入位置。
#[derive(Clone)]
pub struct OutputRoot {
    dir: std::sync::Arc<Dir>,
    path: PathBuf,
}

impl OutputRoot {
    #[cfg(test)]
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let dir = Dir::open_ambient_dir(&path, ambient_authority())?;
        Ok(Self {
            dir: std::sync::Arc::new(dir),
            path,
        })
    }

    /// 使用调用方已经固定的目录句柄构造输出根；用于启动阶段在同一个已校验父目录
    /// capability 内创建并打开输出目录，避免重新按环境路径绑定。
    pub(crate) fn from_dir(path: impl Into<PathBuf>, dir: Dir) -> Self {
        Self {
            dir: std::sync::Arc::new(dir),
            path: path.into(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn clone_dir(&self) -> io::Result<Dir> {
        self.dir.try_clone()
    }

    pub(crate) fn create_subdir(&self, relative: &Path) -> io::Result<Self> {
        self.dir.create_dir_all(relative)?;
        let dir = self.dir.open_dir(relative)?;
        Ok(Self {
            dir: std::sync::Arc::new(dir),
            path: self.path.join(relative),
        })
    }

    pub(crate) fn display(&self, relative: &Path) -> PathBuf {
        self.path.join(relative)
    }

    fn create(&self, relative: &Path) -> io::Result<fs::File> {
        Ok(self.dir.create(relative)?.into_std())
    }

    fn open_file(&self, relative: &Path) -> io::Result<fs::File> {
        Ok(self.dir.open(relative)?.into_std())
    }

    pub(crate) fn write(&self, relative: &Path, content: impl AsRef<[u8]>) -> io::Result<()> {
        let mut file = self.create(relative)?;
        file.write_all(content.as_ref())
    }

    pub(crate) fn open_append(&self, relative: &Path) -> io::Result<fs::File> {
        Ok(self
            .dir
            .open_with(relative, OpenOptions::new().create(true).append(true))?
            .into_std())
    }

    fn remove_file(&self, relative: &Path) -> io::Result<()> {
        self.dir.remove_file(relative)
    }

    pub(crate) fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.dir.rename(source, &self.dir, target)
    }

    pub(crate) fn exists(&self, relative: &Path) -> bool {
        self.dir.symlink_metadata(relative).is_ok()
    }

    pub(crate) fn read(&self, relative: &Path) -> io::Result<Vec<u8>> {
        self.dir.read(relative)
    }

    pub(crate) fn read_dir(&self, relative: &Path) -> io::Result<cap_std::fs::ReadDir> {
        self.dir.read_dir(relative)
    }
}

/// 输出目录的跨进程独占使用权。锁文件固定存在，操作系统在进程退出时自动释放锁。
pub struct OutputLease {
    _file: fs::File,
}

#[derive(Debug, thiserror::Error)]
pub enum OutputLeaseError {
    #[error("无法打开输出目录锁 {path}：{source}")]
    Open { path: PathBuf, source: io::Error },
    #[error("输出目录 {0} 正在被另一个 Formic 作业使用")]
    InUse(PathBuf),
    #[error("无法锁定输出目录 {path}：{source}")]
    Lock { path: PathBuf, source: io::Error },
}

impl OutputLease {
    pub fn acquire(root: &OutputRoot) -> Result<Self, OutputLeaseError> {
        let path = root.path.join(OUTPUT_LOCK_FILE);
        let file = root
            .dir
            .open_with(
                OUTPUT_LOCK_FILE,
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false),
            )
            .map(cap_std::fs::File::into_std)
            .map_err(|source| OutputLeaseError::Open {
                path: path.clone(),
                source,
            })?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file }),
            Err(source) if lock_is_in_use(&source) => {
                Err(OutputLeaseError::InUse(root.path.clone()))
            }
            Err(source) => Err(OutputLeaseError::Lock {
                path: root.path.clone(),
                source,
            }),
        }
    }
}

fn lock_is_in_use(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || cfg!(windows) && matches!(error.raw_os_error(), Some(32) | Some(33))
}

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
    pub anthropic_max_tokens: Option<u64>,
    pub context_safety_tokens: u64,
    pub concurrency: usize,
    pub output_format: RecordFormat,
    pub tools: Vec<String>,
}

/// 一轮任务的 worker 观测目录。自然递增的 run 序号在 worker 启动前确定，临时
/// 审计与 Markdown 视图共享该目录，避免 resume 时把旧轮证据指向新轮现场。
pub struct WorkerRun {
    root: OutputRoot,
    relative_directory: PathBuf,
    directory: PathBuf,
    started_at: DateTime<Utc>,
    facts: JobReportFacts,
}

impl WorkerRun {
    pub fn create(root: &OutputRoot, facts: JobReportFacts) -> io::Result<Self> {
        root.dir.create_dir_all("runs")?;
        let started_at = Utc::now();
        let mut sequence = 1u64;
        let directory = loop {
            let relative = Path::new("runs").join(format!("run-{sequence:06}"));
            match root.dir.create_dir(&relative) {
                Ok(()) => {
                    root.dir.create_dir(relative.join("workers"))?;
                    break relative;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => sequence += 1,
                Err(error) => return Err(error),
            }
        };
        Ok(Self {
            root: root.clone(),
            directory: root.display(&directory),
            relative_directory: directory,
            started_at,
            facts,
        })
    }

    pub(crate) fn audit_path(&self, unit: u64) -> PathBuf {
        self.directory
            .join("workers")
            .join(format!(".tmp-worker-{unit}.jsonl"))
    }

    fn audit_relative(&self, unit: u64) -> PathBuf {
        self.relative_directory
            .join("workers")
            .join(format!(".tmp-worker-{unit}.jsonl"))
    }

    fn request_base_path(&self, unit: u64, kind: RequestKind) -> PathBuf {
        self.relative_directory
            .join("workers")
            .join(format!(".tmp-worker-{unit}-{}-request", kind.key()))
    }

    pub fn report_path(&self, unit: u64) -> PathBuf {
        self.directory.join("workers").join(format!("{unit}.md"))
    }
}

/// 原子发布单元产出，返回记录路径。已经发布的结果不可覆盖。
pub fn publish(
    root: &OutputRoot,
    unit: u64,
    content: &str,
    format: RecordFormat,
) -> io::Result<PathBuf> {
    let tmp = PathBuf::from(format!(".tmp-unit-{unit}"));
    let target = PathBuf::from(format!("{unit}.{}", format.extension()));
    if root.exists(&target) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("单元 {unit} 的结果已经发布，不能覆盖"),
        ));
    }
    root.write(&tmp, content)?;
    root.rename(&tmp, &target)?;
    Ok(root.display(&target))
}

/// 单元审计日志：流式逐条落盘，不在内存累积。协议无关模型输入以磁盘上的上一份同类输入
/// 计算可逆增量，避免最终档案和 worker 内存随“每轮完整历史之和”增长。
/// 文件自创建起存在，空文件表示单元在首次调用前结束。
pub struct AuditLog {
    root: OutputRoot,
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
        let path = run.audit_relative(unit);
        let file = run.root.create(&path)?;
        Ok(Self {
            root: run.root.clone(),
            writer: BufWriter::new(file),
            path: run.audit_path(unit),
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

    /// 保存一次普通模型调用的协议无关输入。首个输入保存完整正文，后续输入保存相对上一份
    /// 同类输入的可逆字节增量。基准正文只在 worker 运行期间保留在临时文件中，避免用内存保存
    /// 随历史增长的大字符串。
    pub fn push_llm_request(&mut self, attempt: u32, body: &str) -> io::Result<()> {
        self.push_request(RequestKind::Llm, attempt, body)
    }

    /// 压缩请求使用独立基准，避免它和普通请求的不同 instructions、工具目录互相打断
    /// 增量序列。
    pub fn push_compaction_request(&mut self, attempt: u32, body: &str) -> io::Result<()> {
        self.push_request(RequestKind::Compaction, attempt, body)
    }

    fn push_request(&mut self, kind: RequestKind, attempt: u32, body: &str) -> io::Result<()> {
        let base_path = match kind {
            RequestKind::Llm => self.llm_request_base.clone(),
            RequestKind::Compaction => self.compaction_request_base.clone(),
        };
        let delta = request_delta(&self.root, &base_path, body)?;
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
        self.root.write(&base_path, body)
    }

    fn next_stamp(&mut self) -> (u64, u64) {
        self.sequence += 1;
        let elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        (self.sequence, elapsed_ms)
    }

    fn cleanup_request_bases(&self) -> io::Result<()> {
        for path in [&self.llm_request_base, &self.compaction_request_base] {
            match self.root.remove_file(path) {
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
            eprintln!("worker 模型输入基准临时文件清理失败：{error}");
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
            eprintln!("worker 模型输入基准临时文件清理失败：{error}");
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

fn request_delta<'a>(
    root: &OutputRoot,
    base_path: &Path,
    body: &'a str,
) -> io::Result<Option<RequestDelta<'a>>> {
    let mut base = match root.open_file(base_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let base_bytes = usize::try_from(base.metadata()?.len())
        .map_err(|_| io::Error::other("模型输入基准文件大小超出当前平台可表示范围"))?;
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
            .map_err(|_| io::Error::other("模型输入基准偏移超出文件 API 可表示范围"))?;
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
    Stopped,
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
            Self::Stopped => "stopped",
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
            Self::Stopped => "已停止",
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
            "stopped" => Self::Stopped,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}

/// 除模型输入之外的一条审计留痕。供应商的 SSE envelope、残帧和错误 payload 不进入
/// worker 档案；成功响应只记录完成语义与解析后的助手正文。
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
    /// 已经完整通过协议解析和回合语义验收的模型响应。
    ModelResponse {
        finish: String,
        text: String,
        tool_calls: usize,
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
    /// 结构化结果的本地校验，不重复保存已在模型响应中的结果正文。
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
            AuditEntry::ModelResponse {
                finish,
                text,
                tool_calls,
            } => serde_json::json!({
                "direction": "model_response",
                "finish": finish,
                "text": text,
                "tool_calls": tool_calls,
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
    pub llm_calls_with_provider_usage: u64,
    pub llm_calls_without_provider_usage: u64,
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
            self.llm_calls_without_provider_usage += 1;
            return;
        }
        self.llm_calls_with_provider_usage += 1;
        self.provider_input_tokens += usage.input_tokens.unwrap_or(0);
        self.provider_output_tokens += usage.output_tokens.unwrap_or(0);
        self.provider_cache_read_tokens += usage.cache_read_tokens.unwrap_or(0);
        self.provider_cache_creation_tokens += usage.cache_creation_tokens.unwrap_or(0);
    }
}

/// 追加一行单元统计到当前 `runs/run-N/stats.jsonl`。附属证据：写失败只产生诊断，
/// 不改写单元的业务结果（§9）。
pub fn append_stats(
    run: &WorkerRun,
    unit: u64,
    outcome: &str,
    stats: &UnitStats,
) -> io::Result<()> {
    let line = stats_value(unit, outcome, stats);
    let mut file = run
        .root
        .dir
        .open_with(
            run.relative_directory.join("stats.jsonl"),
            OpenOptions::new().create(true).append(true),
        )?
        .into_std();
    writeln!(file, "{line}")
}

#[derive(Debug, serde::Serialize)]
pub struct RunSummary {
    pub planned: u64,
    pub already_completed: u64,
    pub started: u64,
    pub published: u64,
    pub failed: u64,
    pub stopped: u64,
    pub not_started: u64,
    pub first_failed: Option<u64>,
    pub failed_samples: Vec<u64>,
    pub first_stopped: Option<u64>,
    pub stopped_samples: Vec<u64>,
    pub first_incomplete: Option<u64>,
    pub incomplete_samples: Vec<u64>,
    pub failure_reasons: std::collections::BTreeMap<String, u64>,
    pub stop_reason: Option<String>,
    pub llm_calls: u64,
    pub llm_calls_with_provider_usage: u64,
    pub llm_calls_without_provider_usage: u64,
}

pub fn write_run_summary(run: &WorkerRun, summary: &RunSummary) -> io::Result<PathBuf> {
    let content = format!(
        "{}\n",
        serde_json::to_string_pretty(summary).expect("运行汇总可以序列化")
    );
    let temporary = run.relative_directory.join(".tmp-summary.json");
    let target = run.relative_directory.join("summary.json");
    run.root.write(&temporary, content)?;
    run.root.rename(&temporary, &target)?;
    Ok(run.root.display(&target))
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
        "llm_calls_with_provider_usage": stats.llm_calls_with_provider_usage,
        "llm_calls_without_provider_usage": stats.llm_calls_without_provider_usage,
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

/// 将一个 worker 的结构化审计流式渲染为 Markdown。普通事件逐行读取；内存只保留
/// LLM 与压缩请求各自的当前正文，以及正在严格校验的一次完整 LLM 事件流。
/// Markdown 成功原子发布后删除临时 JSONL，最终每个 worker 只保留一份完整证据。
pub fn render_worker_report(run: &WorkerRun, report: &WorkerReport<'_>) -> io::Result<PathBuf> {
    let target = run.report_path(report.unit);
    let target_relative = run
        .relative_directory
        .join("workers")
        .join(format!("{}.md", report.unit));
    let temporary = run
        .relative_directory
        .join("workers")
        .join(format!(".tmp-worker-{}.md", report.unit));
    let rendered = (|| -> io::Result<()> {
        let file = run.root.create(&temporary)?;
        let mut writer = BufWriter::new(file);
        write_report_header(&mut writer, run, report)?;
        render_audit_timeline(
            &mut writer,
            &run.root,
            &run.audit_relative(report.unit),
            &run.audit_path(report.unit),
        )?;
        writer.flush()
    })();
    if let Err(error) = rendered {
        let _ = run.root.remove_file(&temporary);
        return Err(error);
    }
    run.root.rename(&temporary, &target_relative)?;
    let audit = run.audit_relative(report.unit);
    if run.root.exists(&audit) {
        run.root.remove_file(&audit)?;
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
        "- 审计编码：同类请求首份保存完整正文，后续保存可逆字节增量；响应只保存解析后的允许事实"
    )?;
    if let Some(format) = report.record_format {
        writeln!(
            writer,
            "- 完成记录：[{}.{}](../../../results/{}.{})",
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
    if let Some(max_tokens) = run.facts.anthropic_max_tokens {
        writeln!(writer, "- Anthropic max_tokens：`{max_tokens}` token")?;
    }
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

fn render_audit_timeline(
    writer: &mut impl Write,
    root: &OutputRoot,
    relative: &Path,
    display_path: &Path,
) -> io::Result<()> {
    let file = match root.open_file(relative) {
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
    let lines = BufReader::new(file).lines().enumerate();
    let mut request_bases = RequestAuditBases::default();
    for (index, line) in lines {
        let line = line?;
        let line_number = index + 1;
        let value: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
            invalid_audit(display_path, line_number, format!("不是合法 JSON：{error}"))
        })?;
        let object = audit_object(&value, display_path, line_number)?;
        audit_string(object, "direction", display_path, line_number)?;
        validate_audit_entry(&value, display_path, line_number, &mut request_bases)?;
        render_audit_entry(writer, &value, line_number)?;
    }
    Ok(())
}

#[derive(Default)]
struct RequestAuditBases {
    llm: Option<String>,
    compaction: Option<String>,
}

impl RequestAuditBases {
    fn slot(&mut self, direction: &str) -> &mut Option<String> {
        match direction {
            "request" => &mut self.llm,
            "compaction_request" => &mut self.compaction,
            _ => unreachable!("只为两类请求审计取得基准"),
        }
    }
}

fn invalid_audit(path: &Path, line: usize, reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("审计文件 {} 第 {line} 行损坏：{reason}", path.display()),
    )
}

fn audit_object<'a>(
    value: &'a serde_json::Value,
    path: &Path,
    line: usize,
) -> io::Result<&'a serde_json::Map<String, serde_json::Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid_audit(path, line, "记录必须是 JSON object"))
}

fn audit_exact_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    expected: &[&str],
    path: &Path,
    line: usize,
) -> io::Result<()> {
    if let Some(field) = expected.iter().find(|field| !object.contains_key(**field)) {
        return Err(invalid_audit(path, line, format!("缺少字段 {field:?}")));
    }
    if let Some(field) = object
        .keys()
        .find(|field| !expected.contains(&field.as_str()))
    {
        return Err(invalid_audit(path, line, format!("含未知字段 {field:?}")));
    }
    Ok(())
}

fn audit_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<&'a str> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid_audit(path, line, format!("字段 {field:?} 必须是字符串")))
}

fn audit_nullable_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<()> {
    match object.get(field) {
        Some(serde_json::Value::Null | serde_json::Value::String(_)) => Ok(()),
        Some(_) => Err(invalid_audit(
            path,
            line,
            format!("字段 {field:?} 必须是字符串或 null"),
        )),
        None => Err(invalid_audit(path, line, format!("缺少字段 {field:?}"))),
    }
}

fn audit_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<u64> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| invalid_audit(path, line, format!("字段 {field:?} 必须是非负整数")))
}

fn audit_usize(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<usize> {
    usize::try_from(audit_u64(object, field, path, line)?)
        .map_err(|_| invalid_audit(path, line, format!("字段 {field:?} 超出当前平台可表示范围")))
}

fn audit_u32(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<u32> {
    u32::try_from(audit_u64(object, field, path, line)?)
        .map_err(|_| invalid_audit(path, line, format!("字段 {field:?} 超出 u32 可表示范围")))
}

fn audit_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
    line: usize,
) -> io::Result<bool> {
    object
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid_audit(path, line, format!("字段 {field:?} 必须是 boolean")))
}

fn validate_audit_stamp(
    object: &serde_json::Map<String, serde_json::Value>,
    path: &Path,
    line: usize,
) -> io::Result<()> {
    if audit_u64(object, "sequence", path, line)? == 0 {
        return Err(invalid_audit(path, line, "字段 \"sequence\" 必须不小于 1"));
    }
    audit_u64(object, "elapsed_ms", path, line)?;
    Ok(())
}

fn validate_audit_entry(
    value: &serde_json::Value,
    path: &Path,
    line: usize,
    request_bases: &mut RequestAuditBases,
) -> io::Result<()> {
    let object = audit_object(value, path, line)?;
    let direction = audit_string(object, "direction", path, line)?;
    match direction {
        "state" => {
            audit_exact_fields(
                object,
                &["direction", "state", "reason", "sequence", "elapsed_ms"],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            let state = audit_string(object, "state", path, line)?;
            if WorkerState::from_key(state).is_none() {
                return Err(invalid_audit(
                    path,
                    line,
                    format!("未知 worker state {state:?}"),
                ));
            }
            audit_string(object, "reason", path, line)?;
        }
        "context_budget" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "estimated_tokens",
                    "input_budget",
                    "force",
                    "action",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_u64(object, "estimated_tokens", path, line)?;
            audit_u64(object, "input_budget", path, line)?;
            audit_bool(object, "force", path, line)?;
            audit_string(object, "action", path, line)?;
        }
        "model_response" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "finish",
                    "text",
                    "tool_calls",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            match audit_string(object, "finish", path, line)? {
                "stop" | "tool_use" => {}
                other => {
                    return Err(invalid_audit(
                        path,
                        line,
                        format!("未知模型完成类别 {other:?}"),
                    ));
                }
            }
            audit_string(object, "text", path, line)?;
            audit_usize(object, "tool_calls", path, line)?;
        }
        "request" | "compaction_request" => {
            validate_request_audit(object, direction, path, line, request_bases)?;
        }
        "tool_call" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "name",
                    "source",
                    "data",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_string(object, "name", path, line)?;
            audit_nullable_string(object, "source", path, line)?;
            audit_string(object, "data", path, line)?;
        }
        "tool_execution" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "name",
                    "cache",
                    "wait_ms",
                    "execution_ms",
                    "result_bytes",
                    "mcp_server",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_string(object, "name", path, line)?;
            audit_string(object, "cache", path, line)?;
            audit_u64(object, "wait_ms", path, line)?;
            audit_u64(object, "execution_ms", path, line)?;
            audit_usize(object, "result_bytes", path, line)?;
            audit_nullable_string(object, "mcp_server", path, line)?;
        }
        "tool_result" => {
            audit_exact_fields(
                object,
                &["direction", "data", "sequence", "elapsed_ms"],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_string(object, "data", path, line)?;
        }
        "retry" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "attempt",
                    "next_attempt",
                    "delay_ms",
                    "reason",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_u32(object, "attempt", path, line)?;
            audit_u32(object, "next_attempt", path, line)?;
            audit_u64(object, "delay_ms", path, line)?;
            audit_string(object, "reason", path, line)?;
        }
        "output_validation" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "valid",
                    "instance_path",
                    "schema_path",
                    "reason",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_bool(object, "valid", path, line)?;
            audit_nullable_string(object, "instance_path", path, line)?;
            audit_nullable_string(object, "schema_path", path, line)?;
            audit_string(object, "reason", path, line)?;
        }
        "context_compaction" => {
            audit_exact_fields(
                object,
                &[
                    "direction",
                    "valid",
                    "before_tokens",
                    "after_tokens",
                    "reason",
                    "sequence",
                    "elapsed_ms",
                ],
                path,
                line,
            )?;
            validate_audit_stamp(object, path, line)?;
            audit_bool(object, "valid", path, line)?;
            audit_u64(object, "before_tokens", path, line)?;
            audit_u64(object, "after_tokens", path, line)?;
            audit_string(object, "reason", path, line)?;
        }
        other => {
            return Err(invalid_audit(
                path,
                line,
                format!("未知 direction {other:?}"),
            ));
        }
    }
    Ok(())
}

fn validate_request_audit(
    object: &serde_json::Map<String, serde_json::Value>,
    direction: &str,
    path: &Path,
    line: usize,
    request_bases: &mut RequestAuditBases,
) -> io::Result<()> {
    let encoding = audit_string(object, "request_encoding", path, line)?;
    match encoding {
        "full" => audit_exact_fields(
            object,
            &[
                "direction",
                "sequence",
                "elapsed_ms",
                "attempt",
                "request_encoding",
                "full_bytes",
                "data",
            ],
            path,
            line,
        )?,
        "delta" => audit_exact_fields(
            object,
            &[
                "direction",
                "sequence",
                "elapsed_ms",
                "attempt",
                "request_encoding",
                "base_bytes",
                "prefix_bytes",
                "removed_bytes",
                "inserted",
                "full_bytes",
            ],
            path,
            line,
        )?,
        other => {
            return Err(invalid_audit(
                path,
                line,
                format!("未知 request_encoding {other:?}"),
            ));
        }
    }
    validate_audit_stamp(object, path, line)?;
    if audit_u32(object, "attempt", path, line)? == 0 {
        return Err(invalid_audit(path, line, "字段 \"attempt\" 必须不小于 1"));
    }
    let full_bytes = audit_usize(object, "full_bytes", path, line)?;
    let slot = request_bases.slot(direction);
    if encoding == "full" {
        let data = audit_string(object, "data", path, line)?;
        if data.len() != full_bytes {
            return Err(invalid_audit(
                path,
                line,
                format!(
                    "完整模型输入记录的 full_bytes 为 {full_bytes}，正文实际为 {} 字节",
                    data.len()
                ),
            ));
        }
        *slot = Some(data.to_owned());
        return Ok(());
    }

    let base_bytes = audit_usize(object, "base_bytes", path, line)?;
    let base = slot.as_ref().ok_or_else(|| {
        invalid_audit(
            path,
            line,
            format!("{direction} 的首条记录不能使用 delta 编码"),
        )
    })?;
    if base_bytes != base.len() {
        return Err(invalid_audit(
            path,
            line,
            format!(
                "delta 基准为 {base_bytes} 字节，上一份请求实际为 {} 字节",
                base.len()
            ),
        ));
    }
    let prefix_bytes = audit_usize(object, "prefix_bytes", path, line)?;
    let removed_bytes = audit_usize(object, "removed_bytes", path, line)?;
    let replaced_end = prefix_bytes.checked_add(removed_bytes).ok_or_else(|| {
        invalid_audit(path, line, "delta 的 prefix_bytes + removed_bytes 发生溢出")
    })?;
    if replaced_end > base_bytes {
        return Err(invalid_audit(
            path,
            line,
            format!("delta 删除区间结束于 {replaced_end} 字节，超过 {base_bytes} 字节基准"),
        ));
    }
    let prefix = base
        .get(..prefix_bytes)
        .ok_or_else(|| invalid_audit(path, line, "delta 的 prefix_bytes 不在 UTF-8 字符边界"))?;
    let suffix = base
        .get(replaced_end..)
        .ok_or_else(|| invalid_audit(path, line, "delta 的删除区间末尾不在 UTF-8 字符边界"))?;
    let inserted = audit_string(object, "inserted", path, line)?;
    let reconstructed_bytes = prefix
        .len()
        .checked_add(inserted.len())
        .and_then(|length| length.checked_add(suffix.len()))
        .ok_or_else(|| invalid_audit(path, line, "delta 重建长度发生溢出"))?;
    if reconstructed_bytes != full_bytes {
        return Err(invalid_audit(
            path,
            line,
            format!("delta 可重建 {reconstructed_bytes} 字节，但 full_bytes 记录为 {full_bytes}"),
        ));
    }
    let mut reconstructed = String::with_capacity(reconstructed_bytes);
    reconstructed.push_str(prefix);
    reconstructed.push_str(inserted);
    reconstructed.push_str(suffix);
    *slot = Some(reconstructed);
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
        "model_response" => {
            let finish = value
                .get("finish")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let tool_calls = value
                .get("tool_calls")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let text = value
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            writeln!(
                writer,
                "完成类别：`{}`；工具调用：`{tool_calls}`；助手正文：`{}` bytes。\n",
                markdown_inline(finish),
                text.len(),
            )?;
            if !text.is_empty() {
                write_details_code(writer, "解析后的助手正文", "text", text)?;
            }
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
                    "第 `{attempt}` 次尝试，模型输入 `{}` bytes；这是该类输入的完整基准。\n",
                    data.len()
                )?;
                write_details_code(writer, "完整模型输入（基准）", "json", data)?;
            } else if encoding == "delta" {
                render_request_delta(writer, value, attempt)?;
            } else {
                writeln!(writer, "第 `{attempt}` 次尝试的请求编码无效。\n")?;
                let data = serde_json::to_string_pretty(value).expect("JSON value 可序列化");
                write_details_code(writer, "完整模型输入审计项", "json", &data)?;
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
        "model_response" => "模型响应".into(),
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
        "第 `{attempt}` 次尝试，模型输入 `{full_bytes}` bytes；相对上一份同类输入保留前 `{prefix_bytes}` bytes 和后 `{suffix_bytes}` bytes，删除 `{removed_bytes}` bytes，插入 `{}` bytes。\n",
        inserted.len()
    )?;
    writeln!(
        writer,
        "> 重建规则：上一份模型输入的前 `{prefix_bytes}` bytes + 下方插入正文 + 从第 `{}` byte 起的剩余正文。\n",
        prefix_bytes.saturating_add(removed_bytes)
    )?;
    write_details_code(writer, "本轮模型输入变化（逐字保留）", "text", inserted)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_corrupt_audit_is_preserved(name: &str, contents: &str) {
        let directory = tempfile::tempdir().unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let root = OutputRoot::open(out).unwrap();
        let run = WorkerRun::create(
            &root,
            JobReportFacts {
                protocol: "responses".into(),
                model: "model-a".into(),
                context_window_tokens: 100_000,
                anthropic_max_tokens: None,
                context_safety_tokens: 4096,
                concurrency: 1,
                output_format: RecordFormat::Markdown,
                tools: Vec::new(),
            },
        )
        .unwrap();
        fs::write(run.audit_path(1), contents).unwrap();
        let now = Utc::now();
        let error = render_worker_report(
            &run,
            &WorkerReport {
                unit: 1,
                shard: "文件 input.txt",
                outcome: "failed",
                failure_reason: Some("测试失败"),
                started_at: now,
                finished_at: now,
                duration: Duration::ZERO,
                stats: &UnitStats::default(),
                record_format: None,
            },
        )
        .expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}: {error}");
        assert!(run.audit_path(1).exists(), "{name}：损坏的原始审计必须保留");
        assert!(!run.report_path(1).exists(), "{name}：不完整报告不得发布");
        assert!(
            !run.directory.join(".tmp-worker-1.md").exists(),
            "{name}：失败的 Markdown 临时文件必须删除"
        );
    }

    // Windows 的文件锁按进程持有，同一测试进程内的第二个句柄可再次加锁；
    // 跨进程行为由 e2e::concurrent_jobs_cannot_share_an_output_directory 验证。
    #[cfg(not(windows))]
    #[test]
    fn output_directory_is_exclusively_leased() {
        let directory = tempfile::tempdir().unwrap();
        let root = OutputRoot::open(directory.path().to_path_buf()).unwrap();
        let first = OutputLease::acquire(&root).unwrap();
        assert!(matches!(
            OutputLease::acquire(&root),
            Err(OutputLeaseError::InUse(_))
        ));
        drop(first);
        OutputLease::acquire(&root).unwrap();
    }

    #[test]
    fn publish_never_overwrites_existing_record() {
        let directory = tempfile::tempdir().unwrap();
        let root = OutputRoot::open(directory.path().to_path_buf()).unwrap();
        publish(&root, 1, "old", RecordFormat::Markdown).unwrap();
        let error = publish(&root, 1, "new", RecordFormat::Markdown).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read_to_string(directory.path().join("1.md")).unwrap(),
            "old"
        );
        assert!(!directory.path().join(".tmp-unit-1").exists());
    }

    #[cfg(unix)]
    #[test]
    fn output_writes_stay_with_opened_root_after_ambient_path_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let ambient = directory.path().join("out");
        let opened_directory = directory.path().join("opened-out");
        fs::create_dir(&ambient).unwrap();
        let root = OutputRoot::open(ambient.clone()).unwrap();
        let _lease = OutputLease::acquire(&root).unwrap();

        fs::rename(&ambient, &opened_directory).unwrap();
        fs::create_dir(&ambient).unwrap();
        fs::write(ambient.join("attacker-marker"), "replacement").unwrap();

        publish(&root, 1, "result", RecordFormat::Markdown).unwrap();
        let run = WorkerRun::create(
            &root,
            JobReportFacts {
                protocol: "responses".into(),
                model: "model-a".into(),
                context_window_tokens: 100_000,
                anthropic_max_tokens: None,
                context_safety_tokens: 4096,
                concurrency: 1,
                output_format: RecordFormat::Markdown,
                tools: Vec::new(),
            },
        )
        .unwrap();
        append_stats(&run, 1, "published", &UnitStats::default()).unwrap();
        let mut audit = AuditLog::create(&run, 1).unwrap();
        audit
            .push(&AuditEntry::State {
                state: WorkerState::Preparing,
                reason: "测试".into(),
            })
            .unwrap();
        audit.finish().unwrap();
        let now = Utc::now();
        render_worker_report(
            &run,
            &WorkerReport {
                unit: 1,
                shard: "文件 input.txt",
                outcome: "published",
                failure_reason: None,
                started_at: now,
                finished_at: now,
                duration: Duration::ZERO,
                stats: &UnitStats::default(),
                record_format: Some(RecordFormat::Markdown),
            },
        )
        .unwrap();

        assert_eq!(
            fs::read_to_string(opened_directory.join("1.md")).unwrap(),
            "result"
        );
        assert!(
            opened_directory
                .join(&run.relative_directory)
                .join("stats.jsonl")
                .exists()
        );
        assert!(
            opened_directory
                .join(&run.relative_directory)
                .join("workers")
                .join("1.md")
                .exists(),
            "最终 worker 档案必须留在启动时打开的输出目录"
        );
        assert_eq!(
            fs::read_dir(&ambient).unwrap().count(),
            1,
            "运行期替换目录不得收到完成记录、统计、锁或 worker 档案"
        );
    }

    #[test]
    fn request_delta_is_reversible_across_large_utf8_prefix_and_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let root = OutputRoot::open(directory.path().to_path_buf()).unwrap();
        let base_path = PathBuf::from("request-base");
        let prefix = "前".repeat(30_000);
        let suffix = "后".repeat(30_000);
        let previous = format!("{prefix}旧值{suffix}");
        let current = format!("{prefix}新的完整值{suffix}");
        root.write(&base_path, &previous).unwrap();

        let delta = request_delta(&root, &base_path, &current).unwrap().unwrap();
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
        let root = OutputRoot::open(out.clone()).unwrap();
        let run = WorkerRun::create(
            &root,
            JobReportFacts {
                protocol: "responses".into(),
                model: "model-a".into(),
                context_window_tokens: 100_000,
                anthropic_max_tokens: None,
                context_safety_tokens: 4096,
                concurrency: 8,
                output_format: RecordFormat::Markdown,
                tools: vec!["read".into(), "search".into()],
            },
        )
        .unwrap();
        assert_eq!(run.directory.parent(), Some(out.join("runs").as_path()));
        let name = run.directory.file_name().unwrap().to_string_lossy();
        assert_eq!(name, "run-000001");

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
            .push(&AuditEntry::ModelResponse {
                finish: "stop".into(),
                text: "甲乙".into(),
                tool_calls: 0,
            })
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
            "共享前后文较大时，增量审计必须小于重复保存完整模型输入"
        );
        let prefix = requests[1]["prefix_bytes"].as_u64().unwrap() as usize;
        let removed = requests[1]["removed_bytes"].as_u64().unwrap() as usize;
        let inserted = requests[1]["inserted"].as_str().unwrap();
        let mut reconstructed = String::new();
        reconstructed.push_str(&first_request[..prefix]);
        reconstructed.push_str(inserted);
        reconstructed.push_str(&first_request[prefix + removed..]);
        assert_eq!(reconstructed, second_request, "UTF-8 增量必须逐字可逆");
        let responses: Vec<&serde_json::Value> = audit_lines
            .iter()
            .filter(|entry| entry["direction"] == "model_response")
            .collect();
        assert_eq!(responses.len(), 1, "一次成功调用只应产生一个响应审计项");
        assert_eq!(responses[0]["text"], "甲乙");
        assert!(
            !fs::read_to_string(run.audit_path(7))
                .unwrap()
                .contains("response.output_text.delta"),
            "供应商 SSE envelope 不得进入 worker 审计"
        );

        let stats = UnitStats {
            llm_calls: 1,
            ..UnitStats::default()
        };
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
            markdown.matches("完整模型输入（基准）").count(),
            1,
            "后续请求不得重复展开完整历史：{markdown}"
        );
        assert!(
            markdown.contains("本轮模型输入变化（逐字保留）"),
            "{markdown}"
        );
        assert_eq!(
            markdown.matches("模型响应").count(),
            1,
            "解析后的响应只生成一个标题：{markdown}"
        );
        assert!(markdown.contains("甲乙"), "{markdown}");
        assert!(!markdown.contains("response.completed"), "{markdown}");
        assert!(markdown.contains("结局：`failed`"), "{markdown}");
        assert!(markdown.contains("模型拒绝执行"), "{markdown}");
        assert!(!run.audit_path(7).exists(), "成功渲染后不得保留重复 JSONL");
        assert!(
            fs::read_dir(run.directory.join("workers"))
                .unwrap()
                .all(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "md")),
            "成功结束后不得残留模型输入基准或其他临时文件"
        );
    }

    #[test]
    fn malformed_audit_prevents_report_and_preserves_original() {
        assert_corrupt_audit_is_preserved("非法 JSON", "{not-json}\n");
    }

    #[test]
    fn structurally_corrupt_audits_are_rejected_and_preserved() {
        let valid_full = serde_json::json!({
            "direction": "request",
            "sequence": 1,
            "elapsed_ms": 0,
            "attempt": 1,
            "request_encoding": "full",
            "full_bytes": 2,
            "data": "{}",
        });
        let invalid_delta = serde_json::json!({
            "direction": "request",
            "sequence": 2,
            "elapsed_ms": 1,
            "attempt": 2,
            "request_encoding": "delta",
            "base_bytes": 2,
            "prefix_bytes": 3,
            "removed_bytes": 0,
            "inserted": "",
            "full_bytes": 3,
        });
        let cases = [
            ("空 object", "{}\n".to_string()),
            (
                "未知 direction",
                serde_json::json!({
                    "direction": "future",
                    "sequence": 1,
                    "elapsed_ms": 0,
                })
                .to_string()
                    + "\n",
            ),
            (
                "full request 缺 data",
                serde_json::json!({
                    "direction": "request",
                    "sequence": 1,
                    "elapsed_ms": 0,
                    "attempt": 1,
                    "request_encoding": "full",
                    "full_bytes": 2,
                })
                .to_string()
                    + "\n",
            ),
            (
                "delta 删除区间越界",
                format!("{valid_full}\n{invalid_delta}\n"),
            ),
            (
                "delta 偏移不在 UTF-8 字符边界",
                format!(
                    "{}\n{}\n",
                    serde_json::json!({
                        "direction": "request",
                        "sequence": 1,
                        "elapsed_ms": 0,
                        "attempt": 1,
                        "request_encoding": "full",
                        "full_bytes": 3,
                        "data": "你",
                    }),
                    serde_json::json!({
                        "direction": "request",
                        "sequence": 2,
                        "elapsed_ms": 1,
                        "attempt": 2,
                        "request_encoding": "delta",
                        "base_bytes": 3,
                        "prefix_bytes": 1,
                        "removed_bytes": 0,
                        "inserted": "",
                        "full_bytes": 3,
                    })
                ),
            ),
            (
                "模型响应含未知完成类别",
                serde_json::json!({
                    "direction": "model_response",
                    "sequence": 1,
                    "elapsed_ms": 0,
                    "finish": "unknown",
                    "text": "",
                    "tool_calls": 0,
                })
                .to_string()
                    + "\n",
            ),
        ];
        for (name, contents) in cases {
            assert_corrupt_audit_is_preserved(name, &contents);
        }
    }
}
