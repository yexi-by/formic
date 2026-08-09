//! 入口：CLI 解析、启动配置读取、错误的人性化呈现。
//! 退出码：0 全部成功；1 存在失败单元；2 启动失败（参数、输入、配置或环境）。

use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use chrono::Utc;
use clap::Parser;
use futures_util::FutureExt;
use same_file::Handle;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

mod cache;
mod compaction;
mod config;
mod llm;
mod mcp;
mod metrics;
mod output;
mod plan;
mod prompt;
mod scheduler;
mod structured;
mod tokenize;
mod tools;
mod worker;

use llm::{LlmClient, Protocol};
use output::Summary;

/// 任务说明的大小上限（结构校验的一部分，语义边界见 design.md §3）。
const MAX_TASK_BYTES: u64 = 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "formic",
    about = "批处理自主执行内核：一次调用 = 一个批处理作业"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// 运行一个批处理作业
    Run(RunArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// 输入数据集根目录
    #[arg(long)]
    data: PathBuf,
    /// 分片计划（JSONL，一行一个单元）
    #[arg(long)]
    plan: PathBuf,
    /// 任务说明（自然语言文本文件，原样装配进 prompt）
    #[arg(long)]
    task: PathBuf,
    /// 输出区目录
    #[arg(long)]
    out: PathBuf,
    /// 并发窗口：同时运行的单元数上限，调用方唯一的策略选择（依据是 LLM 配额）
    #[arg(long)]
    concurrency: usize,
    /// 可选的作业级 JSON Schema；启用后完成记录为 JSON
    #[arg(long)]
    output_schema: Option<PathBuf>,
}

#[derive(thiserror::Error, Debug)]
enum StartupError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Mcp(#[from] mcp::McpStartupError),
    #[error(transparent)]
    Registry(#[from] scheduler::RegistryError),
    #[error("数据目录 {0} 不存在或不是目录")]
    DataRoot(PathBuf),
    #[error("任务说明 {path} 无法读取：{source}")]
    TaskRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("任务说明 {0} 不是合法 UTF-8")]
    TaskEncoding(PathBuf),
    #[error("任务说明 {0} 为空或超过 1 MiB 上限")]
    TaskInvalid(PathBuf),
    #[error(transparent)]
    Plan(#[from] plan::PlanError),
    #[error("--concurrency 必须是不小于 1 的整数")]
    ConcurrencyZero,
    #[error("无法解析数据目录 {path} 的真实路径：{source}")]
    DataCanonical {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("无法解析输出目录 {path} 的真实路径：{source}")]
    OutCanonical {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("数据目录 {data} 与输出目录 {out} 不能相同或互相包含")]
    RootOverlap { data: PathBuf, out: PathBuf },
    #[error("无法根据已打开的目录验证数据目录 {data} 与输出目录 {out}：{source}")]
    RootIdentity {
        data: PathBuf,
        out: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    OutputLease(#[from] output::OutputLeaseError),
    #[error("无法创建 worker 观测目录 {path}：{source}")]
    WorkerObservation {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    OutputContract(#[from] structured::OutputContractError),
    #[error("无法列出数据目录 {path}：{source}")]
    Listing {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let Cli { command } = Cli::parse();
    match command {
        Commands::Run(args) => match run(args).await {
            Ok(code) => ExitCode::from(code),
            Err(e) => {
                eprintln!("错误：{e}");
                ExitCode::from(2)
            }
        },
    }
}

async fn run(args: RunArgs) -> Result<u8, StartupError> {
    let config = config::load()?;

    if args.concurrency == 0 {
        return Err(StartupError::ConcurrencyZero);
    }
    let data_read_root = tools::ReadRoot::open(args.data.clone())
        .map_err(|_| StartupError::DataRoot(args.data.clone()))?;
    let data_dir = data_read_root
        .clone_dir()
        .map_err(|source| StartupError::DataCanonical {
            path: args.data.clone(),
            source,
        })?;
    let data_root = args
        .data
        .canonicalize()
        .map_err(|source| StartupError::DataCanonical {
            path: args.data.clone(),
            source,
        })?;
    verify_dir_path(&data_dir, &data_root).map_err(|source| StartupError::DataCanonical {
        path: args.data.clone(),
        source,
    })?;
    let intended_out =
        canonical_intent(&args.out).map_err(|source| StartupError::OutCanonical {
            path: args.out.clone(),
            source,
        })?;
    if output_overlaps_data(&data_root, &intended_out) {
        return Err(StartupError::RootOverlap {
            data: data_root,
            out: intended_out,
        });
    }
    let (out_dir, out_root) = match open_or_create_output_dir(&intended_out, &data_dir) {
        Ok(bound) => bound,
        Err(OutputBindingError::Overlap) => {
            return Err(StartupError::RootOverlap {
                data: data_root,
                out: intended_out,
            });
        }
        Err(OutputBindingError::Io(source)) => {
            return Err(StartupError::RootIdentity {
                data: data_root.clone(),
                out: intended_out.clone(),
                source,
            });
        }
    };
    if directories_overlap(&data_dir, &out_dir).map_err(|source| StartupError::RootIdentity {
        data: data_root.clone(),
        out: out_root.clone(),
        source,
    })? {
        return Err(StartupError::RootOverlap {
            data: data_root,
            out: out_root,
        });
    }
    let output_root = output::OutputRoot::from_dir(out_root.clone(), out_dir);
    let _output_lease = output::OutputLease::acquire(&output_root)?;
    let out_read_root = tools::ReadRoot::from_dir(output_root.clone_dir().map_err(|source| {
        StartupError::OutCanonical {
            path: out_root.clone(),
            source,
        }
    })?);
    let task = read_task(&args.task)?;
    let units = plan::load(&args.plan, &data_read_root)?;
    let output_contract =
        structured::OutputContract::prepare(args.output_schema.as_deref(), &output_root)?;
    let listing = worker::list_files(&data_read_root).map_err(|e| StartupError::Listing {
        path: data_root.clone(),
        source: e,
    })?;

    let stats_root = output_root.clone();
    let mcp = mcp::McpManager::initialize(&config.mcp_servers).await?;
    let registry = scheduler::ToolRegistry::with_mcp(&config.tools, mcp)?;
    let mut model_tools = registry.specs().to_vec();
    if let Some(spec) = output_contract.submit_spec() {
        model_tools.push(spec);
        model_tools.sort_by(|left, right| left.name.cmp(&right.name));
    }
    let worker_run = output::WorkerRun::create(
        &output_root,
        output::JobReportFacts {
            protocol: protocol_key(config.llm.protocol).into(),
            model: config.llm.model.clone(),
            context_window_tokens: config.llm.context_window_tokens,
            anthropic_max_tokens: config.llm.anthropic_max_tokens,
            context_safety_tokens: config.execution.context_safety_tokens,
            concurrency: args.concurrency,
            output_format: output_contract.format(),
            tools: model_tools.iter().map(|tool| tool.name.clone()).collect(),
        },
    )
    .map_err(|source| StartupError::WorkerObservation {
        path: out_root.join("workers"),
        source,
    })?;
    let scheduler = scheduler::Scheduler::start(
        registry,
        tools::Roots {
            input: data_read_root.clone(),
            output: out_read_root,
            output_format: output_contract.format(),
        },
        &config.tools,
        &config.cache,
        args.concurrency,
    );
    let instructions = prompt::instructions(output_contract.is_structured()).to_string();
    let publish_gate = Arc::new(tokio::sync::RwLock::new(()));
    let ctx = Arc::new(worker::JobContext {
        scheduler,
        data_root: data_read_root,
        task: task.into(),
        listing: listing.into(),
        llm: LlmClient::new(config.llm.clone()),
        out_root: output_root,
        output_contract,
        execution: config.execution.clone(),
        model_tools: model_tools.into(),
        worker_run,
        publish_gate: Arc::clone(&publish_gate),
        instructions,
    });

    // 规模观测：附属证据，不参与业务状态（FORMIC_METRICS=1 时定期汇总到 stderr）
    let metrics_on = env::var("FORMIC_METRICS").ok().as_deref() == Some("1");
    if metrics_on {
        metrics::spawn_reporter();
    }

    // 取消令牌树：根令牌由信号触发，每 worker 持 child token，
    // worker 内部的 LLM 流、工具等待、重试退避再向下派生，一处取消全树收敛。
    let cancel_root = CancellationToken::new();
    {
        let cancel_root = cancel_root.clone();
        let publish_gate = Arc::clone(&publish_gate);
        tokio::spawn(async move {
            termination_signal().await;
            eprintln!("收到终止信号：停止接纳新单元，等待在途单元收敛（再次按下 Ctrl+C 立即退出）");
            tokio::select! {
                _ = termination_signal() => std::process::exit(130),
                guard = publish_gate.write() => {
                    cancel_root.cancel();
                    drop(guard);
                }
            }
            termination_signal().await;
            std::process::exit(130);
        });
    }

    // 并发窗口：取到槽位才 spawn——窗口满时排队的是尚未创建的工作，
    // 任务总量无界，活动槽位有界，不产生容量错误。
    let order: Vec<u64> = units.iter().map(|u| u.unit).collect();
    let window = Arc::new(Semaphore::new(args.concurrency));
    let mut running: JoinSet<(
        u64,
        Result<worker::Outcome, worker::UnitFailure>,
        output::UnitStats,
    )> = JoinSet::new();
    for unit in units {
        let permit = tokio::select! {
            biased;
            _ = cancel_root.cancelled() => break,
            p = Arc::clone(&window).acquire_owned() => p.expect("窗口信号量随作业同生命周期"),
        };
        if cancel_root.is_cancelled() {
            drop(permit);
            break;
        }
        let ctx = Arc::clone(&ctx);
        let cancel = cancel_root.child_token();
        running.spawn(async move {
            let _permit = permit;
            let no = unit.unit;
            let shard = describe_shard(&unit.shard);
            let started_at = Utc::now();
            let started = Instant::now();
            let mut stats = output::UnitStats::default();
            let result =
                std::panic::AssertUnwindSafe(worker::run_unit(&ctx, &unit, cancel, &mut stats))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(worker::UnitFailure::Panicked));
            ctx.scheduler.finish_unit(no).await;
            let finished_at = Utc::now();
            let outcome = match &result {
                Ok(worker::Outcome::Published) => "published",
                Ok(worker::Outcome::Cancelled) => "cancelled",
                Err(_) => "failed",
            };
            let failure_reason = result.as_ref().err().map(ToString::to_string);
            let record_format = matches!(&result, Ok(worker::Outcome::Published))
                .then(|| ctx.output_contract.format());
            if let Err(error) = output::render_worker_report(
                &ctx.worker_run,
                &output::WorkerReport {
                    unit: no,
                    shard: &shard,
                    outcome,
                    failure_reason: failure_reason.as_deref(),
                    started_at,
                    finished_at,
                    duration: started.elapsed(),
                    stats: &stats,
                    record_format,
                },
            ) {
                eprintln!("单元 {no} 运行档案生成失败：{error}");
            }
            // 事实发生后立即报告，不被窗口等待阻塞
            match &result {
                Ok(worker::Outcome::Published) => eprintln!("单元 {no} 完成"),
                Ok(worker::Outcome::Cancelled) => eprintln!("单元 {no} 取消（作业已被终止）"),
                Err(failure) => eprintln!("单元 {no} 失败：{failure}"),
            }
            (no, result, stats)
        });
    }

    let mut completed = 0u64;
    let mut cancelled = 0u64;
    let mut failed = Vec::new();
    while let Some(joined) = running.join_next().await {
        match joined {
            Ok((no, Ok(worker::Outcome::Published), stats)) => {
                completed += 1;
                metrics::counter_inc(&metrics::UNITS_COMPLETED);
                append_stats_line(&stats_root, no, "published", &stats);
            }
            Ok((no, Ok(worker::Outcome::Cancelled), stats)) => {
                cancelled += 1;
                metrics::counter_inc(&metrics::UNITS_CANCELLED);
                append_stats_line(&stats_root, no, "cancelled", &stats);
            }
            Ok((no, Err(_), stats)) => {
                metrics::counter_inc(&metrics::UNITS_FAILED);
                append_stats_line(&stats_root, no, "failed", &stats);
                failed.push(no);
            }
            Err(join_error) => {
                // catch_unwind 已把 panic 转为单元失败；走到这里只能是运行时自身故障
                eprintln!("内部错误：worker task 异常结束：{join_error}");
            }
        }
    }
    ctx.scheduler.shutdown().await;
    // 失败单元按计划文件顺序呈现
    let failed_in_plan_order: Vec<u64> = order
        .iter()
        .filter(|u| failed.contains(u))
        .copied()
        .collect();

    if metrics_on {
        metrics::report_once(); // 捕获终态
    }
    let summary = Summary {
        completed,
        failed: failed_in_plan_order,
        cancelled,
    };
    println!("{}", summary.render());
    // 退出码：收到终止信号 → 3；否则有失败 → 1；全部成功 → 0
    if cancel_root.is_cancelled() {
        Ok(3)
    } else {
        Ok(summary.exit_code())
    }
}

fn output_overlaps_data(data: &Path, out: &Path) -> bool {
    out.starts_with(data) || data.starts_with(out)
}

enum OutputBindingError {
    Overlap,
    Io(std::io::Error),
}

fn open_or_create_output_dir(
    intended: &Path,
    data: &Dir,
) -> Result<(Dir, PathBuf), OutputBindingError> {
    open_or_create_output_dir_with(intended, data, |_, _, _| Ok(()))
}

fn open_or_create_output_dir_with<F>(
    intended: &Path,
    data: &Dir,
    mut before_component: F,
) -> Result<(Dir, PathBuf), OutputBindingError>
where
    F: FnMut(usize, &Dir, &Path) -> std::io::Result<()>,
{
    let (anchor_path, relative) = creation_anchor(intended).map_err(OutputBindingError::Io)?;
    let anchor =
        Dir::open_ambient_dir(&anchor_path, ambient_authority()).map_err(OutputBindingError::Io)?;
    verify_dir_path(&anchor, &anchor_path).map_err(OutputBindingError::Io)?;
    if is_ancestor_or_same(data, &anchor).map_err(OutputBindingError::Io)? {
        return Err(OutputBindingError::Overlap);
    }

    if relative.as_os_str().is_empty() {
        if directories_overlap(data, &anchor).map_err(OutputBindingError::Io)? {
            return Err(OutputBindingError::Overlap);
        }
        return Ok((
            anchor.try_clone().map_err(OutputBindingError::Io)?,
            intended.to_path_buf(),
        ));
    }

    let mut current = anchor;
    for (index, component) in relative.components().enumerate() {
        let std::path::Component::Normal(name) = component else {
            return Err(invalid_output_component(
                Path::new(component.as_os_str()),
                "不是普通目录名",
            ));
        };
        let name = Path::new(name);
        before_component(index, &current, name).map_err(OutputBindingError::Io)?;
        current = open_or_create_output_component(&current, name, data)?;
    }

    Ok((current, intended.to_path_buf()))
}

fn open_or_create_output_component(
    parent: &Dir,
    name: &Path,
    data: &Dir,
) -> Result<Dir, OutputBindingError> {
    match parent.symlink_metadata(name) {
        Ok(metadata) => validate_output_component(name, &metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match parent.create_dir(name) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(OutputBindingError::Io(error)),
            }
            let metadata = parent
                .symlink_metadata(name)
                .map_err(OutputBindingError::Io)?;
            validate_output_component(name, &metadata)?;
        }
        Err(error) => return Err(OutputBindingError::Io(error)),
    }

    let opened = parent.open_dir(name).map_err(OutputBindingError::Io)?;
    let metadata = parent
        .symlink_metadata(name)
        .map_err(OutputBindingError::Io)?;
    validate_output_component(name, &metadata)?;

    // 再从同一个父目录句柄打开一次，确认入口没有在检查与打开之间被替换。
    let confirmed = parent.open_dir(name).map_err(OutputBindingError::Io)?;
    let metadata = parent
        .symlink_metadata(name)
        .map_err(OutputBindingError::Io)?;
    validate_output_component(name, &metadata)?;
    if directory_handle(&opened).map_err(OutputBindingError::Io)?
        != directory_handle(&confirmed).map_err(OutputBindingError::Io)?
    {
        return Err(invalid_output_component(name, "在启动检查期间被替换"));
    }
    if directories_overlap(data, &opened).map_err(OutputBindingError::Io)? {
        return Err(OutputBindingError::Overlap);
    }
    Ok(opened)
}

fn validate_output_component(
    name: &Path,
    metadata: &cap_std::fs::Metadata,
) -> Result<(), OutputBindingError> {
    if is_symlink_or_reparse(metadata) {
        return Err(invalid_output_component(name, "是符号链接或重解析点"));
    }
    if !metadata.is_dir() {
        return Err(invalid_output_component(name, "不是目录"));
    }
    Ok(())
}

#[cfg(not(windows))]
fn is_symlink_or_reparse(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn is_symlink_or_reparse(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

fn invalid_output_component(name: &Path, reason: &str) -> OutputBindingError {
    OutputBindingError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("输出目录路径组件 {}{reason}", name.display()),
    ))
}

fn verify_dir_path(dir: &Dir, path: &Path) -> std::io::Result<()> {
    let opened = directory_handle(dir)?;
    let current = Handle::from_path(path)?;
    if opened == current {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "目录 {} 在启动检查期间被替换",
            path.display()
        )))
    }
}

fn directory_handle(dir: &Dir) -> std::io::Result<Handle> {
    Handle::from_file(dir.try_clone()?.into_std_file())
}

fn directories_overlap(left: &Dir, right: &Dir) -> std::io::Result<bool> {
    Ok(is_ancestor_or_same(left, right)? || is_ancestor_or_same(right, left)?)
}

fn is_ancestor_or_same(ancestor: &Dir, descendant: &Dir) -> std::io::Result<bool> {
    let ancestor = directory_handle(ancestor)?;
    let mut current = descendant.try_clone()?;
    loop {
        let current_handle = directory_handle(&current)?;
        if current_handle == ancestor {
            return Ok(true);
        }
        let parent = current.open_parent_dir(ambient_authority())?;
        if directory_handle(&parent)? == current_handle {
            return Ok(false);
        }
        current = parent;
    }
}

/// 在不创建目标的前提下解析它最终会落到的绝对路径。
///
/// 已存在的最近祖先由文件系统解析 symlink/junction，尚不存在的后缀只做词法拼接；
/// 后续创建从已验证的祖先句柄逐级完成，并重新检查每一级的类型和目录身份。
fn canonical_intent(path: &Path) -> std::io::Result<PathBuf> {
    let mut current = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    let passes = current.components().count().saturating_add(1);
    for _ in 0..passes {
        let next = resolve_existing_prefix(&current, path)?;
        if next == current {
            return Ok(next);
        }
        current = next;
    }
    Err(std::io::Error::other(format!(
        "输出目录 {} 的路径在解析期间持续变化",
        path.display()
    )))
}

fn resolve_existing_prefix(path: &Path, display: &Path) -> std::io::Result<PathBuf> {
    let mut ancestor = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match fs::metadata(&ancestor) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = ancestor.components().next_back().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("找不到输出目录 {} 的现有祖先", display.display()),
                    )
                })?;
                missing.push(component.as_os_str().to_os_string());
                if !ancestor.pop() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("找不到输出目录 {} 的现有祖先", display.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }

    let mut resolved = ancestor.canonicalize()?;
    for component in missing.iter().rev() {
        match Path::new(component).components().next() {
            Some(std::path::Component::CurDir) => {}
            Some(std::path::Component::ParentDir) => {
                resolved.pop();
            }
            Some(std::path::Component::Normal(_)) => resolved.push(component),
            _ => return Err(std::io::Error::other("输出目录含无法解析的路径组件")),
        }
    }
    Ok(resolved)
}

fn creation_anchor(intended: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
    let mut anchor = intended.to_path_buf();
    loop {
        match fs::metadata(&anchor) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !anchor.pop() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("找不到输出目录 {} 的现有祖先", intended.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    let anchor = anchor.canonicalize()?;
    let relative = intended
        .strip_prefix(&anchor)
        .map_err(|_| std::io::Error::other("输出目录不在已解析的现有祖先下"))?
        .to_path_buf();
    Ok((anchor, relative))
}

fn read_task(path: &PathBuf) -> Result<String, StartupError> {
    let file = fs::File::open(path).map_err(|e| StartupError::TaskRead {
        path: path.clone(),
        source: e,
    })?;
    // metadata 只用于减少扩容；文件可能在读取期间变化，实际边界由 Take 保证。
    let capacity = file
        .metadata()
        .ok()
        .map(|metadata| metadata.len().min(MAX_TASK_BYTES + 1) as usize)
        .unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAX_TASK_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| StartupError::TaskRead {
            path: path.clone(),
            source: e,
        })?;
    if bytes.len() as u64 > MAX_TASK_BYTES {
        return Err(StartupError::TaskInvalid(path.clone()));
    }
    let text = String::from_utf8(bytes).map_err(|_| StartupError::TaskEncoding(path.clone()))?;
    if text.trim().is_empty() {
        return Err(StartupError::TaskInvalid(path.clone()));
    }
    Ok(text)
}

fn protocol_key(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Completions => "completions",
        Protocol::Responses => "responses",
        Protocol::Anthropic => "anthropic",
    }
}

fn describe_shard(shard: &plan::Shard) -> String {
    match shard {
        plan::Shard::Files(files) => format!(
            "文件：{}",
            files
                .iter()
                .map(|path| path.to_string_lossy())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        plan::Shard::Lines { file, start, end } => {
            format!("文件 {}，第 {start}–{end} 行", file.display())
        }
    }
}

/// 等待一次终止信号：Ctrl+C，或 Windows 的 Ctrl+Break。
#[cfg(windows)]
async fn termination_signal() {
    let mut ctrl_c = tokio::signal::windows::ctrl_c().expect("注册 Ctrl+C 监听");
    let mut ctrl_break = tokio::signal::windows::ctrl_break().expect("注册 Ctrl+Break 监听");
    tokio::select! {
        _ = ctrl_c.recv() => {}
        _ = ctrl_break.recv() => {}
    }
}

#[cfg(not(windows))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// 追加单元统计行；stats 是附属证据，写失败只产生诊断，不改写业务结果。
fn append_stats_line(
    out_root: &output::OutputRoot,
    unit: u64,
    outcome: &str,
    stats: &output::UnitStats,
) {
    if let Err(e) = output::append_stats(out_root, unit, outcome, stats) {
        eprintln!("单元 {unit} 统计写入失败：{e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_must_not_be_the_data_root_or_its_descendant() {
        let data = Path::new("root/data");
        assert!(output_overlaps_data(data, data));
        assert!(output_overlaps_data(data, Path::new("root/data/out")));
        assert!(!output_overlaps_data(data, Path::new("root/out")));
        assert!(output_overlaps_data(data, Path::new("root")));
    }

    #[test]
    fn absent_output_path_is_resolved_without_being_created() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        fs::create_dir(&data).unwrap();
        let output = data.join("generated").join("out");

        assert_eq!(
            canonical_intent(&output).unwrap(),
            data.canonicalize().unwrap().join("generated").join("out")
        );
        assert!(!output.exists());
    }

    #[test]
    fn opened_directory_identity_rejects_same_and_nested_roots() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        let nested = data.join("nested");
        let sibling = temp.path().join("sibling");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir(&sibling).unwrap();
        let data = Dir::open_ambient_dir(&data, ambient_authority()).unwrap();
        let same = data.try_clone().unwrap();
        let nested = Dir::open_ambient_dir(&nested, ambient_authority()).unwrap();
        let sibling = Dir::open_ambient_dir(&sibling, ambient_authority()).unwrap();

        assert!(directories_overlap(&data, &same).unwrap());
        assert!(directories_overlap(&data, &nested).unwrap());
        assert!(!directories_overlap(&data, &sibling).unwrap());
    }

    #[test]
    fn nested_output_is_rejected_before_any_directory_is_created() {
        let temp = tempfile::tempdir().unwrap();
        let data_path = temp.path().join("data");
        fs::create_dir(&data_path).unwrap();
        let data = Dir::open_ambient_dir(&data_path, ambient_authority()).unwrap();
        let output = data_path.join("generated").join("out");
        let intended = canonical_intent(&output).unwrap();

        assert!(matches!(
            open_or_create_output_dir(&intended, &data),
            Err(OutputBindingError::Overlap)
        ));
        assert!(!output.exists());
        assert!(!data_path.join("generated").exists());
    }

    #[test]
    fn link_inserted_after_anchor_open_is_rejected_without_writing_data() {
        let temp = tempfile::tempdir().unwrap();
        let data_path = temp.path().join("data");
        let first_component = temp.path().join("output");
        fs::create_dir(&data_path).unwrap();
        let data = Dir::open_ambient_dir(&data_path, ambient_authority()).unwrap();
        let intended = canonical_intent(&first_component.join("nested")).unwrap();
        let mut injected = false;

        let result =
            open_or_create_output_dir_with(&intended, &data, |index, _parent, _component| {
                assert_eq!(index, 0);
                create_directory_link(&data_path, &first_component)?;
                injected = true;
                Ok(())
            });

        assert!(injected);
        assert!(matches!(
            result,
            Err(OutputBindingError::Io(error))
                if error.kind() == std::io::ErrorKind::InvalidData
        ));
        assert!(!data_path.join("nested").exists());
    }

    #[cfg(unix)]
    fn create_directory_link(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_link(target: &Path, link: &Path) -> std::io::Result<()> {
        match std::os::windows::fs::symlink_dir(target, link) {
            Ok(()) => Ok(()),
            Err(error) if error.raw_os_error() == Some(1314) => {
                let output = std::process::Command::new("cmd.exe")
                    .args(["/D", "/C", "mklink", "/J"])
                    .arg(link)
                    .arg(target)
                    .output()?;
                if output.status.success() {
                    Ok(())
                } else {
                    Err(std::io::Error::other(format!(
                        "无法创建测试用目录重解析点：{}",
                        String::from_utf8_lossy(&output.stderr)
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_intent_resolves_symlink_before_parent_component() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("base");
        let external = temp.path().join("external");
        let target = external.join("child");
        fs::create_dir(&base).unwrap();
        fs::create_dir_all(&target).unwrap();
        symlink(&target, base.join("link")).unwrap();

        let intended = canonical_intent(&base.join("link").join("..").join("out")).unwrap();
        assert_eq!(intended, external.canonicalize().unwrap().join("out"));
        assert!(!intended.exists());
    }

    #[cfg(unix)]
    #[test]
    fn output_creation_stays_with_verified_parent_handle_after_path_replacement() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let data_path = temp.path().join("data");
        let ambient_parent = temp.path().join("output-parent");
        let opened_parent = temp.path().join("opened-output-parent");
        fs::create_dir(&data_path).unwrap();
        fs::create_dir(&ambient_parent).unwrap();
        let data = Dir::open_ambient_dir(&data_path, ambient_authority()).unwrap();
        let parent = Dir::open_ambient_dir(&ambient_parent, ambient_authority()).unwrap();
        verify_dir_path(&parent, &ambient_parent).unwrap();
        assert!(!is_ancestor_or_same(&data, &parent).unwrap());

        fs::rename(&ambient_parent, &opened_parent).unwrap();
        symlink(&data_path, &ambient_parent).unwrap();
        parent.create_dir_all("out").unwrap();
        let out = parent.open_dir("out").unwrap();

        assert!(!directories_overlap(&data, &out).unwrap());
        assert!(opened_parent.join("out").is_dir());
        assert!(!data_path.join("out").exists());
    }

    #[test]
    fn task_at_byte_limit_is_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("task.txt");
        fs::write(&path, vec![b'a'; MAX_TASK_BYTES as usize]).unwrap();

        let task = read_task(&path).unwrap();
        assert_eq!(task.len() as u64, MAX_TASK_BYTES);
    }

    #[test]
    fn empty_and_invalid_utf8_tasks_keep_their_error_categories() {
        let temp = tempfile::tempdir().unwrap();
        let empty = temp.path().join("empty.txt");
        let invalid_utf8 = temp.path().join("invalid.txt");
        fs::write(&empty, b" \n\t").unwrap();
        fs::write(&invalid_utf8, b"valid-prefix\xff").unwrap();

        assert!(matches!(
            read_task(&empty),
            Err(StartupError::TaskInvalid(path)) if path == empty
        ));
        assert!(matches!(
            read_task(&invalid_utf8),
            Err(StartupError::TaskEncoding(path)) if path == invalid_utf8
        ));
    }

    #[test]
    fn oversized_task_is_rejected_before_utf8_decode() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("oversized.txt");
        let mut bytes = vec![b'a'; MAX_TASK_BYTES as usize + 1];
        *bytes.last_mut().unwrap() = 0xff;
        fs::write(&path, bytes).unwrap();

        assert!(matches!(
            read_task(&path),
            Err(StartupError::TaskInvalid(error_path)) if error_path == path
        ));
    }
}
