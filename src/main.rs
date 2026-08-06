//! 入口：CLI 解析、环境变量读取、错误的人性化呈现。
//! 退出码：0 全部成功；1 存在失败单元；2 启动失败（参数、输入、环境）。

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use futures_util::FutureExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

mod llm;
mod metrics;
mod output;
mod plan;
mod prompt;
mod scheduler;
mod tools;
mod worker;

use llm::{LlmClient, LlmConfig, Protocol};
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
}

#[derive(thiserror::Error, Debug)]
enum StartupError {
    #[error("{0}")]
    Env(String),
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
    #[error("无法创建输出区 {path}：{source}")]
    OutDir {
        path: PathBuf,
        source: std::io::Error,
    },
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
    let config = llm_config_from_env().map_err(StartupError::Env)?;

    if args.concurrency == 0 {
        return Err(StartupError::ConcurrencyZero);
    }
    if !args.data.is_dir() {
        return Err(StartupError::DataRoot(args.data));
    }
    let task = read_task(&args.task)?;
    let units = plan::load(&args.plan, &args.data)?;
    fs::create_dir_all(&args.out).map_err(|e| StartupError::OutDir {
        path: args.out.clone(),
        source: e,
    })?;
    let listing = worker::list_files(&args.data).map_err(|e| StartupError::Listing {
        path: args.data.clone(),
        source: e,
    })?;

    let ctx = Arc::new(worker::JobContext {
        scheduler: scheduler::Scheduler::start(tools::Roots {
            input: args.data.clone(),
            output: args.out.clone(),
        }),
        data_root: args.data,
        task,
        listing,
        llm: LlmClient::new(config),
        out_dir: args.out,
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
        tokio::spawn(async move {
            termination_signal().await;
            eprintln!("收到终止信号：停止接纳新单元，等待在途单元收敛（再次按下 Ctrl+C 立即退出）");
            cancel_root.cancel();
            termination_signal().await;
            std::process::exit(130);
        });
    }

    // 并发窗口：取到槽位才 spawn——窗口满时排队的是尚未创建的工作，
    // 任务总量无界，活动槽位有界，不产生容量错误。
    let order: Vec<u64> = units.iter().map(|u| u.unit).collect();
    let window = Arc::new(Semaphore::new(args.concurrency));
    let mut running: JoinSet<(u64, Result<worker::Outcome, worker::UnitFailure>)> = JoinSet::new();
    for unit in units {
        let permit = tokio::select! {
            _ = cancel_root.cancelled() => break,
            p = Arc::clone(&window).acquire_owned() => p.expect("窗口信号量随作业同生命周期"),
        };
        let ctx = Arc::clone(&ctx);
        let cancel = cancel_root.child_token();
        running.spawn(async move {
            let _permit = permit;
            let no = unit.unit;
            let result = std::panic::AssertUnwindSafe(worker::run_unit(&ctx, &unit, cancel))
                .catch_unwind()
                .await
                .unwrap_or(Err(worker::UnitFailure::Panicked));
            // 事实发生后立即报告，不被窗口等待阻塞
            match &result {
                Ok(worker::Outcome::Published) => eprintln!("单元 {no} 完成"),
                Ok(worker::Outcome::Cancelled) => eprintln!("单元 {no} 取消（作业已被终止）"),
                Err(failure) => eprintln!("单元 {no} 失败：{failure}"),
            }
            (no, result)
        });
    }

    let mut completed = 0u64;
    let mut cancelled = 0u64;
    let mut failed = Vec::new();
    while let Some(joined) = running.join_next().await {
        match joined {
            Ok((_no, Ok(worker::Outcome::Published))) => {
                completed += 1;
                metrics::counter_inc(&metrics::UNITS_COMPLETED);
            }
            Ok((_no, Ok(worker::Outcome::Cancelled))) => {
                cancelled += 1;
                metrics::counter_inc(&metrics::UNITS_CANCELLED);
            }
            Ok((no, Err(_))) => {
                metrics::counter_inc(&metrics::UNITS_FAILED);
                failed.push(no);
            }
            Err(join_error) => {
                // catch_unwind 已把 panic 转为单元失败；走到这里只能是运行时自身故障
                eprintln!("内部错误：worker task 异常结束：{join_error}");
            }
        }
    }
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

fn llm_config_from_env() -> Result<LlmConfig, String> {
    let required = |name: &str, hint: &str| {
        env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("缺少环境变量 {name}：{hint}"))
    };
    let protocol = Protocol::parse(&required(
        "FORMIC_LLM_PROTOCOL",
        "指定 API 协议形状：completions / responses / anthropic",
    )?)?;
    Ok(LlmConfig {
        protocol,
        base_url: required(
            "FORMIC_LLM_BASE_URL",
            "API 基础地址，如 https://api.openai.com/v1",
        )?,
        model: required("FORMIC_LLM_MODEL", "要调用的模型名")?,
        api_key: env::var("FORMIC_LLM_API_KEY")
            .ok()
            .filter(|v| !v.is_empty()),
    })
}

fn read_task(path: &PathBuf) -> Result<String, StartupError> {
    let bytes = fs::read(path).map_err(|e| StartupError::TaskRead {
        path: path.clone(),
        source: e,
    })?;
    let text = String::from_utf8(bytes).map_err(|_| StartupError::TaskEncoding(path.clone()))?;
    if text.trim().is_empty() || text.len() as u64 > MAX_TASK_BYTES {
        return Err(StartupError::TaskInvalid(path.clone()));
    }
    Ok(text)
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
