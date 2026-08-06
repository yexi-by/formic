//! 入口：CLI 解析、环境变量读取、错误的人性化呈现。
//! 退出码：0 全部成功；1 存在失败单元；2 启动失败（参数、输入、环境）。

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

mod llm;
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

    let ctx = worker::JobContext {
        scheduler: scheduler::Scheduler::start(tools::Roots {
            input: args.data.clone(),
            output: args.out.clone(),
        }),
        data_root: args.data,
        task,
        listing,
        llm: LlmClient::new(config),
        out_dir: args.out,
    };

    let mut summary = Summary {
        completed: 0,
        failed: Vec::new(),
    };
    for unit in &units {
        match worker::run_unit(&ctx, unit).await {
            Ok(()) => {
                summary.completed += 1;
                eprintln!("单元 {} 完成", unit.unit);
            }
            Err(failure) => {
                summary.failed.push(unit.unit);
                eprintln!("单元 {} 失败：{failure}", unit.unit);
            }
        }
    }

    println!("{}", summary.render());
    Ok(summary.exit_code())
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
