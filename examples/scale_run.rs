//! 规模实验（design §9）：mock LLM 驱动几千个 worker 全链路，观测内存曲线、
//! 历史体积与调度器行为。自生成数据集与计划，以内置 mock（可控延迟、按回合
//! 数返回固定 tool call 序列）驱动同 profile 的 formic.exe 真实入口。
//!
//! 用法：`cargo run [--release] --example scale_run -- [units=5000] [concurrency=1000] [turns=8] [delay_ms=20]`
//! 产物：stderr 进度 + stdout 汇总表 + 当前目录 scale-metrics.csv。

use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let units: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(5000);
    let concurrency: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1000);
    let turns: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(8);
    let delay_ms: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);
    println!("规模实验：units={units} concurrency={concurrency} turns={turns} delay={delay_ms}ms");

    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), units);
    let mock = start_scale_mock(turns, delay_ms);
    let formic = formic_binary();
    println!("formic：{}", formic.display());

    let stderr_file = dir.path().join("formic-stderr.log");
    let started = Instant::now();
    let mut child = Command::new(&formic)
        .current_dir(dir.path())
        .arg("run")
        .arg("--data")
        .arg(&data)
        .arg("--plan")
        .arg(&plan)
        .arg("--task")
        .arg(&task)
        .arg("--out")
        .arg(&out)
        .arg("--concurrency")
        .arg(concurrency.to_string())
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env(
            "FORMIC_LLM_BASE_URL",
            format!("http://127.0.0.1:{}/v1", mock.port),
        )
        .env("FORMIC_LLM_MODEL", "scale-model")
        .env("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "131072")
        .env("FORMIC_METRICS", "1")
        .env_remove("FORMIC_LLM_API_KEY")
        // 本地 mock 必须直连，避免开发机代理把规模实验变成代理压力测试。
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env_remove("HTTP_PROXY")
        .env_remove("HTTPS_PROXY")
        .env_remove("ALL_PROXY")
        .stdout(Stdio::inherit())
        .stderr(Stdio::from(fs::File::create(&stderr_file).unwrap()))
        .spawn()
        .expect("启动 formic 失败");
    let status = child.wait().expect("等待 formic 失败");
    let wall = started.elapsed();

    // 汇总：从 stderr 提取 metrics 行 → 峰值与斜率
    let samples = parse_metrics(&stderr_file);
    let csv_path = dir.path().join("scale-metrics.csv");
    write_csv(csv_path.clone(), &samples);
    fs::copy(&csv_path, Path::new("scale-metrics.csv")).ok();

    let peak = |key: &str| samples.iter().map(|s| s[key]).max().unwrap_or(0);
    let first_rss = samples.first().map(|s| s["rss_mb"]).unwrap_or(0);
    let peak_rss = peak("rss_mb");
    let peak_history_kb = peak("history_kb");
    let peak_history = peak_history_kb / 1024;
    let ratio = if peak_history_kb > 0 {
        (peak_rss.saturating_sub(first_rss)) as f64 / (peak_history_kb as f64 / 1024.0)
    } else {
        0.0
    };
    let done = samples.last().map(|s| s["done"]).unwrap_or(0);
    let failed = samples.last().map(|s| s["failed"]).unwrap_or(0);
    let observations = count_worker_reports(&out);

    println!("\n==== 规模实验汇总 ====");
    println!(
        "退出码：{:?}，墙钟：{:.1}s，吞吐：{:.0} 单元/秒",
        status.code(),
        wall.as_secs_f64(),
        units as f64 / wall.as_secs_f64()
    );
    println!("单元：done={done} failed={failed}（期望 {units}）");
    println!("峰值 RSS：{peak_rss} MB（起始 {first_rss} MB）");
    println!("峰值 history：{peak_history} MB");
    println!("ΔRSS/Δhistory ≈ {ratio:.2}（≈1 则历史体积主导内存成立）");
    println!("峰值 llm_in_flight：{}", peak("llm_in_flight"));
    println!("峰值 tool_inflight：{}", peak("tool_inflight"));
    println!(
        "search_max_ms（终值）：{}",
        samples.last().map(|s| s["search_max_ms"]).unwrap_or(0)
    );
    println!(
        "mock 处理请求总数：{}",
        mock.total_requests.load(Ordering::SeqCst)
    );
    println!(
        "worker 档案：任务目录 {}，Markdown {}，重复或临时文件 {}",
        observations.run_directories, observations.markdown, observations.unexpected
    );
    println!("指标序列：scale-metrics.csv");
    if !status.success()
        || done != units
        || failed != 0
        || observations.run_directories != 1
        || observations.markdown != units
        || observations.unexpected != 0
    {
        let diagnostic = Path::new("scale-stderr.log");
        fs::copy(&stderr_file, diagnostic).ok();
        eprintln!(
            "规模实验失败：Formic 未完成全部单元；完整 stderr 已复制到 {}",
            diagnostic.display()
        );
        std::process::exit(1);
    }
}

#[derive(Default)]
struct ObservationCounts {
    run_directories: u64,
    markdown: u64,
    unexpected: u64,
}

fn count_worker_reports(out: &Path) -> ObservationCounts {
    let mut counts = ObservationCounts::default();
    let Ok(entries) = fs::read_dir(out.join("workers")) else {
        return counts;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            counts.unexpected += 1;
            continue;
        }
        counts.run_directories += 1;
        let Ok(files) = fs::read_dir(path) else {
            counts.unexpected += 1;
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            if path.is_file() && path.extension().is_some_and(|extension| extension == "md") {
                counts.markdown += 1;
            } else {
                counts.unexpected += 1;
            }
        }
    }
    counts
}

fn write_job(dir: &Path, units: u64) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let data = dir.join("data");
    fs::create_dir_all(&data).unwrap();
    // 苹果行：单行约 200B、共 80 行，低于默认匹配上限；完整 input 结果可进入作业缓存。
    let filler = "规".repeat(180);
    let apples: String = (1..=80)
        .map(|i| format!("第 {i} 行 苹果 {filler}\n"))
        .collect();
    fs::write(data.join("apples.txt"), apples).unwrap();
    // 每个单元两行分片：unit i → big.txt 第 2i-1..2i 行
    let big: String = (1..=units * 2).map(|i| format!("记录 {i}\n")).collect();
    fs::write(data.join("big.txt"), big).unwrap();

    let plan = dir.join("plan.jsonl");
    let plan_text: String = (1..=units)
        .map(|i| {
            format!(
                "{{\"unit\":{i},\"file\":\"big.txt\",\"start\":{},\"end\":{}}}\n",
                2 * i - 1,
                2 * i
            )
        })
        .collect();
    fs::write(&plan, plan_text).unwrap();
    let task = dir.join("task.md");
    fs::write(&task, "分析你的分片，用一句话给出结论。\n").unwrap();
    let out = dir.join("out");
    (data, plan, task, out)
}

/// 先按 example 当前 profile 构建主程序，再定位同目录二进制。Cargo 运行 example
/// 时不会自动重建 bin；缺少这一步会把旧二进制误当成当前实现参与实验。
fn formic_binary() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let profile_dir = exe.parent().and_then(|p| p.parent()).unwrap();
    let profile_directory = profile_dir.file_name().unwrap().to_string_lossy();
    let mut build = Command::new("cargo");
    build
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("build")
        .arg("--bin")
        .arg("formic");
    match profile_directory.as_ref() {
        "debug" => {}
        "release" => {
            build.arg("--release");
        }
        profile => {
            build.arg("--profile").arg(profile);
        }
    }
    let status = build.status().expect("无法调用 cargo 构建 formic");
    assert!(status.success(), "构建 formic 主程序失败");
    profile_dir.join(if cfg!(windows) {
        "formic.exe"
    } else {
        "formic"
    })
}

struct ScaleMock {
    port: u16,
    total_requests: Arc<AtomicUsize>,
}

fn start_scale_mock(turns: usize, delay_ms: u64) -> ScaleMock {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let total = Arc::new(AtomicUsize::new(0));
    let shared = Arc::clone(&total);
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(handle(stream, turns, delay_ms, Arc::clone(&shared)));
            }
        });
    });
    ScaleMock {
        port,
        total_requests: total,
    }
}

fn tool_call_sse(k: usize) -> String {
    // 按回合交替等价 glob（字节不同、结果相同），规避连续相同调用的停滞检测
    let glob = if k.is_multiple_of(2) {
        "*.txt"
    } else {
        "**/*.txt"
    };
    let args = format!(
        "{{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\",\\\"glob\\\":\\\"{glob}\\\"}}"
    );
    format!(
        "data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"s\",\"choices\":[{{\"index\":0,\"delta\":{{\"role\":\"assistant\",\"tool_calls\":[{{\"index\":0,\"id\":\"call_{k}\",\"type\":\"function\",\"function\":{{\"name\":\"search\",\"arguments\":\"{args}\"}}}}]}},\"finish_reason\":null}}]}}\n\ndata: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"s\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"tool_calls\"}}]}}\n\ndata: [DONE]\n\n"
    )
}

const FINAL_SSE: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"s\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"s\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"规模实验产出。\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"s\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

async fn handle(
    stream: tokio::net::TcpStream,
    turns: usize,
    delay_ms: u64,
    total_requests: Arc<AtomicUsize>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

    let (reader, mut writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    loop {
        let mut request_line = String::new();
        match reader.read_line(&mut request_line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }

        let mut content_length = 0usize;
        let mut close_requested = false;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                break;
            }
            let lower = trimmed.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap_or(0);
            } else if let Some(value) = lower.strip_prefix("connection:") {
                close_requested = value.trim() == "close";
            }
        }
        let mut body = vec![0u8; content_length];
        if reader.read_exact(&mut body).await.is_err() {
            return;
        }
        total_requests.fetch_add(1, Ordering::SeqCst);

        let body_text = String::from_utf8_lossy(&body);
        // 已完成回合数 = 请求体里 tool 结果消息数
        let k = body_text.matches("\"role\":\"tool\"").count();
        let sse = if k < turns {
            tool_call_sse(k)
        } else {
            FINAL_SSE.to_string()
        };
        let connection = if close_requested {
            "close"
        } else {
            "keep-alive"
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: {connection}\r\n\r\n{sse}",
            sse.len()
        );
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        if writer.write_all(response.as_bytes()).await.is_err() || writer.flush().await.is_err() {
            return;
        }
        if close_requested {
            return;
        }
    }
}

fn parse_metrics(path: &Path) -> Vec<HashMap<String, u64>> {
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let line = line.strip_prefix("metrics ")?;
            let mut sample = HashMap::new();
            for kv in line.split_whitespace() {
                let (k, v) = kv.split_once('=')?;
                sample.insert(k.to_string(), v.parse().ok()?);
            }
            Some(sample)
        })
        .collect()
}

fn write_csv(path: PathBuf, samples: &[HashMap<String, u64>]) {
    let mut text = String::from(
        "second,rss_mb,history_kb,llm_in_flight,tool_inflight,done,failed,cancelled\n",
    );
    for (i, s) in samples.iter().enumerate() {
        text.push_str(&format!(
            "{},{},{},{},{},{},{},{}\n",
            i + 1,
            s["rss_mb"],
            s["history_kb"],
            s["llm_in_flight"],
            s["tool_inflight"],
            s["done"],
            s["failed"],
            s["cancelled"]
        ));
    }
    fs::write(path, text).ok();
}
