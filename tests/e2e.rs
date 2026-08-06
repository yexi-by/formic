//! 端到端：真实生产入口（编译产物 formic）× 真实 HTTP/SSE（手写 mock server）。
//! 覆盖三种协议的主成功路径、失败路径与启动期计划校验错误。

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;

/// 三种协议罐装响应的共同最终消息，主成功路径断言产出与它一致。
const FINAL_TEXT: &str = "你好世界";

/// 请求体含此标记时 mock 返回 500，用于驱动失败路径。
const FAIL_MARKER: &str = "FAIL-UNIT";

const COMPLETIONS_SSE: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"世界\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_SSE: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"你好\"}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"世界\"}\n\n",
    "data: {\"type\":\"response.output_text.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"你好世界\"}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_SSE: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"世界\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

struct Recorded {
    path: String,
    body: String,
}

struct MockServer {
    port: u16,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

fn start_mock() -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let shared = Arc::clone(&requests);
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || handle_conn(stream, shared));
                }
                Err(_) => break,
            }
        }
    });
    MockServer { port, requests }
}

fn handle_conn(mut stream: TcpStream, requests: Arc<Mutex<Vec<Recorded>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .to_string();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; content_length];
    if reader.read_exact(&mut body).is_err() {
        return;
    }
    let body_text = String::from_utf8_lossy(&body).to_string();
    requests.lock().unwrap().push(Recorded {
        path: path.clone(),
        body: body_text.clone(),
    });

    let (status, response_body) = if body_text.contains(FAIL_MARKER) {
        ("500 Internal Server Error", "boom".to_string())
    } else if path.ends_with("/chat/completions") {
        ("200 OK", COMPLETIONS_SSE.to_string())
    } else if path.ends_with("/responses") {
        ("200 OK", RESPONSES_SSE.to_string())
    } else if path.ends_with("/messages") {
        ("200 OK", ANTHROPIC_SSE.to_string())
    } else {
        ("404 Not Found", "unknown path".to_string())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
        response_body.len()
    );
    stream.write_all(response.as_bytes()).ok();
}

/// 搭一个两单元作业：单元 1 文件清单形状，单元 2 行区间形状。
fn write_job(dir: &Path, fail_unit_2: bool) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let data = dir.join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "苹果是一种水果。\n").unwrap();
    let big = if fail_unit_2 {
        "一\nFAIL-UNIT\n三\n"
    } else {
        "一\n二\n三\n"
    };
    fs::write(data.join("big.txt"), big).unwrap();

    let plan = dir.join("plan.jsonl");
    fs::write(
        &plan,
        "{\"unit\":1,\"files\":[\"a.txt\"]}\n{\"unit\":2,\"file\":\"big.txt\",\"start\":2,\"end\":3}\n",
    )
    .unwrap();
    let task = dir.join("task.md");
    fs::write(&task, "判断分片内容，给出结论。\n").unwrap();
    let out = dir.join("out");
    (data, plan, task, out)
}

fn run_formic(
    protocol: &str,
    port: u16,
    data: &Path,
    plan: &Path,
    task: &Path,
    out: &Path,
) -> Output {
    Command::new(env!("CARGO_BIN_EXE_formic"))
        .arg("run")
        .arg("--data")
        .arg(data)
        .arg("--plan")
        .arg(plan)
        .arg("--task")
        .arg(task)
        .arg("--out")
        .arg(out)
        .env("FORMIC_LLM_PROTOCOL", protocol)
        .env("FORMIC_LLM_BASE_URL", format!("http://127.0.0.1:{port}/v1"))
        .env("FORMIC_LLM_MODEL", "test-model")
        .env_remove("FORMIC_LLM_API_KEY")
        .output()
        .unwrap()
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// 主成功路径：三协议各跑一次，断言产出、审计、汇总、退出码与请求路径。
fn assert_success(protocol: &str, expected_path: &str) {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), false);
    let mock = start_mock();
    let output = run_formic(protocol, mock.port, &data, &plan, &task, &out);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{protocol} 退出码应为 0：{}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("完成 2"),
        "{protocol} 汇总应含完成数：{stdout}"
    );
    assert!(
        stdout.contains("失败 0"),
        "{protocol} 汇总应含失败数：{stdout}"
    );

    for unit in [1, 2] {
        let record = out.join(format!("{unit}.md"));
        assert_eq!(
            fs::read_to_string(&record).unwrap(),
            FINAL_TEXT,
            "{protocol} 单元 {unit} 产出应为最终消息原文"
        );
        let audit = fs::read_to_string(out.join("audit").join(format!("{unit}.jsonl"))).unwrap();
        assert!(
            audit.contains("\"direction\":\"request\""),
            "{protocol} 审计缺请求：{audit}"
        );
        assert!(
            audit.contains("test-model"),
            "{protocol} 审计应含请求体：{audit}"
        );
        assert!(
            audit.contains("你好"),
            "{protocol} 审计应含原始 SSE 负载：{audit}"
        );
    }

    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 2, "{protocol} 应收两次调用");
    assert!(
        requests.iter().all(|r| r.path.ends_with(expected_path)),
        "{protocol} 请求路径应为 {expected_path}：{:?}",
        requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>()
    );
    assert!(
        requests.iter().all(|r| r.body.contains("判断分片内容")),
        "{protocol} 请求体应含任务说明原文"
    );
}

#[test]
fn completions_success() {
    assert_success("completions", "/chat/completions");
}

#[test]
fn responses_success() {
    assert_success("responses", "/responses");
}

#[test]
fn anthropic_success() {
    assert_success("anthropic", "/messages");
}

#[test]
fn failed_unit_leaves_no_record() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), true);
    let mock = start_mock();
    let output = run_formic("completions", mock.port, &data, &plan, &task, &out);

    assert_eq!(
        output.status.code(),
        Some(1),
        "存在失败单元时退出码应为 1：{}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(stdout.contains("完成 1"), "{stdout}");
    assert!(stdout.contains("失败 1"), "{stdout}");
    assert!(stdout.contains("失败单元：2"), "{stdout}");
    assert!(
        !stdout.contains("全部成功"),
        "失败路径不得出现成功文案：{stdout}"
    );

    assert!(out.join("1.md").exists(), "成功单元的记录应在");
    assert!(!out.join("2.md").exists(), "失败单元不得留下记录");
    assert!(
        !out.join(".tmp-unit-2").exists(),
        "失败单元不得留下临时文件"
    );

    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("单元 2 失败"),
        "失败诊断应含单元号：{stderr}"
    );
    assert!(stderr.contains("500"), "失败诊断应含直接原因：{stderr}");
}

#[test]
fn invalid_plan_reports_unit_and_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "内容\n").unwrap();
    // 真实存在于数据根之外的文件，canonicalize 成功后触发逃逸判定
    fs::write(dir.path().join("escape.txt"), "根外文件\n").unwrap();
    let plan = dir.path().join("plan.jsonl");
    fs::write(&plan, "{\"unit\":7,\"files\":[\"../escape.txt\"]}\n").unwrap();
    let task = dir.path().join("task.md");
    fs::write(&task, "任务。\n").unwrap();
    let mock = start_mock();

    let output = run_formic(
        "completions",
        mock.port,
        &data,
        &plan,
        &task,
        &dir.path().join("out"),
    );
    assert_eq!(output.status.code(), Some(2), "启动失败退出码应为 2");
    let stderr = stderr_of(&output);
    assert!(stderr.contains("单元 7"), "错误应含单元号：{stderr}");
    assert!(stderr.contains("逃逸"), "错误应说明原因：{stderr}");
}
