//! 端到端：真实生产入口（编译产物 formic）× 真实 HTTP/SSE（手写 mock server）。
//! mock 脚本化两轮：首轮返回 search 工具调用（参数分片传输），携带工具结果的
//! 请求返回最终文本。覆盖三协议主成功、HTTP 500 失败、停滞检测、计划校验错误。

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// 三种协议罐装响应的共同最终消息，主成功路径断言产出与它一致。
const FINAL_TEXT: &str = "你好世界";

/// 请求体含此标记时 mock 返回 500，用于驱动失败路径。
const FAIL_MARKER: &str = "FAIL-UNIT";

/// 请求体含此标记时 mock 永远返回工具调用，用于驱动停滞检测路径。
const LOOP_MARKER: &str = "LOOP-MARKER";

/// 请求体含此标记时 mock 对首次请求返回 500、后续恢复正常，用于驱动重试成功路径。
const FLAKY_MARKER: &str = "FLAKY-UNIT";

// ---- 罐装 SSE：文本最终帧（三协议）----

const COMPLETIONS_FINAL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"世界\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_FINAL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"你好\"}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"世界\"}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_FINAL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"世界\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

// ---- 罐装 SSE：search 工具调用（三协议，参数分片传输）----

const COMPLETIONS_TOOLCALL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"scope\\\":\\\"input\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_TOOLCALL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"\"}}\n\n",
    "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc1\",\"output_index\":0,\"delta\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}\n\n",
    "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc1\",\"output_index\":0,\"delta\":\",\\\"scope\\\":\\\"input\\\"}\"}\n\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_TOOLCALL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"pattern\\\":\\\"苹果\\\"\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

fn sse_for(path: &str, tool_call: bool) -> Option<&'static str> {
    match (path, tool_call) {
        (p, true) if p.ends_with("/chat/completions") => Some(COMPLETIONS_TOOLCALL),
        (p, false) if p.ends_with("/chat/completions") => Some(COMPLETIONS_FINAL),
        (p, true) if p.ends_with("/responses") => Some(RESPONSES_TOOLCALL),
        (p, false) if p.ends_with("/responses") => Some(RESPONSES_FINAL),
        (p, true) if p.ends_with("/messages") => Some(ANTHROPIC_TOOLCALL),
        (p, false) if p.ends_with("/messages") => Some(ANTHROPIC_FINAL),
        _ => None,
    }
}

/// 请求体里协议对应的「已携带工具结果」标记。
fn tool_result_marker(path: &str) -> &'static str {
    if path.ends_with("/chat/completions") {
        "\"role\":\"tool\""
    } else if path.ends_with("/responses") {
        "function_call_output"
    } else {
        "tool_result"
    }
}

struct Recorded {
    path: String,
    body: String,
}

struct MockServer {
    port: u16,
    requests: Arc<Mutex<Vec<Recorded>>>,
    max_in_flight: Arc<AtomicUsize>,
}

fn start_mock() -> MockServer {
    start_mock_with_delay(0)
}

fn start_mock_with_delay(delay_ms: u64) -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let flaky_seen = Arc::new(AtomicBool::new(false));
    let shared = Arc::clone(&requests);
    let shared_in_flight = Arc::clone(&in_flight);
    let shared_max = Arc::clone(&max_in_flight);
    let shared_flaky = Arc::clone(&flaky_seen);
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let shared = Arc::clone(&shared);
                    let in_flight = Arc::clone(&shared_in_flight);
                    let max = Arc::clone(&shared_max);
                    let flaky = Arc::clone(&shared_flaky);
                    thread::spawn(move || {
                        handle_conn(stream, shared, in_flight, max, flaky, delay_ms)
                    });
                }
                Err(_) => break,
            }
        }
    });
    MockServer {
        port,
        requests,
        max_in_flight,
    }
}

fn handle_conn(
    mut stream: TcpStream,
    requests: Arc<Mutex<Vec<Recorded>>>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    flaky_seen: Arc<AtomicBool>,
    delay_ms: u64,
) {
    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    max_in_flight.fetch_max(current, Ordering::SeqCst);
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        in_flight.fetch_sub(1, Ordering::SeqCst);
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
    } else if body_text.contains(FLAKY_MARKER) && !flaky_seen.swap(true, Ordering::SeqCst) {
        ("500 Internal Server Error", "transient".to_string())
    } else if body_text.contains(LOOP_MARKER) {
        match sse_for(&path, true) {
            Some(sse) => ("200 OK", sse.to_string()),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else {
        let is_second_turn = body_text.contains(tool_result_marker(&path));
        match sse_for(&path, !is_second_turn) {
            Some(sse) => ("200 OK", sse.to_string()),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
        response_body.len()
    );
    if delay_ms > 0 {
        thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
    stream.write_all(response.as_bytes()).ok();
    in_flight.fetch_sub(1, Ordering::SeqCst);
}

/// 搭一个两单元作业：单元 1 文件清单形状，单元 2 行区间形状。
/// marker 为 Some 时注入到单元 2 的分片内容里（FAIL-UNIT / LOOP-MARKER）。
fn write_job(dir: &Path, marker: Option<&str>) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let data = dir.join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "苹果是水果。\n香蕉也是。\n").unwrap();
    let big = match marker {
        Some(m) => format!("一\n{m}\n三\n"),
        None => "一\n二\n三\n".to_string(),
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
    concurrency: usize,
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
        .arg("--concurrency")
        .arg(concurrency.to_string())
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

/// 主成功路径：三协议各跑一次，断言产出、审计、多轮请求、汇总与退出码。
fn assert_success(protocol: &str, expected_path: &str) {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), None);
    let mock = start_mock();
    let output = run_formic(protocol, mock.port, 2, &data, &plan, &task, &out);

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
        assert_eq!(
            fs::read_to_string(out.join(format!("{unit}.md"))).unwrap(),
            FINAL_TEXT,
            "{protocol} 单元 {unit} 产出应为最终回合文本"
        );
        let audit = fs::read_to_string(out.join("audit").join(format!("{unit}.jsonl"))).unwrap();
        for direction in ["request", "event", "tool_call", "tool_result"] {
            assert!(
                audit.contains(&format!("\"direction\":\"{direction}\"")),
                "{protocol} 单元 {unit} 审计缺 {direction}：{audit}"
            );
        }
        assert!(
            audit.contains("\\\"pattern\\\":\\\"苹果\\\""),
            "{protocol} 审计应含工具参数原文（JSON 转义形态）：{audit}"
        );
    }

    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 4, "{protocol} 每单元两轮共 4 次请求");
    assert!(
        requests.iter().all(|r| r.path.ends_with(expected_path)),
        "{protocol} 请求路径应为 {expected_path}"
    );
    // 并发下请求不按单元顺序到达，按多重集断言：两个首轮（携带 tools schema），
    // 两个次轮（携带协议工具结果消息与 search 命中行）
    let marker = tool_result_marker(expected_path);
    let first_turns: Vec<_> = requests
        .iter()
        .filter(|r| !r.body.contains(marker))
        .collect();
    let second_turns: Vec<_> = requests
        .iter()
        .filter(|r| r.body.contains(marker))
        .collect();
    assert_eq!(first_turns.len(), 2, "{protocol} 应有两个首轮请求");
    assert_eq!(second_turns.len(), 2, "{protocol} 应有两个次轮请求");
    assert!(
        first_turns.iter().all(|r| r.body.contains("\"search\"")),
        "{protocol} 首轮请求应携带 tools schema"
    );
    assert!(
        second_turns.iter().all(|r| r.body.contains("苹果是水果")),
        "{protocol} 次轮请求应回注 search 结果"
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
    let (data, plan, task, out) = write_job(dir.path(), Some(FAIL_MARKER));
    let mock = start_mock();
    let output = run_formic("completions", mock.port, 2, &data, &plan, &task, &out);

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
    assert!(
        stderr.contains("重试 2 次"),
        "瞬时故障应重试到预算耗尽：{stderr}"
    );

    // 单元 1 两轮 2 次请求 + 单元 2 三次尝试均 500
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 5, "重试预算应耗尽：{:?}", requests.len());
}

/// 瞬时故障重试成功：首次 500 → 重发 → 正常完成两轮。
#[test]
fn retry_succeeds_after_transient_failure() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "苹果是水果。\nFLAKY-UNIT\n").unwrap();
    let plan = dir.path().join("plan.jsonl");
    fs::write(&plan, "{\"unit\":1,\"files\":[\"a.txt\"]}\n").unwrap();
    let task = dir.path().join("task.md");
    fs::write(&task, "任务。\n").unwrap();
    let out = dir.path().join("out");

    let mock = start_mock();
    let output = run_formic("completions", mock.port, 1, &data, &plan, &task, &out);
    assert_eq!(
        output.status.code(),
        Some(0),
        "重试后应成功：{}",
        stderr_of(&output)
    );
    assert_eq!(
        fs::read_to_string(out.join("1.md")).unwrap(),
        FINAL_TEXT,
        "产出应为最终回合文本"
    );

    // 500 一次 + 重试的工具调用 + 最终回合，共 3 次请求
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "{:?}", requests.len());

    let audit = fs::read_to_string(out.join("audit").join("1.jsonl")).unwrap();
    assert!(audit.contains("\"attempt\":1"), "审计应含首次尝试：{audit}");
    assert!(audit.contains("\"attempt\":2"), "审计应含重试尝试：{audit}");
}

#[test]
fn stalled_unit_is_terminated() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), Some(LOOP_MARKER));
    let mock = start_mock();
    let output = run_formic("completions", mock.port, 2, &data, &plan, &task, &out);

    assert_eq!(
        output.status.code(),
        Some(1),
        "停滞单元应判失败：{}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(stdout.contains("失败单元：2"), "{stdout}");
    assert!(!out.join("2.md").exists(), "停滞单元不得留下记录");

    let stderr = stderr_of(&output);
    assert!(stderr.contains("单元 2 失败"), "{stderr}");
    assert!(stderr.contains("停滞"), "诊断应说明停滞原因：{stderr}");

    let audit = fs::read_to_string(out.join("audit").join("2.jsonl")).unwrap();
    assert!(
        audit.contains("\"direction\":\"tool_call\""),
        "停滞单元的工具调用应留痕：{audit}"
    );
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
        1,
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

/// 窗口证据：mock 端在途请求峰值 == 并发窗口（真实入口验证，不是单元推断）。
#[test]
fn concurrency_window_bounds_in_flight() {
    let run = |concurrency: usize| {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("a.txt"), "苹果是水果。\n").unwrap();
        let plan = dir.path().join("plan.jsonl");
        fs::write(
            &plan,
            "{\"unit\":1,\"files\":[\"a.txt\"]}\n{\"unit\":2,\"files\":[\"a.txt\"]}\n{\"unit\":3,\"files\":[\"a.txt\"]}\n{\"unit\":4,\"files\":[\"a.txt\"]}\n",
        )
        .unwrap();
        let task = dir.path().join("task.md");
        fs::write(&task, "判断分片内容。\n").unwrap();
        let out = dir.path().join("out");

        let mock = start_mock_with_delay(100);
        let output = run_formic(
            "completions",
            mock.port,
            concurrency,
            &data,
            &plan,
            &task,
            &out,
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "concurrency={concurrency} 应全部成功：{}",
            stderr_of(&output)
        );
        let max = mock.max_in_flight.load(Ordering::SeqCst);
        drop(dir);
        max
    };

    assert_eq!(run(2), 2, "窗口 2 + 慢响应时，在途峰值应恰好达到 2");
    assert_eq!(run(1), 1, "窗口 1 时请求必须严格串行");
}

/// 汇总确定性：并发改变完成时间，不改变自然顺序——失败单元按计划文件顺序呈现。
#[test]
fn summary_follows_plan_order() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "FAIL-UNIT 甲\n").unwrap();
    fs::write(data.join("b.txt"), "FAIL-UNIT 乙\n").unwrap();
    // 计划行乱序：单元 2 在前、单元 1 在后；两者都触发 500
    let plan = dir.path().join("plan.jsonl");
    fs::write(
        &plan,
        "{\"unit\":2,\"files\":[\"a.txt\"]}\n{\"unit\":1,\"files\":[\"b.txt\"]}\n",
    )
    .unwrap();
    let task = dir.path().join("task.md");
    fs::write(&task, "任务。\n").unwrap();
    let out = dir.path().join("out");

    let mock = start_mock_with_delay(50);
    let output = run_formic("completions", mock.port, 2, &data, &plan, &task, &out);
    assert_eq!(output.status.code(), Some(1));
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("失败单元：2, 1"),
        "失败单元必须按计划文件顺序呈现，与完成时间无关：{stdout}"
    );
}
