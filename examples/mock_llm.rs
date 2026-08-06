//! 手工演示用 mock LLM server：脚本化两轮对话——首轮返回 search 工具调用
//! （检索「苹果」），携带工具结果的请求返回最终文本。按请求路径区分三种协议。
//!
//! 用法：`cargo run --example mock_llm -- [端口=18080]`，然后把
//! FORMIC_LLM_BASE_URL 指向打印出的地址即可跑通 `formic run` 全流程。

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::thread;

const COMPLETIONS_FINAL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"demo\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"demo\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"结论：分片内容正常（演示产出）。\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"demo\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_FINAL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"结论：分片内容正常（演示产出）。\"}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_FINAL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"demo\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"结论：分片内容正常（演示产出）。\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

const COMPLETIONS_TOOLCALL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"demo\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"demo\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_TOOLCALL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"call_1\",\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_TOOLCALL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"demo\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
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

fn tool_result_marker(path: &str) -> &'static str {
    if path.ends_with("/chat/completions") {
        "\"role\":\"tool\""
    } else if path.ends_with("/responses") {
        "function_call_output"
    } else {
        "tool_result"
    }
}

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(18080);
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    println!("mock LLM 就绪，FORMIC_LLM_BASE_URL=http://127.0.0.1:{port}/v1");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle(stream));
            }
            Err(e) => eprintln!("接受连接失败：{e}"),
        }
    }
}

fn handle(mut stream: std::net::TcpStream) {
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
    let body_text = String::from_utf8_lossy(&body);

    let second_turn = body_text.contains(tool_result_marker(&path));
    let Some(sse) = sse_for(&path, !second_turn) else {
        eprintln!("未知路径：{path}");
        let response = "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
        stream.write_all(response.as_bytes()).ok();
        return;
    };
    println!(
        "{path}（{}）",
        if second_turn {
            "最终回合"
        } else {
            "工具调用"
        }
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{sse}",
        sse.len()
    );
    stream.write_all(response.as_bytes()).ok();
}
