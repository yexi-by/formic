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

/// 请求体含此标记时，completions mock 改为调用测试 MCP 工具。
const MCP_MARKER: &str = "MCP-UNIT";

/// 请求体含此标记时，mock 直接提交符合 schema 的结构化结果。
const STRUCTURED_MARKER: &str = "STRUCTURED-UNIT";
const INVALID_STRUCTURED_MARKER: &str = "BAD-SCHEMA-UNIT";
const MIXED_STRUCTURED_MARKER: &str = "MIXED-SUBMIT-UNIT";
const EXHAUST_STRUCTURED_MARKER: &str = "EXHAUST-SCHEMA-UNIT";
const REFUSAL_MARKER: &str = "REFUSAL-UNIT";
const TRUNCATION_MARKER: &str = "TRUNCATION-UNIT";
const COMPACTION_MARKER: &str = "COMPACTION-UNIT";
const MCP_TIMEOUT_MARKER: &str = "MCP-TIMEOUT-UNIT";

// ---- 罐装 SSE：文本最终帧（三协议）----

const COMPLETIONS_FINAL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"世界\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":60,\"cache_creation_tokens\":20}}}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_FINAL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"你好\"}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"世界\"}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":5,\"input_tokens_details\":{\"cached_tokens\":60,\"cache_creation_tokens\":20}}}}\n\n",
);

const ANTHROPIC_FINAL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":100,\"output_tokens\":1,\"cache_read_input_tokens\":60,\"cache_creation_input_tokens\":20}}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"世界\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":5}}\n\n",
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

const COMPLETIONS_MCP_TOOLCALL: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_mcp\",\"type\":\"function\",\"function\":{\"name\":\"demo__echo\",\"arguments\":\"{\\\"text\\\":\\\"hello\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const COMPLETIONS_SLOW_MCP_TOOLCALL: &str = concat!(
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_slow\",\"type\":\"function\",\"function\":{\"name\":\"demo__slow\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const COMPLETIONS_READ_FOR_COMPACTION: &str = concat!(
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"read_1\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"scope\\\":\\\"input\\\",\\\"path\\\":\\\"big.txt\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const COMPLETIONS_COMPACTION_SUBMIT: &str = concat!(
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"compact_1\",\"type\":\"function\",\"function\":{\"name\":\"formic_submit_compaction\",\"arguments\":\"{\\\"summary\\\":\\\"已读取大文件\\\",\\\"verified_facts\\\":[\\\"文件可读\\\"],\\\"evidence\\\":[\\\"read big.txt\\\"],\\\"remaining_work\\\":[\\\"形成答案\\\"]}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
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

const COMPLETIONS_STRUCTURED: &str = concat!(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"submit_1\",\"type\":\"function\",\"function\":{\"name\":\"formic_submit_result\",\"arguments\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

const RESPONSES_STRUCTURED: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"submit_1\",\"name\":\"formic_submit_result\",\"arguments\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
);

const ANTHROPIC_STRUCTURED: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"submit_1\",\"name\":\"formic_submit_result\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

fn structured_sse(path: &str) -> Option<&'static str> {
    if path.ends_with("/chat/completions") {
        Some(COMPLETIONS_STRUCTURED)
    } else if path.ends_with("/responses") {
        Some(RESPONSES_STRUCTURED)
    } else if path.ends_with("/messages") {
        Some(ANTHROPIC_STRUCTURED)
    } else {
        None
    }
}

fn structured_sse_with_validity(path: &str, valid: bool) -> Option<String> {
    let response = structured_sse(path)?;
    Some(if valid {
        response.to_string()
    } else {
        response.replace("{\\\"answer\\\":\\\"ok\\\"}", "{}")
    })
}

fn terminal_sse(path: &str, refusal: bool) -> Option<String> {
    if path.ends_with("/chat/completions") {
        let reason = if refusal { "content_filter" } else { "length" };
        Some(format!(
            "data: {{\"id\":\"c1\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{reason}\"}}]}}\n\ndata: [DONE]\n\n"
        ))
    } else if path.ends_with("/responses") {
        let reason = if refusal {
            "content_filter"
        } else {
            "max_output_tokens"
        };
        Some(format!(
            "data: {{\"type\":\"response.incomplete\",\"response\":{{\"status\":\"incomplete\",\"incomplete_details\":{{\"reason\":\"{reason}\"}}}}}}\n\n"
        ))
    } else if path.ends_with("/messages") {
        let reason = if refusal { "refusal" } else { "max_tokens" };
        Some(format!(
            "data: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{reason}\"}}}}\n\ndata: {{\"type\":\"message_stop\"}}\n\n"
        ))
    } else {
        None
    }
}

fn mixed_structured_sse(path: &str) -> Option<String> {
    if path.ends_with("/chat/completions") {
        Some(concat!(
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[",
            "{\"index\":0,\"id\":\"search_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}},",
            "{\"index\":1,\"id\":\"submit_1\",\"type\":\"function\",\"function\":{\"name\":\"formic_submit_result\",\"arguments\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}",
            "]},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        )
        .to_string())
    } else if path.ends_with("/responses") {
        Some(concat!(
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc1\",\"call_id\":\"search_1\",\"name\":\"search\",\"arguments\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc2\",\"call_id\":\"submit_1\",\"name\":\"formic_submit_result\",\"arguments\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
        )
        .to_string())
    } else if path.ends_with("/messages") {
        Some(concat!(
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"search_1\",\"name\":\"search\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"pattern\\\":\\\"苹果\\\",\\\"scope\\\":\\\"input\\\"}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"submit_1\",\"name\":\"formic_submit_result\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )
        .to_string())
    } else {
        None
    }
}

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

fn tools_value(body: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(body).unwrap()["tools"].clone()
}

struct Recorded {
    path: String,
    body: String,
    authorization: Option<String>,
}

struct MockServer {
    port: u16,
    requests: Arc<Mutex<Vec<Recorded>>>,
    max_in_flight: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct McpHttpRecorded {
    rpc_method: String,
    authorization: Option<String>,
    custom_header: Option<String>,
    session_id: Option<String>,
}

struct McpHttpMock {
    port: u16,
    requests: Arc<Mutex<Vec<McpHttpRecorded>>>,
}

fn start_mcp_http_mock() -> McpHttpMock {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let shared = Arc::clone(&requests);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let shared = Arc::clone(&shared);
            thread::spawn(move || handle_mcp_http(stream, shared));
        }
    });
    McpHttpMock { port, requests }
}

fn handle_mcp_http(mut stream: TcpStream, requests: Arc<Mutex<Vec<McpHttpRecorded>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let http_method = request_line.split_whitespace().next().unwrap_or("");
    let mut content_length = 0usize;
    let mut authorization = None;
    let mut custom_header = None;
    let mut session_id = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            match name.to_ascii_lowercase().as_str() {
                "content-length" => content_length = value.trim().parse().unwrap_or(0),
                "authorization" => authorization = Some(value.trim().to_string()),
                "x-formic-test" => custom_header = Some(value.trim().to_string()),
                "mcp-session-id" => session_id = Some(value.trim().to_string()),
                _ => {}
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).ok();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let rpc_method = value["method"].as_str().unwrap_or(http_method).to_string();
    requests.lock().unwrap().push(McpHttpRecorded {
        rpc_method: rpc_method.clone(),
        authorization,
        custom_header,
        session_id,
    });

    if http_method == "DELETE" {
        write_http_response(&mut stream, "200 OK", None, "");
        return;
    }
    let Some(id) = value.get("id").cloned() else {
        write_http_response(&mut stream, "202 Accepted", None, "");
        return;
    };
    let result = match rpc_method.as_str() {
        "initialize" => serde_json::json!({
            "protocolVersion": value.pointer("/params/protocolVersion").cloned().unwrap(),
            "capabilities": {"tools": {"listChanged": true}},
            "serverInfo": {"name":"http-test-mcp","version":"1"}
        }),
        "tools/list" if value.pointer("/params/cursor").is_none() => serde_json::json!({
            "tools":[{
                "name":"echo",
                "description":"HTTP echo",
                "inputSchema":{
                    "type":"object",
                    "properties":{"text":{"type":"string"}},
                    "required":["text"],
                    "additionalProperties":false
                }
            }],
            "nextCursor":"page-2"
        }),
        "tools/list" => serde_json::json!({"tools":[]}),
        "tools/call" => serde_json::json!({
            "content":[{"type":"text","text":"http:hello"}],
            "structuredContent":{"value":"hello"},
            "isError":false
        }),
        _ => serde_json::json!({}),
    };
    let response = serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}).to_string();
    let new_session = (rpc_method == "initialize").then_some("test-session");
    write_http_response(&mut stream, "200 OK", new_session, &response);
}

fn write_http_response(stream: &mut TcpStream, status: &str, session_id: Option<&str>, body: &str) {
    let session = session_id
        .map(|value| format!("mcp-session-id: {value}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{session}content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).ok();
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
    let mut authorization = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            match name.to_ascii_lowercase().as_str() {
                "content-length" => content_length = value.trim().parse().unwrap_or(0),
                "authorization" => authorization = Some(value.trim().to_string()),
                _ => {}
            }
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
        authorization,
    });

    let (status, response_body) = if body_text.contains(FAIL_MARKER) {
        ("500 Internal Server Error", "boom".to_string())
    } else if body_text.contains(FLAKY_MARKER) && !flaky_seen.swap(true, Ordering::SeqCst) {
        ("500 Internal Server Error", "transient".to_string())
    } else if body_text.contains(COMPACTION_MARKER)
        || body_text.contains("formic_submit_compaction")
        || body_text.contains("此前历史的已验证压缩摘要")
    {
        if body_text.contains("formic_submit_compaction") {
            ("200 OK", COMPLETIONS_COMPACTION_SUBMIT.to_string())
        } else if body_text.contains("此前历史的已验证压缩摘要") {
            ("200 OK", COMPLETIONS_FINAL.to_string())
        } else {
            ("200 OK", COMPLETIONS_READ_FOR_COMPACTION.to_string())
        }
    } else if body_text.contains(INVALID_STRUCTURED_MARKER) {
        let corrected = body_text.contains(tool_result_marker(&path));
        match structured_sse_with_validity(&path, corrected) {
            Some(sse) => ("200 OK", sse),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(MIXED_STRUCTURED_MARKER) {
        let corrected = body_text.contains(tool_result_marker(&path));
        let response = if corrected {
            structured_sse_with_validity(&path, true)
        } else {
            mixed_structured_sse(&path)
        };
        match response {
            Some(sse) => ("200 OK", sse),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(EXHAUST_STRUCTURED_MARKER) {
        match structured_sse_with_validity(&path, false) {
            Some(sse) => ("200 OK", sse),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(REFUSAL_MARKER) {
        match terminal_sse(&path, true) {
            Some(sse) => ("200 OK", sse),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(TRUNCATION_MARKER) {
        match terminal_sse(&path, false) {
            Some(sse) => ("200 OK", sse),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(STRUCTURED_MARKER) {
        match structured_sse(&path) {
            Some(sse) => ("200 OK", sse.to_string()),
            None => ("404 Not Found", "unknown path".to_string()),
        }
    } else if body_text.contains(MCP_TIMEOUT_MARKER) {
        ("200 OK", COMPLETIONS_SLOW_MCP_TOOLCALL.to_string())
    } else if body_text.contains(MCP_MARKER) {
        let is_second_turn = body_text.contains(tool_result_marker(&path));
        if path.ends_with("/chat/completions") && !is_second_turn {
            ("200 OK", COMPLETIONS_MCP_TOOLCALL.to_string())
        } else {
            match sse_for(&path, false) {
                Some(sse) => ("200 OK", sse.to_string()),
                None => ("404 Not Found", "unknown path".to_string()),
            }
        }
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
    let mut command = formic_command(concurrency, data, plan, task, out);
    command
        .env("FORMIC_LLM_PROTOCOL", protocol)
        .env("FORMIC_LLM_BASE_URL", format!("http://127.0.0.1:{port}/v1"))
        .env("FORMIC_LLM_MODEL", "test-model")
        .env("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "131072")
        .env("FORMIC_LLM_MAX_OUTPUT_TOKENS", "16384")
        .env_remove("FORMIC_LLM_API_KEY");
    command.output().unwrap()
}

fn formic_command(
    concurrency: usize,
    data: &Path,
    plan: &Path,
    task: &Path,
    out: &Path,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_formic"));
    command
        .current_dir(out.parent().expect("测试输出目录有父目录"))
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
        .arg(concurrency.to_string());
    command
}

fn run_structured_formic(
    protocol: &str,
    port: u16,
    data: &Path,
    plan: &Path,
    task: &Path,
    out: &Path,
    schema: &Path,
) -> Output {
    let mut command = formic_command(2, data, plan, task, out);
    command
        .arg("--output-schema")
        .arg(schema)
        .env("FORMIC_LLM_PROTOCOL", protocol)
        .env("FORMIC_LLM_BASE_URL", format!("http://127.0.0.1:{port}/v1"))
        .env("FORMIC_LLM_MODEL", "test-model")
        .env("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "131072")
        .env("FORMIC_LLM_MAX_OUTPUT_TOKENS", "16384")
        .env_remove("FORMIC_LLM_API_KEY");
    command.output().unwrap()
}

fn write_answer_schema(directory: &Path) -> PathBuf {
    let schema = directory.join("schema.json");
    fs::write(
        &schema,
        r#"{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}"#,
    )
    .unwrap();
    schema
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn compile_fake_mcp(directory: &Path) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake_mcp_stdio.rs");
    let executable = directory.join(if cfg!(windows) {
        "fake-mcp-stdio.exe"
    } else {
        "fake-mcp-stdio"
    });
    let compiled = Command::new("rustc")
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "fake MCP 编译失败：{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    executable
}

fn toml_string(path: &Path) -> String {
    serde_json::to_string(&path.to_string_lossy()).unwrap()
}

/// 读取 out/stats.jsonl，返回按单元号索引的 JSON 行。
fn stats_lines(out: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(out.join("stats.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn stats_of(lines: &[serde_json::Value], unit: u64) -> &serde_json::Value {
    lines
        .iter()
        .find(|l| l["unit"] == unit)
        .unwrap_or_else(|| panic!("stats 缺单元 {unit} 的行"))
}

fn worker_run_dir(out: &Path) -> PathBuf {
    let mut directories: Vec<PathBuf> = fs::read_dir(out.join("workers"))
        .expect("缺少 workers 观测目录")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    directories.sort();
    assert_eq!(directories.len(), 1, "一次测试作业应只有一个任务时间戳目录");
    directories.pop().unwrap()
}

fn worker_report(out: &Path, unit: u64) -> String {
    let directory = worker_run_dir(out);
    assert!(
        fs::read_dir(&directory).unwrap().all(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_none_or(|ext| ext == "md")),
        "成功渲染后不得残留重复 JSONL 或临时文件：{}",
        directory.display()
    );
    fs::read_to_string(directory.join(format!("{unit}.md"))).unwrap()
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
        let report = worker_report(&out, unit);
        for section in [
            "状态：准备输入",
            "上下文预算判断",
            "LLM 请求",
            "LLM 原始事件",
            "模型请求工具",
            "工具执行事实",
            "工具结果",
        ] {
            assert!(
                report.contains(section),
                "{protocol} 单元 {unit} 运行档案缺 {section}：{report}"
            );
        }
        assert!(
            report.contains("\"pattern\":\"苹果\""),
            "{protocol} 运行档案应含工具参数原文：{report}"
        );
        assert_eq!(
            report.matches("完整请求体（基准）").count(),
            1,
            "{protocol} 运行档案只应保存首轮完整请求：{report}"
        );
        assert!(
            report.contains("本轮请求变化（逐字保留）"),
            "{protocol} 后续请求应保存可逆变化量：{report}"
        );
        assert_eq!(
            report.matches("LLM 原始事件流").count(),
            2,
            "{protocol} 两次模型调用应各有一个响应流标题：{report}"
        );
        assert!(report.contains("结局：`published`"), "{report}");
    }

    // 单元统计：轮数、调用数、工具计数、token 估算
    let stats = stats_lines(&out);
    assert_eq!(stats.len(), 2, "{protocol} stats 应有两个单元行");
    for unit in [1, 2] {
        let s = stats_of(&stats, unit);
        assert_eq!(s["outcome"], "published", "{protocol} 单元 {unit}：{s}");
        assert_eq!(s["turns"], 2, "{protocol} 单元 {unit} 应为 2 轮：{s}");
        assert_eq!(
            s["llm_calls"], 2,
            "{protocol} 单元 {unit} 应为 2 次调用：{s}"
        );
        assert_eq!(
            s["tool_calls"]["search"], 1,
            "{protocol} 单元 {unit} 应调用 search 一次：{s}"
        );
        assert!(
            s["input_tokens_est"].as_u64().unwrap() > 0,
            "{protocol} 单元 {unit} input：{s}"
        );
        assert!(
            s["output_tokens_est"].as_u64().unwrap() > 0,
            "{protocol} 单元 {unit} output：{s}"
        );
        assert_eq!(s["provider_usage_reports"], 1, "{protocol}：{s}");
        assert_eq!(s["provider_usage_missing_calls"], 1, "{protocol}：{s}");
        assert_eq!(s["provider_input_tokens"], 100, "{protocol}：{s}");
        assert_eq!(s["provider_output_tokens"], 5, "{protocol}：{s}");
        assert_eq!(s["provider_cache_read_tokens"], 60, "{protocol}：{s}");
        assert_eq!(s["provider_cache_creation_tokens"], 20, "{protocol}：{s}");
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
    let frozen_tools = tools_value(&requests[0].body);
    assert!(
        requests
            .iter()
            .all(|request| tools_value(&request.body) == frozen_tools),
        "{protocol} 的工具名称、顺序和 schema 必须在全部单元与回合中保持一致"
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
fn structured_output_succeeds_for_all_protocols() {
    for (protocol, expected_path) in [
        ("completions", "/chat/completions"),
        ("responses", "/responses"),
        ("anthropic", "/messages"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (data, plan, task, out) = write_job(dir.path(), None);
        fs::write(
            &task,
            format!("按结构化契约提交结果。{STRUCTURED_MARKER}\n"),
        )
        .unwrap();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}"#,
        )
        .unwrap();
        let mock = start_mock();
        let mut command = formic_command(2, &data, &plan, &task, &out);
        let output = command
            .arg("--output-schema")
            .arg(&schema)
            .env("FORMIC_LLM_PROTOCOL", protocol)
            .env(
                "FORMIC_LLM_BASE_URL",
                format!("http://127.0.0.1:{}/v1", mock.port),
            )
            .env("FORMIC_LLM_MODEL", "test-model")
            .env("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "131072")
            .env("FORMIC_LLM_MAX_OUTPUT_TOKENS", "16384")
            .env_remove("FORMIC_LLM_API_KEY")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{protocol} 结构化模式失败：{}",
            stderr_of(&output)
        );
        for unit in [1, 2] {
            assert_eq!(
                fs::read_to_string(out.join(format!("{unit}.json"))).unwrap(),
                "{\n  \"answer\": \"ok\"\n}\n"
            );
            assert!(!out.join(format!("{unit}.md")).exists());
            let report = worker_report(&out, unit);
            assert!(report.contains("结构化结果校验"), "{report}");
            assert!(report.contains("校验通过：`true`"), "{report}");
        }
        assert!(out.join("output-schema.json").exists());
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "{protocol} 每单元只需一次提交");
        assert!(requests.iter().all(|request| {
            request.path.ends_with(expected_path)
                && request.body.contains("formic_submit_result")
                && request.body.contains("additionalProperties")
        }));
        assert_eq!(
            tools_value(&requests[0].body),
            tools_value(&requests[1].body),
            "{protocol} 的结构化 schema 与工具顺序必须在全部单元中一致"
        );
    }
}

#[test]
fn structured_invalid_and_mixed_turns_are_corrected_for_all_protocols() {
    for protocol in ["completions", "responses", "anthropic"] {
        for (marker, mixed) in [
            (INVALID_STRUCTURED_MARKER, false),
            (MIXED_STRUCTURED_MARKER, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (data, plan, task, out) = write_job(dir.path(), None);
            fs::write(&task, format!("提交结构化结果。{marker}\n")).unwrap();
            let schema = write_answer_schema(dir.path());
            let mock = start_mock();
            let output =
                run_structured_formic(protocol, mock.port, &data, &plan, &task, &out, &schema);
            assert_eq!(
                output.status.code(),
                Some(0),
                "{protocol} marker={marker}：{}",
                stderr_of(&output)
            );
            for unit in [1, 2] {
                assert!(out.join(format!("{unit}.json")).exists());
                let report = worker_report(&out, unit);
                assert!(report.contains("校验通过：`false`"), "{report}");
                assert!(report.contains("校验通过：`true`"), "{report}");
            }
            let stats = stats_lines(&out);
            for unit in [1, 2] {
                let value = stats_of(&stats, unit);
                assert_eq!(value["structured_corrections"], 1, "{value}");
                if mixed {
                    assert_eq!(value["tool_calls"]["search"], 1, "{value}");
                }
            }
            assert_eq!(mock.requests.lock().unwrap().len(), 4);
        }
    }
}

#[test]
fn refusal_truncation_and_structured_exhaustion_publish_nothing() {
    for protocol in ["completions", "responses", "anthropic"] {
        for (marker, expected) in [(REFUSAL_MARKER, "拒绝"), (TRUNCATION_MARKER, "截断")] {
            let dir = tempfile::tempdir().unwrap();
            let (data, plan, task, out) = write_job(dir.path(), None);
            fs::write(&task, format!("终态测试。{marker}\n")).unwrap();
            let mock = start_mock();
            let output = run_formic(protocol, mock.port, 2, &data, &plan, &task, &out);
            assert_eq!(output.status.code(), Some(1), "{}", stderr_of(&output));
            assert!(
                stderr_of(&output).contains(expected),
                "{}",
                stderr_of(&output)
            );
            assert!(!out.join("1.md").exists());
            assert!(!out.join("2.md").exists());
            assert_eq!(mock.requests.lock().unwrap().len(), 2);
        }

        let dir = tempfile::tempdir().unwrap();
        let (data, plan, task, out) = write_job(dir.path(), None);
        fs::write(
            &task,
            format!("持续提交无效结构。{EXHAUST_STRUCTURED_MARKER}\n"),
        )
        .unwrap();
        let schema = write_answer_schema(dir.path());
        let mock = start_mock();
        let output = run_structured_formic(protocol, mock.port, &data, &plan, &task, &out, &schema);
        assert_eq!(output.status.code(), Some(1), "{}", stderr_of(&output));
        assert!(stderr_of(&output).contains("连续 3 次无效"));
        assert!(!out.join("1.json").exists());
        assert!(!out.join("2.json").exists());
        assert_eq!(mock.requests.lock().unwrap().len(), 6);
    }
}

#[test]
fn context_budget_compacts_complete_tool_group_then_continues() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.txt"), "small shard\n").unwrap();
    let mut large = String::new();
    for line in 1..=260 {
        large.push_str(&format!(
            "line-{line:04} alpha beta gamma delta epsilon value-{line:04}\n"
        ));
    }
    fs::write(data.join("big.txt"), large).unwrap();
    let plan = dir.path().join("plan.jsonl");
    fs::write(&plan, "{\"unit\":1,\"files\":[\"a.txt\"]}\n").unwrap();
    let task = dir.path().join("task.md");
    fs::write(
        &task,
        format!("先读取 big.txt，再完成任务。{COMPACTION_MARKER}\n"),
    )
    .unwrap();
    let out = dir.path().join("out");
    fs::write(
        dir.path().join("config.toml"),
        "[execution]\ncontext_safety_tokens = 500\n[tools.read]\nmax_result_bytes = 30000\n",
    )
    .unwrap();
    let mock = start_mock();
    let mut command = formic_command(1, &data, &plan, &task, &out);
    let output = command
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env(
            "FORMIC_LLM_BASE_URL",
            format!("http://127.0.0.1:{}/v1", mock.port),
        )
        .env("FORMIC_LLM_MODEL", "test-model")
        .env("FORMIC_LLM_CONTEXT_WINDOW_TOKENS", "6200")
        .env("FORMIC_LLM_MAX_OUTPUT_TOKENS", "500")
        .env_remove("FORMIC_LLM_API_KEY")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "压缩后应继续完成：{}",
        stderr_of(&output)
    );
    assert_eq!(fs::read_to_string(out.join("1.md")).unwrap(), FINAL_TEXT);
    let report = worker_report(&out, 1);
    assert!(report.contains("上下文压缩请求"), "{report}");
    assert!(report.contains("上下文压缩结果"), "{report}");
    assert!(report.contains("校验通过：`true`"), "{report}");
    let stats = stats_lines(&out);
    let value = stats_of(&stats, 1);
    assert_eq!(value["compactions"], 1, "{value}");
    assert!(
        value["compaction_before_tokens"].as_u64().unwrap()
            > value["compaction_after_tokens"].as_u64().unwrap(),
        "{value}"
    );
    let requests = mock.requests.lock().unwrap();
    let kinds: Vec<_> = requests
        .iter()
        .map(|request| {
            if request.body.contains("formic_submit_compaction") {
                "compaction"
            } else if request.body.contains("此前历史的已验证压缩摘要") {
                "after"
            } else {
                "normal"
            }
        })
        .collect();
    assert_eq!(
        requests.len(),
        4,
        "两轮完整工具组后压缩，再继续正常调用：{kinds:?}"
    );
    assert_eq!(kinds, ["normal", "normal", "compaction", "after"]);
}

#[test]
fn config_file_supplies_http_settings() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), None);
    let mock = start_mock();
    fs::write(
        dir.path().join("config.toml"),
        format!(
            "url = \"http://127.0.0.1:{}/v1\"\napi_key = \"config-key\"\nmodel = \"config-model\"\ncontext_window_tokens = 131072\nmax_output_tokens = 16384\n",
            mock.port
        ),
    )
    .unwrap();

    let mut command = formic_command(2, &data, &plan, &task, &out);
    let output = command
        .current_dir(dir.path())
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env_remove("FORMIC_LLM_BASE_URL")
        .env_remove("FORMIC_LLM_API_KEY")
        .env_remove("FORMIC_LLM_MODEL")
        .env_remove("FORMIC_LLM_CONTEXT_WINDOW_TOKENS")
        .env_remove("FORMIC_LLM_MAX_OUTPUT_TOKENS")
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "config.toml 应能完成真实调用：{}",
        stderr_of(&output)
    );
    let requests = mock.requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(
        requests
            .iter()
            .all(|request| request.path.ends_with("/v1/chat/completions")),
        "请求应使用 config.toml 的 url"
    );
    assert!(
        requests
            .iter()
            .all(|request| request.body.contains("\"model\":\"config-model\"")),
        "请求应使用 config.toml 的 model"
    );
    assert!(
        requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some("Bearer config-key")),
        "请求应使用 config.toml 的明文 api_key"
    );
}

#[test]
fn custom_stdio_mcp_auto_discovers_all_tools_and_is_audited() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), Some(MCP_MARKER));
    fs::write(&task, format!("调用配置的外部工具完成任务。{MCP_MARKER}\n")).unwrap();
    let mock = start_mock_with_delay(40);
    let fake_mcp = compile_fake_mcp(dir.path());
    let start_log = dir.path().join("mcp-start.log");
    let call_log = dir.path().join("mcp-call.log");
    fs::write(
        dir.path().join("config.toml"),
        format!(
            concat!(
                "url = \"http://127.0.0.1:{}/v1\"\n",
                "model = \"test-model\"\n",
                "context_window_tokens = 131072\n",
                "max_output_tokens = 16384\n",
                "[tools.search]\nenabled = false\n",
                "[tools.read]\nenabled = false\n",
                "[mcp_servers.demo]\n",
                "enabled = true\n",
                "command = {}\n",
                "env = {{ FAKE_MCP_START_LOG = {}, FAKE_MCP_CALL_LOG = {} }}\n",
                "session_scope = \"job\"\n",
                "max_in_flight = 1\n",
                "startup_timeout_sec = 10\n",
                "tool_timeout_sec = 10\n",
            ),
            mock.port,
            toml_string(&fake_mcp),
            toml_string(&start_log),
            toml_string(&call_log),
        ),
    )
    .unwrap();

    let mut command = formic_command(2, &data, &plan, &task, &out);
    let output = command
        .current_dir(dir.path())
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env_remove("FORMIC_LLM_BASE_URL")
        .env_remove("FORMIC_LLM_MODEL")
        .env_remove("FORMIC_LLM_API_KEY")
        .env_remove("FORMIC_LLM_CONTEXT_WINDOW_TOKENS")
        .env_remove("FORMIC_LLM_MAX_OUTPUT_TOKENS")
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "自定义 MCP 作业应成功：{}",
        stderr_of(&output)
    );
    assert_eq!(
        fs::read_to_string(&start_log).unwrap().lines().count(),
        1,
        "job scope 应只建立一个 stdio 会话"
    );
    assert_eq!(
        fs::read_to_string(&call_log).unwrap().lines().count(),
        2,
        "两个单元各调用一次，不能自动重放"
    );
    for unit in [1, 2] {
        assert_eq!(
            fs::read_to_string(out.join(format!("{unit}.md"))).unwrap(),
            FINAL_TEXT
        );
        let report = worker_report(&out, unit);
        assert!(
            report.contains("mcp:demo/echo"),
            "运行档案应记录 MCP 来源：{report}"
        );
        assert!(
            report.contains("structuredContent"),
            "文本与结构结果应使用固定包装：{report}"
        );
    }
    let stats = stats_lines(&out);
    for unit in [1, 2] {
        let value = stats_of(&stats, unit);
        assert_eq!(value["mcp_peak_in_flight"]["demo"], 1, "{value}");
    }
    let requests = mock.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| !request.body.contains("\"role\":\"tool\""))
            .all(|request| request.body.contains("demo__echo")),
        "冻结工具目录必须出现在每个首轮请求中"
    );
}

#[test]
fn stdio_mcp_unit_scope_creates_and_reclaims_one_session_per_unit() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), None);
    fs::write(&task, format!("调用 unit scope MCP。{MCP_MARKER}\n")).unwrap();
    let llm = start_mock();
    let fake_mcp = compile_fake_mcp(dir.path());
    let start_log = dir.path().join("unit-start.log");
    let call_log = dir.path().join("unit-call.log");
    fs::write(
        dir.path().join("config.toml"),
        format!(
            concat!(
                "url = \"http://127.0.0.1:{}/v1\"\n",
                "model = \"test-model\"\ncontext_window_tokens = 131072\nmax_output_tokens = 16384\n",
                "[tools.search]\nenabled = false\n[tools.read]\nenabled = false\n",
                "[mcp_servers.demo]\nenabled = true\ncommand = {}\n",
                "env = {{ FAKE_MCP_START_LOG = {}, FAKE_MCP_CALL_LOG = {} }}\n",
                "enabled_tools = [\"echo\"]\nsession_scope = \"unit\"\nmax_in_flight = 2\n",
            ),
            llm.port,
            toml_string(&fake_mcp),
            toml_string(&start_log),
            toml_string(&call_log),
        ),
    )
    .unwrap();
    let mut command = formic_command(2, &data, &plan, &task, &out);
    let output = command
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env_remove("FORMIC_LLM_BASE_URL")
        .env_remove("FORMIC_LLM_MODEL")
        .env_remove("FORMIC_LLM_API_KEY")
        .env_remove("FORMIC_LLM_CONTEXT_WINDOW_TOKENS")
        .env_remove("FORMIC_LLM_MAX_OUTPUT_TOKENS")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{}", stderr_of(&output));
    assert_eq!(
        fs::read_to_string(start_log).unwrap().lines().count(),
        3,
        "启动发现使用一个临时会话，两个活动单元各使用一个独立会话"
    );
    assert_eq!(fs::read_to_string(call_log).unwrap().lines().count(), 2);
}

#[test]
fn timed_out_mcp_call_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), None);
    fs::write(&plan, "{\"unit\":1,\"files\":[\"a.txt\"]}\n").unwrap();
    fs::write(&task, format!("调用慢 MCP。{MCP_TIMEOUT_MARKER}\n")).unwrap();
    let llm = start_mock();
    let fake_mcp = compile_fake_mcp(dir.path());
    let call_log = dir.path().join("timeout-call.log");
    fs::write(
        dir.path().join("config.toml"),
        format!(
            concat!(
                "url = \"http://127.0.0.1:{}/v1\"\n",
                "model = \"test-model\"\ncontext_window_tokens = 131072\nmax_output_tokens = 16384\n",
                "[tools.search]\nenabled = false\n[tools.read]\nenabled = false\n",
                "[mcp_servers.demo]\nenabled = true\ncommand = {}\n",
                "env = {{ FAKE_MCP_CALL_LOG = {} }}\n",
                "enabled_tools = [\"slow\"]\ntool_timeout_sec = 1\nreconnect = true\n",
            ),
            llm.port,
            toml_string(&fake_mcp),
            toml_string(&call_log),
        ),
    )
    .unwrap();
    let mut command = formic_command(1, &data, &plan, &task, &out);
    let output = command
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env_remove("FORMIC_LLM_BASE_URL")
        .env_remove("FORMIC_LLM_MODEL")
        .env_remove("FORMIC_LLM_API_KEY")
        .env_remove("FORMIC_LLM_CONTEXT_WINDOW_TOKENS")
        .env_remove("FORMIC_LLM_MAX_OUTPUT_TOKENS")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{}", stderr_of(&output));
    assert!(stderr_of(&output).contains("调用超时"));
    assert!(!out.join("1.md").exists());
    assert_eq!(
        fs::read_to_string(call_log).unwrap().lines().count(),
        1,
        "超时的原调用不得自动重放；reconnect 只影响后续新调用"
    );
}

#[test]
fn custom_streamable_http_mcp_uses_session_auth_and_frozen_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let (data, plan, task, out) = write_job(dir.path(), None);
    fs::write(&task, format!("调用 HTTP MCP。{MCP_MARKER}\n")).unwrap();
    let llm = start_mock();
    let mcp = start_mcp_http_mock();
    fs::write(
        dir.path().join("config.toml"),
        format!(
            concat!(
                "url = \"http://127.0.0.1:{}/v1\"\n",
                "model = \"test-model\"\n",
                "context_window_tokens = 131072\n",
                "max_output_tokens = 16384\n",
                "[tools.search]\nenabled = false\n",
                "[tools.read]\nenabled = false\n",
                "[mcp_servers.demo]\n",
                "enabled = true\n",
                "url = \"http://127.0.0.1:{}/mcp\"\n",
                "bearer_token = \"secret-token\"\n",
                "headers = {{ \"x-formic-test\" = \"yes\" }}\n",
                "enabled_tools = [\"echo\"]\n",
                "session_scope = \"job\"\n",
                "max_in_flight = 2\n",
            ),
            llm.port, mcp.port,
        ),
    )
    .unwrap();
    let mut command = formic_command(2, &data, &plan, &task, &out);
    let output = command
        .env("FORMIC_LLM_PROTOCOL", "completions")
        .env_remove("FORMIC_LLM_BASE_URL")
        .env_remove("FORMIC_LLM_MODEL")
        .env_remove("FORMIC_LLM_API_KEY")
        .env_remove("FORMIC_LLM_CONTEXT_WINDOW_TOKENS")
        .env_remove("FORMIC_LLM_MAX_OUTPUT_TOKENS")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "Streamable HTTP MCP 作业应成功：{}",
        stderr_of(&output)
    );
    let requests = mcp.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.rpc_method == "initialize")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.rpc_method == "tools/list")
            .count(),
        2,
        "tools/list 应遍历分页"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.rpc_method == "tools/call")
            .count(),
        2
    );
    assert!(requests.iter().all(|request| {
        request.authorization.as_deref() == Some("Bearer secret-token")
            && request.custom_header.as_deref() == Some("yes")
    }));
    assert!(
        requests
            .iter()
            .filter(|request| request.rpc_method != "initialize")
            .any(|request| request.session_id.as_deref() == Some("test-session")),
        "初始化后的请求应复用 MCP session id"
    );
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
    drop(requests);

    let stats = stats_lines(&out);
    assert_eq!(stats_of(&stats, 1)["outcome"], "published");
    let failed = stats_of(&stats, 2);
    assert_eq!(failed["outcome"], "failed", "{failed}");
    assert_eq!(failed["llm_calls"], 3, "失败单元应尝试 3 次：{failed}");
    assert_eq!(failed["retries"], 2, "失败单元应重试 2 次：{failed}");
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
    drop(requests);

    let report = worker_report(&out, 1);
    assert!(report.contains("模型调用重试"), "{report}");
    assert!(report.contains("第 `1` 次尝试失败"), "{report}");
    assert!(report.contains("第 `2` 次尝试"), "{report}");

    let stats = stats_lines(&out);
    let s = stats_of(&stats, 1);
    assert_eq!(s["outcome"], "published", "{s}");
    assert_eq!(s["retries"], 1, "应恰好重试 1 次：{s}");
    assert_eq!(s["llm_calls"], 3, "{s}");
    assert_eq!(s["tool_calls"]["search"], 1, "{s}");
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

    let report = worker_report(&out, 2);
    assert!(
        report.contains("模型请求工具"),
        "停滞单元的工具调用应留痕：{report}"
    );
    assert!(report.contains("结局：`failed`"), "{report}");
    assert!(report.contains("停滞"), "{report}");

    let stats = stats_lines(&out);
    let s = stats_of(&stats, 2);
    assert_eq!(s["outcome"], "failed", "{s}");
    assert_eq!(
        s["tool_calls"]["search"], 3,
        "停滞前应执行 3 次相同调用：{s}"
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
