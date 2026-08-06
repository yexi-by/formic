//! token 估算：内部计算值，协议无关，参考 codex 等开源工具的 tiktoken 实现
//! （o200k BPE）。用于预算与指标分析，不是计费依据——三个协议的供应商
//! usage 字段含义各异，内部估算保证一个数字一个含义。

use std::sync::OnceLock;

use tiktoken_rs::CoreBPE;

use crate::llm::{Message, ToolCallReq};

/// 每条消息的结构开销（角色/封装的估算常量）。
const MESSAGE_OVERHEAD: u64 = 4;

fn bpe() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| tiktoken_rs::o200k_base().expect("o200k BPE 初始化失败"))
}

/// 估算一段文本的 token 数。
pub fn count(text: &str) -> u64 {
    bpe().encode_ordinary(text).len() as u64
}

/// 估算一条内部消息的 token 数（内容 + 结构开销）。
pub fn count_message(message: &Message) -> u64 {
    let content = match message {
        Message::User(text) => count(text),
        Message::Assistant { text, tool_calls } => {
            count(text) + tool_calls.iter().map(count_tool_call).sum::<u64>()
        }
        Message::ToolResult { call_id, content } => count(call_id) + count(content),
    };
    content + MESSAGE_OVERHEAD
}

/// 估算一次工具调用的 token 数（模型输出的一部分）。
pub fn count_tool_call(tc: &ToolCallReq) -> u64 {
    count(&tc.call_id) + count(&tc.name) + count(&tc.arguments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_zero() {
        assert_eq!(count(""), 0);
    }

    #[test]
    fn chinese_text_reasonable() {
        let n = count("你是一次性自主执行单元。");
        assert!((8..=30).contains(&n), "中文应按字级别估算：{n}");
    }

    #[test]
    fn message_has_overhead() {
        let m = Message::User("你好".into());
        let content = count("你好");
        assert_eq!(count_message(&m), content + MESSAGE_OVERHEAD);
    }

    #[test]
    fn tool_call_counted() {
        let m = Message::Assistant {
            text: "看".into(),
            tool_calls: vec![ToolCallReq {
                call_id: "call_1".into(),
                name: "search".into(),
                arguments: "{\"pattern\":\"苹果\"}".into(),
            }],
        };
        assert!(count_message(&m) > count("看") + MESSAGE_OVERHEAD);
    }
}
