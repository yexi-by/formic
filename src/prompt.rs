//! worker 输入装配：instructions 静态文本 + 用户消息。
//!
//! 不变量：同一作业内全部单元的 instructions 与任务说明前缀字节一致，当前分片
//! 的文件名、内容与定位永远在消息末尾——这是 prompt cache 跨单元命中的前提。
//! Formic 对任务说明只做机械搬运，不理解其中的自然语言。

use std::fmt;
use std::path::Path;

/// 文本模式系统提示词：Formic 拥有的静态文本，全 worker 字节一致、作业内不改。
const TEXT_INSTRUCTIONS: &str = "\
你是一次性自主执行单元，独立完成分配给你的数据单元。没有人在场回答你的问题：\
不要提问、不要请示，根据任务说明自行判断；无法取得进展时说明原因并停止，不要重复相同操作。

你可以使用请求中列出的内置只读工具和外部 MCP 工具。内置工具的 input 指整个输入数据集，\
output 指当前输出模式下已完成单元的数字编号记录。工具结果有大小上限，截断或错误会显式说明。\
不要无进展地重复相同调用。

你的最后一条消息就是你的产出，运行时原样持久化。完成交付后立即停止。";

/// 结构化模式把最终结果交给内部终止工具，不把普通最终文本误当成完成事实。
const STRUCTURED_INSTRUCTIONS: &str = "\
你是一次性自主执行单元，独立完成分配给你的数据单元。没有人在场回答你的问题：\
不要提问、不要请示，根据任务说明自行判断；无法取得进展时说明原因并停止，不要重复相同操作。

你可以使用请求中列出的内置只读工具和外部 MCP 工具。内置工具的 input 指整个输入数据集，\
output 指当前输出模式下已完成单元的数字编号记录。工具结果有大小上限，截断或错误会显式说明。\
不要无进展地重复相同调用。

完成后必须单独调用 formic_submit_result 提交最终对象；提交回合不能包含文本或其他工具调用。\
普通最终文本不会成为完成记录。提交成功后立即停止。";

/// 返回本作业冻结的系统提示词。
pub fn instructions(structured: bool) -> &'static str {
    if structured {
        STRUCTURED_INSTRUCTIONS
    } else {
        TEXT_INSTRUCTIONS
    }
}

/// 已读出的分片内容，路径均为面向模型的根内相对表示。
#[cfg(test)]
pub enum ShardContent {
    Files(Vec<(String, String)>),
    Lines {
        file: String,
        start: u64,
        end: u64,
        content: String,
    },
}

/// 写入任务说明和分片标题。完整 input 根可以通过只读工具搜索，不在每个 prompt
/// 中重复整份文件清单。调用方可在每次 write 时实施预算。
pub(crate) fn write_user_prefix(output: &mut impl fmt::Write, task: &str) -> fmt::Result {
    output.write_str(task.trim_end_matches('\n'))?;
    output.write_str(
        "\n\n完整 input 根可通过只读 search/read 工具按需检索；不要主动扩大当前分片。\n\n# 你的分片\n",
    )
}

pub(crate) fn write_file_header(
    output: &mut impl fmt::Write,
    index: usize,
    path: &str,
) -> fmt::Result {
    if index > 0 {
        output.write_char('\n')?;
    }
    writeln!(output, "## 文件 {path}")
}

pub(crate) fn write_line_header(
    output: &mut impl fmt::Write,
    file: &str,
    start: u64,
    end: u64,
) -> fmt::Result {
    writeln!(output, "## 文件 {file} 第 {start}-{end} 行")
}

/// 装配用户消息：任务说明原文 + 当前单元的分片内容与自然位置。
#[cfg(test)]
pub fn build_user_message(task: &str, shard: &ShardContent) -> String {
    let mut msg = String::new();
    write_user_prefix(&mut msg, task).expect("String 写入不会失败");
    match shard {
        ShardContent::Files(files) => {
            for (i, (path, content)) in files.iter().enumerate() {
                write_file_header(&mut msg, i, path).expect("String 写入不会失败");
                msg.push_str(content.trim_end_matches('\n'));
                msg.push('\n');
            }
        }
        ShardContent::Lines {
            file,
            start,
            end,
            content,
        } => {
            write_line_header(&mut msg, file, *start, *end).expect("String 写入不会失败");
            msg.push_str(content.trim_end_matches('\n'));
            msg.push('\n');
        }
    }
    msg
}

/// 面向模型与计划的统一路径表示：始终以 `/` 分隔，与平台无关。
pub fn slash_path(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TASK: &str = "判断分片内容，给出结论。";

    #[test]
    fn shared_prefix_is_byte_identical_across_units() {
        let shard1 = ShardContent::Files(vec![("a.txt".into(), "内容甲".into())]);
        let shard2 = ShardContent::Lines {
            file: "dir/b.txt".into(),
            start: 3,
            end: 9,
            content: "内容乙".into(),
        };
        let m1 = build_user_message(TASK, &shard1);
        let m2 = build_user_message(TASK, &shard2);
        let prefix_len = m1.find("# 你的分片").unwrap();
        assert!(m2.len() > prefix_len);
        assert_eq!(
            &m1[..prefix_len],
            &m2[..prefix_len],
            "分片前的前缀必须字节一致"
        );
        assert!(m1.starts_with(TASK), "任务说明在消息最前");
        assert_ne!(m1, m2);
    }

    #[test]
    fn shard_is_at_message_end() {
        let shard = ShardContent::Files(vec![("a.txt".into(), "尾巴内容".into())]);
        let m = build_user_message(TASK, &shard);
        assert!(m.trim_end().ends_with("尾巴内容"), "分片内容在消息末尾");
    }

    #[test]
    fn complete_input_is_searchable_without_repeating_its_file_list() {
        let shard = ShardContent::Files(vec![("a.txt".into(), "x".into())]);
        let m = build_user_message(TASK, &shard);
        let prefix = &m[..m.find("# 你的分片").unwrap()];
        assert!(prefix.contains("完整 input 根"));
        assert!(!prefix.contains("dir/b.txt"));
    }

    #[test]
    fn lines_shard_carries_location() {
        let shard = ShardContent::Lines {
            file: "big.txt".into(),
            start: 100,
            end: 200,
            content: "切片".into(),
        };
        let m = build_user_message(TASK, &shard);
        assert!(m.contains("第 100-200 行"), "行区间定位进入消息");
    }

    #[test]
    fn slash_path_uses_forward_slashes() {
        let p = Path::new("dir").join("sub").join("f.txt");
        assert_eq!(slash_path(&p), "dir/sub/f.txt");
    }

    #[test]
    fn output_mode_has_unambiguous_completion_contract() {
        assert!(instructions(false).contains("最后一条消息"));
        assert!(!instructions(false).contains("formic_submit_result"));
        assert!(instructions(true).contains("必须单独调用 formic_submit_result"));
        assert!(instructions(true).contains("普通最终文本不会成为完成记录"));
    }
}
