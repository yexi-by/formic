//! worker 输入装配：instructions 静态文本 + 用户消息。
//!
//! 不变量：同一作业内全部单元的 instructions、任务说明与数据集文件清单前缀
//! 字节一致，分片内容与定位永远在消息末尾——这是 prompt cache 跨单元命中的前提。
//! Formic 对任务说明只做机械搬运，不理解其中的自然语言。

use std::path::Path;

/// 系统提示词：Formic 拥有的静态文本，全 worker 字节一致、作业内不改。
/// 只含批处理特有的行为约束；输出格式的语义要求由调用方写进任务说明，不进内核。
pub const INSTRUCTIONS: &str = "\
你是一次性自主执行单元，独立完成分配给你的数据单元。没有人在场回答你的问题：\
不要提问、不要请示，根据任务说明自行判断；无法取得进展时说明原因并停止，不要重复相同操作。

你可以使用 search 工具在两棵只读根内检索：input 是整个输入数据集，output 是已完成单元的产出记录。\
搜索结果有大小上限，截断时会显式标记。不要重复发起相同的搜索。

你的最后一条消息就是你的产出，运行时原样持久化。完成交付后立即停止。";

/// 已读出的分片内容，路径均为面向模型的根内相对表示。
pub enum ShardContent {
    Files(Vec<(String, String)>),
    Lines {
        file: String,
        start: u64,
        end: u64,
        content: String,
    },
}

/// 装配用户消息：任务说明原文 + 数据集文件清单 + 分片内容与定位。
pub fn build_user_message(task: &str, listing: &[String], shard: &ShardContent) -> String {
    let mut msg = String::new();
    msg.push_str(task.trim_end_matches('\n'));
    msg.push_str("\n\n# 数据集文件清单\n");
    for f in listing {
        msg.push_str(f);
        msg.push('\n');
    }
    msg.push_str("\n# 你的分片\n");
    match shard {
        ShardContent::Files(files) => {
            for (i, (path, content)) in files.iter().enumerate() {
                if i > 0 {
                    msg.push('\n');
                }
                msg.push_str(&format!("## 文件 {path}\n"));
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
            msg.push_str(&format!("## 文件 {file} 第 {start}-{end} 行\n"));
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

    fn listing() -> Vec<String> {
        vec!["a.txt".into(), "dir/b.txt".into()]
    }

    #[test]
    fn shared_prefix_is_byte_identical_across_units() {
        let shard1 = ShardContent::Files(vec![("a.txt".into(), "内容甲".into())]);
        let shard2 = ShardContent::Lines {
            file: "dir/b.txt".into(),
            start: 3,
            end: 9,
            content: "内容乙".into(),
        };
        let m1 = build_user_message(TASK, &listing(), &shard1);
        let m2 = build_user_message(TASK, &listing(), &shard2);
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
        let m = build_user_message(TASK, &listing(), &shard);
        assert!(m.trim_end().ends_with("尾巴内容"), "分片内容在消息末尾");
    }

    #[test]
    fn listing_is_in_prefix() {
        let shard = ShardContent::Files(vec![("a.txt".into(), "x".into())]);
        let m = build_user_message(TASK, &listing(), &shard);
        let prefix = &m[..m.find("# 你的分片").unwrap()];
        assert!(prefix.contains("a.txt"));
        assert!(prefix.contains("dir/b.txt"));
    }

    #[test]
    fn lines_shard_carries_location() {
        let shard = ShardContent::Lines {
            file: "big.txt".into(),
            start: 100,
            end: 200,
            content: "切片".into(),
        };
        let m = build_user_message(TASK, &listing(), &shard);
        assert!(m.contains("第 100-200 行"), "行区间定位进入消息");
    }

    #[test]
    fn slash_path_uses_forward_slashes() {
        let p = Path::new("dir").join("sub").join("f.txt");
        assert_eq!(slash_path(&p), "dir/sub/f.txt");
    }
}
