//! 单单元执行：读分片 → 装配 prompt → 调用 LLM → 发布产出或给出诊断。
//! 第一轮为单轮调用：模型返回工具调用即判单元失败（原型未配置工具）。

use std::fs;
use std::path::{Path, PathBuf};

use crate::llm::{CallInput, Finish, LlmClient, LlmError, LlmEvent};
use crate::output;
use crate::plan::{PlanUnit, Shard};
use crate::prompt::{self, ShardContent};

/// 一个作业的运行上下文：全部单元共享的只读事实与依赖。
pub struct JobContext {
    pub data_root: PathBuf,
    pub task: String,
    pub listing: Vec<String>,
    pub llm: LlmClient,
    pub out_dir: PathBuf,
}

/// 单元失败：带对象与直接原因，细节留在审计文件。
#[derive(thiserror::Error, Debug)]
pub enum UnitFailure {
    #[error("读取分片文件 {path} 失败：{source}")]
    ReadShard {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("分片文件 {path} 不是合法 UTF-8，无法装配进 prompt")]
    ShardEncoding { path: PathBuf },
    #[error(transparent)]
    Llm(#[from] LlmError),
    #[error("模型请求工具调用，但原型未配置任何工具")]
    ToolCallUnsupported,
    #[error("模型输出达到协议长度上限，产出被截断")]
    Truncated,
    #[error("写入输出区失败：{0}")]
    Output(#[from] std::io::Error),
}

/// 执行一个单元：成功则产出已发布；失败不留下完成记录。
pub async fn run_unit(ctx: &JobContext, unit: &PlanUnit) -> Result<(), UnitFailure> {
    let shard = read_shard(&ctx.data_root, &unit.shard)?;
    let user = prompt::build_user_message(&ctx.task, &ctx.listing, &shard);
    let mut call = ctx
        .llm
        .call(&CallInput {
            instructions: prompt::INSTRUCTIONS,
            user: &user,
        })
        .await?;

    let mut text = String::new();
    let mut finish = None;
    let mut tool_call = false;
    while let Some(event) = call.next_event().await? {
        match event {
            LlmEvent::TextDelta(delta) => text.push_str(&delta),
            LlmEvent::ToolCall => {
                tool_call = true;
                break;
            }
            LlmEvent::Finished(f) => {
                finish = Some(f);
                break;
            }
        }
    }

    // 证据完整是契约要求：审计写不进去时单元不算完成，由调用方续跑重做。
    output::write_audit(&ctx.out_dir, unit.unit, &call.request_body, call.raw_log())?;

    if tool_call {
        return Err(UnitFailure::ToolCallUnsupported);
    }
    match finish {
        Some(Finish::Stop) => {
            output::publish(&ctx.out_dir, unit.unit, &text)?;
            Ok(())
        }
        Some(Finish::MaxTokens) => Err(UnitFailure::Truncated),
        None => Err(UnitFailure::Llm(LlmError::protocol(
            "流结束但没有收到完成事件",
            "",
        ))),
    }
}

fn read_shard(root: &Path, shard: &Shard) -> Result<ShardContent, UnitFailure> {
    match shard {
        Shard::Files(files) => {
            let mut contents = Vec::with_capacity(files.len());
            for f in files {
                contents.push((prompt::slash_path(f), read_utf8(root, f)?));
            }
            Ok(ShardContent::Files(contents))
        }
        Shard::Lines { file, start, end } => {
            let text = read_utf8(root, file)?;
            let lines: Vec<&str> = text.lines().collect();
            let from = (*start as usize).saturating_sub(1);
            let to = (*end as usize).min(lines.len());
            Ok(ShardContent::Lines {
                file: prompt::slash_path(file),
                start: *start,
                end: *end,
                content: lines[from..to].join("\n"),
            })
        }
    }
}

fn read_utf8(root: &Path, rel: &Path) -> Result<String, UnitFailure> {
    let bytes = fs::read(root.join(rel)).map_err(|e| UnitFailure::ReadShard {
        path: rel.to_path_buf(),
        source: e,
    })?;
    String::from_utf8(bytes).map_err(|_| UnitFailure::ShardEncoding {
        path: rel.to_path_buf(),
    })
}

/// 递归列出数据根内全部文件，返回排序后的根内相对表示（`/` 分隔）。
pub fn list_files(root: &Path) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                let rel = path.strip_prefix(root).expect("遍历结果必在根内");
                out.push(prompt::slash_path(rel));
            }
        }
    }
    out.sort();
    Ok(out)
}
