//! 统一调度器：工具调用的唯一入口。本轮形态：mpsc 收件箱串行准入，
//! 执行 spawn 到有界 blocking 池（按 CPU 核数），oneshot 回执。
//! 限流记账与跨 worker 去重缓存随并发轮（第 3 轮）进入——顺序世界没有竞争。

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::llm::ToolSpec;
use crate::tools::{self, Roots};

/// 调度器句柄：worker 经它发起一切工具调用。
pub struct Scheduler {
    inbox: mpsc::UnboundedSender<Request>,
}

/// 调度器通道断裂（调度器任务已死）——运行时故障，单元无法继续。
#[derive(thiserror::Error, Debug)]
#[error("调度器不可用")]
pub struct SchedulerGone;

struct Request {
    name: String,
    arguments: Value,
    reply: oneshot::Sender<String>,
}

impl Scheduler {
    /// 启动调度器任务。roots 是两棵只读根；工具命名空间由构造固定（只有 search）。
    pub fn start(roots: Roots) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Request>();
        let roots = Arc::new(roots);
        let permits = Arc::new(Semaphore::new(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        ));
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let permit = Arc::clone(&permits)
                    .acquire_owned()
                    .await
                    .expect("信号量随调度器同生命周期");
                let roots = Arc::clone(&roots);
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let result = tools::execute(&roots, &req.name, &req.arguments);
                    let _ = req.reply.send(result);
                });
            }
        });
        Self { inbox: tx }
    }

    /// 工具规格集：发给模型的 tools 字段的唯一来源。
    pub fn specs(&self) -> Vec<ToolSpec> {
        tools::registered_specs()
    }

    /// 执行一次工具调用，返回模型可读的结果文本（工具级错误以 `错误：` 文本携带）。
    pub async fn execute(&self, name: &str, arguments: Value) -> Result<String, SchedulerGone> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(Request {
                name: name.to_string(),
                arguments,
                reply: tx,
            })
            .map_err(|_| SchedulerGone)?;
        rx.await.map_err(|_| SchedulerGone)
    }
}
