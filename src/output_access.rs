use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Worker 是否可以读取同一作业已经发布的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum WorkerOutputAccess {
    None,
    Published,
}

impl WorkerOutputAccess {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Published => "published",
        }
    }

    pub const fn allows_published(self) -> bool {
        matches!(self, Self::Published)
    }
}
