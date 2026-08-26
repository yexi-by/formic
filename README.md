# Formic

Formic 是面向大量独立语义任务的 Rust CLI。调用方提供输入目录、JSONL 分片计划和共同任务说明；Formic 为每个单元运行一个独立的多轮 `LLM ↔ 工具` worker，并负责并发、限流、缓存、取消、上下文压缩、续跑、原子发布和运行档案。

它适合“同一套判断规则执行很多次”的工作，例如逐文件抽取、逐对象核验、结构化元数据补全、分类审阅和证据查证。

## 什么时候适合使用

一个 Formic 作业应满足：

- 每个单元有明确且有限的主动处理范围；
- 单元能够独立完成、独立重试和独立验收；
- 调整并发或完成顺序不会改变结果含义；
- worker 只需要共同任务、当前分片、只读证据和被允许的工具；
- 部分单元失败时，其他已发布结果仍然有用。

全局去重、统一命名、总排名、唯一编号分配和跨单元最终汇总，应由调用方在结果齐备后统一完成。详细判断方法见[任务设计](docs/task-design.md)。

## 主要能力

- 每个计划单元一个独立微型 agent，多轮调用模型与工具；
- Chat Completions、Responses、Anthropic Messages 三种协议；
- 文本与 JPEG、PNG、GIF、WebP 图片输入；
- 内置 `search`、`read`、`read_image`，以及任意 stdio 或 Streamable HTTP MCP；
- 显式 worker 输出权限：完全隔离或只读已发布结果；
- 文本结果或受 JSON Schema 校验的结构化结果；
- 供应商专有请求 JSON 透传；
- 工具缓存、共享请求门控、上下文预算与历史压缩；
- 中断后按不可变作业身份续跑，已发布结果不覆盖；
- 每个 worker 的模型输入、工具往返、状态与失败原因可审计。

Formic 不限制计划单元总量、对话总回合数或普通工具调用总数。配置中的并发值只限制当前活动工作。

## 快速开始

### 1. 构建

需要 Rust 1.88 或更高版本：

```bash
cargo build --release
```

### 2. 配置模型

复制 `config.example.toml` 为自己的 `config.toml`，填写服务地址、密钥、模型和真实上下文大小：

```toml
url = "https://api.example.com/v1"
api_key = ""
model = "model-name"
context_window_tokens = 131072

model_input_modalities = ["text"]
```

设置模型协议：

```text
FORMIC_LLM_PROTOCOL=completions
```

可选值为 `completions`、`responses`、`anthropic`。只有确认模型支持图片时，才把输入模态改为：

```toml
model_input_modalities = ["text", "image"]
```

供应商专有参数可以原样加入每次请求：

```toml
extra_body_json = '''{"temperature":0.2,"reasoning":{"effort":"high"}}'''
```

不要把真实密钥提交到 Git。

### 3. 准备作业

```text
job/
├─ data/
│  ├─ item-001.txt
│  └─ item-002.txt
├─ plan.jsonl
└─ task.md
```

`plan.jsonl` 一行一个单元：

```jsonl
{"unit":1,"files":["item-001.txt"]}
{"unit":2,"files":["item-002.txt"]}
```

`task.md` 说明每个 worker 的单元目标、范围、证据规则、未知情况、输出含义和完成条件。例如：

```markdown
只处理“你的分片”中的对象，抽取名称、日期和直接证据。
证据不足时明确写 unknown，不要猜测。
当前对象所有字段都有值或未知状态后立即提交。
```

### 4. 运行

```bash
formic run \
  --data job/data \
  --plan job/plan.jsonl \
  --task job/task.md \
  --out job/out \
  --worker-output-access none \
  --config config.toml
```

`--worker-output-access` 首次运行和续跑都必填：

- `none`：worker 只能读取冻结 input，彼此隔离；
- `published`：还可读取当时已经发布的数字编号结果。

独立任务优先使用 `none`。`published` 不能用于等待、认领、去重或推断全局完成状态，因为可见结果受并发完成顺序影响。

### 5. 可选结构化输出

```bash
formic run \
  --data job/data \
  --plan job/plan.jsonl \
  --task job/task.md \
  --out job/out \
  --worker-output-access none \
  --config config.toml \
  --output-schema result.schema.json
```

Formic 会让模型通过内部提交工具交付 object，并在本地通过 schema 校验后发布 JSON。schema 负责形状，任务说明仍负责字段业务含义。

### 6. 续跑

中断或部分失败后，使用完全相同的作业输入和权限增加 `--resume`：

```bash
formic run \
  --data job/data \
  --plan job/plan.jsonl \
  --task job/task.md \
  --out job/out \
  --worker-output-access none \
  --config config.toml \
  --resume
```

续跑只处理 failed、stopped 和未开始单元。plan、task、schema、完整 input、输出权限或模型输入模态变化时，会在任何 MCP/LLM 请求前拒绝续跑。

## 图片和工具

声明 `image` 后，`files` 分片可以混合文字和支持图片，worker 也会获得只读取冻结 input 的 `read_image`。行区间只支持 UTF-8 文本。

图片保持原始字节，不下载 URL，不自动缩放、转码或截断。实际请求中的 data URL/base64 不进入日志和公开档案。MCP 返回的图片会原样保存到当前 run 的 `media/`。

内置文字工具可以检索完整 input；启用 `published` 后才可读取已发布结果。远端搜索、浏览器或其他能力由操作者通过 MCP 配置，Formic 不硬编码具体产品。

## 输出

```text
out/
├─ results/
│  ├─ 1.md 或 1.json
│  └─ output-schema.json           # 仅结构化模式
└─ runs/
   └─ run-000001/
      ├─ workers/1.md              # 完整 worker 运行档案
      ├─ media/1/1.png             # 仅 MCP 图片
      ├─ stats.jsonl
      └─ summary.json
```

每次运行创建新的 `run-N`。结果先写临时文件，再原子发布；已存在的完成记录不会覆盖。

退出码：

| 代码 | 含义 |
| --- | --- |
| `0` | 本次作业完整 |
| `1` | 存在失败、停止或未开始单元 |
| `2` | 启动配置、输入或续跑现场无效 |
| `3` | 收到终止信号 |

## 文档

- [文档索引](docs/README.md)
- [详细使用说明](docs/usage.md)
- [任务设计与提示词](docs/task-design.md)
- [可观测性与排错](docs/observability.md)
- [项目地图](docs/project-map.md)
- [维护指南](docs/maintenance.md)

## 开发验证

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo +1.88.0 check --all-targets
```

并发、内存、缓存或调度变化还应运行 release 规模实验。完整维护流程见[维护指南](docs/maintenance.md)。

## 开源协议

[GNU Affero General Public License v3.0](LICENSE)。Copyright (C) 2026 yexi。
