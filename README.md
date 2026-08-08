# Formic（蚁群）

> 面向大量独立语义任务的轻量 LLM 批处理运行时：文件输入，记录输出。

Formic 是一个 Rust CLI。它按调用方提供的计划读取相互独立的数据分片，为每个分片
启动一次性、只读的 LLM worker，并负责并发、重试、取消、结果发布和调用审计。作业
通过数据、分片计划和自然语言任务说明定义，不需要接入 SDK，也不需要运行常驻服务。

当前项目是可运行原型，已经完成三种 LLM API 协议、多轮工具调用、并发窗口、失败重试、
优雅终止、逐次调用审计和逐单元统计。5000 个单元、1000 并发窗口的 mock 全流程实验
及其测量结果记录在[第 5 轮档案](docs/rounds/round-05.md)中。

## Formic 解决什么问题

有一类批量工作无法用固定脚本可靠完成，因为每个数据单元都需要理解上下文并作出判断，
例如：

- 从长篇文本的每个章节中提取实体、术语或事实；
- 逐条分类、审核或解释大量非结构化记录；
- 在大批页面或文档中判断目标信息是否存在，并给出依据；
- 对彼此独立的数据分片执行相同的研究或分析任务。

直接循环调用模型只能发出请求，不能自然地处理多轮工具调用、并发窗口、失败隔离、原子
输出和完整审计。通用交互 Agent 的 subagent 通常还会继承完整工具集、交互规则和宿主
选择的模型；当任务数量增大时，这些能力会增加不必要的资源、权限和调度成本。

Formic 把这类工作收敛为一个明确契约：

| 批处理中的问题 | Formic 的处理方式 |
| --- | --- |
| 单元需要自主判断，不能预先写死步骤 | 每个单元运行独立的多轮 `LLM ↔ search` 会话 |
| 合法任务很多，但同时请求数受模型配额限制 | 只限制活动窗口，窗口外单元等待，不限制任务总量 |
| 单个单元失败不应丢失其他进展 | 每个单元独立结算，已完成结果立即发布 |
| 中断时不能留下半截结果 | 结果先写临时文件，再原子发布为 `<单元号>.md` |
| 模型行为需要检查和复现 | 完整记录每次请求、响应、工具调用和重试次数 |
| 调用方需要比较成本与行为 | 逐单元输出回合数、调用数、工具计数和 token 估算 |

## 适用范围

Formic 适合满足以下条件的作业：

- 工作可以拆成大量相互独立的单元；
- 每个单元需要语义理解，而不是单纯的数据转换；
- worker 只需读取输入数据和已完成结果；
- 调用方需要保留可检查的独立产出，而不是只要一个聚合答案。

以下情况不属于当前内核的目标：

- 单元之间需要频繁同步或共同修改状态；
- worker 需要 shell、写文件或任意代码执行能力；
- 只有一个短任务，普通单次模型调用已经足够；
- 需要 Formic 自动决定如何分片、汇总结果或跨运行续跑。

## 工作方式

```text
数据目录 + plan.jsonl + task.md
               │
               ▼
       有界并发的一次性 worker
          │       │       │
          └── LLM ↔ search ──┘
               │
               ├── <单元号>.md       最终产出
               ├── audit/*.jsonl     完整调用记录
               └── stats.jsonl       逐单元统计
```

运行时负责：

- 按计划顺序接纳单元，并在调用方指定的并发窗口内执行；
- 为每个单元维持独立的多轮 LLM 会话；
- 提供只读 `search` 工具，允许检索输入根和已完成输出根；
- 对可重试故障使用有界重试，隔离单元失败；
- 原子发布结果，并完整记录模型请求与原始响应；
- 收到 Ctrl+C 或 Ctrl+Break 后停止接纳新单元，取消在途调用并保留已发布结果。

调用方负责：

- 生成分片计划；
- 在任务说明中定义判断标准和输出格式；
- 汇总各单元结果；
- 跨运行续跑：读取已有 `<单元号>.md`，剔除已完成单元后生成新计划。

## 快速开始

### 1. 构建

需要 Rust 1.85 或更高版本。

```bash
cargo build --release
```

生成的程序位于 `target/release/formic`；Windows 下为
`target/release/formic.exe`。

### 2. 使用本地 mock LLM 跑通示例

先在第一个终端启动仓库自带的 mock 服务：

```bash
cargo run --example mock_llm -- 18080
```

在第二个 PowerShell 终端运行示例作业：

```powershell
$env:FORMIC_LLM_PROTOCOL = "completions"
$env:FORMIC_LLM_BASE_URL = "http://127.0.0.1:18080/v1"
$env:FORMIC_LLM_MODEL = "demo-model"

cargo run -- run `
  --data examples/demo/data `
  --plan examples/demo/plan.jsonl `
  --task examples/demo/task.md `
  --out tmp/demo-out `
  --concurrency 2
```

Linux 或 macOS 可使用：

```bash
FORMIC_LLM_PROTOCOL=completions \
FORMIC_LLM_BASE_URL=http://127.0.0.1:18080/v1 \
FORMIC_LLM_MODEL=demo-model \
cargo run -- run \
  --data examples/demo/data \
  --plan examples/demo/plan.jsonl \
  --task examples/demo/task.md \
  --out tmp/demo-out \
  --concurrency 2
```

## 生产调用

```bash
formic run \
  --data <数据集目录> \
  --plan <plan.jsonl> \
  --task <task.md> \
  --out <输出目录> \
  --concurrency <同时运行的单元数>
```

`--concurrency` 是必填项。它只控制同时运行的单元数，应依据 LLM 供应商配额和机器
资源设置；超出的单元等待，不会被判为容量错误。

### LLM 配置

Formic 启动时读取当前工作目录下的 `config.toml`。复制
[`config.example.toml`](config.example.toml) 后填写：

```toml
url = "https://api.openai.com/v1"
api_key = "sk-..."
model = "gpt-5"
```

`url` 和 `model` 必填，`api_key` 可留空。`config.toml` 中的 API key 是明文，文件已被
`.gitignore` 排除；不要强制提交它，也不要把 key 写入任务文件或计划文件。

环境变量仍然可用。非空环境变量按字段覆盖 `config.toml`，因此可以只覆盖其中一项：

| 环境变量 | 必填 | 含义 |
| --- | --- | --- |
| `FORMIC_LLM_PROTOCOL` | 是 | API 协议形状：`completions`、`responses` 或 `anthropic`；只从环境变量读取 |
| `FORMIC_LLM_BASE_URL` | 否 | 覆盖 `config.toml` 的 `url` |
| `FORMIC_LLM_MODEL` | 否 | 覆盖 `config.toml` 的 `model` |
| `FORMIC_LLM_API_KEY` | 否 | 覆盖 `config.toml` 的 `api_key` |
| `FORMIC_METRICS` | 否 | 设为 `1` 时，每 250 ms 向 stderr 输出运行指标 |

没有 `config.toml` 时，原有的纯环境变量配置仍然有效。缺少必填配置时，Formic 会在读取
作业前明确失败。

### 分片计划

计划文件采用 JSONL，一行一个单元。`unit` 是从 1 开始、在当前计划中唯一的自然编号。

按文件分配：

```json
{"unit":1,"files":["chapter-01.txt","notes/context.txt"]}
```

按行区间分配（1 起始、双端闭区间）：

```json
{"unit":2,"file":"records.jsonl","start":100,"end":199}
```

任务说明是 UTF-8 文本。Formic 将其原样放入每个 worker 的输入；判断规则、输出结构和
无法判断时的处理方法都应由调用方在这里写清楚。

### 输出契约

| 路径 | 内容 | 业务含义 |
| --- | --- | --- |
| `out/<单元号>.md` | worker 的最终消息 | 单元完成的权威记录；失败单元没有该文件 |
| `out/audit/<单元号>.jsonl` | 每次请求与原始响应 | 调试和复现依据，包含完整上下文 |
| `out/stats.jsonl` | 每行一个单元的统计 | 便于分析结果、重试、工具调用和 token 估算 |

`stats.jsonl` 示例：

```json
{"unit":1,"outcome":"published","turns":2,"llm_calls":2,"retries":0,"tool_calls":{"search":1},"input_tokens_est":4025,"output_tokens_est":201}
```

token 使用 `o200k` BPE 在本地估算，用于预算和作业分析，不是供应商计费数据。审计文件
会包含任务内容、数据分片、完整会话和模型原始输出；共享前请先检查其中是否有敏感信息。

### 退出状态

| 退出码 | 含义 |
| --- | --- |
| `0` | 所有单元成功 |
| `1` | 至少一个单元失败，其余已完成结果仍保留 |
| `2` | 作业未启动，例如参数、输入或环境配置无效 |
| `3` | 收到终止信号，已发布结果保留 |

第一次按 Ctrl+C 或 Ctrl+Break 会执行优雅终止；再次按下会立即退出。跨运行续跑由调用方
根据输出目录中的单元编号计算差集。

## 规模实验

仓库提供 mock 驱动的全流程实验，用于观察 worker 历史、LLM 在途请求、调度器队列、
search 耗时和进程内存：

```bash
cargo build
cargo run --example scale_run -- 5000 1000 8 20
```

参数依次为单元数、并发窗口、每单元工具调用回合数和 mock 延迟毫秒。实验会在当前目录
生成 `scale-metrics.csv`；已记录的基准和分析见
[docs/rounds/round-05.md](docs/rounds/round-05.md)。

## 文档

- [设计文档](docs/design.md)：当前职责边界、数据契约和实现依据；
- [模块拓扑](docs/topology.html)：已验证模块与候选模块的关系；
- [实现档案](docs/rounds/)：每轮目标、实测结果和验证证据。

## 反馈问题

请从 [GitHub Issue 模板](https://github.com/yexi-by/formic/issues/new/choose)中选择最合适的
类型：运行缺陷、功能建议或规模与性能问题。运行缺陷和性能问题应附上最小复现、退出码、
stderr 以及经过删减的审计或统计信息；不要提交 API key、完整私有数据或未处理的模型
上下文。

## 开源协议

[GNU Affero General Public License v3.0](LICENSE)。Copyright (C) 2026 yexi。
