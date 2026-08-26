# Formic 可观测性与排错

Formic 为每个实际启动的 worker 生成事后运行档案，用来回答：模型收到了什么、调用了哪些工具、为什么进入下一步，以及最终为何发布、失败或停止。

档案只记录 Formic 能观察到的控制流和输入输出，不声称还原模型不可见的内部思维。

## 1. 输出布局

```text
out/
├─ results/
│  ├─ 1.md                         # 文本完成记录
│  └─ output-schema.json           # 仅结构化模式
└─ runs/
   └─ run-000001/
      ├─ workers/
      │  ├─ 1.md                   # Worker 1 运行档案
      │  └─ 2.md
      ├─ media/
      │  └─ 1/1.png                # MCP 返回的原始图片
      ├─ stats.jsonl
      └─ summary.json
```

结构化模式的完成记录为 `results/<unit>.json`。每次运行创建新的自然序号 `run-N`；续跑不会覆盖旧档案。

worker ID 就是计划中的 `unit`。运行序号把多次续跑的现场分开，因此旧档案不会指向新一轮媒体或统计。

## 2. Worker 档案包含什么

每份 `workers/<unit>.md` 包含：

- 分片、开始和结束时间、耗时与最终结局；
- 冻结的模型协议、模型名、输入模态、上下文、输出权限、并发和工具目录；
- 回合、请求、重试、token、缓存、工具和压缩统计；
- 按自然序号排列的状态时间线；
- 协议无关的模型输入；
- 通过协议和回合验收后的助手正文与完成类别；
- 工具名称、来源、参数、结果、等待和执行时间；
- 结构化校验位置、上下文预算与压缩结果；
- 当前 worker 的直接失败或停止原因。

大段输入、助手正文和工具结果放在折叠区。普通调用和压缩调用分别保存第一份完整输入，后续输入保存可逆字节增量，避免每轮重复相同前缀。

## 3. 哪些内容不会记录

以下内容不会进入终端、stats 或 worker 档案：

- LLM API key、MCP bearer、Authorization 和秘密 header；
- 实际请求 URL；
- HTTP 错误正文；
- SSE envelope、残帧和无效协议 payload；
- reqwest 或传输库原始错误文字；
- 图片 data URL 和 base64；
- Responses 的 opaque/encrypted replay payload。

本地图片只记录冻结 input 相对路径、MIME、原始字节数、尺寸和视觉 token 估算。MCP 图片另存到当前 run 的 `media/`，档案只链接相对路径。

档案仍可能包含任务正文、数据分片、模型输出和工具结果。共享前应按业务数据的保密要求检查，而不能因为没有 API key 就视为可公开。

## 4. Worker 状态

| 状态 | 含义 |
| --- | --- |
| `preparing` | 正在读取分片并构造首条用户消息 |
| `ready` | 当前历史可以进入下一轮 |
| `requesting_model` | 请求已构造，正在等待模型 |
| `retrying_model` | 可重试错误已分类，等待下一次网络尝试 |
| `interpreting_model` | 响应流结束，正在判断正文或工具调用 |
| `compacting_context` | 预计越界或收到明确上下文超限，正在压缩 |
| `waiting_for_tool` | 工具已进入 Scheduler，等待准入或结果 |
| `correcting_tool_call` | 工具参数无效，错误将回注模型 |
| `correcting_output` | 结构化提交无效，校验原因将回注模型 |
| `ready_to_publish` | 结果满足契约，等待原子发布 |
| `stopped` | 收到取消或全局停发，未发布结果被丢弃 |
| `failed` | worker 已确定无法继续 |

状态之外还有事实事件，例如 `context_budget`、`retry`、`tool_execution`、`tool_media`、`output_validation` 和 `context_compaction`。状态说明“正在做什么”，事实事件说明“什么条件导致了这个状态”。

## 5. 生成与失败语义

运行中先写 `workers/.tmp-worker-<unit>.jsonl`。模型输入增量使用另一个仅保存上一份同类输入的临时基准文件。

worker 结束后：

1. 删除输入基准；
2. 逐行校验临时审计；
3. 渲染临时 Markdown；
4. 同目录原子发布 `<unit>.md`；
5. 删除已经被 Markdown 完整承接的 JSONL。

Markdown 生成失败时，终端会明确报告 worker，临时 JSONL 保留以便抢救现场。成功、普通失败和停止都使用同一渲染入口。

当前契约要求审计完整后才允许发布完成记录。`stats.jsonl` 和进程 metrics 是派生证据；它们写入失败不能把已确定的业务结果改成另一种结局。

## 6. 作业统计

`stats.jsonl` 每个实际启动单元一行，包含：

- 模型回合和真实调用数；
- 网络重试和结构修正；
- 本地 input/output token 估算；
- 供应商报告的 usage；
- 工具计数、缓存命中/合并/淘汰；
- 工具等待与执行时间；
- MCP 当前与峰值在途数；
- 上下文压缩前后 token。

供应商没有报告 usage 时保持缺失，不使用本地估算冒充计费事实。

`summary.json` 汇总计划和本轮结局，满足：

```text
planned = already_completed + started + not_started
started = published + failed + stopped
llm_calls = llm_calls_with_provider_usage + llm_calls_without_provider_usage
```

终端只展示失败总数、首个未完成单元和有限样例；逐单元详细原因在 worker 档案中。

## 7. 推荐排错顺序

### 7.1 作业启动失败

先读终端。启动失败通常没有 worker 档案，常见原因包括：

- 配置缺失或字段冲突；
- 计划、任务或 schema 无效；
- input 与 output 重叠；
- 输出目录已被其他作业占用；
- 续跑身份变化或结果现场损坏；
- MCP initialize 或工具目录冻结失败。

启动错误退出码为 2。确认问题发生在 MCP/LLM 请求前，不要用重试掩盖输入错误。

### 7.2 单个 worker 失败

打开最新 run 中对应的 `workers/<unit>.md`，按顺序检查：

1. 最终结局与直接原因；
2. 首条模型输入是否包含正确分片；
3. 上下文预算是否在第一次请求前已超限；
4. 模型完成类别是否与正文或工具调用一致；
5. 工具参数、权限和结果是否满足预期；
6. 是否发生网络重试、共享停发或重复调用停滞；
7. 结构化提交在哪个 instance/schema path 失败；
8. 上下文压缩是否真正减小并回到预算。

### 7.3 结果内容偏离

档案可以检查任务设计：

1. 当前分片是否被写成唯一主动范围；
2. 跨分片或联网查证是否由当前对象触发；
3. worker 是否读取 output 做协调或去重；
4. 工具是否在满足证据后停止；
5. 最终记录是否只包含当前单元能负责的事实；
6. 工具错误、截断或压缩是否改变了模型可见证据。

任务设计方法见[任务设计](task-design.md)。

### 7.4 续跑失败

根据终端指出的对象检查 plan、task、schema、完整 input、输出权限、输入模态、结果文件和追加状态。不要删除单个状态文件或覆盖已发布结果来强行继续；无法恢复一致现场时使用新的输出目录重新运行。

## 8. 进程级 metrics

设置 `FORMIC_METRICS=1` 后，Formic 每 250 毫秒向 stderr 输出一行：

```text
metrics rss_mb=... llm_in_flight=... tool_inflight=... history_kb=... search_avg_ms=... search_max_ms=... done=... failed=... cancelled=...
```

这些值只记录当前进程事实，不参与并发准入、恢复、缓存、发布或成功判断。

字段含义：

| 字段 | 含义 |
| --- | --- |
| `rss_mb` | 进程工作集内存 |
| `llm_in_flight` | 已进入模型调用且尚未结束的数量 |
| `tool_inflight` | Scheduler 已收未复的工具数量 |
| `history_kb` | 活动 worker 对话历史原始字节总量 |
| `search_avg_ms` | 本轮进程内 search 平均耗时 |
| `search_max_ms` | search 最大耗时 |
| `done` | 已完成单元数 |
| `failed` | 已失败单元数 |
| `cancelled` | 已取消单元数 |

## 9. `scale-metrics.csv`

运行规模实验：

```bash
cargo run --release --example scale_run -- 5000 1000 8 20
```

实验会解析 stderr metrics，并在当前目录生成 `scale-metrics.csv`。CSV 保存采样序号、RSS、历史大小、模型/工具在途数和单元结局，用于画内存与吞吐曲线。

第一列目前名为 `second`，实际写入的是从 1 开始的采样序号；每个样本约间隔 250 毫秒，不能把它直接当成真实秒数。

该文件被 Git 忽略，正常运行、续跑和结果发布都不会读取它，可以安全删除；下次规模实验会重新生成。
