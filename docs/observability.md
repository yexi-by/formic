# Worker 可观测性

Formic 为每个实际启动的 worker 生成一份事后运行档案，用来回答“当时处于什么
状态、依据什么条件进入下一步、最终为什么成功或失败”。它记录 Formic 能观察到的
控制流和模型输入输出，不声称还原模型不可见的内部思维。

## 文件布局

每次 `formic run` 在输出区创建一个自然递增的运行目录：

```text
out/
├─ results/
│  └─ 1.md                         # 文本模式的已发布结果
└─ runs/
   └─ run-000001/
      ├─ workers/
      │  ├─ 1.md                   # Worker 1 运行档案
      │  └─ 2.md                   # Worker 2 运行档案
      ├─ stats.jsonl
      └─ summary.json
```

结构化输出模式的完成记录是 `results/<unit>.json`，并另有
`results/output-schema.json`；worker 档案仍为 Markdown。每轮只创建一个新的 `run-N`，
已有运行档案和结果都不会覆盖。

worker ID 就是计划中的自然 `unit` 编号。自然运行序号把 resume 各轮隔开，因此复用
同一输出目录不会让旧档案引用新任务的现场。

## 档案内容

每份档案包含：

- 任务时间、worker ID、分片、开始/结束时间、耗时和最终结局；
- 当次任务冻结的模型协议、模型名、上下文窗口、输出预算、并发窗口和工具目录；
- 回合、重试、token、缓存、工具耗时、MCP 在途峰值和压缩统计；
- 带自然序号及相对毫秒时间的状态时间线；
- 可逐字重建的协议无关 LLM 输入、通过协议与回合验收后的助手正文、完成类别、工具调用数量、
  工具参数与结果；
- 上下文预算决定、压缩前后 token、结构化校验位置和失败原因。

大段请求、解析后的助手正文和工具结果放在 Markdown 的折叠区。折叠只影响阅读方式，不
截断这些允许公开的证据。普通调用和上下文压缩调用各自保存第一份协议无关输入；后续同类
输入只保存相对上一份的可逆字节增量。增量包含保留的前缀字节数、删除字节数和插入原文，
按“上一份输入前缀 + 插入原文 + 上一份输入剩余后缀”即可逐字重建。这样不会在每一轮重复
抄写相同的 instructions、历史和工具 schema。实际 HTTP body、URL 和 header 不进入档案；
Responses 的 opaque/encrypted replay item 只记录数量，不保存 payload。

一次调用只有在协议流完整结束、完成类别与回合内容一致后，才写一个 `model_response` 事实。
它只含完成类别、解析后的助手正文和工具调用数量。供应商 SSE envelope、残帧、超限块、
错误正文和无效协议 payload 只在内存中完成有界解析，随后丢弃；超时、取消、传输错误、
流量超限和协议错误路径都不会把收到的原始字节写入 worker 档案。
档案可能包含任务正文、数据分片和模型输出，共享前应按业务数据的保密要求处理。
HTTP Authorization、LLM API key、MCP bearer 和由环境变量注入的秘密 header 不进入档案。
LLM HTTP 错误正文、无效协议 payload、请求 URL 和 reqwest 原始错误文字也不进入档案、
stats 或终端。错误只保留 HTTP 状态、公开类别、allowlist 中的 provider code、
`Retry-After` 和不含原始值的结构原因。

## 用档案检查任务设计

运行档案可以确认 `task.md` 是否符合独立单元条件。排查结果偏离时，依次检查：

1. 首条用户消息是否把当前分片写成唯一的主动处理范围；
2. 跨分片或远端检索是否由分片内的具体候选触发，并在取得规定证据后停止；
3. worker 是否读取 `output` 来等待、去重或推断全局状态；
4. 最终记录是否只包含当前单元能够负责的事实，把全局命名、排序和汇总留给调用方；
5. 工具错误、截断、结构校验或上下文压缩是否改变了模型原本可见的证据。

提示词的职责划分和检查清单见[任务设计与提示词](task-design.md)。档案只能呈现模型实际收到
和返回的内容，不能证明模型使用了某段不可见的内部推理。

## 状态语义

当前状态集合由 worker 主循环直接写入，渲染器不解析日志文字猜测：

| 状态 | 含义 |
| --- | --- |
| `preparing` | 读取分片并构造首条用户消息 |
| `ready` | 当前历史已经可以继续下一轮 |
| `requesting_model` | 已计算本次请求并等待模型 |
| `retrying_model` | 可重试错误已经分类，正在等待下一次尝试 |
| `interpreting_model` | 响应流结束，正在判断最终文本或工具调用 |
| `compacting_context` | 请求预计超出安全预算或触发一次紧急压缩 |
| `waiting_for_tool` | 普通工具已进入唯一 Scheduler，等待准入或执行 |
| `correcting_tool_call` | 工具参数无效，错误结果将回注模型 |
| `correcting_output` | 结构化提交无效，校验原因将回注模型 |
| `ready_to_publish` | 最终结果满足当前输出契约，等待原子发布 |
| `stopped` | 收到取消或全局停发信号，未发布的结果被丢弃 |
| `failed` | worker 已确定无法继续，并保存直接原因 |

状态事件之外还有独立事实事件，例如 `context_budget`、`retry`、`tool_execution`、
`output_validation` 和 `context_compaction`。这样可以同时看到状态和触发该状态的数值证据。

## 生成与失败语义

运行中先向 `runs/run-N/workers/` 写 `.tmp-worker-<unit>.jsonl`，避免把不断增长的允许审计事实保存在内存。
计算模型输入增量时，运行时另用一个只含上一份同类输入的临时基准文件；它不进入最终档案，
也不会把全部历史输入重新放进内存。worker 结束后先删除输入基准，再由渲染器逐行读取
JSONL 并写临时 Markdown，通过同目录原子替换发布 `<unit>.md`。Markdown 发布成功后
立即删除 JSONL，最终不保留重复证据或模型输入基准。

如果 Markdown 无法生成，worker 已经确定的业务结局不被日志系统改写；stderr 会明确
报告对应 worker，临时 JSONL 保留以便抢救现场。正常成功、失败和停止路径都不会残留
JSONL 或临时 Markdown。

`stats.jsonl` 每个实际启动的单元一行。`summary.json` 保存 planned、already_completed、
started、published、failed、stopped、not_started、自然顺序样例、按原因计数，以及真实
LLM 调用的 provider usage 覆盖。三条恒等式为：

```text
planned = already_completed + started + not_started
started = published + failed + stopped
llm_calls = llm_calls_with_provider_usage + llm_calls_without_provider_usage
```

终端进度使用 stderr 普通换行，只在整数百分比变化时输出；成功终态才显示 100%。失败、
停止或取消不补写 100%，每阶段最多 101 行。逐单元失败细节留在 worker 档案，终端只汇总
原因、总数、首个未完成单元和最多五个样例。
