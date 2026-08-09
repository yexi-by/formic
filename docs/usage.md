# Formic 使用说明

## 构建与运行

需要 Rust 1.88 或更高版本。

```bash
cargo build --release
```

```bash
formic run \
  --data <数据集目录> \
  --plan <plan.jsonl> \
  --task <task.md> \
  --out <输出目录> \
  --concurrency <同时活动的单元数>
```

结构化输出作业额外传入一份 JSON Schema：

```bash
formic run \
  --data data \
  --plan plan.jsonl \
  --task task.md \
  --out out \
  --concurrency 32 \
  --output-schema result.schema.json
```

`--output-schema` 是本次作业的输入，不是部署配置。Formic 不提供指定配置文件路径的参数。

## 调用前提

计划中的每一行不是一个可以与其他 worker 协商的角色，而是一项能够独立完成的工作。
单元应当能只依靠共同任务说明、当前分片、只读输入和按需调用的工具得出自己的结果；它的
正确性不能依赖其他单元是否已经完成、先后顺序或共享可变状态。

适合的工作包括按统一规则进行的大批量抽取、分类、核验、审阅和结构转换。全局去重、统一
命名、总排名、跨单元最终报告以及前后步骤互相依赖的流程，需要由调用方在 Formic 作业之外
负责，或先重新划分成真正互不依赖的新单元。确定计划和编写 `task.md` 前，先阅读
[任务设计与提示词](task-design.md)。

## 配置

Formic 只在进程启动时读取当前工作目录的 `config.toml`，不搜索父目录、不接受其他路径，
也不热加载。复制 [`config.example.toml`](../config.example.toml) 后修改即可。`config.toml`
可保存明文 API key，仓库已通过 `.gitignore` 排除它；不要把真实密钥提交到版本库。

LLM 的最小配置：

```toml
url = "https://api.example.com/v1"
api_key = ""
model = "model-name"
context_window_tokens = 131072
```

`context_window_tokens` 是本地预算依据，不会发送给供应商。协议形状由
`FORMIC_LLM_PROTOCOL` 指定，可选 `completions`、`responses` 或 `anthropic`。
Anthropic Messages 还必须单独配置 `anthropic_max_tokens`；该字段只用于 Anthropic，
其他协议出现它会直接报错。

Completions 与 Responses 请求不发送 temperature、top_p、任何输出 token 上限、reasoning、
verbosity、seed、stop、penalty、tool_choice 等生成控制字段。Anthropic 除协议必填的
`max_tokens` 外也不发送这些字段。

下列非空环境变量按字段覆盖 `config.toml`：

| 环境变量 | 含义 |
| --- | --- |
| `FORMIC_LLM_PROTOCOL` | 必填；选择 API 协议形状 |
| `FORMIC_LLM_BASE_URL` | 覆盖 `url` |
| `FORMIC_LLM_API_KEY` | 覆盖 `api_key` |
| `FORMIC_LLM_MODEL` | 覆盖 `model` |
| `FORMIC_LLM_CONTEXT_WINDOW_TOKENS` | 覆盖模型上下文大小 |
| `FORMIC_ANTHROPIC_MAX_TOKENS` | 仅 Anthropic Messages；覆盖其必填 `max_tokens` |
| `FORMIC_METRICS=1` | 每 250 ms 向 stderr 输出进程级观测值 |

配置采用严格字段校验：未知字段、`0`、互斥传输字段或缺失的必填项都会在读取作业前
报错。完整字段和注释见配置示例。

未填写资源字段时，正式默认值面向大规模作业：内置工具和每个 MCP server 最多同时执行
64 次，单次工具结果为 1 MiB，搜索最多返回 1000 个匹配及 100 行上下文，作业内存缓存为
1 GiB，连续相同调用阈值为 16，MCP 为后续调用自动重连。`--concurrency` 仍决定同时活动的
worker 数；这些值只控制活动工作，不限制单元总量、回合总数或普通工具调用总数。外部服务
有明确配额或机器经实测无法承受时，可以在自己的 `config.toml` 中降低对应值。

## 分片计划

计划文件采用 JSONL，一行一个单元。`unit` 是从 1 开始且在当前计划中唯一的自然编号，
也是运行档案中的 worker ID。

按文件分配：

```json
{"unit":1,"files":["chapter-01.txt","notes/context.txt"]}
```

按行区间分配，行号从 1 开始且区间双端闭合：

```json
{"unit":2,"file":"records.jsonl","start":100,"end":199}
```

Formic 将任务说明、数据集文件清单和当前分片装入首条用户消息。任务说明与清单位于
共享前缀，分片内容只追加在末尾，以便不同单元复用相同的供应商 prompt cache 前缀。

当前分片是 worker 的主动处理范围。完整文件清单和 `input` 工具允许它为分片中已经发现的
对象查找支持或反证，不表示它应主动扫描整个数据集、扩大任务范围或建立全局目录。计划应使
单元即使单独运行也能产出含义完整的结果。

## 工具执行

worker 不持有本地工具实现或 MCP client。全部普通工具调用只经过：

```text
worker → 冻结的 ToolRegistry → Scheduler 有界收件箱
                              ├─ 内置只读工具
                              └─ 用户配置的任意 MCP
```

每个 worker 同时只等待一个工具调用。调度器收件箱容量等于 `--concurrency`，排队和
执行阶段都携带取消令牌。用户配置各层并发，内核不依据 MCP 产品名设置特殊上限。
`identical_tool_call_limit` 只检测单 worker 连续完全相同且没有进展的调用，不是工具
调用总数上限。

内置工具：

- `search`：在 `input` 或 `output` 根搜索正则或字面文本，可设置 glob 和上下文行数；
- `read`：读取根内相对路径的 UTF-8 文本，可指定 1 起始的闭区间行号。

两者拒绝绝对路径、`.`、`..`、符号链接和根目录逃逸。input 与 output 在启动时打开为
目录 capability；计划校验、遍历、读取以及 output 的锁和全部写入都相对固定根句柄执行，
运行中替换路径既不能把访问引到根外，也不能改变发布位置。`output` 只暴露当前输出模式下
顶层的数字编号完成记录，不暴露 worker 档案、stats 或 schema。

`[mcp_servers.<name>]` 可以配置任意 MCP server。当前支持直接启动的 stdio 子进程和
Streamable HTTP；启用后默认暴露 `tools/list` 发现的全部工具。只有需要主动筛选时才配置
非空 `enabled_tools`。模型可见名称固定为 `<server>__<alias_or_remote_name>`。启动阶段完成
initialize、分页发现、可选筛选和别名校验，并为当前任务冻结目录。

工具目录对当前作业的所有 worker 相同。任务说明应写明何时需要查证、什么证据足够以及
何时停止，但不必复述工具参数；模型已经收到工具 schema。操作者可以完整开放已配置的
MCP，也可以主动筛选；Formic 不依据产品名替用户限制能力。提示词说明工具的使用条件，
配置才决定模型实际拥有的能力。

MCP 调用可配置 job/unit 会话、server/tool 并发、server 级结果大小、超时及只服务后续调用的
重连。stdio 子进程不会继承父进程中的 LLM 密钥等任意环境变量，只得到必要系统变量和
server 显式配置；stderr 单行、stdio JSON、HTTP JSON 与 SSE 在解析前受大小约束。调用
达到超时就返回，已发送的工具请求会收到协议取消，旧会话立即停止复用，中断调用绝不自动
重放。已经收到明确工具结果后，本地结果处理不再把它改报为远端超时；本地处理失败会明确
说明远端调用已经完成、不得重放。当前结果支持 text 与 `structuredContent`；图片、音频、
资源和客户端工具错误也受最终结果字节上限约束。
结果流可能同时承载多个工具，因此每个 server 只有一个统一的解码前与最终结果上限；
`tool_limits` 只收紧单工具并发。

Streamable HTTP 的慢速 `initialize` 有一项底层限制：作业启动按 `startup_timeout_sec` 返回，
工具调用中的重连还受该次 `tool_timeout_sec` 限制；但 rmcp/reqwest future 被取消后，Hyper
不保证已经写入请求的 TCP 连接立即关闭。该连接可能继续存在到远端、Hyper 或操作系统超时；
Formic 不复用它，也不承诺超时后固定时间内出现 TCP EOF。

## 结构化输出

未传 `--output-schema` 时，最终文本原子发布为 `out/<unit>.md`。传入后，Formic 编译并
规范化一份作业级 JSON Schema，通过内部 `formic_submit_result` 接收结果，再执行本地
校验；成功 object 发布为 `out/<unit>.json`。

当前公共 schema 子集包括根 object、基础类型、`properties`、`required`、
`additionalProperties=false`、数组 `items` 和基础值 `enum`。外部 `$ref`、组合、条件和
未知关键字会在启动时失败。拒绝、输出截断、非法 JSON 或修正耗尽不会发布完成记录。

schema 负责结果形状，`task.md` 负责解释字段的业务含义、证据要求和不确定情况。不要让模型
重复单元号、分片位置、任务时间或重试次数等运行时已经确定的事实，也不要把全局归并后才能
确定的字段放入单元 schema。

`out/output-schema.json` 保存输出目录的权威 schema。同一输出目录不能混用 Markdown
与 JSON 完成记录，也不能在已有 JSON 记录上更换 schema。输出目录与数据目录不能相同或
互相包含；一个输出目录同一时刻只允许一个 Formic 作业使用。

## 缓存与上下文压缩

作业内，`scope=input` 的 `search`/`read` 使用规范参数作键。相同在途调用只执行一次，
完整成功结果进入按字节容量淘汰的内存 LRU。`scope=output`、MCP、错误和截断结果不进入
完成缓存。

每次 LLM 调用前，Formic 按最终协议请求估算完整输入，并预留
`context_safety_tokens`；Anthropic 另外预留其 `anthropic_max_tokens`。预计越界时只压缩最旧的完整
`assistant(tool_calls) → tool_result` 组，保留初始任务、分片和最近历史。压缩使用同一
模型，但只开放内部 `formic_submit_compaction`，结果必须通过固定结构和本地校验。

摘要只有在替换后更小且重新进入预算时生效，否则单元失败且原始现场进入 worker 档案。
压缩没有人为总次数上限；一次压缩没有进展时立即失败。未知 HTTP 400 不根据显示文字
猜测上下文错误。

## 输出、统计与退出码

| 路径 | 内容 |
| --- | --- |
| `out/<unit>.md` | 文本模式的权威完成记录 |
| `out/<unit>.json` | 结构化模式的权威完成记录 |
| `out/output-schema.json` | 结构化输出目录的规范化 schema |
| `out/workers/<任务时间戳>/<unit>.md` | worker 完整运行档案 |
| `out/stats.jsonl` | 每单元回合、重试、缓存、工具、压缩和 token 统计 |

运行档案格式及失败语义见 [Worker 可观测性](observability.md)。本地 `o200k` 估算值使用
`*_est` 字段；供应商报告的 usage 单独保存，缺失值不会由估算值冒充。

退出码：`0` 表示全部成功，`1` 表示至少一个单元失败，`2` 表示启动配置或输入无效，
`3` 表示收到终止信号。已发布记录始终保留，跨运行续跑由调用方生成剩余计划。

## 本地验证

```bash
cargo test
cargo +1.88.0 check --all-targets
cargo run --release --example scale_run -- 5000 1000 8 20
```

规模实验参数依次为单元数、worker 并发、每单元工具回合数和 mock 延迟毫秒。
