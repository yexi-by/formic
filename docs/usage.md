# Formic 详细使用说明

Formic 把一个大作业拆成许多能够独立完成的单元。每个单元由一个 worker 读取自己的分片，运行多轮 `LLM ↔ 工具` 会话，并独立发布结果。调用方负责准备数据、计划和任务说明，也负责需要全局视图的最终归并。

## 1. 安装与启动条件

从源码构建需要 Rust 1.88 或更高版本：

```bash
cargo build --release
```

Windows 产物位于 `target/release/formic.exe`。仓库中的 `dist/` 是本机发布目录，被 Git 忽略，不是源码的一部分。

一次作业至少需要：

- 一个输入数据目录；
- 一个 JSONL 分片计划；
- 一份所有 worker 共用的任务说明；
- 一个新的或可续跑的输出目录；
- 有效的模型配置；
- 明确的 worker 输出读取权限。

## 2. 最小配置

复制根目录的 `config.example.toml` 为自己的 `config.toml`，填写模型信息。不要提交真实密钥。

```toml
protocol = "completions"
url = "https://api.example.com/v1"
api_key = ""
model = "model-name"
context_window_tokens = 131072

model_input_modalities = ["text"]
metrics = false
```

协议可选值：

| 值 | 请求格式 |
| --- | --- |
| `completions` | OpenAI Chat Completions 兼容格式 |
| `responses` | OpenAI Responses 格式 |
| `anthropic` | Anthropic Messages 格式 |

`anthropic` 还必须配置正整数 `anthropic_max_tokens`。其他协议出现该字段会直接失败。

### 2.1 模型输入能力

`model_input_modalities` 是操作者对远端模型能力的明确声明，只接受两种写法：

```toml
model_input_modalities = ["text"]
```

```toml
model_input_modalities = ["text", "image"]
```

Formic 无法替远端供应商证明模型真的支持图片。声明 `image` 后，worker 才能接收图片分片，并自动获得 `read_image` 工具。供应商实际不支持时，请求按普通模型请求错误处理。

### 2.2 透传供应商 JSON

供应商需要专有参数时，可把一个 JSON 对象写进 `extra_body_json`：

```toml
extra_body_json = '''{"temperature":0.2,"reasoning":{"effort":"high"}}'''
```

对象中的数组、嵌套对象、布尔值和 `null` 会保持原类型加入每次普通请求和上下文压缩请求。Formic 不解释这些字段的供应商含义。

扩展 JSON 不能覆盖 Formic 自己管理的协议字段：

- Completions：`model`、`stream`、`messages`、`tools`；
- Responses：`model`、`stream`、`instructions`、`input`、`tools`；
- Anthropic：`model`、`max_tokens`、`stream`、`system`、`messages`、`tools`。

无效 JSON、非 object 或字段冲突都会在任何模型请求前失败。

如果透传参数会扩大供应商可能生成的输出长度，应相应增加 `execution.context_safety_tokens`。Formic 不会解释专有字段并自动推导新的输出预留。

### 2.3 配置来源

Formic 的部署、模型、协议、密钥、MCP 和观测配置只来自一份 TOML。省略 `--config` 时读取当前
工作目录的 `config.toml`，不会搜索父目录；显式路径与默认文件都必须存在并使用 `.toml`
扩展名。进程环境中的 `FORMIC_*`、代理变量或其他同名值不会覆盖配置。

HTTP MCP 的 bearer 与 header 直接写入对应 server；stdio MCP 的业务环境放在该 server 的
`env` 表。Formic 只额外保留启动子进程所需的最小操作系统环境，不把这些系统值解释为产品配置。

### 2.4 资源与失败策略

配置文件还可以调整当前活动资源和失败处理：

| 字段 | 控制内容 |
| --- | --- |
| `connect_timeout_ms` | 建立模型服务连接的最长等待 |
| `read_timeout_ms` | 等待下一段模型流数据的最长时间 |
| `request_timeout_ms` | 一次完整模型请求的最长时间 |
| `retry_delays_ms` | 网络临时失败后依次等待多久；空数组表示不重试 |
| `max_retry_after_ms` | 供应商要求等待超过该值时停止接纳后续模型调用 |
| `requests_per_minute` | 可选的真实模型请求频率限制 |
| `metrics` | 是否每 250 毫秒向 stderr 输出规模观测 |
| `execution.llm_attempts` | 模型工具参数或结构化结果无效后的修正次数 |
| `execution.max_concurrent_units` | 同时活动的 worker 数；不限制计划总量 |
| `execution.identical_tool_call_limit` | 单 worker 连续重复完全相同工具调用的停止线 |
| `execution.context_safety_tokens` | 从模型上下文中预留的安全空间 |
| `tools.max_result_bytes` | 内置文字工具的默认结果上限 |
| `tools.max_in_flight` | 全部内置工具的共同在途上限 |
| `tools.search.*` | 搜索开关、匹配数、上下文和可选覆盖限制 |
| `tools.read.*` | 文字读取开关和可选覆盖限制 |
| `cache.enabled` / `cache.max_bytes` | input 文字工具缓存开关和内存容量 |

正式默认值见 `config.example.toml`。这些参数限制当前活动工作，不是单元总数、回合总数或工具调用总数上限。不要用 worker 并发替代供应商请求频率限制。

### 2.5 验证配置与连接

```bash
formic test --config config.toml
```

`test` 按以下顺序输出逐项结果：

1. 读取并验证完整 TOML 配置；
2. 使用生产协议发送一次最小 LLM 流式请求；
3. 按 server 名称稳定顺序逐个完成 MCP `initialize`、分页 `tools/list` 和工具目录校验，最后统一检查跨 server 的模型可见名称。

LLM 或某个 MCP 失败时，命令继续检查后续 server，最后返回汇总。LLM 自检使用一次实际请求；MCP 自检在工具目录发现完成后结束。Formic 只在终端输出这份报告，input、plan、output、run 和 worker 档案均保持原状。

自检全部通过时退出码为 `0`；任一 LLM/MCP 项失败时为 `1`；配置无效时为 `2`；收到终止信号时为 `3`。Ctrl+C 会停止后续项，并关闭已启动的 MCP 会话。

## 3. 准备输入与分片计划

输入目录是作业的只读证据根。Formic 启动时冻结其中的普通文件集合和内容摘要；运行中出现新文件、内容替换、符号链接或目录联接不会静默改变作业输入。

计划文件采用 JSONL，一行一个 object。`unit` 必须是从 1 开始的唯一自然编号。

### 3.1 文件分片

```json
{"unit":1,"files":["chapter-01.txt","notes/context.txt"]}
```

`files` 至少包含一个输入根相对路径。声明图片能力后，同一个文件分片可以混合 UTF-8 文本与 JPEG、PNG、GIF、WebP 图片。

### 3.2 行区间分片

```json
{"unit":2,"file":"records.jsonl","start":100,"end":199}
```

行号从 1 开始，`start` 和 `end` 都包含在范围内。`end` 超过文件末尾时读取到文件结束。行区间只接受 UTF-8 文本，不接受图片。

计划拒绝绝对路径、`.`、`..`、根目录逃逸、缺失文件、空分片、重复单元号和零单元号。

## 4. 编写任务说明

`task.md` 原样进入每个 worker 的首条用户消息。它应说明：

- 当前单元要交付什么；
- 当前分片中的哪些对象属于主动处理范围；
- 什么时候需要读取完整 input 或调用远端工具；
- 什么证据足够，冲突和未知怎样表示；
- 输出字段或段落的业务含义；
- 什么条件成立后立即结束。

任务文件必须是非空 UTF-8 文本，最大 1 MiB。超限、空白文件或无效编码都会在 worker 启动前失败。

不要在任务说明中重复 unit、并发数、模型名、分片路径或工具参数 schema；运行时已经提供这些事实。完整方法和元数据补全示例见[任务设计](task-design.md)。

## 5. 运行作业

```bash
formic run \
  --data data \
  --plan plan.jsonl \
  --task task.md \
  --out out \
  --worker-output-access none \
  --config config.toml
```

参数说明：

| 参数 | 含义 |
| --- | --- |
| `--data` | 输入数据目录 |
| `--plan` | JSONL 分片计划 |
| `--task` | 共同任务说明 |
| `--out` | 输出目录 |
| `--worker-output-access` | 必填；`none` 或 `published` |
| `--config` | 可选；显式配置文件 |
| `--concurrency` | 可选；仅覆盖本次活动 worker 数 |
| `--output-schema` | 可选；启用结构化 JSON 输出 |
| `--resume` | 继续同一输出目录中的未完成单元 |

`--concurrency` 只限制同时活动的 worker，不限制计划单元总量。工具和 MCP 另有自己的在途并发配置。

## 6. Worker 输出读取权限

首次运行和续跑都必须明确填写权限：

```text
--worker-output-access none
```

`none` 表示 worker 只能搜索和读取冻结 input。工具 schema 不包含 output，调度器也不持有结果目录读取句柄；伪造 `scope=output` 会收到工具错误。

```text
--worker-output-access published
```

`published` 额外允许读取 `out/results/` 中已经发布的数字编号记录。它不暴露 worker 档案、统计、schema 或临时文件。

已发布结果会随并发完成顺序出现，因此不能用于认领任务、全局去重、等待其他 worker 或判断整个作业是否完成。独立任务通常选择 `none`。

## 7. 内置工具与图片

Formic 根据配置冻结每个 worker 可见的工具目录：

| 工具 | 能力 |
| --- | --- |
| `search` | 在允许的根中进行正则或字面文本搜索 |
| `read` | 读取 UTF-8 文本，可指定闭合行区间 |
| `read_image` | 仅图片模型拥有；读取冻结 input 中的支持图片 |

工具只接受根内相对路径，拒绝绝对路径、`.`、`..`、符号链接和目录联接。`read_image` 不接受 output 或 HTTP URL。

模型在同一回合返回多个普通工具调用时，Formic 会让它们同时进入 Scheduler；全局、
server 和单工具的在途窗口负责实际准入。全部调用结束后，结果仍按模型给出的调用顺序
写回对话，因此并发只改变耗时，不改变历史含义。

图片保持原始字节，不缩放、不转码、不截断。Formic 不增加图片字节数、像素数或数量常量上限；文件系统、内存、上下文和供应商硬限制仍会正常报错。图片 base64 只存在于实际待发送请求中，不进入日志、审计 JSON 或 worker Markdown 档案。

## 8. MCP 工具

`[mcp_servers.<name>]` 可以配置 stdio 子进程或 Streamable HTTP，两种传输只能选择一种。完整字段见 `config.example.toml`。

stdio 的命令和参数不经过 shell。子进程只继承启动所需的系统变量及该 server 显式配置的环境变量，不会自动获得 LLM 密钥等任意父进程秘密。HTTP 可以显式配置 bearer 和自定义 header。

启动阶段会完成 initialize、分页 `tools/list`、可选筛选、别名检查和稳定排序。默认暴露 server 发现的全部工具；只有确实需要限制时才填写非空 `enabled_tools`。模型看到的名称为 `<server>__<alias_or_remote_name>`。

MCP 可配置：

- `session_scope=job|unit`；
- server 和单工具并发；
- 启动与调用超时；
- 原始传输消息字节上限；
- 最终结果字节上限；
- 只服务后续新调用的重连。

已经发送但超时或中断的调用不会自动重放。原始消息与最终模型结果分别由
`max_message_bytes` 和 `max_result_bytes` 限制；未配置前者时继续从最终结果上限推导传输上限。
收到明确远端结果后，如果本地图片留档失败，worker 会失败并明确说明不得重放该工具调用。

MCP server 明确返回的 JSON-RPC 工具错误会作为普通工具结果回注模型。已经发出的调用等待结果
超时，或共享传输关闭时，Formic 同样把“原调用结局未知且未重放”作为不可缓存的工具错误回注，
使模型改用其他工具；它不得重复可能产生副作用的原调用。等待会话资源、会话建立或发送请求的
本地截止时间耗尽，以及收到明确结果后的本地处理超时或失败，仍会终止当前 worker。

结果支持 text、`structuredContent` 和 JPEG/PNG/GIF/WebP 图片。音频、resource 和 resource link 不支持。MCP 图片在发送模型前原样保存到当前运行目录的 `media/`。

Streamable HTTP 有一项底层限制：initialize 超时会让 Formic 按配置返回并停止复用该会话，但取消请求不能保证 Hyper 或操作系统立即关闭已经写入的 TCP 连接。该连接可能继续存在到远端或系统超时；Formic 不把“超时后固定时间收到 TCP EOF”作为契约。

## 9. 结构化输出

传入 JSON Schema 后启用结构化模式：

```bash
formic run \
  --data data \
  --plan plan.jsonl \
  --task task.md \
  --out out \
  --worker-output-access none \
  --config config.toml \
  --output-schema result.schema.json
```

当前支持根 object、基础类型、`properties`、`required`、`additionalProperties=false`、数组 `items` 和基础值 `enum`。外部 `$ref`、组合、条件和未知关键字会在启动时失败。

Formic 为模型增加内部 `formic_submit_result`。提交 object 经过本地 schema 校验后才会发布。schema 负责形状，`task.md` 仍需解释字段含义、证据要求和何时允许空值。

文本模式发布 `results/<unit>.md`；结构化模式发布 `results/<unit>.json`，并保存一份 `results/output-schema.json`。同一输出目录不能混用两种模式或更换 schema。

## 10. 续跑

作业中断或部分失败后，使用相同输入增加 `--resume`：

```bash
formic run \
  --data data \
  --plan plan.jsonl \
  --task task.md \
  --out out \
  --worker-output-access none \
  --config config.toml \
  --resume
```

续跑前会核对：

- plan、task、schema 和完整 input；
- 单元集合与输出模式；
- worker 输出权限；
- 模型输入模态；
- 作业状态与已发布结果是否一致。

任何变化、损坏或未知结果文件都会在 MCP 和 LLM 请求前失败。已发布结果不会覆盖；failed、stopped 和未开始单元可以重试。旧契约作业清单不迁移，需要按当前参数重新运行。

收到终止信号后，Formic 停止接纳新单元并取消尚未发布的活动工作，已经原子发布的结果保留。本次命令以退出码 3 结束后，可以使用同一输入和 `--resume` 继续。

## 11. 输出目录与退出码

```text
out/
├─ results/
│  ├─ 1.md 或 1.json
│  └─ output-schema.json           # 仅结构化模式
└─ runs/
   └─ run-000001/
      ├─ workers/1.md
      ├─ media/1/1.png             # 仅 MCP 图片
      ├─ stats.jsonl
      └─ summary.json
```

每次运行创建新的自然序号 `run-N`，不会覆盖旧档案。结果目录与数据目录不能相同或互相包含；一个输出目录同一时刻只允许一个 Formic 作业使用。

| 退出码 | 含义 |
| --- | --- |
| `0` | 作业完整，或自检全部通过 |
| `1` | 作业存在 failed、stopped 或 not_started，或自检项失败 |
| `2` | 启动配置、输入或续跑一致性无效 |
| `3` | 收到终止信号 |

运行档案和统计的详细解释见[可观测性与排错](observability.md)。

## 12. 上下文、缓存和重试

Formic 用 o200k 估算文字与协议结构，按解码尺寸估算图片视觉 token，图片 base64 不按文字计费。普通协议输入预算为上下文窗口减去 `context_safety_tokens`；Anthropic 还要减去 `anthropic_max_tokens`。

预计越界时，只压缩最旧的完整工具往返组。初始任务与分片不可压缩；压缩请求自身也必须进入预算。有效摘要替换旧组后，活动历史中的工具图片字节才会释放。

完整、成功、未截断的 `scope=input` 搜索和文字读取结果可以进入作业内存缓存。相同在途调用只执行一次。output、MCP、图片、错误和截断结果不进入完成缓存。

连接、单次读取、整个请求、网络重试和结构修正分别配置。`llm_attempts` 不代表网络重试次数。明确的鉴权、权限、额度或账户错误，以及过长 `Retry-After` 和网络重试耗尽，会停止接纳后续模型调用；已经发布的结果保留。

## 13. 常见问题

### 启动时提示缺少输入模态

在 TOML 中加入 `model_input_modalities = ["text"]`。只有确认模型支持图片时才使用 `["text", "image"]`。

### 图片没有送到模型

确认分片使用 `files` 形状、扩展名属于支持格式、内容签名与扩展名一致，并且模型声明包含 image。行区间不能承载图片。

### Worker 看不到其他结果

检查命令是否使用 `--worker-output-access published`。即使开启，该能力也只适合辅助查阅，不应成为单元正确性的依赖。

### 续跑被拒绝

不要修改首次运行使用的 plan、task、schema、input、输出权限或输入模态。先查看终端原因和最新 `runs/run-N/summary.json`；损坏现场不要覆盖，按提示恢复或重新建立输出目录。

### 工具重复调用后失败

`identical_tool_call_limit` 只阻止单 worker 连续发送完全相同且没有进展的调用。先从 worker 档案确认参数与结果，再改任务停止条件或工具使用规则，不要单纯提高限制掩盖循环。
