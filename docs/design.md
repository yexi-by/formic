# Formic 当前设计

> 本文只描述已经进入生产入口并由测试验证的当前契约。分片规划和跨单元结果汇总仍由调用方负责。

## 1. 目标与边界

Formic 是一次命令对应一个作业的自主批处理调度中心。调用方提供数据目录、JSONL 计划、任务说明、输出目录和 worker 并发。每个计划单元独立运行多轮 LLM 会话；完成记录以自然单元编号为键原子发布。

系统不限制计划单元总量、对话总回合数或工具调用总数。它只限制当前活动 worker、排队工具请求和单次资源占用。合法工作在窗口满时等待，不能因为总量较大而变成容量错误。真实文件系统错误、模型上下文限制、用户配置的并发和结果大小不能绕过。

worker 没有 shell、写文件或任意代码执行能力。普通工具只有只读内置工具和操作者明确启用的 MCP。结构化结果提交与历史压缩提交是 worker 内部控制通道，不是普通工具。

### 1.1 作业成立的语义条件

Formic 调度的是独立结果，不是会互相协商的角色。一个计划单元只有同时满足以下条件，才适合进入同一作业：

1. 它有明确且有限的主处理范围；
2. 共同任务说明、当前分片、只读输入和允许使用的工具足以判断成败；
3. 它不需要读取其他单元的新结果、等待特定完成顺序或修改共享业务状态；
4. 单独重试、取消或更改并发只改变完成时间，不改变其他单元结果的含义；
5. 它的记录可以以当前 `unit` 独立发布，部分进展对调用方仍然有用。

完整 `input` 可见性只用于核实当前单元结论。例如，worker 可以从分片内的候选对象出发，在只读输入数据集中查找支持或反证；它不能把检索命中继续扩张成无边界的新处理清单。`output` 中已经发布的记录随并发完成顺序变化，因此不能作为单元正确性的输入，也不能用来实现跨 worker 协调。

分片规划和全局结果所有权属于调用方。全局去重、统一命名、总排名、最终汇总以及存在前后依赖的工作，可以接在独立单元作业之后，但不是同一 worker 池中的另一个“单元”。只有当后续工作重新划分后也满足上述五项条件时，才适合作为新的 Formic 作业。

## 2. 外部契约

生产入口为：

```text
formic run --data <dir> --plan <jsonl> --task <file> --out <dir>
           --concurrency <n> [--output-schema <schema.json>]
```

Formic 只读取当前工作目录的 `config.toml`，不接受配置路径参数，也不热加载。LLM 的非空环境变量逐字段覆盖文件值。协议由 `FORMIC_LLM_PROTOCOL` 选择；上下文窗口必须由配置或环境变量明确给出。只有 Anthropic Messages 另需供应商专用的 `anthropic_max_tokens`，其他协议不接受输出 token 配置。

计划是一行一个 object 的 JSONL。单元可以指定文件集合，或一个文件的 1 起始闭区间行范围。启动边界会拒绝空分片、重复/零单元号、缺失文件、绝对路径和根目录逃逸。

文本模式的完成事实是 `out/<unit>.md`。结构化模式的完成事实是 `out/<unit>.json`，并由 `out/output-schema.json` 记录该目录唯一的 schema。两种模式不得在同一输出目录混用。输出目录与数据目录不得相同或互相包含，并由进程锁保证同一时刻只有一个作业使用；锁、schema、审计、报告、统计和完成记录都相对启动时打开的同一输出目录句柄操作，运行中替换 ambient 路径不能改写写入位置或绕过锁。每次任务另建 `out/workers/<UTC任务时间戳>/`，每个计划单元对应一个 `<unit>.md` 运行档案。

## 3. 执行与取消

主入口只有取得 `--concurrency` 许可后才创建 worker task。计划可以很大，但同时存在的活动 worker 数有界。每个 worker 同时最多等待一个工具调用。

取消令牌从作业传到 worker、LLM 流、调度器排队、信号量等待、本地遍历和 MCP 调用。排队时已经取消的请求不会启动。本地 `search` 和 `read` 在文件/行/匹配边界检查取消；MCP 和 LLM 的异步等待直接与取消令牌竞争。收到第一次终止信号后不再接纳单元，已发布结果保留。

单元 panic、协议错误、工具运行错误、拒绝、输出截断和结构修正耗尽彼此隔离。作业按计划顺序汇总失败编号，不用并发完成顺序改变可观察结果。

## 4. 唯一工具执行入口

```text
worker
  │  单元号、名称、规范参数、取消令牌
  ▼
冻结的 ToolRegistry
  ▼
Scheduler 有界收件箱（容量 = worker 并发）
  ├─ 内置工具：全局 semaphore + 逐工具 semaphore + blocking task
  └─ MCP：逐服务器 semaphore + 逐工具 semaphore + async client
```

`ToolRegistry` 在 worker 启动前冻结模型可见的名称、schema、来源和执行目标，并按名称稳定排序。worker 不能直接持有 MCP client 或本地 executor，因此限流、取消、缓存、计时和审计没有第二套规则。

并发限制完全来自操作者配置。内核不会依据 server 或工具产品名设置特殊吞吐上限。不同 MCP server 使用独立许可，一个慢 server 不占用其他 server 或内置工具的许可。统计记录每个工具的排队/执行时间及每个 MCP server 的当前和峰值在途数。

连续完全相同的工具名称与原始参数达到 `identical_tool_call_limit` 才判定停滞。该规则只发现单 worker 无进展循环，不是工具总调用次数限制。

## 5. 内置工具与数据可见性

每个 worker 可见两棵只读根：`input` 是完整输入数据集，`output` 是当前模式下已发布的顶层数字编号记录。worker 运行档案、stats、schema、临时文件和其他扩展名不向模型暴露。

`search` 支持正则/字面匹配、glob 和上下文行。`read` 支持 UTF-8 文本及可选闭区间行号。input 与 output 根在启动时各打开一次目录 capability，计划校验、遍历和实际读取始终相对同一根句柄完成；输出写入也使用这个固定根。运行期间替换 ambient 路径既不能把访问引向根外，也不能改变发布位置。参数在工具边界解析一次；绝对路径、`.`、`..` 和符号链接被拒绝，遍历不跟随链接。

匹配数、上下文行数、结果字节数、全局并发和逐工具并发来自当前配置。结果达到边界时带明确截断标记。两项工具在 blocking task 中运行，不占用 Tokio runtime worker 线程。

## 6. 通用 MCP

每个启用的 `[mcp_servers.<name>]` 必须选择且只选择一种传输：直接启动的 stdio 命令，或 Streamable HTTP URL。stdio 的 `command`/`args` 不经过 shell，只继承启动所需的系统环境变量和该 server 显式配置的环境变量；stderr 按固定单行上限持续排空；Windows 使用 Job Object、Unix 使用进程组，以便会话关闭时回收进程树。HTTP 支持 bearer 与自定义 headers，且禁止 session 过期后自动重放原请求。stdio JSON、HTTP JSON 和 SSE 都在解析前执行 server 级消息字节上限；公共结果流无法诚实提供互不相同的单工具解码上限，因此 `tool_limits` 只配置并发。

作业启动时，Formic 对所有启用 server 完成 initialize、分页 `tools/list`、可选 `enabled_tools` 筛选、显式别名处理和稳定排序；未配置筛选时暴露发现的全部工具。任一 server 失败则 worker 不启动。模型名称固定为 `<server>__<alias_or_remote_name>`；非法、过长或碰撞会明确失败。`tools/list_changed` 只产生诊断，当前作业继续使用冻结目录。

`session_scope=job` 复用一个会话。`unit` 在活动单元第一次调用时建立独立会话，并在单元成功、失败、取消或 panic 后回收。server semaphore 跨所有 unit 会话共享。工具调用期限从进入调用开始，包含等待 session slot、连接或重连、发送请求和等待响应；期限到达后立即使旧会话不可复用并向调用方返回。已经发送的工具调用会发送协议取消并中止本地 transport，无 session 的 Streamable HTTP server 也会收到取消通知。配置的重连只为下一次新调用建立会话，绝不重放状态未知的旧调用。明确的工具结果一旦到达，后续本地有界转换不再改写为远端超时；本地处理失败会明确标记远端调用已经完成、不得重放。

Streamable HTTP 有一个明确限制：如果 `initialize` 请求已经写入连接、但 server 一直不返回响应，取消 rmcp/reqwest future 不能保证 Hyper 立即关闭底层 TCP 连接。作业启动按 `startup_timeout_sec` 返回；工具调用中的重连还受该次 `tool_timeout_sec` 限制。该连接不会成为可复用的 Formic session，但连接本身可能继续存在，直到远端、Hyper 或操作系统结束它。当前不把“超时后固定时间内收到 TCP EOF”作为 Formic 契约。

MCP 结果接受 text 与 `structuredContent`。纯结构数据稳定序列化；两者并存时生成固定 object 包装。text 可按 UTF-8 边界截断；结构数据不能在保持合法 JSON 的前提下进入限制时返回工具错误。图片、音频、资源和 resource link 返回不支持的结果类型。正常结果、`isError` 前缀和客户端生成的工具错误都受同一个最终字节上限约束。`isError` 是可回注模型的工具失败；超时、传输和会话故障是单元运行错误。

中断或超时的调用绝不自动重放。`reconnect=true` 只允许下一次新调用在同一 session slot 内单飞重连，并重新获取允许工具、确认 schema 与冻结目录一致。

客户端只声明 tools 能力。当前不实现 OAuth、resources、prompts、sampling、elicitation、旧 SSE MCP 或多媒体输入。

## 7. 结构化输出

`--output-schema` 是作业输入。运行时接受三个模型协议共同能表达和本地验证的 JSON Schema 子集：根 object、基础类型、properties、required、`additionalProperties=false`、数组 items 和基础值 enum。未知关键字、外部 `$ref`、组合和条件 schema 在启动时失败。schema 规范化并编译一次，全部单元共享。

结构化模式在普通工具目录之外增加内部 `formic_submit_result`。模型提交的参数就是用户 schema 定义的 object，不附加 unit 字段。worker 截获调用并本地校验，绝不发送 Scheduler。提交与文本或普通工具混在同一回合时，普通工具照常执行，提交收到必须单独调用的错误。

最终文本、非法 JSON 和 schema 不匹配会把类型化校验结果附加到历史，并按 `llm_attempts` 修正。审计保存 instance path、schema path 和原因，不重复保存原始响应正文。成功 object 重新缩进序列化并以末尾换行发布；拒绝、max tokens、修正耗尽或写入失败不产生完成记录。

## 8. 缓存与请求稳定性

工具目录、工具 schema、输出 schema、名称和顺序在作业内不变。系统 instructions、任务说明和数据清单位于分片前；动态分片与后续历史只追加在末尾。当前不发送供应商专有 cache hint，避免将后端特性冒充公共契约。

Completions 与 Responses 的请求只包含模型名、实际消息或 input、工具目录和 `stream`；
不发送 temperature、top_p、任何 max token、reasoning、verbosity、seed、stop、penalty 或
tool choice 等生成控制字段。Anthropic Messages 只额外发送其协议必填且显式配置的
`max_tokens`。

`scope=input` 的 `search`/`read` 在参数解析和默认值合并后生成规范键。第一个调用成为 owner，相同在途调用等待同一结果；完整成功结果进入作业内存 LRU，并按 `cache.max_bytes` 淘汰。输出根调用、MCP、错误和截断结果不会留在完成缓存。取消 owner 会唤醒等待者重新竞争，不留下永远 pending 的条目。

三种 LLM transform 独立解析供应商明确报告的 input、output、cache-read 和 cache-creation token。缺失字段保持缺失；本地 `o200k` 估算使用独立统计字段，不冒充计费值。

## 9. 上下文预算与压缩

每次调用前，LLM client 先构造最终协议请求 JSON，再用它估算输入 token。安全预算为：

```text
completions/responses: context_window_tokens - context_safety_tokens
anthropic: context_window_tokens - anthropic_max_tokens - context_safety_tokens
```

因此 instructions、初始消息、完整历史、工具 schema、结构化 schema、协议包装和压缩工具都参与预算。初始任务/分片与冻结工具目录本身超过预算时明确失败。

普通请求预计越界时，worker 从最旧处选择一个或多个完整 `assistant(tool_calls) → 对应 tool_result` 组。初始用户消息和最近能保留的完整组不变。压缩调用使用同一 LLM，但工具目录只有内部 `formic_submit_compaction`；提交固定包含 `summary`、`verified_facts`、`evidence` 和 `remaining_work`。

压缩请求本身也在同一安全预算内，每次无效修正前重新计算。只有本地校验通过、候选历史更小且重新进入预算时才一次性替换内存历史。否则保持原历史并使单元失败。一次压缩后仍越界会立即失败；后续对话再次增长时可以再次压缩，没有人为总次数上限。

供应商 HTTP 错误只有在结构化 `code`/`type` 明确属于已知 context-limit 值时才触发一次紧急压缩。未知 400 是请求错误，不解析显示文本，也不连续重发相同请求。

## 10. Worker 运行档案、统计与验证

worker 运行时把状态变化、上下文预算、LLM 请求、原始 SSE、工具参数/来源/结果、排队与执行时间、缓存决定、重试、结构校验和压缩事件写入当前任务目录中的临时 JSONL。普通请求与压缩请求分别以第一份完整正文为基准，后续同类请求保存可逆字节增量；上一份请求只放在磁盘临时基准中，不随历史增长占用 worker 内存。一次调用的 SSE data 负载按到达顺序合并为一个事件流审计项。响应流超过 64 MiB 时调用失败；审计保存此前全部字节，并为触发超限的网络块保存最多 64 KiB 原始前缀及块长度、遗漏长度、超限量和编码，避免静默丢失证据或再次产生无界分配。每个时间线项带自然序号和相对 worker 启动时间；审计写入失败会阻止单元发布。

worker 结束后，运行时流式读取临时事件并原子生成 `workers/<UTC任务时间戳>/<unit>.md`。文档先呈现结局、冻结配置、统计和按时间排序的逻辑状态；首份请求、后续请求变化量和未触发流上限的完整原始响应流放在折叠区，不推测模型不可见的内部思维。请求增量按记录中的前缀、删除量和插入原文可逐字还原，因此去重不以丢失审计证据为代价。Markdown 成功后删除临时 JSONL，因此一个 worker 最终只有一份完整证据；渲染失败会产生用户可见诊断并保留临时文件，避免现场丢失。成功、失败、取消和 panic 都走同一渲染入口。

`stats.jsonl` 保存回合、调用、重试、工具计数、本地 token 估算、供应商 usage、缓存命中/合并/淘汰、工具等待/执行时间、MCP 在途峰值以及压缩前后 token。stats 是派生观测，写入失败只产生诊断，不改写已经确定的业务结果。

测试覆盖三种 LLM 协议的文本/结构化成功、非法修正、混合工具、拒绝、截断和耗尽；本地 read/search 路径边界；缓存 singleflight/LRU；真实 stdio 与 Streamable HTTP fake MCP 的分页发现、认证、job/unit 会话、超时无重放；上下文压缩；以及 1000 个单元经过有界调度器而不产生容量错误。

## 11. 参考实现的取舍

`reference/codex` 提供了 headless 会话、统一工具调度、Responses 请求映射和结构化结果处理的参考；`reference/grok-build` 提供了多协议 transform、伪终止工具、本地 schema 二次校验和有界修正的参考。Formic 采用这些职责划分，不采用它们的 TUI、审批、插话、会话恢复或通用 shell 能力。

网络搜索不是 Formic 内核中的特殊协议。内置 `search` 只检索本地输入/输出文件；远端搜索由操作者选择任意 MCP server 提供，并与其他 MCP 一样经过冻结目录、限流、取消和审计。这样不会把 Firecrawl、SearXNG、Playwright 或其他实现硬编码成产品限制。
