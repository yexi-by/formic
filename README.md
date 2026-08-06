# Formic（蚁群）

## 项目定位

Formic 是一个 CLI 应用，专门解决传统 Agent subagent 的局限性问题。

越来越多的任务——通读长篇小说并提取术语、逐条分析上万条记录、逐个判断页面里有没有目标信息——没有固定流程可写，每一步都需要理解与判断，只能交给具备智能的执行单元自主完成，而且数量巨大、相互独立。传统 Agent 的 subagent 做不了这件事：它本身就是一个完整的 Agent，带着完整的工具套件与提示词约束，设计过重，开几十个、几百个并行就力不从心，用哪个模型也由宿主决定。

Formic 把执行单元做到最轻：一次性、只读、模型任选，几万个任务排队、几百个同时跑是设计常态。

嵌入式是核心理念：Formic 不是一个自成一体的应用，而是设计为被嵌入的组件。它的接口就是文件与目录——不发明协议、不开端口、不需要 SDK——人、上层 Agent（Claude Code、Codex 等）、CI 或另一个程序，都能把它当作普通命令行工具嵌进自己的流程。

## 核心抽象

> 海量独立的自主任务 → 一个任务分配一个执行单元 → 给定输入与任务说明 → 多轮「LLM ↔ 工具调用」循环、全程自主判断 → 以单元为键的产出记录

围绕这条抽象，运行时负责：

- 并发调度：同时运行的单元数量有上限，超出的排队等待
- API 限流：不超过模型供应商的调用配额
- 失败重试：运行内失败自动重试，次数有上限
- 权限隔离：单元只能读数据，不能写
- 统一输出：产出由运行时写入输出区，按单元编号命名，写完才对外可见
- 调用审计：每次模型调用的输入与输出都可查、可复现

以下三件事刻意交给调用方：分片规划、断点续跑（跨运行）、结果汇总。

## 执行单元（worker，工蚁）

执行单元是完整但一次性的 Agent 会话：

- 领取自己的数据分片与任务说明，独立循环，完成后销毁，腾出的位置立即由下一个任务填补。
- 对数据默认只读，结果只能经运行时的统一输出通道写入输出区。
- 刻意轻量：无交互概念、无写工具、极小提示词。它不是任何上层 Agent 的子代理，而是运行时的内部执行单元。

## 调用方式

调用方一次给齐四样东西：一批数据、一份分片计划（哪个单元处理哪段数据）、一份自然语言任务说明、一个并发上限。之后可以等待最后一个单元完成，也可以中途查看输出区、决定任务继续还是停止。

## 拓展性

Formic 只做通用的批处理内核。高度可拓展性是原型的基石：内核概念最少、职责单一、对外契约稳定，保证原型能向任何方向轻松发展——新能力在内核之外生长，不改动内核。

## 使用

```bash
formic run --data <数据集目录> --plan <plan.jsonl> --task <task.md> --out <输出区目录> --concurrency <N>
```

`--concurrency <N>` 是并发窗口（同时运行的单元数上限，必填），依据是你对
LLM 供应商配额的判断；超出的单元排队等待，不产生容量错误。

模型通过环境变量配置（缺失必填项启动即失败）：

- `FORMIC_LLM_PROTOCOL`：API 协议形状，`completions` / `responses` / `anthropic`；
- `FORMIC_LLM_BASE_URL`：API 基础地址，如 `https://api.openai.com/v1`；
- `FORMIC_LLM_MODEL`：模型名；
- `FORMIC_LLM_API_KEY`：可选，设置后按协议附带认证头。

计划文件是 JSONL，一行一个单元：`{"unit":1,"files":["a.txt",...]}`（文件清单）或
`{"unit":2,"file":"big.txt","start":100,"end":200}`（行区间，1 起始、双端闭区间）。

输出区：完成单元的产出是 `out/<单元号>.md`（原子发布，失败单元无记录）；每次模型
调用的请求与原始响应在 `out/audit/<单元号>.jsonl`（含每次重试的 attempt 序号）。
退出码：0 全部成功，1 存在失败单元，2 启动失败，3 被终止。

`out/stats.jsonl` 是逐单元的统计视图（每行一个单元，含失败与取消）：

```json
{"unit":1,"outcome":"published","turns":2,"llm_calls":2,"retries":0,"tool_calls":{"search":1},"input_tokens_est":4025,"output_tokens_est":201}
```

token 为内部估算值（tiktoken o200k BPE 按内容计算，协议无关，参考 codex 等开源
工具实现），用于预算与指标分析，不是计费依据。权威事实在审计里；stats 是可推导
的便捷视图。完整对话内容在审计的 request 条目（每回合含全量历史），例如用
`jq -r 'select(.direction=="request") | .data' out/audit/1.jsonl` 提取。

中断语义：Ctrl+C（或 Ctrl+Break）一次 = 优雅终止——停止接纳新单元、在途单元取消
收敛、已发布记录保留、退出码 3；再按一次立即退出。被终止不是破坏：调用方读输出区
算差集、剔除已完成单元重新生成计划即可续跑。

观测：设 `FORMIC_METRICS=1` 时，运行期间每 250ms 向 stderr 写一行机器可 grep 的
指标（RSS、在途 LLM 调用、调度器队列深度、在途历史字节、search 耗时、单元计数），
不设置则无输出；指标只是附属证据，不参与业务行为。

规模实验（mock 驱动几千 worker 全链路，观测内存与调度器）：

```bash
cargo build && cargo run --example scale_run -- 5000 1000 8 20
# 参数：单元数 并发窗口 每单元工具调用回合数 mock 延迟毫秒
# 产出：stdout 汇总表 + 当前目录 scale-metrics.csv
```

本地无模型体验全流程：

```bash
cargo run --example mock_llm -- 18080   # 起 mock LLM
FORMIC_LLM_PROTOCOL=completions \
FORMIC_LLM_BASE_URL=http://127.0.0.1:18080/v1 \
FORMIC_LLM_MODEL=demo-model \
cargo run -- run --data examples/demo/data --plan examples/demo/plan.jsonl \
  --task examples/demo/task.md --out /tmp/formic-out --concurrency 2
```

## 文档

- [设计文档](docs/design.md)（候选设计，实现验证后修订为事实描述）
- [模块拓扑](docs/topology.html)（已验证 / 候选两态，随轮更新）
- 各轮档案：[docs/rounds/](docs/rounds/)（1 最小全链路骨架；2 search 工具与多轮循环；3 并发窗口；4 重试预算与取消令牌树；5 规模验证）
