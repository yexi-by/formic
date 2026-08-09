# 第 1 轮：最小全链路骨架

> 状态：**已完成并验证**（2026-08-06）。本文件是该轮的独立档案：目标、停止线、
> 契约、模块所有者与验证证据。后续每轮同构新建 `round-NN.md`。

## 目标（可观察结果）

`formic run --data <dir> --plan <plan.jsonl> --task <task.md> --out <dir>`：
对计划中每个单元，worker 读分片、装配 prompt，经真实 HTTP/SSE 调用 LLM
（completions / responses / anthropic 三协议选一），把最后一条助手消息原样
原子发布为 `out/<单元号>.md`；每次调用的请求体与原始 SSE 负载落盘到
`out/audit/<单元号>.jsonl`；stdout 汇总完成数、失败数、失败单元号。

## 停止线（本轮不做什么）

- 无工具、无调度器：模型返回 tool call → 单元失败 + 结构化诊断；
- 单轮调用，无多轮循环（第 2 轮随 search 引入）；
- 顺序处理计划单元，无 `--concurrency`（第 3 轮）；
- 无重试预算、停滞检测、取消令牌树（第 4 轮）；
- 无历史压缩、token 预算、web fetch、跨运行续跑设施。

## 契约（本轮冻结的对外面）

- **CLI**：`formic run --data <dir> --plan <file> --task <file> --out <dir>`。
- **退出码**：`0` 全部成功；`1` 存在失败单元；`2` 启动失败（参数、输入、环境）。
- **计划文件**：JSONL 一行一个单元，两种形状——
  `{"unit":1,"files":["a.txt",...]}` 或
  `{"unit":2,"file":"big.txt","start":100,"end":200}`（1 起始、双端闭区间，
  end 允许超过文件行数视为到文件尾）。校验：单元号是不重复的自然数；路径为
  数据根内相对路径、通过启动时打开的根目录 capability 解析且不能逃逸、指向存在的文件；分片非空。
  错误报计划文件、行号、单元号与原因。空白行跳过。
- **环境变量**（缺失必填项启动即失败，报变量名与用途）：
  - `FORMIC_LLM_PROTOCOL`：`completions` / `responses` / `anthropic`；
  - `FORMIC_LLM_BASE_URL`：API 基础地址（协议路径由运行时拼接）；
  - `FORMIC_LLM_MODEL`：模型名；
  - `FORMIC_LLM_API_KEY`：可空，空则不带认证头。
- **输出区**：完成单元 → `out/<单元号>.md`（同目录临时文件 + rename 原子可见，
  重复发布以新记录替换）；失败单元无记录。完成记录 = 完成事实的权威表示，
  调用方差集 = 计划单元 − 输出区记录。
- **审计**：`out/audit/<单元号>.jsonl`，首行请求体原文，后续每行一条原始 SSE
  data 负载（JSON 字符串逐字留痕，不重新解析）。审计写不进去时单元不算完成
  （证据完整是契约要求），由调用方续跑重做。
- **prompt 结构**：instructions（静态两段：角色与自主性条款、通道约定）+
  用户消息 = 任务说明原文 + 数据集文件清单 + 分片内容与定位。任务说明在前、
  分片在末；同作业全部单元「分片前的前缀」字节一致（测试锁定）。
- **任务说明校验**：存在、合法 UTF-8、非空、≤ 1 MiB。

## 模块与所有者

| 模块 | 职责（不变量） |
| --- | --- |
| `src/main.rs` | CLI、环境变量、错误呈现、退出码 |
| `src/plan.rs` | 计划格式解析与校验的唯一所有者 |
| `src/prompt.rs` | instructions 与用户消息装配（前缀一致性） |
| `src/llm/mod.rs` | 内部事件枚举、HTTP/SSE 传输、协议分发、审计留痕缓冲 |
| `src/llm/{completions,responses,anthropic}.rs` | 各协议请求构造 + SSE→事件 transform |
| `src/worker.rs` | 单单元执行：读分片 → 装配 → 调用 → 发布或诊断 |
| `src/output.rs` | 原子发布、审计落盘、汇总（输出区不变量唯一所有者） |

内部事件枚举：`TextDelta / ToolCall / Finished(Stop|MaxTokens)`；worker 不感知
后端差异。Anthropic 的 `max_tokens` 是协议专用必填参数，由部署配置明确提供
（`src/llm/anthropic.rs`），也是唯一允许的供应商专用生成参数配置。

## 验证证据

- `cargo test`：37 项全绿（32 单元 + 5 端到端）。
  - 单元：plan 两种形状与全部非法分支（报错含单元号）；prompt 前缀字节一致、
    分片在末尾；三协议录制样例 → 事件序列；SSE 帧跨 chunk / CRLF / 多行 data。
  - 端到端（`tests/e2e.rs`，真实二进制 × 手写 mock server 真实 HTTP/SSE）：
    三协议各跑 2 单元作业（覆盖两种分片形状）→ 产出 = 罐装最终消息、审计含
    请求体与原始负载、汇总与退出码 0、请求路径与协议匹配；mock 500 → 退出码
    1、失败单元无记录无临时文件、stderr 含单元号与原因、stdout 无成功文案；
    逃逸路径计划 → 退出码 2、错误含单元号与「逃逸」。
- 手工 demo（`examples/mock_llm.rs` + `examples/demo/` 夹具，anthropic 协议）：
  退出码 0，`1.md`/`2.md` 产出正确，`audit/2.jsonl` 可见完整请求体
  （任务说明 → 文件清单 → 行区间分片的装配顺序符合契约）。

## 后续轮的接缝（未实现，候选）

worker 目前直接持有 `LlmClient`；调度器进入时作为工具调用与 LLM 记账的准入点
插在工作循环内（design.md §5）。多轮循环、工具事件枚举扩展、取消令牌均在
对应轮次生长，本轮无预留抽象。
