# 第 2 轮：search 工具 + 调度器准入 + 多轮循环

> 状态：**已完成并验证**（2026-08-06）。本轮档案同 round-01 模板。

## 目标（可观察结果）

`formic run` 的 worker 现在跑「LLM ↔ 工具调用」多轮循环：模型可用 `search`
在两棵只读根内检索（input = 整个输入数据集，output = 已完成单元记录）；一切
工具调用经调度器准入并在有界 blocking 池执行；每次 LLM 调用与工具调用的
输入输出全部落审计；连续相同调用达到配置阈值时触发停滞检测终止单元；搜索出参硬边界
截断时显式标记。

## 停止线（本轮不做什么）

- 仍顺序处理单元，无 `--concurrency`（第 3 轮）；调度器的限流记账与跨 worker
  去重缓存随并发进入；
- 无重试预算、无取消令牌树（第 4 轮）；无历史压缩、token 预算（第 5 轮实测）；
- 无 web fetch。

## 契约（本轮新增/变更）

- **工具命名空间**：只有 `search`（只读 enforcement 由构造保证）。入参
  `pattern` / `scope`(input|output) 必填，`glob` / `context`(≤20) / `literal`
  可选；出参硬边界：匹配 ≤ 100、总字节 ≤ 32 KiB，截断以 `[已截断：…]` 标记。
- **可见性边界**：遍历不跟随符号链接；output 根只搜顶层 `<单元号>.md` 记录
  （audit/ 与临时文件天然排除）。`worker::list_files` 与搜索共用同一遍历。
- **审计条目**：`request` / `event` / `tool_call` / `tool_result` 四类，
  单元结束一次性落盘 `out/audit/<单元号>.jsonl`。
- **prompt**：instructions 增至三段（新增 search 使用规则）；工具信息走请求
  tools 字段，不进提示词文字。跨单元前缀字节一致不变量与测试不变。
- **失败语义新增**：`Stalled`（连续相同调用达到配置阈值，诊断含工具名与参数）、
  `BadToolArguments`（模型参数非合法 JSON）、`EmptyOutput`（最终回合无文本）。
  工具级错误（非法正则/glob、缺参数、未知工具）以 `错误：…` 文本回注模型，
  不判单元失败。

## 模块与所有者

| 模块 | 职责 |
| --- | --- |
| `src/tools.rs`（新） | search 语义唯一所有者：规格、执行、遍历、边界、截断标记 |
| `src/scheduler.rs`（新） | 工具调用唯一入口：mpsc 串行准入 + 有界 blocking 池 + oneshot 回执 |
| `src/llm/mod.rs` | 内部对话模型 `Message`/`ToolCallReq`；`call(instructions, history, tools)` |
| `src/llm/{completions,responses,anthropic}.rs` | 双向翻译：历史→协议请求；SSE 增量→完整 ToolCallReq（有状态 transform） |
| `src/worker.rs` | 多轮循环、停滞检测、按回合累积审计、最终回合文本发布 |
| `src/output.rs` | AuditEntry 四类条目；write_audit 重构 |

## 决策与设计张力记录

- **审计归属**：design.md §5 把审计列为调度器不变量。本轮审计保持单元维度
  （输出区模块所有），每次 LLM/工具调用的输入输出已完整可追踪，不变量成立；
  跨 worker 的集中视图待第 3 轮并发真实化后再评估是否需要调度器级记录。
- **停滞检测提前**：工具循环存在，其失败语义就必须存在，否则循环无界。
  阈值 3 为内部常量（`src/worker.rs` STALL_LIMIT）。重试预算与取消仍属第 4 轮。
- **arguments 非合法 JSON 判单元失败**：结构边界归 Formic；Anthropic 历史
  回嵌要求 input 为 JSON 对象，无法携带非法参数继续。第 4 轮重试预算软化。
- **worker 级停滞单测未做**：停滞逻辑由 e2e（LOOP-MARKER 路径）经真实入口
  覆盖；不为单测引入 LLM 替身抽象（AGENTS.md §1）。

## 验证证据

- `cargo test`：51 项全绿（45 单元 + 6 端到端）。
  - 单元新增：三协议工具调用增量组装（参数分片跨帧 → 恰好一个完整
    ToolCallReq）；三协议历史→请求映射（completions 的 role:tool、responses
    的 function_call_output、anthropic 连续 ToolResult 折叠）；search 的
    regex/literal/glob/context/截断标记/output 根边界/错误文本。
  - 端到端：mock 升级脚本化两轮；三协议各 2 单元 → 4 请求，第二轮请求体含
    search 结果（数据集中的匹配行）与协议工具结果消息；停滞路径（LOOP-MARKER）
    → 退出码 1、stderr 含「停滞」与单元号、无记录、审计含 tool_call；
    保留 500 失败与计划逃逸校验路径。
- 手工 demo（responses 协议）：两轮循环真实跑通，审计四类条目齐全
  （2 request / 6 event / 1 tool_call / 1 tool_result），退出码 0。

## 后续轮的接缝

调度器已持有两棵根与收件箱：第 3 轮并发窗口进入时，worker 变为多 task，
限流记账与去重缓存在收件箱串行段内生长，worker 与工具层不动。
