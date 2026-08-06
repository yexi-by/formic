# 第 6 轮：单元统计视图（stats.jsonl 与 token 内部估算）

> 状态：**已完成并验证**（2026-08-07）。小轮次，动机来自首个真实任务：
> 调用方需要按单元看到 token 消耗与工具调用总结——「工具调用积极性是指标
> 的一部分」。

## 目标（可观察结果）

输出区新增 `out/stats.jsonl`，一行一单元（含失败与取消）：`outcome`、
`turns`、`llm_calls`、`retries`、`tool_calls`（分工具计数）、
`input_tokens_est`、`output_tokens_est`。调用方据此计算工具调用积极性、
token 成本分布等作业级指标，无需解析审计原文。

## 关键决策

1. **token 是内部估算值，不采供应商 usage**（用户拍板）：三协议的 usage
   字段含义各异（completions 还需 stream_options 才有，网关未必支持），
   内部估算保证「一个数字一个含义」且零请求兼容风险。实现参考 codex 等
   开源工具：tiktoken o200k BPE（`src/tokenize.rs`），按内部消息模型
   `Message` 的内容估算 + 每消息固定开销 4——协议无关，三协议天然共用。
   文档注明：用于预算与指标分析，不是计费依据。
2. **估算成本 O(1) 摊还**：历史逐消息只在进入时算一次（`history_tokens`
   累计），每次调用的 input = instructions + 当前历史合计，不重复全量
   编码（避免与审计同形的二次增长）。
3. **stats 是派生视图**：权威事实在审计（§14）；stats 由运行时在单元结束
   时聚合（§10 使用 时计算），写失败只产生诊断，不改写单元业务结果（§9）。
   完整对话内容仍在审计 request 条目（每回合含全量历史），本轮不另建查看器。
4. **计入口径**：input 含每次调用的全量历史（重试也计）；output 只计成功
   回合的文本与工具调用（失败尝试的半成品增量不计，属估算简化，已注明）。

## 模块改动

- `src/tokenize.rs`（新）：tiktoken o200k BPE 估算（count / count_message /
  count_tool_call）；
- `src/output.rs`：`UnitStats` 类型与 `append_stats`（stats.jsonl 格式所有者）；
- `src/worker.rs`：`Metering` 结构合并字节计量与 token 计量，`run_unit`
  新增 `stats: &mut UnitStats` 参数；
- `src/main.rs`：任务元组携带 stats，join 循环按结局追加 stats 行；
- 协议层零改动（估算不依赖响应字段）。

## 验证证据

- `cargo test`：61 项全绿（52 单元 + 9 端到端）；clippy 零警告。
  - 单元：tokenize 中文/空串/消息开销/工具调用计数；
  - 端到端：三协议主成功断言 stats 行（published、2 轮、2 调用、search×1、
    token > 0）；失败单元 failed 且 llm_calls 3 / retries 2；FLAKY 路径
    retries 1；停滞路径 search×3。
- 真实供应商冒烟（deepseek-v4-flash，2 单元）：stats.jsonl 正确产出
  （input ≈ 4.0k/4.3k est，output ≈ 200 est，1 轮无工具调用，与行为一致）。

## 后续轮的接缝

- 历史预算（2 MB，第 5 轮实测值）实现时可直接复用 `history_tokens` 与
  `tracked` 两个累计量；
- JoinSet 批量结算滞后（首个真实任务观察到的 done 曲线滞后）仍未处理，
  记入下轮候选。
