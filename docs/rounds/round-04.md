# 第 4 轮：重试预算 + 取消令牌树

> 状态：**已完成并验证**（2026-08-07）。本轮档案同 round-01 模板。

## 目标（可观察结果）

- 单次 LLM 调用的瞬时故障（HTTP 429/5xx、连接错误、流断裂、协议错误、
  模型参数非法 JSON）以同一历史重发，预算 3 次（内部常量 `RETRY_BUDGET`），
  耗尽判失败，诊断含直接原因与重试次数；每次尝试独立落审计（`attempt` 序号）；
- Ctrl+C（或 Windows 的 Ctrl+Break）触发取消令牌树：停止接纳新单元、在途
  单元收敛、已发布记录保留、汇总含取消数、退出码 3；第二次信号立即退出。

## 停止线（本轮不做什么）

- 无历史压缩/token 预算（第 5 轮实测后）；
- 工具执行不重试、不被取消（search 本地幂等、短时；取消只解除等待）；
- 无调度器 RPM 记账/去重（第 3 轮结论不变）；无 web fetch。

## 契约（本轮新增/变更）

- **可重试集合**：Transport、Http(429|5xx)、Protocol（含流中无完成事件、
  finish 声称工具调用但无内容、参数非法 JSON——重发后模型可重新生成）。
  **不可重试**：Http(4xx 其他，配置错误)、MaxTokens、Stalled、EmptyOutput。
  退避固定 `1s × 尝试序`（无实测证据，不引入指数/抖动）。
- **审计**：request 条目带 `attempt`（1 起始）；失败/取消单元的已收集条目
  照常落盘——证据不是业务状态，取消现场可定位。
- **取消**：`run_unit` 返回 `Outcome::{Published, Cancelled}`；取消不是失败，
  不留产出记录（§8 在途丢弃）。汇总文案追加「取消 N（作业已被终止）」。
- **退出码**：0 全部成功；1 存在失败；2 启动失败；3 被终止（新增）。

## 模块改动

- `src/worker.rs`：`run_unit(ctx, unit, token)`；三处取消点（LLM 事件流、
  工具等待、退避 sleep，全部 `select!` 竞争）；`one_turn` 回合函数（发调用、
  收事件、解释结局、参数一次性校验）；重试循环与可重试分类；`Outcome`；
  `RetriesExhausted` 诊断。
- `src/main.rs`：`termination_signal()`（Windows 同时监听 Ctrl+C 与
  Ctrl+Break）→ 根令牌；spawn 前检查 + acquire 竞争取消；二次信号
  exit(130)；退出码 3。
- `src/output.rs`：`AuditEntry::LlmRequest` 带 `attempt`；`Summary` 加
  `cancelled`。
- `examples/send_ctrlc.rs`（新）：以新进程组拉起命令并发送
  CTRL_BREAK_EVENT（Windows 验证工具；新进程组的 CTRL+C 默认禁用而
  BREAK 恒启用）。
- `src/llm/` / `scheduler.rs` / `tools.rs` / `plan.rs` / `prompt.rs`：零改动
  （取消经 `select!` drop future 实现，协议层无感知）。

## 决策与设计张力记录

- **验证分工**：取消链路（信号→令牌→收敛）由 worker 单测（慢 server +
  手动 cancel）与 send_ctrlc 手工验证覆盖；Git Bash 的 `kill -INT` 无法向
  Windows 原生进程投递控制台事件（实测：进程跑到正常结束后才报 130），
  故不做脆弱的进程信号 e2e。
- **Ctrl+Break 一并监听**：Windows 验证工具只能向新进程组发 BREAK；同时
  它本身是控制台用户的合法终止手段。这是为可验证性做的最小产品扩展。
- **重试粒度为单次调用而非单元**：历史不变重发，已收到的部分增量丢弃
  （审计保留原文）；工具结果已回注的历史不受影响。

## 验证证据

- `cargo test`：57 项全绿（48 单元 + 9 端到端）；clippy 零警告。
  - 单元新增：慢 server + 100ms 后 cancel → `Outcome::Cancelled`、无记录、
    审计落盘；400 → 不重试（1 次请求、诊断无「重试」）；500 → 重试到预算
    （3 次请求、「重试 2 次后仍失败」）。
  - e2e 新增：FLAKY 标记（首次 500 后恢复）→ 重试成功、3 次请求、审计含
    `attempt` 1/2；既有 500 失败路径更新为耗尽预算（5 次请求、stderr 含
    「重试 2 次」）。
- 手工验证（send_ctrlc + 慢 mock 8 单元并发 2）：3s 时发送终止事件 →
  「收到终止信号」→ 单元 1/2 取消、单元 3–8 未创建、汇总「完成 0，失败 0；
  取消 2（作业已被终止）」、退出码 3、输出区仅 audit/（在途丢弃）且取消
  单元的审计留痕。

## 后续轮的接缝

第 5 轮规模验证：mock 驱动 5000 worker，观测内存曲线（历史体积为主导项的
假设）、调度器记账路径与窗口行为，为历史压缩/窗口默认值提供实测值。运行时
结构（窗口、令牌树、调度器）本轮已就绪，规模实验不改结构只改参数与观测。
