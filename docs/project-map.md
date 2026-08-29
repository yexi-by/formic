# Formic 项目地图

本文面向维护者，说明当前生产入口、模块职责和数据流。它用于快速找到规则真正归属的位置；具体字段和错误类型仍以源码与测试为准。

## 1. 产品边界

Formic 是单个 Rust 二进制程序。一次命令启动一个批处理作业，每个计划单元拥有独立 worker。worker 可以读取自己的分片、只读输入和被允许的工具，但不能直接修改共享业务状态。

调用方负责：

- 准备输入数据；
- 把工作划成独立单元；
- 编写共同任务说明和可选输出 schema；
- 消费、校验并在需要时归并全部结果。

Formic 负责：

- 启动配置与输入校验；
- 有界并发调度；
- 多轮 LLM 与工具循环；
- 上下文预算、缓存、重试和取消；
- 单元结果的原子发布；
- 续跑状态与可审计运行档案。

Formic 不负责自动规划全局目标、跨 worker 协调、全局去重或最终汇总，也不向 worker 提供 shell 和任意文件写入。

## 2. 从命令到结果

`formic run` 使用完整作业数据流：

```text
CLI
 │
 ├─ 读取并校验配置、任务、计划、schema 和冻结 input
 ├─ 建立或核对可恢复作业身份
 ├─ 初始化 MCP，冻结模型可见工具目录
 │
 ▼
活动 worker 窗口
 │
 ├─ 读取当前分片，构造有序文字/图片用户消息
 ├─ 预算上下文，必要时压缩完整工具往返
 ├─ 请求 LLM，验收流式协议事件
 ├─ 最终文本 ────────────────┐
 ├─ 结构化内部提交 ──────────┤
 └─ 普通工具调用              │
       │                      │
       ▼                      │
    Scheduler                 │
       ├─ 内置只读工具         │
       └─ MCP                 │
              │               │
              └─ 工具结果回注 ┘
                              │
                              ▼
                    审计完成并原子发布结果
```

取消令牌贯穿主入口、worker、模型请求、Scheduler、本地读取和 MCP。观察指标不参与准入、恢复或发布判断。

`formic test` 使用独立的轻量连接路径：

```text
CLI → 验证配置 → 一次 LLM 流式请求 → 按名称逐个发现 MCP 工具 → 终端汇总
```

这条路径复用配置、LLM 和 MCP 生产边界，在每个 MCP 项结束时关闭会话，并在终端汇总后完成。计划、作业、Scheduler、worker 和输出目录链路仍专属于 `formic run`。

## 3. 仓库结构

```text
Formic/
├─ src/                    # 生产代码
│  └─ llm/                 # 三种模型协议适配
├─ tests/                  # 进程级端到端测试与测试 MCP
├─ examples/               # 本地 mock、规模实验和信号辅助程序
├─ examples/demo/          # 最小输入、计划和任务示例
├─ docs/                   # 使用与维护文档
├─ AGENTS.md               # 宏观项目维护指南
├─ README.md               # 项目入口
├─ config.example.toml     # 无密钥配置示例
├─ Cargo.toml              # 包、MSRV 和依赖契约
└─ Cargo.lock              # 应用依赖解析结果
```

`dist/`、`target/`、本机 `config.toml`、规模指标和临时目录均被 Git 忽略。它们是本地生成物，不是源码事实。

## 4. 生产模块职责

| 模块 | 唯一职责 | 主要调用关系 |
| --- | --- | --- |
| `main.rs` | CLI、自检、启动顺序、活动 worker 窗口、信号与作业级汇总 | 组合其余模块，不拥有各模块内部规则 |
| `config.rs` | 唯一 TOML、默认值和外部资源参数校验 | 产出已验证的 `AppConfig` |
| `plan.rs` | JSONL 计划解析、分片形状与路径校验 | 产出自然编号 `PlanUnit` |
| `job.rs` | 不可变作业身份、追加式单元状态和续跑选择 | 在任何远端请求前核对现场 |
| `prompt.rs` | 共享 instructions 与首条用户消息文本装配 | 保持跨单元稳定前缀 |
| `image_input.rs` | 图片格式、签名、尺寸、原始字节和来源事实 | 供分片、内置工具、MCP 和 LLM 共用 |
| `worker.rs` | 单单元多轮状态机与发布前结局判断 | 只消费统一 LLM 事件和 Scheduler 响应 |
| `llm/mod.rs` | 协议无关消息、调用门控、HTTP 策略和统一事件 | 分派给三个协议模块 |
| `llm/completions.rs` | Chat Completions 请求与 SSE 转换 | 不处理业务状态 |
| `llm/responses.rs` | Responses 请求、原生输出项重放与 SSE 转换 | 不处理业务状态 |
| `llm/anthropic.rs` | Anthropic Messages 请求与 SSE 转换 | 不处理业务状态 |
| `scheduler.rs` | 冻结工具注册表、唯一执行入口、排队、并发和计时 | 调用内置工具或 MCP |
| `tools.rs` | `search`、`read`、`read_image` 的参数与路径语义 | 不绕过 Scheduler 直接暴露给 worker |
| `mcp.rs` | MCP 发现、会话、传输、取消、结果转换和重连 | 向 Scheduler 提供冻结工具实现 |
| `cache.rs` | input 文字工具的 singleflight 与 LRU | 只缓存调用方标记的完整成功结果 |
| `structured.rs` | 文本/JSON 输出模式、受限 schema 和内部结果提交 | 结构化提交不进入 Scheduler |
| `compaction.rs` | 上下文压缩候选、内部提交与原子历史替换 | 普通工具在压缩请求中不可见 |
| `tokenize.rs` | o200k 本地估算 | 不冒充供应商计费 usage |
| `output.rs` | 结果、运行档案、统计、MCP 图片和原子文件操作 | 拥有公开输出格式与审计校验 |
| `output_access.rs` | `none`/`published` 权限值 | 由 CLI、作业身份、提示和工具共同消费 |
| `metrics.rs` | 可选进程级规模观测 | 只记录事实，不参与业务控制 |

## 5. 规则所有权

修改行为时应先找到下表中的责任位置，避免在调用方增加重复分支。

| 规则 | 责任位置 |
| --- | --- |
| CLI 参数及退出码 | `main.rs` |
| 配置字段、环境覆盖、默认值 | `config.rs` |
| 计划形状与路径合法性 | `plan.rs` |
| 续跑中哪些事实不可变化 | `job.rs` |
| 模型看到的共同说明与分片布局 | `prompt.rs`、`worker.rs` |
| 内部消息和三协议共同语义 | `llm/mod.rs` |
| 单个供应商协议 JSON/SSE 形状 | 对应 `llm/*.rs` |
| 工具名称、来源和唯一执行入口 | `scheduler.rs` |
| 内置工具参数、路径、截断 | `tools.rs` |
| MCP 传输、目录冻结和会话生命周期 | `mcp.rs` |
| 图片校验与媒体事实 | `image_input.rs` |
| 输出 schema 子集与结果验收 | `structured.rs` |
| 历史压缩完整性 | `compaction.rs` |
| 结果原子性、审计和报告格式 | `output.rs` |
| 任务是否适合独立 worker | 调用方；规则说明位于 `docs/task-design.md` |

## 6. 启动阶段数据流

`formic run` 按以下因果顺序启动作业：

1. 解析 CLI，并加载严格配置；
2. 打开 input 目录 capability，确认 output 与 input 不重叠；
3. 打开或创建 output，取得进程级租约；
4. 冻结 input 普通文件及内容摘要；
5. 读取 task、计划和可选 schema；
6. 建立或核对作业清单与结果现场；
7. 若没有待处理单元，直接写本轮汇总；
8. 根据输出权限决定是否打开已发布结果只读 capability；
9. 初始化 MCP 并冻结完整工具目录；
10. 创建 worker 运行目录、Scheduler 和共享作业上下文；
11. 在活动窗口内启动单元，并按计划自然顺序汇总结局。

缺少配置、计划无效、续跑身份变化或 MCP 初始化失败时，不会发出模型请求。纯文本模型的计划图片也在 MCP 和 LLM 请求前拒绝。

## 7. Worker 状态流

一个 worker 的主要路径是：

```text
preparing
  → ready
  → requesting_model
  → interpreting_model
      ├─ 最终文本 → ready_to_publish
      ├─ 结构化提交 → 校验 → ready_to_publish 或 correcting_output
      └─ 普通工具 → waiting_for_tool → ready
```

可重试模型失败进入 `retrying_model`；预计上下文越界进入 `compacting_context`；工具参数错误进入 `correcting_tool_call`。取消或全局停发进入 `stopped`，无法继续则进入 `failed`。

worker 只有在最终结果满足输出契约、审计完成且发布门仍允许时才写结果。单元失败不会创建完成记录。

## 8. 模型边界

内部历史使用协议无关的用户文字/图片、助手正文、工具调用、工具结果和 Responses 原生重放项。协议模块只负责把这些事实翻译成远端请求，并把 SSE 翻译成统一事件。

实际 HTTP body、URL、header、错误正文和无效协议 payload 不进入公开档案。允许审计的是协议无关输入、通过验收的助手正文、工具事实和类型化完成/失败原因。图片 base64 只存在于待发送请求内。

每次请求先用去除图片 base64 的真实协议结构估算文字 token，再增加按尺寸估算的视觉 token。上下文压缩保留首条用户消息，只替换完整且已完成的旧工具往返。

## 9. 工具边界

worker 不持有具体工具实现。所有普通工具调用必须经过冻结的 `ToolRegistry` 和 Scheduler，因而共享：

- 名称与 schema 冻结；
- 有界收件箱与 semaphore；
- 取消与超时；
- 缓存决定；
- 排队、执行和结果统计；
- 统一审计来源。

内部 `formic_submit_result` 和 `formic_submit_compaction` 是 worker 控制通道，不是普通外部工具，不进入 Scheduler。

## 10. 持久状态

输出区中的权威事实分为三类：

| 事实 | 写入方 | 读取方 |
| --- | --- | --- |
| 作业清单 | 首次运行的 `job.rs` | 续跑身份校验 |
| 追加式单元状态 | 每次单元状态转换 | 续跑选择与结果一致性检查 |
| `results/<unit>` | 通过发布门的 worker | 调用方和可选 published 工具 |

运行档案、stats 和 metrics 通常是行为证据，不参与恢复或结果有效性判断。例外是 worker 审计完整性：当前契约要求审计能够完成后才允许发布单元结果。

## 11. 测试与示例入口

| 路径 | 用途 |
| --- | --- |
| `tests/e2e.rs` | 通过真实二进制入口验证三协议、续跑、MCP、图片、取消和失败语义 |
| `tests/fixtures/fake_mcp_stdio.rs` | stdio MCP 测试服务 |
| `examples/mock_llm.rs` | 本地三协议兼容 mock |
| `examples/scale_run.rs` | 大量 worker、工具往返、内存和调度规模实验 |
| `examples/send_ctrlc.rs` | Windows 信号辅助验证 |
| `examples/demo/` | 最小任务、计划和输入示例 |

具体维护步骤和验证矩阵见[维护指南](maintenance.md)。
