# Formic

Formic 是面向大量独立语义任务的 Rust CLI。调用方提供数据目录、JSONL 分片计划和
任务说明，Formic 为每个单元运行独立的多轮 `LLM ↔ 工具` 会话，并负责并发、限流、
取消、缓存、上下文压缩、原子发布和 worker 运行档案。

Formic 不限制单元总量、总回合数或工具调用总数。`execution.max_concurrent_units` 限制
同时活动的 worker，`--concurrency` 只对本次运行显式覆盖；工具调度器再按用户配置限制
当前在途调用。并发窗口不是请求频率限制，需要时另用 `requests_per_minute`。

## 什么时候使用 Formic

Formic 的工作单元必须能够独立完成、独立重试、独立发布。一次调用最好同时满足：

- 同一份任务规则适用于大量文件、记录或行区间；
- 每个单元有明确且有限的主处理范围；
- 单元可以查阅完整输入或外部工具来核实自己的结论，但不依赖其他 worker 的结果；
- 调整并发、完成顺序或只重试一个单元，不会改变其他单元结果的含义；
- 单元结果本身有用，部分单元失败不应抹掉已经确认的进展。

它适合需要逐单元判断、抽取、分类、核验或转换的大批量任务。全局去重、统一命名、总排名、
跨单元最终汇总和依赖前一步结果的工作，不是同一批独立 worker 的职责；这类步骤应由调用方
在单元结果齐备后统一处理。完整判断方法和通用提示词模板见
[任务设计与提示词](docs/task-design.md)。

## 快速开始

需要 Rust 1.88 或更高版本。复制 [`config.example.toml`](config.example.toml)，填写模型
信息，并设置 `FORMIC_LLM_PROTOCOL`。配置可以留在固定位置，不需要复制到作业目录。

```bash
cargo build --release

formic run \
  --data <数据集目录> \
  --plan <plan.jsonl> \
  --task <task.md> \
  --out <输出目录> \
  --config <config.toml>
```

省略 `--config` 时读取当前目录的 `config.toml`；显式指定的文件不存在会直接报错。结构化输出额外传入
`--output-schema <schema.json>`；需要继续同一输出区时增加 `--resume`。已发布结果不会覆盖，
续跑只处理失败、停止和未开始的单元，并在请求前确认 plan、task、schema 与 input 未变。

每个 worker 结束后都会生成：

```text
out/results/<worker编号>.md
out/runs/run-000001/workers/<worker编号>.md
out/runs/run-000001/stats.jsonl
out/runs/run-000001/summary.json
```

档案包含运行状态、触发条件、协议无关的模型输入、验收后的模型响应事实、工具调用、缓存、
重试、压缩、校验和最终结局。HTTP 错误正文和无效协议负载不会进入终端或档案；成功生成
档案后不再保留重复的 audit JSONL。

## 文档

- [完整使用说明](docs/usage.md)
- [任务设计与提示词](docs/task-design.md)
- [Worker 可观测性](docs/observability.md)
- [当前设计](docs/design.md)
- [模块拓扑](docs/topology.html)
- [实现与验证记录](docs/rounds/)

## 开源协议

[GNU Affero General Public License v3.0](LICENSE)。Copyright (C) 2026 yexi。
