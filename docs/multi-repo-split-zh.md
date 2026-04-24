# zarrow 分库方案（compute / flight / parquet）

本文档给出把 `zarrow-compute`、`zarrow-flight`、`zarrow-parquet` 拆分为独立仓库时的依赖与发布约定。

## 1. 总体原则

- `zarrow` 保持为核心数据结构与 IPC 基础库。
- `zarrow-compute`、`zarrow-flight`、`zarrow-parquet` 独立发布，按需依赖 `zarrow`。
- 各子库只扩展自己的能力边界，不回拷核心类型定义。

## 2. 依赖边界

- `zarrow-compute` 依赖 `zarrow.compute` 提供的框架 API（registry/signature/context/lifecycle）。
- `zarrow-flight` 基于 `zarrow` 的 schema/array/ipc 进行传输层实现。
- `zarrow-parquet` 基于 `zarrow` 的 columnar 类型完成 parquet 映射与读写。

## 3. 版本策略

- 建议采用 `zarrow` 作为上游基线，子库按 `minor` 跟随。
- 新增 API：`zarrow` 先发版，再由子库升级依赖并发布。
- 破坏性变更：先在 `zarrow` 做迁移说明，再分阶段升级各子库。

## 4. 仓库初始化建议

建议子库初始目录：

- `src/`
- `examples/`
- `tests/`
- `build.zig`
- `README.md`

## 5. CI 最小建议

每个子库至少保留：

- `zig build test`
- `zig build examples`
- 与 `zarrow` 当前支持版本一致的 Zig matrix（当前以 0.14.x 为准）

另外建议增加一条“与最新 zarrow 兼容”任务，避免依赖漂移。

## 6. 集成验证建议

- `zarrow-compute`：对 `Datum`/`KernelSignature`/`ExecContext` 做端到端 kernel 回归。
- `zarrow-flight`：增加跨语言 IPC smoke（至少 Python + Rust）。
- `zarrow-parquet`：增加 Arrow IPC <-> Parquet 往返一致性用例。

## 7. 开发流程建议

- 先在 `zarrow` 提交核心 API（含文档、测试、example）。
- 再在子库接入并落地真实功能。
- 对跨库变更使用联动 PR（或草稿 PR）减少接口漂移。
