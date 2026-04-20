# Compute API（zarrow-core）使用说明

本文档说明 `zarrow.compute` 的下游开发接口，面向 `zarrow-compute` 这类独立库。

## 1. 目标与边界

- `zarrow` 当前提供 **compute 框架层**：注册、调度、类型检查、结果类型推导、执行上下文、聚合生命周期。
- `cast/add/filter/sum/...` 等具体 kernel 实现应由下游库（如 `zarrow-compute`）提供。

## 2. 核心类型

- `FunctionRegistry`：函数注册表（按 `FunctionKind` + 名称索引 kernel）。
- `KernelSignature`：输入 arity/type/options 检查 + 结果类型推导。
- `Kernel`：执行函数 +（可选）聚合生命周期。
- `ExecContext`：执行上下文（allocator/registry/config）。
- `Datum`：`array` / `chunked` / `scalar` 三种输入输出封装。

## 3. 类型安全 Options

`KernelExecFn` 已使用类型安全 options：

- `Options.none`
- `Options.cast`
- `Options.arithmetic`
- `Options.filter`
- `Options.custom`

可在 `KernelSignature.options_check` 中限制允许的 options 类型。

## 4. 结果类型推导

`KernelSignature.result_type_fn` 用于返回类型分发：

- `inferResultType(args, options)`：推导输出 `DataType`
- `resolveResultType(...)`：注册表级推导

当 options 或类型不匹配时，会返回 `KernelError`（如 `InvalidOptions` / `NoMatchingKernel`）。
为方便上层库做策略处理，错误模型还包含：

- `Overflow`
- `DivideByZero`
- `InvalidCast`
- `NotImplemented`

建议在算术/转换 kernel 内统一复用：

- `arithmeticDivI64(...)`（除零时返回 `DivideByZero`）
- `intCastOrInvalidCast(...)`（转换失败时返回 `InvalidCast`）

## 5. ScalarValue 语义

`ScalarValue` 已覆盖：

- 整数 / 浮点：`i8..i64`、`u8..u64`、`f16/f32/f64`
- 时间日期：`date32/date64/time32/time64/timestamp/duration`
- interval：`interval_months/interval_day_time/interval_month_day_nano`
- decimal：`decimal32/decimal64/decimal128/decimal256`
- 文本/二进制：`string` / `binary`

其中 `string` / `binary` 为 **借用切片**：

- 可由调用方自行管理生命周期
- 或使用 `ExecContext.dupScalarString` / `dupScalarBinary` 复制到 `arena_allocator`

## 6. ExecContext 执行配置

`ExecConfig` 包含：

- `safe_cast`
- `overflow_mode`（`checked` / `wrapping` / `saturating`）
- `threads`
- `arena_allocator`

初始化：

- `ExecContext.init(allocator, &registry)`（默认配置）
- `ExecContext.initWithConfig(allocator, &registry, config)`

## 7. 聚合生命周期接口

针对状态化聚合（`sum/min/max/grouped`）：

- `AggregateLifecycle.init`
- `AggregateLifecycle.update`
- `AggregateLifecycle.merge`
- `AggregateLifecycle.finalize`
- `AggregateLifecycle.deinit`

调用路径：

- 注册时设置 `Kernel.aggregate_lifecycle`
- 运行时通过 `ctx.beginAggregate(...)` 创建 `AggregateSession`
- 然后 `update/merge/finalize/deinit`

如果 aggregate kernel 未提供生命周期，`beginAggregate` 会返回 `error.MissingLifecycle`。

## 8. 调试辅助

为便于定位问题，框架提供可读诊断：

- `KernelSignature.explainMismatch`
- `KernelSignature.explainInferResultTypeFailure`
- `FunctionRegistry.explainResolveKernelFailure`
- `FunctionRegistry.explainResolveResultTypeFailure`

## 9. 示例与测试

- 示例：`examples/compute_lifecycle.zig`
- 单测入口：`src/compute/core.zig`

## 10. 数组执行工具层（新）

为下游 `zarrow-compute` 提供了通用执行 helper：

- null 传播：
  - `unaryNullPropagates(...)`
  - `binaryNullPropagates(...)`
- scalar broadcast：
  - `inferBinaryExecLen(lhs, rhs)`（支持 scalar 与 array/chunked 自动广播）
- chunked 对齐迭代：
  - `UnaryExecChunkIterator`
  - `BinaryExecChunkIterator`

典型流程：

- 先用 `BinaryExecChunkIterator.init(...)` 创建迭代器
- 每次 `next()` 得到对齐分块（`BinaryExecChunk`）
- 在分块内用 `binaryNullAt` 或 `binaryNullPropagates` 处理 null 语义

## 11. Compute 兼容矩阵（CI）

CI 增加了专门的 compute 对齐门禁（`pyarrow.compute` 对比）：

- 入口脚本：`tools/compute/pyarrow_compute_compat.py`
- Zig 执行器：`zig build compute-compat-check -- <cases.json>`
- 覆盖维度：`numeric` / `nulls` / `boundary` / `chunked`
