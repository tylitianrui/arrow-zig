# zarrow 生态扩展就绪性（Compute 刷新）

> 刷新日期：2026-05-02  
> 范围：compute core 与下游 kernel 扩展边界（zarrow-compute 等）

---

## 0. 执行结论

当前结论：**compute 框架已具备 array/scalar/chunked 三态数据入口，且已提供 chunk-aware permutation 基础能力；下游仓库应在此基础上实现具体业务 kernel。**

- `Datum` 现状是 `array / scalar / chunked` 三态，不再是单 chunk 限制。
- compute core 的职责是：类型与调度框架 + 通用 helper + options 规范；不负责承诺完整函数目录（例如所有 cast/aggregate/sort 内核实现）。
- `sort/take/filter` 方向建议优先复用 core 的 chunk-aware helper，避免下游在 kernel 中先 `concat` 再处理。
- options 家族已含 `sort`（`order/null_placement/nan_placement/stable`），可用于统一排序语义。

---

## 1. 当前 Compute 模块结构（公开 API 视角）

`src/compute/` 已按职责拆分，`core.zig` 作为统一入口导出：

- `core.zig`：统一 public API 导出（类型、注册表、context、helpers）
- `options.zig`：`OptionsTag` 与各 options payload
- `signature.zig`：`KernelSignature` 与匹配/推导逻辑
- `registry.zig`：`FunctionRegistry / Kernel / ExecContext`
- `datum*.zig`：Datum 访问、构建、permutation 辅助
- `iterators.zig`：array/chunked/scalar 执行迭代器
- `helpers.zig`：对外 helper 聚合导出

---

## 2. Datum 当前状态（array/scalar/chunked）

`Datum` 当前定义与行为：

- 变体：`array: ArrayRef`、`scalar: Scalar`、`chunked: ChunkedArray`
- 生命周期：`retain()` / `release()` 覆盖三种变体
- 类型获取：`dataType()` 覆盖三种变体
- 判别与提取：`isArray/isScalar/isChunked`、`asArray/asScalar/asChunked`

这意味着：

- 下游 kernel 可以直接接收 `chunked` 输入；
- 不需要把“支持 chunked”作为前置改造任务；
- 重点应转向“如何高效利用 chunked helper”而非“是否有 chunked 入口”。

---

## 3. Compute Core vs 下游仓库职责边界

### 3.1 compute core 已提供

- 调度与注册框架：`FunctionRegistry`、`Kernel`、`KernelSignature`、`ExecContext`
- 统一错误域：`KernelError`（含 `InvalidOptions/InvalidInput/UnsupportedType` 等）
- 执行辅助：`unary/binary/nary` 迭代与 null 传播 helper
- Datum helper：
  - 访问类：`datumListValueAt`、`datumStructField` 等
  - 构建类：`datumBuildNullLike`、`datumBuildEmptyLike`
  - permutation 类：`datumTake`、`datumTakeNullable`、`datumSelect`、`datumFilterSelectionIndices`、`datumFilterChunkAware`
- chunk-aware 基础能力：
  - `chunkedResolveLogicalIndices`
  - chunked take/filter 路径中的一致索引与 null 处理

### 3.2 下游仓库应负责

- 具体业务 kernel 的语义与实现（例如 sort keys 解释、聚合策略、表达式级优化）
- 函数目录与命名约定（哪些函数作为稳定 API 暴露）
- 性能策略（并行度、局部排序算法、跨 chunk 合并策略）
- 与上层引擎/执行计划的集成

---

## 4. sort / take / filter 扩展建议路径

### 4.1 sort（推荐）

1. 在下游实现 `sort_indices`（array 与 chunked 共用一套逻辑语义）。
2. 复用 core `Datum`/chunk helper 构建逻辑索引到 chunk-local 的映射。
3. 用 `SortOptions` 驱动排序行为，不在 kernel 内硬编码“升序 + nulls last”。
4. 若需要 `array_sort_indices`，建议作为 `sort_indices` 的 array 特化包装，而非复制语义分支。

### 4.2 take（推荐）

- 优先调用 `datumTake` / `datumTakeNullable`。
- 对 chunked 输入保持 chunked 输出，避免“先 concat 再 gather”。
- 对 nullable index 路径沿用 core 的 null 行输出语义。

### 4.3 filter（推荐）

- 谓词到选择向量：`datumFilterSelectionIndices`
- chunk-aware 执行：`datumFilterChunkAware`
- 若需要历史数组输出形态，可使用 `datumFilter`（兼容路径）

### 4.4 一致性检查清单（下游实现时）

- array 与 chunked 输入是否得到一致逻辑结果
- misaligned chunk 边界场景（跨 chunk 连续/非连续索引）
- null 与 NaN 语义是否仅由 options 驱动
- 越界索引与非法 options 是否返回一致错误

---

## 5. Options 家族现状与方向

### 5.1 当前已公开的 options 家族

`OptionsTag`：

- `none`
- `cast`
- `arithmetic`
- `filter`
- `sort`
- `custom`

对应 payload：

- `CastOptions`
- `ArithmeticOptions`
- `FilterOptions`
- `SortOptions`
- `CustomOptions`

### 5.2 SortOptions 当前字段

- `order`: `ascending | descending`
- `null_placement`: `at_start | at_end`
- `nan_placement`: `?at_start | ?at_end`
- `stable`: `bool`

### 5.3 近期开扩方向（兼容当前模型）

- 多 key 排序（上层组合多个 `SortOptions` 或扩展自定义 payload）
- 与执行计划层对齐的排序元数据（例如列选择、比较器配置）
- 在不破坏 `Options.noneValue()` 兼容性的前提下持续扩展

---

## 6. 下游仓库接入建议（简版）

- 第一步：仅依赖 `compute/core.zig` 导出的框架与 helper。
- 第二步：先落地 `sort_indices + take + filter` 三条路径，复用现有 chunk-aware 基建。
- 第三步：把 options 校验放在 `KernelSignature.options_check`，避免在 exec 内散落类型判断。
- 第四步：补齐回归测试：
  - defaults
  - tag dispatch
  - invalid option mismatch
  - array/chunked 一致性

---

## 7. 本次刷新后的事实对齐点

- 文档已与当前 compute public API 对齐：`Datum` 为 `array/scalar/chunked`。
- 已去除“缺失 chunked datum 支持”的旧表述。
- 已明确：compute core 提供框架与 helper，下游仓库实现具体 kernel。
- 已补充：`sort/take/filter` 推荐扩展路径与 options 家族（含 `sort`）。
