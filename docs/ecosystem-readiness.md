# zarrow 生态扩展就绪性分析报告

> 分析日期：2026-04-18  
> 目标：评估 zarrow（Arrow 核心库）的现状，明确开发 zarrow-compute、zarrow-parquet 等下游库时，核心库需要补充哪些接口。

---

## 目录

0. [执行结论](#0-执行结论)
1. [当前模块全景](#1-当前模块全景)
2. [已满足的基础能力（可直接复用）](#2-已满足的基础能力可直接复用)
3. [zarrow-compute 需求分析](#3-zarrow-compute-需求分析)
4. [zarrow-parquet 需求分析](#4-zarrow-parquet-需求分析)
5. [两个库共同依赖的基础缺口](#5-两个库共同依赖的基础缺口)
6. [build.zig 模块拆分](#6-buildzigg-模块拆分)
7. [优先级汇总与路线图](#7-优先级汇总与路线图)
8. [两周落地计划（建议）](#8-两周落地计划建议)

---

## 0. 执行结论

结论分级：**基础能力可用，生态能力未就绪**。

- 可以开工 `zarrow-compute` / `zarrow-parquet`，但需要先补齐核心抽象（`ChunkedArray` / `Table` / `Datum.chunked` / `DataType` 工具谓词）。
- 当前 `compute` 层是注册与调度框架，不是内核实现层；下游库应承接具体 kernel。
- 当前 `zarrow` 仍是单模块（携带 IPC 依赖），对 `zarrow-parquet` 这种非 IPC 库不够友好，建议拆分 `zarrow-core`。

---

## 1. 当前模块全景

```
zarrow/src/
├── buffer.zig            SharedBuffer / OwnedBuffer（引用计数 + 64 字节对齐）
├── bitmap.zig            ValidityBitmap / MutableValidityBitmap（LSB 位图）
├── datatype.zig          DataType union（44+ 类型）+ Field + Schema metadata
├── schema.zig            Schema / SchemaRef（引用计数 arena 深拷贝）
├── record_batch.zig      RecordBatch + RecordBatchBuilder
├── array/
│   ├── array_data.zig    ArrayData（物理布局 + validateLayout）
│   ├── array_ref.zig     ArrayRef（引用计数句柄 + slice）
│   ├── primitive_array / boolean_array / string_array / binary_array
│   ├── list_array / struct_array / fixed_size_array / dictionary_array
│   ├── advanced_array    （union / map / run-end-encoded / view）
│   └── extension_array
├── ipc/
│   ├── stream_writer / stream_reader  （IPC Arrow Stream 格式）
│   ├── file_writer / file_reader      （IPC Arrow File 格式）
│   ├── tensor_types.zig               （Tensor / SparseTensor 元数据）
│   └── compression_dynlib.zig         （zstd / lz4 动态加载）
├── ffi/
│   ├── c_data.zig         ArrowSchema / ArrowArray（C Data Interface）
│   └── c_stream.zig       ArrowArrayStream
└── compute/
    ├── core.zig           FunctionKind / ScalarValue / Datum / FunctionRegistry / ExecContext
    └── mod.zig            公开导出
```

### 公开 API 统计（src/root.zig）

| 类别 | 符号数 |
|---|---|
| Buffer / Bitmap | 4 |
| DataType 相关（TypeId、参数结构体等） | 18 |
| Schema / SchemaRef | 2 |
| RecordBatch + Builder + 错误集 | 4 |
| Array 类型（含 Builder，全 44 种） | ~80 |
| IPC（Stream/File 读写、Tensor、压缩） | 18 |
| FFI（C Data Interface） | 10 |
| Compute 框架骨架 | 11 |
| **合计** | **~147** |

---

## 2. 已满足的基础能力（可直接复用）

下列能力在当前版本中较成熟，下游库可直接复用。  
注意：这不等于“生态层能力已齐备”，仍需补齐第 3～7 节列出的核心缺口。

| 能力 | 状态 | 备注 |
|---|---|---|
| 全部 Arrow 数据类型定义 | ✅ | 44 个 TypeId，DataType union，`eql()`/`name()`/`id()` |
| Array 物理布局 + 验证 | ✅ | `ArrayData.validateLayout()` 覆盖所有布局规则 |
| 全部数组类型 + Builder | ✅ | 原始/bool/string/binary/list/struct/dict/union/map/REE/view/fsb/fsl |
| 引用计数内存模型 | ✅ | `ArrayRef` / `SchemaRef` atomic retain/release，线程安全 |
| `RecordBatch` | ✅ | `init`/`initBorrowed`/`deinit`/`slice`/`columnByName`/`numRows`/`numColumns` |
| `RecordBatchBuilder` | ✅ | `setColumn`/`setColumnByName`/`finish` |
| IPC Arrow Stream 读写 | ✅ | 压缩/字节序/dict delta/Tensor/SparseTensor 均已实现 |
| IPC Arrow File 读写 | ✅ | footer 索引/Tensor block/custom_metadata 均已实现 |
| C Data Interface（FFI） | ✅ | 完整 `ArrowSchema`/`ArrowArray`/`ArrowArrayStream` import/export |
| Compute 框架骨架 | ✅ | `FunctionRegistry`/`Kernel`/`ExecContext`/`Datum` 框架已就位 |

---

## 3. zarrow-compute 需求分析

### 3.1 zarrow-compute 的职责

- 类型转换（cast）
- 算术运算（add / subtract / multiply / divide）
- 比较运算（equal / less / greater …）
- 布尔逻辑（and / or / not）
- 聚合（sum / min / max / count / mean）
- 向量化筛选（filter / take）
- 排序（sort_indices）

所有这些 kernel 均在 zarrow 提供的 `FunctionRegistry` + `Datum` 框架之上实现。

当前代码现状补充：

- `src/compute/core.zig` 与 `src/compute/mod.zig` 已提供框架导出。
- `cast/add/filter` 等内核尚未作为核心库稳定 API 对外导出，建议放在 `zarrow-compute` 实现，并由 zarrow 仅提供接口层与基础数据结构。

### 3.2 当前已满足的接口

| 接口 | 用途 |
|---|---|
| `ArrayRef` / `ArrayData.buffers` | kernel 直接读写物理数据 |
| `DataType` + `.eql()` | type dispatch |
| `FunctionRegistry` / `Kernel` / `ExecContext` | kernel 注册与调用入口 |
| `Datum{ array, scalar }` | 当前单 chunk 列传递 |
| `RecordBatch.column()` / `columnByName()` | 按列取数据 |
| `RecordBatch.slice()` | 向量化批次切片 |
| `ValidityBitmap` / `MutableValidityBitmap` | null 处理 |

### 3.3 需要在 zarrow 补充的接口

#### ① `ChunkedArray`（P0，最高优先级）

`filter` / `cast` / `sort` 等操作的输出可能跨多个 chunk，当前 `Datum` 只能装单个 `ArrayRef`。  
需要在 `src/chunked_array.zig` 新建：

```zig
/// 多 chunk 列，同一逻辑类型，共享所有权。
pub const ChunkedArray = struct {
    data_type: DataType,
    chunks: []ArrayRef,   // 每个 chunk 独立 retain
    total_len: usize,

    pub fn init(allocator, data_type, chunks) !ChunkedArray
    pub fn deinit(self: *ChunkedArray) void
    pub fn fromSingle(arr: ArrayRef) ChunkedArray   // 包装单 chunk（零分配）
    pub fn chunk(self, i: usize) ArrayRef
    pub fn len(self) usize
    pub fn numChunks(self) usize
    pub fn retain(self) ChunkedArray
    pub fn release(self: *ChunkedArray) void
};
```

#### ② `Datum` 扩展 `chunked` 变体（P0）

当前：

```zig
pub const Datum = union(enum) { array: ArrayRef, scalar: Scalar };
```

聚合 kernel（`sum(ChunkedArray) → Scalar`）和列式扫描需要 `chunked` 输入：

```zig
// 需扩展为：
pub const Datum = union(enum) {
    array:   ArrayRef,
    scalar:  Scalar,
    chunked: ChunkedArray,    // 新增
};
```

同步更新 `Datum.retain()` / `Datum.release()` / `Datum.dataType()`。

#### ③ `ScalarValue` 类型不完整（P1）

当前只有 5 种变体：

```zig
pub const ScalarValue = union(enum) { null, bool: bool, i64: i64, u64: u64, f64: f64 };
```

`sum(Int8Array)` 返回 `i8` scalar，`min(StringArray)` 返回字符串 scalar，均无法表示。需补全：

```zig
pub const ScalarValue = union(enum) {
    null,
    bool:      bool,
    i8:        i8,
    i16:       i16,
    i32:       i32,
    i64:       i64,
    u8:        u8,
    u16:       u16,
    u32:       u32,
    u64:       u64,
    f16:       f16,
    f32:       f32,
    f64:       f64,
    decimal128: i128,
    string:    []const u8,   // arena 管理，借用生命周期
    binary:    []const u8,
};
```

#### ④ `DataType` 工具谓词函数（P1）

kernel 做 type dispatch 时需要按类别判断，当前 `DataType` 只有 `.id()` / `.name()` / `.eql()`。  
在 `src/datatype.zig` 的 `DataType` 结构体中补充：

```zig
/// 整数类型：int8..int64 / uint8..uint64
pub fn isInteger(self: DataType) bool

/// 浮点类型：half_float / float / double
pub fn isFloating(self: DataType) bool

/// 数值类型：isInteger or isFloating
pub fn isNumeric(self: DataType) bool

/// 十进制类型：decimal32/64/128/256
pub fn isDecimal(self: DataType) bool

/// 字符串/二进制类型：string/binary/large_string/large_binary/
///                      string_view/binary_view/fixed_size_binary
pub fn isStringLike(self: DataType) bool

/// 时间相关类型：date32/64/time32/64/timestamp/duration/interval
pub fn isTemporal(self: DataType) bool

/// 嵌套类型：list/large_list/struct/map/union/dict/ree/extension
pub fn isNested(self: DataType) bool

/// 固定宽度类型的位宽；可变宽度类型返回 null
pub fn bitWidth(self: DataType) ?usize

/// 去掉 dictionary / extension 包装，返回物理存储类型
pub fn physicalType(self: DataType) DataType
```

上述函数全为纯计算，无内存分配，加在 `DataType` union 末尾即可。

#### ⑤ `KernelError` 不完整（P1）

当前：

```zig
pub const KernelError = error{ OutOfMemory, FunctionNotFound, InvalidArity, NoMatchingKernel };
```

实际 kernel 实现（safe cast、filter、arithmetic overflow）会产生更多错误：

```zig
pub const KernelError = error{
    OutOfMemory,
    FunctionNotFound,
    InvalidArity,
    NoMatchingKernel,
    InvalidOptions,    // 选项类型错误
    InvalidInput,      // 输入数据错误（如 safe cast overflow）
    UnsupportedType,   // 该 kernel 不支持此 DataType 组合
};
```

---

## 4. zarrow-parquet 需求分析

### 4.1 zarrow-parquet 的职责

- 读取 Parquet 文件：Footer 解析 → RowGroup 扫描 → Column Chunk 解码 → 输出 `Table`
- 写入 Parquet 文件：接收 `Table` / `[]RecordBatch` → 编码 → 写出 RowGroup
- 类型映射：Parquet physical type ↔ Arrow logical type

> **注意**：zarrow-parquet **完全不依赖** IPC（flatbufferz），只依赖 zarrow 的 Array、Schema、RecordBatch 层。

### 4.2 当前已满足的接口

| 接口 | 用途 |
|---|---|
| `DataType` 全部类型 | Parquet physical type → Arrow logical type 映射 |
| `Field` / `Schema` / `SchemaRef` | 从 Parquet schema 树构建 Arrow schema |
| 全部 Builder | 按 Column Chunk 解码后填充列数据 |
| `RecordBatch` | 单 RowGroup 输出 |
| `SharedBuffer` / `OwnedBuffer` | zero-copy page buffer 管理 |
| `ValidityBitmap` / `MutableValidityBitmap` | definition level → validity bitmap |

### 4.3 需要在 zarrow 补充的接口

#### ① `ChunkedArray` + `Table`（P0）

Parquet 的天然读取单元是 RowGroup → Column Chunk；一个文件读出来是 `Table`，不是单个 `RecordBatch`。

需在 `src/table.zig` 新建：

```zig
/// 多 RowGroup 组成的完整表，每列为一个 ChunkedArray。
pub const Table = struct {
    allocator: std.mem.Allocator,
    schema_ref: SchemaRef,
    columns: []ChunkedArray,
    num_rows: usize,

    pub fn init(allocator, schema_ref, []ChunkedArray) !Table
    pub fn deinit(self: *Table) void
    pub fn column(self, i: usize) ChunkedArray
    pub fn columnByName(self, name: []const u8) ?ChunkedArray
    pub fn numRows(self) usize
    pub fn numColumns(self) usize
    pub fn select(self, field_names: []const []const u8) !Table   // projection
    pub fn slice(self, offset, length: usize) !Table
    pub fn toRecordBatches(self, allocator, max_chunk_rows: usize) ![]RecordBatch
};
```

#### ② `RecordBatchReader` 流式接口（P1）

IPC reader 和 Parquet reader 需要一个统一的流式读取抽象，供上层（SQL 引擎、ETL pipeline）面向接口编程。

新建 `src/record_batch_reader.zig`：

```zig
/// 流式 RecordBatch 迭代器 vtable 接口。
pub const RecordBatchReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// 返回流的 schema（不转移所有权）。
        schema:    *const fn (*anyopaque) *const Schema,
        /// 读取下一批；返回 null 表示流结束；调用方负责 deinit。
        readNext:  *const fn (*anyopaque, allocator: std.mem.Allocator) anyerror!?RecordBatch,
        /// 关闭并释放底层资源。
        deinit:    *const fn (*anyopaque) void,
    };

    pub fn schema(self: RecordBatchReader) *const Schema
    pub fn readNext(self, allocator) !?RecordBatch
    pub fn deinit(self: *RecordBatchReader) void
    /// 默认实现：drain 整个流，返回所有批次。
    pub fn readAll(self, allocator) ![]RecordBatch
};
```

#### ③ `DataType` 工具函数（与 compute 共享，P0/P1）

Parquet encoding 决策需要：

| 函数 | 用途 |
|---|---|
| `bitWidth(dt) ?usize` | 确定 PLAIN 编码的每元素字节数 |
| `physicalType(dt) DataType` | unwrap dict/extension，得到真实存储类型 |
| `isInteger(dt) bool` | 决定是否使用 DELTA_BINARY_PACKED 编码 |
| `isFloating(dt) bool` | 决定是否使用 BYTE_STREAM_SPLIT 编码 |

#### ④ `Schema` 工具方法（P2）

zarrow-parquet projection（只读部分列）需要：

```zig
// 加在 schema.zig 的 Schema struct 中
pub fn fieldIndex(self: Schema, name: []const u8) ?usize
pub fn eql(self: Schema, other: Schema) bool

// 加在 SchemaRef 中
/// 按列索引投影，返回新 SchemaRef（arena 深拷贝）。
pub fn project(self: SchemaRef, allocator: std.mem.Allocator, indices: []const usize) !SchemaRef
```

---

## 5. 两个库共同依赖的基础缺口

| 缺口 | zarrow-compute | zarrow-parquet |
|---|---|---|
| `ChunkedArray` | ✅ 需要 | ✅ 需要 |
| `DataType.bitWidth` | ✅ | ✅ |
| `DataType.physicalType` | ✅ | ✅ |
| `DataType.isInteger/isFloating/isNumeric/…` | ✅ | ✅ |
| `Schema.fieldIndex` / `Schema.eql` / `SchemaRef.project` | 可选 | ✅ 需要 |

---

## 6. build.zig 模块拆分

### 当前问题

zarrow 目前是**单一模块**，强制携带 `flatbufferz` / `arrow_fbs` 依赖：

```zig
// 当前 build.zig（简化）
const zarrow = b.addModule("zarrow", .{
    .root_source_file = b.path("src/root.zig"),
});
zarrow.addImport("flatbufferz", flatbufferz_mod);
zarrow.addImport("arrow_fbs",   arrow_fbs_mod);
```

zarrow-parquet **不需要** IPC，但被迫引入 flatbufferz，徒增依赖和编译时间。

### 解决方案：增加 `zarrow-core` 模块

```zig
// 新增：无 IPC 依赖的纯核心模块
const zarrow_core = b.addModule("zarrow-core", .{
    .root_source_file = b.path("src/core.zig"),
    // 不 addImport flatbufferz / arrow_fbs
});
```

新建 `src/core.zig`，只导出非 IPC 部分：

```zig
// src/core.zig — 无 flatbufferz 依赖
pub const SharedBuffer  = @import("buffer.zig").SharedBuffer;
pub const OwnedBuffer   = @import("buffer.zig").OwnedBuffer;
pub const ValidityBitmap = @import("bitmap.zig").ValidityBitmap;
pub const MutableValidityBitmap = @import("bitmap.zig").MutableValidityBitmap;
pub const DataType      = @import("datatype.zig").DataType;
// ... 所有 DataType 相关导出 ...
pub const Schema        = @import("schema.zig").Schema;
pub const SchemaRef     = @import("schema.zig").SchemaRef;
pub const RecordBatch   = @import("record_batch.zig").RecordBatch;
// ... 所有 Array 类型 + Builder ...
pub const ChunkedArray  = @import("chunked_array.zig").ChunkedArray;
pub const Table         = @import("table.zig").Table;
pub const RecordBatchReader = @import("record_batch_reader.zig").RecordBatchReader;
// compute framework（不含 IPC）
pub const ComputeFunctionKind  = @import("compute/mod.zig").FunctionKind;
// ...
```

下游库引用方式：

```zig
// zarrow-parquet/build.zig
const zarrow_dep = b.dependency("zarrow", .{});
// 只引入核心，无 flatbufferz 负担
my_module.addImport("zarrow", zarrow_dep.module("zarrow-core"));

// zarrow-compute/build.zig（同上）
my_module.addImport("zarrow", zarrow_dep.module("zarrow-core"));

// 如果某个库需要 IPC，引入完整模块
my_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

---

## 7. 优先级汇总与路线图

### P0 — 开工即需，无法绕过

| 任务 | 文件 | 说明 |
|---|---|---|
| 新增 `ChunkedArray` | `src/chunked_array.zig` | compute + parquet 均依赖 |
| 新增 `Table` | `src/table.zig` | parquet 核心输出格式 |
| `DataType` 工具谓词 | `src/datatype.zig` | `isInteger/isFloating/isNumeric/isDecimal/isStringLike/isTemporal/isNested/bitWidth/physicalType` |
| `Datum` 增加 `chunked` 变体 | `src/compute/core.zig` | compute 聚合 kernel 输入 |
| 新建 `zarrow-core` 模块 | `build.zig` + `src/core.zig` | parquet 不依赖 flatbufferz |

### P1 — 第一个 kernel / 第一个 RowGroup 完成前需要

| 任务 | 文件 | 说明 |
|---|---|---|
| `ScalarValue` 补全 | `src/compute/core.zig` | i8/i16/i32/u8/u16/u32/f32/f16/string/binary/decimal128 |
| `KernelError` 补全 | `src/compute/core.zig` | `InvalidOptions` / `InvalidInput` / `UnsupportedType` |
| 新增 `RecordBatchReader` vtable | `src/record_batch_reader.zig` | IPC + Parquet 统一读取接口 |

### P2 — 投影查询、schema 比较时需要

| 任务 | 文件 | 说明 |
|---|---|---|
| `Schema.fieldIndex` | `src/schema.zig` | 按名查列索引 |
| `Schema.eql` | `src/schema.zig` | schema 相等性判断 |
| `SchemaRef.project` | `src/schema.zig` | 按索引列投影，返回新 SchemaRef |

### 不需要改动的部分

以下内容完整稳定，下游库直接使用：

- `ArrayData` 物理布局 + `validateLayout` — 所有 layout 规则已实现
- `ArrayRef` 引用计数 + `slice` — 跨 chunk 共享内存零拷贝
- `RecordBatch.initBorrowed` / `columnByName` / `slice` — 已实现
- 全部 44 种 Builder — zarrow-parquet 直接复用
- IPC 全部读写（Stream / File / Tensor） — zarrow-ipc 或 zarrow 本体可复用
- C Data Interface（FFI） — 所有下游库均可通过 FFI 与 C/Python/Rust 互操作

---

## 8. 两周落地计划（建议）

### Week 1（先打通核心抽象）

目标：让 `zarrow` 具备被 `zarrow-compute` / `zarrow-parquet` 共同复用的最小内核接口。

任务：

1. 新增 `src/chunked_array.zig`
2. 新增 `src/table.zig`
3. 扩展 `src/compute/core.zig`：`Datum.chunked`、`ScalarValue`、`KernelError`
4. 补齐 `src/datatype.zig` 工具函数：`isInteger/isFloating/isNumeric/isDecimal/isStringLike/isTemporal/isNested/bitWidth/physicalType`
5. `src/root.zig` 导出新增符号，并补对应测试入口

交付物：

- `zig build test` 通过
- `docs/` 新增 `chunked/table` 使用示例（最少 1 个）
- 可运行一个最小 demo：`ChunkedArray -> Datum.chunked -> 空壳 kernel 调用`

验收标准：

- `ChunkedArray` / `Table` 的 retain/release 不泄漏（测试分配器通过）
- `DataType` 工具函数覆盖全部 TypeId，行为可预测（含 dict/extension unwrap）

### Week 2（模块边界与下游接入）

目标：降低依赖耦合，给 `zarrow-compute` / `zarrow-parquet` 提供稳定接入点。

任务：

1. `build.zig` 新增 `zarrow-core` 模块
2. 新增 `src/core.zig`（仅导出非 IPC 能力）
3. 新增 `src/record_batch_reader.zig`（统一流式读取接口）
4. `schema.zig` 增补：`Schema.fieldIndex` / `Schema.eql` / `SchemaRef.project`
5. 编写下游接入样例：
   - `zarrow-compute` 依赖 `zarrow-core` 完成一个 `add` kernel
   - `zarrow-parquet` 依赖 `zarrow-core` 完成一个 `Table -> []RecordBatch` 演示

交付物：

- `zarrow` 暴露双模块：`zarrow`（全量）与 `zarrow-core`（无 IPC 依赖）
- `tools/` 或 `examples/` 提供 2 个下游接入示例

验收标准：

- 非 IPC 项目只引入 `zarrow-core` 即可编译通过
- `RecordBatchReader` 可同时包裹 IPC reader 与 mock parquet reader
- `Schema.project` 可支撑列投影并保持元数据一致性
