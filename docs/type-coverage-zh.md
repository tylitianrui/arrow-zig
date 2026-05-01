# 类型覆盖矩阵

这份矩阵用于说明 `zarrow` 当前数据类型覆盖情况，依据是源码中的公开 API、实现、测试和示例。

说明：

- `已声明`：`DataType` 中存在该逻辑类型
- `已实现`：导出了对应 Array/Builder API
- `已测试`：在 `src/array/*` 或 IPC 读写测试中有单元测试
- `有示例`：`examples/*` 下有可运行示例

## 1. 定长原始类型

| 类型组 | 已声明 | 已实现 | 已测试 | 有示例 | 证据 |
| --- | --- | --- | --- | --- | --- |
| 有符号整数 (`Int8/16/32/64`) | 是 | 是（`Int8Builder`...`Int64Builder`） | 走同一套 primitive 泛型路径并有测试 | `Int32` 示例 | `src/datatype.zig`, `src/array/array.zig`, `src/array/primitive_array.zig`, `examples/primitive_builder.zig` |
| 无符号整数 (`UInt8/16/32/64`) | 是 | 是（`UInt8Builder`...`UInt64Builder`） | 走同一套 primitive 泛型路径并有测试 | 暂无单独示例 | `src/datatype.zig`, `src/array/array.zig`, `src/array/primitive_array.zig` |
| 浮点 (`Half/Float32/Float64`) | 是 | 是（`HalfFloatBuilder`, `Float32Builder`, `Float64Builder`） | 有 half-float alias 测试 | `Float32` 在 union 示例中使用 | `src/array/array.zig`, `examples/union_builder.zig` |
| 布尔 (`Bool`) | 是 | 是（`BooleanBuilder`） | 是 | 是 | `src/array/boolean_array.zig`, `examples/boolean_builder.zig` |
| Decimal (`32/64/128/256`) | 是 | 是（`Decimal32Builder`...`Decimal256Builder`） | 是 | 是 | `src/array/array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig`, `examples/decimal_builder.zig` |

## 2. 变长类型

| 类型组 | 已声明 | 已实现 | 已测试 | 有示例 | 证据 |
| --- | --- | --- | --- | --- | --- |
| UTF-8 (`Utf8`) | 是（`string`） | 是（`StringBuilder`） | 是 | 是 | `src/array/string_array.zig`, `examples/string_builder.zig` |
| Large UTF-8 (`LargeUtf8`) | 是（`large_string`） | 是（`LargeStringBuilder`） | 是 | 暂无单独示例 | `src/array/string_array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |
| Binary (`Binary`) | 是（`binary`） | 是（`BinaryBuilder`） | 是 | 是 | `src/array/binary_array.zig`, `examples/binary_builder.zig` |
| Large Binary (`LargeBinary`) | 是（`large_binary`） | 是（`LargeBinaryBuilder`） | 是 | 暂无单独示例 | `src/array/binary_array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |

## 3. 嵌套类型

| 类型组 | 已声明 | 已实现 | 已测试 | 有示例 | 证据 |
| --- | --- | --- | --- | --- | --- |
| Struct | 是 | 是（`StructBuilder`） | 是（含较多 builder 生命周期测试） | 是 | `src/array/struct_array.zig`, `examples/struct_builder.zig` |
| Map | 是 | 是（`MapBuilder`） | 是（成功路径 + schema/不变量/复用错误路径） | 是 | `src/array/advanced_array.zig`, `examples/map_builder.zig` |
| Union（Sparse/Dense） | 是 | 是（`SparseUnionBuilder`, `DenseUnionBuilder`） | 是（成功路径 + 校验/错误路径） | 是 | `src/array/advanced_array.zig`, `examples/union_builder.zig`, `src/ipc/stream_reader.zig` |

## 4. 时间类型

| 类型组 | 已声明 | 已实现 | 已测试 | 有示例 | 证据 |
| --- | --- | --- | --- | --- | --- |
| Date32/Date64 | 是 | 是 | 是 | 是 | `src/array/array.zig`, `examples/temporal_builder.zig` |
| Time32/Time64 | 是 | 是 | 是 | 是 | `src/array/array.zig`, `examples/temporal_builder.zig` |
| Timestamp | 是 | 是 | 是（含 unit/timezone/null timezone） | 是 | `src/array/array.zig`, `examples/temporal_builder.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |
| Duration | 是 | 是 | 是 | 是 | `src/array/array.zig`, `examples/temporal_builder.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |

## 说明与当前缺口

- `Int8/Int16/Int64/UInt*` 等别名复用同一套 `PrimitiveBuilder`/`PrimitiveArray` 泛型实现，所以不是每个别名都有单独示例。
- `LargeUtf8` / `LargeBinary` 已实现且有单测，但 `examples/` 里目前没有单独 demo。
- 本矩阵反映的是“源码实现 + 单测覆盖”层面的结论，不等同于“所有生态组合都已生产级验证”。
