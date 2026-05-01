# Type Coverage Matrix

This matrix documents the current data type coverage in `zarrow` based on source-level evidence (public API exports, implementations, tests, and examples).

Legend:

- `Declared`: logical type exists in `DataType`
- `Implemented`: public Array/Builder API is exported
- `Tested`: unit tests exist in `src/array/*` or IPC reader/writer tests
- `Example`: runnable example exists in `examples/*`

## 1. Fixed-width primitives

| Type group | Declared | Implemented | Tested | Example | Evidence |
| --- | --- | --- | --- | --- | --- |
| Signed integers (`Int8/16/32/64`) | Yes | Yes (`Int8Builder`...`Int64Builder`) | Generic primitive path tested | `Int32` example | `src/datatype.zig`, `src/array/array.zig`, `src/array/primitive_array.zig`, `examples/primitive_builder.zig` |
| Unsigned integers (`UInt8/16/32/64`) | Yes | Yes (`UInt8Builder`...`UInt64Builder`) | Generic primitive path tested | No dedicated example | `src/datatype.zig`, `src/array/array.zig`, `src/array/primitive_array.zig` |
| Floating point (`Half/Float32/Float64`) | Yes | Yes (`HalfFloatBuilder`, `Float32Builder`, `Float64Builder`) | Alias tests include half float | `Float32` appears in union example | `src/array/array.zig`, `examples/union_builder.zig` |
| Boolean | Yes | Yes (`BooleanBuilder`) | Yes | Yes | `src/array/boolean_array.zig`, `examples/boolean_builder.zig` |
| Decimal (`32/64/128/256`) | Yes | Yes (`Decimal32Builder`...`Decimal256Builder`) | Yes | Yes | `src/array/array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig`, `examples/decimal_builder.zig` |

## 2. Variable-length types

| Type group | Declared | Implemented | Tested | Example | Evidence |
| --- | --- | --- | --- | --- | --- |
| UTF-8 (`Utf8`) | Yes (`string`) | Yes (`StringBuilder`) | Yes | Yes | `src/array/string_array.zig`, `examples/string_builder.zig` |
| Large UTF-8 (`LargeUtf8`) | Yes (`large_string`) | Yes (`LargeStringBuilder`) | Yes | No dedicated example | `src/array/string_array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |
| Binary (`Binary`) | Yes (`binary`) | Yes (`BinaryBuilder`) | Yes | Yes | `src/array/binary_array.zig`, `examples/binary_builder.zig` |
| Large Binary (`LargeBinary`) | Yes (`large_binary`) | Yes (`LargeBinaryBuilder`) | Yes | No dedicated example | `src/array/binary_array.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |

## 3. Nested types

| Type group | Declared | Implemented | Tested | Example | Evidence |
| --- | --- | --- | --- | --- | --- |
| Struct | Yes | Yes (`StructBuilder`) | Yes (many builder lifecycle tests) | Yes | `src/array/struct_array.zig`, `examples/struct_builder.zig` |
| Map | Yes | Yes (`MapBuilder`) | Yes (success + invalid schema + invariants + reset/clear) | Yes | `src/array/advanced_array.zig`, `examples/map_builder.zig` |
| Union (Sparse/Dense) | Yes | Yes (`SparseUnionBuilder`, `DenseUnionBuilder`) | Yes (success + validation/error paths) | Yes | `src/array/advanced_array.zig`, `examples/union_builder.zig`, `src/ipc/stream_reader.zig` |

## 4. Temporal types

| Type group | Declared | Implemented | Tested | Example | Evidence |
| --- | --- | --- | --- | --- | --- |
| Date32/Date64 | Yes | Yes | Yes | Yes | `src/array/array.zig`, `examples/temporal_builder.zig` |
| Time32/Time64 | Yes | Yes | Yes | Yes | `src/array/array.zig`, `examples/temporal_builder.zig` |
| Timestamp | Yes | Yes | Yes (unit + timezone/null timezone) | Yes | `src/array/array.zig`, `examples/temporal_builder.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |
| Duration | Yes | Yes | Yes | Yes | `src/array/array.zig`, `examples/temporal_builder.zig`, `src/ipc/stream_writer.zig`, `src/ipc/stream_reader.zig` |

## Notes and current gaps

- Primitive aliases share one generic implementation (`PrimitiveBuilder` / `PrimitiveArray`), so not every alias has a dedicated standalone example.
- `LargeUtf8` / `LargeBinary` are implemented and unit-tested, but currently do not have dedicated example programs under `examples/`.
- Coverage here reflects source-level implementation and tests; it does not claim full production-hardening across all interop ecosystems for every type combination.
