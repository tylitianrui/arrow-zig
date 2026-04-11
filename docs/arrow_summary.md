# Apache Arrow 规范、数组类型与实现总览

## 0. 基础概念
### 内存对齐要求

Arrow 规范要求所有 buffer 的起始地址必须是 **64 字节对齐**（推荐，最低 8 字节对齐）。每个 buffer 的长度必须是 **8 字节的倍数**（不足部分用零填充0）。这一要求使 SIMD 操作可以安全地以向量宽度读取边界字节。

### Buffer 的通用语义

每个 Array 由以下元数据和 buffer 集合构成：

- `length`：逻辑元素个数（非负整数）
- `offset`：逻辑起始偏移（用于 slice，非负整数，初始为 0）
- `null_count`：空值数量（ c++: -1 表示未知，zig：null 需要扫描计算）
- `buffers`：有序的物理 buffer 列表，各类型有固定数量和语义
- `children`：子 Array 列表（嵌套类型使用）
- `dictionary`：字典值 Array（字典类型使用）

逻辑元素下标范围是 `[offset, offset + length)`，物理 buffer 中 `[0, offset)` 的数据是历史遗留或 padding，合法但不属于当前 Array 的内容。

### Validity Bitmap（有效性位图）

几乎所有类型的 `buffers[0]` 是 validity bitmap，规则如下：

- 使用 **LSB（最低有效位）优先** 的位序：第 i 个逻辑元素对应 `byte[i/8]` 的第 `i%8` 位
- **1 表示有效**，0 表示 null
- 存储字节数为 `ceil((offset + length) / 8)`
- `[offset + length, ...)` 范围内的 padding bit 必须为 **0**
- 当 `null_count == 0` 时，可以省略 validity bitmap（`buffers[0]` 为空或 null），此时所有元素视为有效
- **Null 类型**和 **Union 类型**没有 validity bitmap

---

## 1. Arrow 的统一模型

Arrow 的底层可以统一看成：

```text
Schema
  -> Field[]
  -> RecordBatch
  -> ArrayData tree
       - data_type
       - length
       - offset
       - null_count
       - buffers[]
       - children[]
       - dictionary?
```

关键点：

- `Schema / Field / DataType` 描述逻辑结构
- `ArrayData` 描述物理内存
- `RecordBatch` 把等长列组成一批
- IPC 与 FFI 传输的本质都是这棵 `ArrayData` 树

通用规则：

- fixed-width 数值采用 little-endian
- validity bitmap 使用 LSB-first 位序
- 多数类型都用 `buffers[0]` 表示 validity
- `offset` 是零拷贝切片的基础
- null 与 values buffer 解耦

---

## 2. Schema、Field 与 DataType

`Field` 通常包含：

- `name`
- `data_type`
- `nullable`
- `metadata`

常见 `DataType` 大类：

- `Null`
- `Boolean`
- fixed-width primitive：`Int*`、`UInt*`、`Float*`
- decimal：`Decimal32/64/128/256`
- temporal：`Date32/64`、`Time32/64`、`Timestamp`、`Duration`
- interval：`YearMonth`、`DayTime`、`MonthDayNano`
- binary families：`FixedSizeBinary`、`Binary`、`String`、`LargeBinary`、`LargeString`
- view families：`BinaryView`、`StringView`
- nested：`List`、`LargeList`、`FixedSizeList`、`ListView`、`LargeListView`、`Struct`、`Map`
- special：`SparseUnion`、`DenseUnion`、`Dictionary`、`RunEndEncoded`
- `Extension`

---

## 3. 数组类型与内存图

### 3.1 Null

**解释**：`Null` 表示整列所有元素都为 null，是最简单也最特殊的 Arrow 类型。

**实现**：适合作为 `ArrayData` 基础路径的最小验证类型。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Null` |
| `物理 buffer` | 无 |
| `children` | 无 |
| `字节宽度` | 无 values buffer |

**访问公式**：

```text
value(i) = null
```

**细节与约束**：
- `null_count` 必须等于 `length`
- 没有 validity bitmap
- 没有 values buffer
- slice 后只改变 `offset/length` 语义，不增加任何物理数据需求

**内存图**：

```text
┌──────────────────────── NullArray ────────────────────────┐
│ length = 4                                                │
│ offset = 0                                                │
│ null_count = 4                                            │
├────────────────────────────────────────────────────────────┤
│ no buffers                                                │
│ no children                                               │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- `null_count` 最好在构造时就保持正确，不要依赖后算
- 不要错误地给 `Null` 类型分配 validity bitmap
- slice 后如果只保留子视图，`length` 和 `null_count` 语义仍要自洽

**C++ / Rust 对照**：
- C++: `arrow::NullArray` / `arrow::NullBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::NullArray` / `arrow_array::builder::NullBuilder` / `arrow_data::ArrayData` / `arrow_buffer::Buffer`

### 3.2 Boolean

**解释**：`Boolean` 是 bit-packed 的布尔数组，和其他 fixed-width 类型不同，它的值区不是 byte-per-value。

**实现**：通常需要单独的 bitmap 读写工具，不适合直接复用普通 fixed-width 访问逻辑。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Bool` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` values bitmap |
| `children` | 无 |
| `字节宽度` | validity 按 bit；values 按 bit |

**访问公式**：

```text
bit_index = offset + i
if validity(bit_index) == 0:
    null
else:
    values(bit_index) ? true : false
```

**细节与约束**：
- `offset` 作用在 bit index 上
- validity 与 values 都采用 LSB-first
- null 槽位的 values bit 不影响语义

**内存图**：

```text
┌────────────────────── BooleanArray ───────────────────────┐
│ length = 4                                                │
│ offset = 0                                                │
│ null_count = 1                                            │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  1  0  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values bitmap                                   │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- bitmap 读写必须统一 LSB-first，否则跨语言一定错
- `offset` 是 bit offset，不是 byte offset
- validity bitmap 和 values bitmap 不要混成一个 buffer

**C++ / Rust 对照**：
- C++: `arrow::BooleanArray` / `arrow::BooleanBuilder` / `arrow::ArrayData` / bitmap helpers in Arrow C++
- Rust: `arrow_array::BooleanArray` / `arrow_array::builder::BooleanBuilder` / `arrow_data::ArrayData` / `arrow_buffer::BooleanBuffer`

### 3.3 Fixed-Width Primitive

**解释**：这是 Arrow 中最基础、最通用的一组物理布局。整数、浮点等大多共享这一模板。

**实现**：最适合最先实现，因为它们的 buffer 规则最稳定，也最容易带动后续 builder、slice、IPC 逻辑。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Int8/16/32/64`、`UInt8/16/32/64`、`HalfFloat`、`Float`、`Double` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` values |
| `children` | 无 |
| `字节宽度` | `Int8/UInt8=1`，`Int16/UInt16/HalfFloat=2`，`Int32/UInt32/Float=4`，`Int64/UInt64/Double=8` |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    read_little_endian<T>(buffer[1], idx * byte_width)
```

**细节与约束**：
- values 使用 little-endian
- null 槽位的 bytes 可以是任意值
- 读取时使用 `offset + i`
- buffer 最小长度应覆盖 `(offset + length) * byte_width`

**内存图**：

`Int32Array`

```text
┌──────────────────────── Int32Array ───────────────────────┐
│ length = 4                                                │
│ offset = 0                                                │
│ null_count = 1                                            │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values (int32)                                  │
│   index:  0    1    2    3                                │
│   value: [10] [20] [30] [40]                              │
│   slot1 是 null，但 values[1] 可为任意值                  │
└────────────────────────────────────────────────────────────┘
```

`Float64Array`

```text
┌──────────────────────── Float64Array ─────────────────────┐
│ length = 3                                                │
│ offset = 0                                                │
│ null_count = 1                                            │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  0  1                                          │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values (f64)                                    │
│   index:  0      1      2                                 │
│   value: [1.5] [9.9] [3.25]                               │
│   slot1 是 null，但 values[1] 可为任意值                  │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 先把 fixed-width 模板抽象好，再派生出具体 typed array 视图
- null 槽位的 bytes 不应参与比较、hash 或语义判断
- slice 时不要复制 values，只移动逻辑起点

**C++ / Rust 对照**：
- C++: `arrow::Int32Array`、`arrow::DoubleArray` 等 typed array / `arrow::NumericBuilder<T>` / `arrow::ArrayData`
- Rust: `arrow_array::PrimitiveArray<T>`、`arrow_array::types::*` / `arrow_array::builder::PrimitiveBuilder<T>` / `arrow_data::ArrayData` / `arrow_buffer::ScalarBuffer`

### 3.4 Decimal

**解释**：Decimal 在逻辑上是十进制定点数，在物理上仍是固定宽度整数，只是需要配合 `precision/scale` 解释。

**实现**：可以复用 fixed-width values buffer 管理，但需要额外验证 `precision` 和类型宽度是否匹配。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Decimal32`、`Decimal64`、`Decimal128`、`Decimal256` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` fixed-width integer storage |
| `children` | 无 |
| `字节宽度` | `Decimal32=4`，`Decimal64=8`，`Decimal128=16`，`Decimal256=32` |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    raw = read_little_endian_integer(buffer[1], idx * byte_width, byte_width)
    value = raw * 10^(-scale)
```

**细节与约束**：
- 真值 = `raw * 10^(-scale)`
- 内存按 little-endian 存储
- `precision` 决定允许的最大十进制位数
- `Decimal32` 的 `precision ∈ [1, 9]`
- `Decimal64` 的 `precision ∈ [1, 18]`
- `Decimal128` 的 `precision ∈ [1, 38]`
- `Decimal256` 的 `precision ∈ [1, 76]`

**内存图**：

`Decimal32Array`

```text
┌────────────────────── Decimal32Array ─────────────────────┐
│ precision = 5, scale = 1                                  │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1   2                                         │
│   bit :  1  0   1                                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] raw values (i32)                                │
│   index:  0    1    2                                     │
│   raw  : [123] [77] [-45]                                 │
│   logical value: [12.3] [null] [-4.5]                     │
└────────────────────────────────────────────────────────────┘
```

`Decimal64Array`

```text
┌────────────────────── Decimal64Array ─────────────────────┐
│ precision = 10, scale = 2                                 │
│ length = 2                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap omitted                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] raw values (i64)                                │
│   index:  0        1                                      │
│   raw  : [100001] [200]                                   │
│   logical value: [1000.01] [2.00]                         │
└────────────────────────────────────────────────────────────┘
```

`Decimal128Array`

```text
┌────────────────────── Decimal128Array ────────────────────┐
│ precision = 10, scale = 2                                 │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1   2                                         │
│   bit :  1  0   1                                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] raw values (16 bytes per slot)                  │
│   index:  0       1      2                                │
│   raw  : [12345] [88] [-700]                              │
│   logical value: [123.45] [null] [-7.00]                  │
└────────────────────────────────────────────────────────────┘
```

`Decimal256Array`

```text
┌────────────────────── Decimal256Array ────────────────────┐
│ precision = 50, scale = 6                                 │
│ length = 2, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1                                             │
│   bit :  1  0                                             │
├────────────────────────────────────────────────────────────┤
│ buffer[1] raw values (32 bytes per slot)                  │
│   slot0 -> huge_raw_0                                     │
│   slot1 -> huge_raw_1 (ignored because null)              │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 宽度校验要和 precision/scale 一起看，不能只看 buffer 长度
- 负数与大整数的 little-endian 解码容易写错
- decimal 的“显示值”不要反写回底层 raw buffer 语义

  

**C++ / Rust 对照**：
- C++: `arrow::Decimal32Array`、`arrow::Decimal64Array`、`arrow::Decimal128Array`、`arrow::Decimal256Array` / 对应 decimal builders / `arrow::ArrayData`
- Rust: `arrow_array::PrimitiveArray<Decimal*Type>` / `arrow_array::builder::Decimal*Builder` / `arrow_data::ArrayData` / `arrow_buffer::ScalarBuffer`

### 3.5 Date / Time / Timestamp / Duration

**解释**：这些类型在逻辑上是时间语义，在物理上大多是 `int32` 或 `int64`。

**实现**：可以复用 fixed-width 模板；差异主要在 schema 参数解释而不是 buffer 布局。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Date32`、`Date64`、`Time32`、`Time64`、`Timestamp`、`Duration` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` fixed-width integer storage |
| `children` | 无 |
| `字节宽度` | `Date32=4`，`Date64=8`，`Time32=4`，`Time64=8`，`Timestamp=8`，`Duration=8` |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    raw = read_little_endian_integer(buffer[1], idx * byte_width, byte_width)
    interpret(raw, unit, timezone?)
```

**细节与约束**：
- `Date32` 按天计数
- `Date64` 按毫秒计数
- `Time32/64` 受单位参数约束
- `Timestamp` 可带 timezone metadata
- `Duration` 是带单位的有符号时间间隔
- `Date32` 单位固定为 `DAY`
- `Date64` 单位固定为 `MILLISECOND`
- `Date64` 的值应为 `86_400_000` 的整数倍
- `Time32` 单位只能是 `SECOND` 或 `MILLISECOND`
- `Time64` 单位只能是 `MICROSECOND` 或 `NANOSECOND`
- `Time32` 取值范围分别是 `[0, 86400)` 秒或 `[0, 86400000)` 毫秒
- `Time64` 取值范围分别是 `[0, 86400000000)` 微秒或 `[0, 86400000000000)` 纳秒
- `Timestamp` 单位可以是 `SECOND`、`MILLISECOND`、`MICROSECOND`、`NANOSECOND`
- `Timestamp` 的时区可以为空，也可以是 IANA 时区字符串，或 UTC 偏移字符串
- `timezone == null` 表示无时区的本地时间解释
- `timezone == "UTC"` 表示 UTC 时间解释
- `Duration` 单位也可以是 `SECOND`、`MILLISECOND`、`MICROSECOND`、`NANOSECOND`

**内存图**：

`Date32Array`

```text
┌──────────────────────── Date32Array ──────────────────────┐
│ values = int32 days since epoch                           │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0      1    2                                     │
│   bit :  1      0    1                                     │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0       1     2                                  │
│   raw  : [19723] [0] [19725]                              │
│   logical date: [2024-01-01] [null] [2024-01-03]          │
└────────────────────────────────────────────────────────────┘
```

`Date64Array`

```text
┌──────────────────────── Date64Array ──────────────────────┐
│ values = int64 millis since epoch                         │
│ length = 2, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0               1                                 │
│   bit :  1               0                                 │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0               1                                │
│   raw  : [1704067200000] [1]                              │
│   logical date: [2024-01-01] [null]                       │
└────────────────────────────────────────────────────────────┘
```

`Time32Array`

```text
┌──────────────────────── Time32Array ──────────────────────┐
│ unit = millisecond                                        │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0     1   2                                       │
│   bit :  1     0   1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0      1   2                                     │
│   raw  : [1000] [9] [2500]                                │
│   logical time: [00:00:01.000] [null] [00:00:02.500]      │
└────────────────────────────────────────────────────────────┘
```

`Time64Array`

```text
┌──────────────────────── Time64Array ──────────────────────┐
│ unit = microsecond                                        │
│ length = 2                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap omitted                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0         1                                     │
│   raw  : [1000000] [2000000]                              │
│   logical time: [00:00:01.000000] [00:00:02.000000]       │
└────────────────────────────────────────────────────────────┘
```

`TimestampArray`

```text
┌────────────────────── TimestampArray ─────────────────────┐
│ unit = millisecond, timezone = null                       │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0              1   2                             │
│   bit :  1              0   1                             │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0              1   2                            │
│   raw  : [1700000000000] [0] [1700000000123]              │
│   logical ts: [t0] [null] [t0+123ms]                      │
└────────────────────────────────────────────────────────────┘
```

`DurationArray`

```text
┌──────────────────────── DurationArray ────────────────────┐
│ unit = millisecond                                        │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0    1   2                                        │
│   bit :  1    0   1                                        │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values                                          │
│   index:  0    1   2                                       │
│   raw  : [100] [7] [-25]                                  │
│   logical duration: [100ms] [null] [-25ms]                │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 这些类型的主要差异在 schema 参数，而不是底层 buffer 形状
- `Timestamp` 的 timezone 不改变底层整数，只改变解释方式
- `Date64` / `Time*` / `Duration` 的单位不要在 reader / writer 中混淆

**C++ / Rust 对照**：
- C++: `arrow::Date32Array`、`arrow::Date64Array`、`arrow::Time32Array`、`arrow::Time64Array`、`arrow::TimestampArray`、`arrow::DurationArray` / 对应 builder / `arrow::ArrayData`
- Rust: `arrow_array::PrimitiveArray<Date32Type/Date64Type/Time32*Type/Time64*Type/Timestamp*Type/Duration*Type>` / 对应 `PrimitiveBuilder` / `arrow_data::ArrayData` / `arrow_buffer::ScalarBuffer`

### 3.6 Interval

**解释**：Interval 是带多个逻辑分量的时间间隔，但物理上仍是固定宽度槽位。

**实现**：读取时需要按具体 interval 变体解释固定宽度字段组合。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Interval(Months)`、`Interval(DayTime)`、`Interval(MonthDayNano)` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` fixed-width interval storage |
| `children` | 无 |
| `字节宽度` | `YearMonth=4`，`DayTime=8`，`MonthDayNano=16` |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    decode_interval_record(buffer[1], idx * byte_width, interval_kind)
```

**细节与约束**：
- `YearMonth` 本质是 months 数
- `DayTime` 常表示 days + millis
- `MonthDayNano` 表示 months + days + nanos
- `YearMonth` 的物理类型是 `int32`
- `DayTime` 的物理类型是 8 字节记录 `{days: int32, milliseconds: int32}`
- `MonthDayNano` 的物理类型是 16 字节记录 `{months: int32, days: int32, nanoseconds: int64}`
- 三者都支持正负区间值

**内存图**：

`IntervalYearMonthArray`

```text
┌────────────────── IntervalYearMonthArray ────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0   1   2                                         │
│   bit :  1   0   1                                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values (int32 months)                           │
│   index:  0    1   2                                       │
│   raw  : [14] [1] [3]                                     │
│   logical interval: [1y2m] [null] [3m]                    │
└────────────────────────────────────────────────────────────┘
```

`IntervalDayTimeArray`

```text
┌─────────────────── IntervalDayTimeArray ──────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0   1   2                                         │
│   bit :  1   0   1                                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values (8 bytes per slot)                       │
│   slot0 -> [days=1, ms=500]                               │
│   slot1 -> [days=9, ms=9] ignored                         │
│   slot2 -> [days=0, ms=200]                               │
└────────────────────────────────────────────────────────────┘
```

`IntervalMonthDayNanoArray`

```text
┌──────────────── IntervalMonthDayNanoArray ────────────────┐
│ length = 2, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0   1                                             │
│   bit :  1   0                                             │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values (16 bytes per slot)                      │
│   slot0 -> [months=1, days=2, nanos=30]                   │
│   slot1 -> [months=7, days=8, nanos=9] ignored            │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 三种 interval 的物理宽度不同，不能混用读取器
- `DayTime` 与 `MonthDayNano` 都是复合记录，注意字段边界和顺序
- 比较与格式化逻辑要基于逻辑结构，不要直接拿 raw bytes 比

**C++ / Rust 对照**：
- C++: `arrow::MonthIntervalArray`、`arrow::DayTimeIntervalArray`、`arrow::MonthDayNanoIntervalArray` / 对应 builder / `arrow::ArrayData`
- Rust: `arrow_array::PrimitiveArray<IntervalYearMonthType/IntervalDayTimeType/IntervalMonthDayNanoType>` / `PrimitiveBuilder` / `arrow_data::ArrayData`

### 3.7 FixedSizeBinary

**解释**：每个元素都是固定长度字节块，适合哈希、摘要、定长编码场景。

**实现**：可以复用 fixed-width 索引计算，但值本体按 `u8[]` 解释而不是数值。

| 字段 | 内容 |
|---|---|
| `TypeID` | `FixedSizeBinary` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` values bytes |
| `children` | 无 |
| `字节宽度` | 由 schema 中 `byte_width` 指定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = idx * byte_width
    end = start + byte_width
    value = buffer[1][start..end]
```

**细节与约束**：
- `slot i` 起点是 `(offset + i) * byte_width`
- null 槽位对应字节块可忽略
- `byte_width` 必须大于 0
- `buffer[1]` 至少应覆盖 `(offset + length) * byte_width` 字节

**内存图**：

```text
┌────────────────── FixedSizeBinaryArray ───────────────────┐
│ byte_width = 2                                            │
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] values bytes                                    │
│   slot0 -> [AA BB]                                        │
│   slot1 -> [?? ??]                                        │
│   slot2 -> [CC DD]                                        │
│   slot3 -> [EE FF]                                        │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- `byte_width` 是 schema 约束，不要只靠运行时 buffer 长度猜
- slice 仍然按元素宽度切，不是任意字节切片
- null 槽位的 payload bytes 可忽略，不要求清零

**C++ / Rust 对照**：
- C++: `arrow::FixedSizeBinaryArray` / `arrow::FixedSizeBinaryBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::FixedSizeBinaryArray` / `arrow_array::builder::FixedSizeBinaryBuilder` / `arrow_data::ArrayData` / `arrow_buffer::Buffer`

### 3.8 String / Binary

**解释**：这是最经典的变长布局：offsets + data。

**实现**：必须严查 offsets 单调性、数量和边界。

| 字段 | 内容 |
|---|---|
| `TypeID` | `String`、`Binary` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` offsets(`i32`)；`buffer[2]` data |
| `children` | 无 |
| `字节宽度` | offsets 为 4 bytes each；data 为逐字节连续存放 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = offsets_i32[idx]
    end = offsets_i32[idx + 1]
    value = data[start..end]
```

**细节与约束**：
- 最少需要 `length + offset + 1` 个 offsets 槽
- `String` 要求 data 区符合 UTF-8
- `Binary` 对 data 内容不做文本约束
- offsets 中所有值都必须非负
- offsets 必须单调非递减
- `offsets[0]` 不要求一定为 0，因为 slice 可以共享原始 buffer
- `buffer[2].len >= offsets[offset + length]`
- 该布局受 `int32` offsets 限制，总数据量通常不超过约 2GB

**内存图**：

`StringArray`

```text
┌──────────────────────── StringArray ──────────────────────┐
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i32)                                   │
│   [0] [5] [5] [8] [8]                                     │
├────────────────────────────────────────────────────────────┤
│ buffer[2] data                                            │
│   "alicebob"                                              │
│   slot0 -> "alice"                                        │
│   slot1 -> null                                           │
│   slot2 -> "bob"                                          │
│   slot3 -> ""                                             │
└────────────────────────────────────────────────────────────┘
```

`BinaryArray`

```text
┌──────────────────────── BinaryArray ──────────────────────┐
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i32)                                   │
│   [0] [2] [2] [3] [6]                                     │
├────────────────────────────────────────────────────────────┤
│ buffer[2] data                                            │
│   [01 02 AA FF 00 10]                                     │
│   slot0 -> [01 02]                                        │
│   slot1 -> null                                           │
│   slot2 -> [AA]                                           │
│   slot3 -> [FF 00 10]                                     │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- offsets 数量必须至少是 `offset + length + 1`
- offsets 必须单调不减，且最后一个 offset 不能越过 data buffer
- `String` 还要额外校验 UTF-8；`Binary` 不需要

**C++ / Rust 对照**：
- C++: `arrow::StringArray`、`arrow::BinaryArray` / `arrow::StringBuilder`、`arrow::BinaryBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::StringArray`、`arrow_array::BinaryArray` / `arrow_array::builder::StringBuilder`、`BinaryBuilder` / `arrow_data::ArrayData` / `arrow_buffer::OffsetBuffer`

### 3.9 LargeString / LargeBinary

**解释**：与普通 varlen 完全同构，只是 offsets 扩展为 `i64`。

**实现**：逻辑与 `String/Binary` 相同，注意 64 位偏移与更大容量场景。

| 字段 | 内容 |
|---|---|
| `TypeID` | `LargeString`、`LargeBinary` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` offsets(`i64`)；`buffer[2]` data |
| `children` | 无 |
| `字节宽度` | offsets 为 8 bytes each；data 为逐字节 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = offsets_i64[idx]
    end = offsets_i64[idx + 1]
    value = data[start..end]
```

**细节与约束**：
- 适合超大 data buffer
- 读取公式与 `String/Binary` 相同，只是 offsets 类型不同
- offsets 中所有值都必须非负
- offsets 必须单调非递减
- `buffer[2].len >= offsets[offset + length]`
- 与普通变长布局相比，核心区别只是 offsets 宽度从 `int32` 变成 `int64`

**内存图**：

`LargeStringArray`

```text
┌────────────────────── LargeStringArray ───────────────────┐
│ length = 2                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap omitted                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i64)                                   │
│   [0] [5] [10]                                            │
├────────────────────────────────────────────────────────────┤
│ buffer[2] data = "helloworld"                             │
│   slot0 -> "hello"                                        │
│   slot1 -> "world"                                        │
└────────────────────────────────────────────────────────────┘
```

`LargeBinaryArray`

```text
┌────────────────────── LargeBinaryArray ───────────────────┐
│ length = 2                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap omitted                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i64)                                   │
│   [0] [1] [4]                                             │
├────────────────────────────────────────────────────────────┤
│ buffer[2] data = [AA BB CC DD]                            │
│   slot0 -> [AA]                                           │
│   slot1 -> [BB CC DD]                                     │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 除 offsets 宽度变成 `i64` 外，别引入另一套语义
- 超大数据时注意 usize / i64 转换边界
- writer / IPC 中不要误把 large 版本编码成普通 32 位 offsets

**C++ / Rust 对照**：
- C++: `arrow::LargeStringArray`、`arrow::LargeBinaryArray` / `arrow::LargeStringBuilder`、`arrow::LargeBinaryBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::LargeStringArray`、`arrow_array::LargeBinaryArray` / `arrow_array::builder::LargeStringBuilder`、`LargeBinaryBuilder` / `arrow_data::ArrayData` / `arrow_buffer::OffsetBuffer<i64>`

### 3.10 StringView / BinaryView

**解释**：View 系列不是 offsets + data，而是每个槽位持有一个固定大小的 view record，用来 inline 小值或引用外部 data 区。

**实现**：必须先解析 view record，不能直接套用 varlen offsets 模型。

| 字段 | 内容 |
|---|---|
| `TypeID` | `StringView`、`BinaryView` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` views；可选附加 data region |
| `children` | 无 |
| `字节宽度` | 每个 view record 固定宽度；长 payload 位于外部 data region |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    view = read_view_record(buffer[1], idx)
    if view.is_inline:
        value = view.inline_bytes[0..view.length]
    else:
        value = external_data[view.start .. view.start + view.length]
```

**细节与约束**：
- 小值可 inline
- 大值通过指针/偏移式描述引用数据区
- validation 要确保 view 不越界
- `buffer[1]` 中每个 view record 固定为 16 字节
- 当 `length <= 12` 时，view 可直接 inline 值内容
- 当 `length > 12` 时，view 会记录前缀、buffer index 与 offset
- 长值路径下 `buf_index >= 0`、`offset >= 0`
- 长值路径下 `offset + length <= variadic_buffer.len`
- 长值前缀必须与真实值的前 4 字节一致
- `buffers[2..]` 的数量在 IPC 中通常通过 variadic buffer count 记录
- 不同元素可以合法地共享同一 variadic buffer，甚至引用重叠范围

**内存图**：

`StringViewArray`

```text
┌────────────────────── StringViewArray ────────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
├────────────────────────────────────────────────────────────┤
│ buffer[1] views (fixed-size records)                      │
│   slot0 -> inline small string "hi"                       │
│   slot1 -> external data reference                        │
│   slot2 -> null                                           │
├────────────────────────────────────────────────────────────┤
│ extra data region stores long payload bytes               │
└────────────────────────────────────────────────────────────┘
```

`BinaryViewArray`

```text
┌────────────────────── BinaryViewArray ────────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
├────────────────────────────────────────────────────────────┤
│ buffer[1] views (fixed-size records)                      │
│   slot0 -> inline bytes [AA BB]                           │
│   slot1 -> external data reference                        │
│   slot2 -> null                                           │
├────────────────────────────────────────────────────────────┤
│ extra data region stores long payload bytes               │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 这是最容易误套用 offsets 模板的类型之一
- inline 和 out-of-line 两条路径都要测
- view record 指向外部数据区时，边界校验必须严格

**C++ / Rust 对照**：
- C++: `arrow::StringViewArray`、`arrow::BinaryViewArray` / 对应 view-family builder / `arrow::ArrayData`
- Rust: `arrow_array::StringViewArray`、`arrow_array::BinaryViewArray` / 对应 view-family builder / `arrow_data::ArrayData` / `arrow_buffer::Buffer`

### 3.11 List / LargeList

**解释**：父数组用 offsets 描述每个槽位对应 child values 的哪一段。

**实现**：读值时需要把父 offsets 映射到 child slice，构造子 array view。

| 字段 | 内容 |
|---|---|
| `TypeID` | `List`、`LargeList` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` offsets；`children[0]` values child |
| `children` | 1 个 |
| `字节宽度` | `List` offsets=4；`LargeList` offsets=8；child 由其自身 type 决定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = offsets[idx]
    end = offsets[idx + 1]
    value = child.slice(start, end - start)
```

**细节与约束**：
- `offsets[i]..offsets[i+1]` 是 child 逻辑范围
- null 槽位的 offsets 仍必须合法
- offsets 必须单调非递减
- offsets 中所有值都必须非负
- `children[0].length >= offsets[offset + length]`
- `List` 使用 `int32` offsets，`LargeList` 使用 `int64`

**内存图**：

`ListArray`

```text
┌───────────────────────── ListArray ───────────────────────┐
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i32)                                   │
│   [0] [2] [2] [2] [3]                                     │
├────────────────────────────────────────────────────────────┤
│ child[0] values = Int32Array [1, 2, 3]                    │
│   slot0 -> child[0..2]                                    │
│   slot1 -> null                                           │
│   slot2 -> child[2..2]                                    │
│   slot3 -> child[2..3]                                    │
└────────────────────────────────────────────────────────────┘
```

`LargeListArray`

```text
┌─────────────────────── LargeListArray ────────────────────┐
│ length = 2                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap omitted                         │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i64)                                   │
│   [0] [1] [3]                                             │
├────────────────────────────────────────────────────────────┤
│ child[0] values = Int32Array [10, 20, 30]                 │
│   slot0 -> [10]                                           │
│   slot1 -> [20, 30]                                       │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 父 offsets 与 child 长度关系必须验证
- child slice 的 offset/length 要正确传播
- null 槽位的 offsets 依然必须合法

**C++ / Rust 对照**：
- C++: `arrow::ListArray`、`arrow::LargeListArray` / `arrow::ListBuilder`、`arrow::LargeListBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::ListArray`、`arrow_array::LargeListArray` / `arrow_array::builder::ListBuilder` / `arrow_data::ArrayData` / `arrow_buffer::OffsetBuffer`

### 3.12 FixedSizeList

**解释**：每个槽位对应 child 中固定数量的元素，不再需要 offsets。

**实现**：最重要的是验证 child 容量是否覆盖 `(offset + length) * list_size`。

| 字段 | 内容 |
|---|---|
| `TypeID` | `FixedSizeList` |
| `物理 buffer` | `buffer[0]` validity bitmap；`children[0]` values child |
| `children` | 1 个 |
| `字节宽度` | 父节点无固定 values 宽度；每槽逻辑宽度由 `list_size` 指定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = idx * list_size
    value = child.slice(start, list_size)
```

**细节与约束**：
- `slot i` 映射到 child 的固定跨度
- null 槽位对应 child 物理段可以忽略
- `list_size` 必须大于 0
- `children[0].length` 至少应覆盖 `(offset + length) * list_size`

**内存图**：

```text
┌────────────────────── FixedSizeListArray ─────────────────┐
│ list_size = 2                                             │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  0  1                                          │
├────────────────────────────────────────────────────────────┤
│ child[0] values = Int32Array                              │
│   [1, 2, ??, ??, 5, 6]                                    │
│   slot0 -> child[0..2]                                    │
│   slot1 -> ignored because null                           │
│   slot2 -> child[4..6]                                    │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- `list_size` 是 schema 常量，不应从数据推断
- child 容量校验最容易遗漏
- slice 后逻辑索引到 child 的映射要重新基于 `offset + i`

**C++ / Rust 对照**：
- C++: `arrow::FixedSizeListArray` / `arrow::FixedSizeListBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::FixedSizeListArray` / `arrow_array::builder::FixedSizeListBuilder` / `arrow_data::ArrayData`

### 3.13 ListView / LargeListView

**解释**：与普通 list 相比，这一族显式记录每个槽位的起点和长度。

**实现**：需要同时校验 offsets 与 sizes；逻辑模型更像“显式视图表”。

| 字段 | 内容 |
|---|---|
| `TypeID` | `ListView`、`LargeListView` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` offsets；`buffer[2]` sizes；`children[0]` values child |
| `children` | 1 个 |
| `字节宽度` | `ListView` offsets/sizes=4；`LargeListView` offsets/sizes=8 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = offsets[idx]
    len = sizes[idx]
    value = child.slice(start, len)
```

**细节与约束**：
- 起点和长度彼此独立记录
- 空列表由 size=0 表示
- null 槽位仍需有合法元数据
- offsets 不要求单调
- 不同行的视图范围可以重叠
- `offsets[idx] >= 0`
- `sizes[idx] >= 0`
- `offsets[idx] + sizes[idx] <= children[0].length + children[0].offset`
- `ListView` 使用 `int32` offsets/sizes，`LargeListView` 使用 `int64`
- 规范上 null 槽位的 offset/size 可不解释，但实现里仍建议保持合法值

**内存图**：

`ListViewArray`

```text
┌──────────────────────── ListViewArray ────────────────────┐
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets = [0, 2, 2, 3]                          │
│ buffer[2] sizes   = [2, 0, 1, 0]                          │
├────────────────────────────────────────────────────────────┤
│ child[0] values = Int32Array [1, 2, 3]                    │
│   slot0 -> [1, 2]                                         │
│   slot1 -> null                                           │
│   slot2 -> [3]                                            │
│   slot3 -> []                                             │
└────────────────────────────────────────────────────────────┘
```

`LargeListViewArray`

```text
┌────────────────────── LargeListViewArray ─────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  1  0                                          │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets (i64) = [0, 1, 3]                       │
│ buffer[2] sizes   (i64) = [1, 2, 0]                       │
├────────────────────────────────────────────────────────────┤
│ child[0] values = Int32Array [10, 20, 30]                 │
│   slot0 -> [10]                                           │
│   slot1 -> [20, 30]                                       │
│   slot2 -> null                                           │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 与普通 list 不同，这里长度来自 `sizes[idx]`，不要偷用 `offsets[idx+1] - offsets[idx]`
- offsets 和 sizes 要同时做越界校验
- null 槽位的元数据也要保持合法

**C++ / Rust 对照**：
- C++: `arrow::ListViewArray`、`arrow::LargeListViewArray` / 对应 list-view builders / `arrow::ArrayData`
- Rust: `arrow_array::ListViewArray`、`arrow_array::LargeListViewArray` / 对应 list-view builders / `arrow_data::ArrayData`

### 3.14 Struct

**解释**：Struct 自己不存 values，所有字段都分散在 children 中。

**实现**：父级 validity 控制整条 struct 是否存在，child 自己也可以各自有 null。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Struct` |
| `物理 buffer` | `buffer[0]` validity bitmap |
| `children` | `n = field count` |
| `字节宽度` | 父节点无固定 values 宽度；每个 child 由其自身类型决定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    value = {
        field_0 = child_0[idx],
        field_1 = child_1[idx],
        ...
    }
```

**细节与约束**：
- 父 null 不要求 child 对应位置也为 null
- 读取时先看 parent validity，再决定是否解读 children
- struct 父节点自己没有 values buffer
- 所有 child 的顺序必须与 schema field 顺序一致

**内存图**：

```text
┌──────────────────────── StructArray ──────────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  0  1                                          │
├────────────────────────────────────────────────────────────┤
│ child[0] = Int32Array  [1, 2, 3]                          │
│ child[1] = StringArray ["aa", "bb", "cc"]                 │
│   slot0 -> {id=1, name="aa"}                              │
│   slot1 -> null                                           │
│   slot2 -> {id=3, name="cc"}                              │
│ parent slot1 is null; child bytes remain but ignored      │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 父 null 与 child null 是两层语义，不能互相替代
- 不要因为 parent 为 null 就要求 child 对应位置也清零
- field 数量、顺序与 schema 必须一致

**C++ / Rust 对照**：
- C++: `arrow::StructArray` / `arrow::StructBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::StructArray` / `arrow_array::builder::StructBuilder` / `arrow_data::ArrayData`

### 3.15 Map

**解释**：Map 物理上通常等价于 `list<struct<key, value>>`。

**实现**：可以复用 list + struct 的校验逻辑，只是在 schema 上施加 map 语义。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Map` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` offsets；`children[0]` entries struct |
| `children` | 1 个 entries child |
| `字节宽度` | offsets=4；entries child 由 struct children 决定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    start = offsets[idx]
    end = offsets[idx + 1]
    value = entries.slice(start, end - start)
```

**细节与约束**：
- key 通常要求 non-null
- 每个 map slot 对应 entries child 的一段范围
- 物理上可直接理解为 `list<struct<key, value>>`
- `children[0]` 必须是 entries struct
- entries struct 必须恰好包含 key 与 value 两个 child

**内存图**：

```text
┌───────────────────────── MapArray ────────────────────────┐
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  0  1                                          │
├────────────────────────────────────────────────────────────┤
│ buffer[1] offsets = [0, 2, 2, 3]                          │
├────────────────────────────────────────────────────────────┤
│ child[0] = entries StructArray                            │
│   entries.key   = Int32Array [1, 2, 3]                    │
│   entries.value = Int32Array [10, 20, 30]                 │
│   slot0 -> {1:10, 2:20}                                   │
│   slot1 -> null                                           │
│   slot2 -> {3:30}                                         │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 从实现上最好真的按 `list<struct<key,value>>` 理解
- key non-null 约束不要丢
- offsets 校验与 entries child 校验要一起做

**C++ / Rust 对照**：
- C++: `arrow::MapArray` / `arrow::MapBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::MapArray` / `arrow_array::builder::MapBuilder` / `arrow_data::ArrayData`

### 3.16 DenseUnion

**解释**：每个槽位通过 `type_id + value_offset` 指向某个 child 的某个位置。

**实现**：不要把 union 当成带 null 的 struct；它更像“带标签的分发器”。

| 字段 | 内容 |
|---|---|
| `TypeID` | `DenseUnion` |
| `物理 buffer` | `buffer[0]` type_ids；`buffer[1]` value_offsets |
| `children` | `n` 个 union children |
| `字节宽度` | type_id 通常 1；value_offset 通常 4 |

**访问公式**：

```text
idx = offset + i
type_id = type_ids[idx]
child = child_for_type_id(type_id)
child_idx = value_offsets[idx]
value = child[child_idx]
```

**细节与约束**：
- child 不要求与父等长
- `type_id` 到 child 的映射来自 schema
- dense union 没有普通 validity bitmap
- 每个 `value_offset` 都必须落在对应 child 的合法范围内

**内存图**：

```text
┌────────────────────── DenseUnionArray ────────────────────┐
│ length = 3                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] type_ids      = [5, 7, 5]                       │
│ buffer[1] value_offsets = [0, 0, 1]                       │
├────────────────────────────────────────────────────────────┤
│ child[Int32]  = [1, 2]                                    │
│ child[String] = ["aa"]                                    │
│ slot0 -> Int32[0], slot1 -> String[0], slot2 -> Int32[1]  │
│ logical values -> [1, "aa", 2]                            │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- `type_id -> child` 映射来自 schema，不应硬编码在 reader 里
- `value_offsets` 越界是 dense union 的高风险 bug
- union 不使用普通 validity bitmap，不要误套 null 规则

**C++ / Rust 对照**：
- C++: `arrow::DenseUnionArray` / `arrow::DenseUnionBuilder` / `arrow::ArrayData`
- Rust: `arrow_array::UnionArray`（dense mode）/ 对应 union builder / `arrow_data::ArrayData`

### 3.17 SparseUnion

**解释**：每个 child 长度都和父一致，`type_id` 只决定当前槽位从哪个 child 读取。

**实现**：比 dense union 少一个 offsets buffer，但 child 空洞更多。

| 字段 | 内容 |
|---|---|
| `TypeID` | `SparseUnion` |
| `物理 buffer` | `buffer[0]` type_ids |
| `children` | `n` 个 union children |
| `字节宽度` | type_id 通常 1 |

**访问公式**：

```text
idx = offset + i
type_id = type_ids[idx]
child = child_for_type_id(type_id)
value = child[idx]
```

**细节与约束**：
- 所有 child 都与父等长
- 非当前类型 child 对应槽位的值要忽略
- sparse union 没有普通 validity bitmap
- 各 child 的长度应与父长度一致

**内存图**：

```text
┌────────────────────── SparseUnionArray ───────────────────┐
│ length = 3                                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] type_ids = [5, 7, 5]                            │
├────────────────────────────────────────────────────────────┤
│ child[Int32]  = [1, ?, 2]                                 │
│ child[String] = ["", "aa", ""]                            │
│ every child has parent length                             │
│ logical values -> [1, "aa", 2]                            │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- 所有 child 都和父等长，内存占用通常高于 dense union
- 非当前分支 child 的对应槽位要忽略
- type id 分发表与 schema 必须一致

**C++ / Rust 对照**：
- C++: `arrow::SparseUnionArray` / `arrow::BasicUnionBuilder` 或对应 sparse-union builder / `arrow::ArrayData`
- Rust: `arrow_array::UnionArray`（sparse mode）/ 对应 union builder / `arrow_data::ArrayData`

### 3.18 Dictionary

**解释**：父数组存索引，真实值保存在 `dictionary` 数组中。

**实现**：需要同时处理索引 buffer 与 dictionary values 的生命周期；IPC 中还要处理 dictionary batch 与 delta。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Dictionary` |
| `物理 buffer` | `buffer[0]` validity bitmap；`buffer[1]` indices；`dictionary` values array |
| `children` | 无普通 children，但有 `dictionary` |
| `字节宽度` | indices 由 index type 决定：1/2/4/8；dictionary values 由 value type 决定 |

**访问公式**：

```text
idx = offset + i
if invalid(idx):
    null
else:
    dict_idx = read_index(buffer[1], idx, index_type)
    value = dictionary[dict_idx]
```

**细节与约束**：
- index 必须在 dictionary 范围内
- dictionary value type 可以是 primitive、string、list、struct 等
- index type 必须是整数类型
- schema 中通常还会携带 dictionary id 与 ordered 标记
- ordered 影响语义解释，但不改变基础物理布局

**内存图**：

```text
┌────────────────────── DictionaryArray ────────────────────┐
│ index_type = int32                                        │
│ length = 4, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2  3                                       │
│   bit :  1  0  1  1                                       │
├────────────────────────────────────────────────────────────┤
│ buffer[1] indices = [1, 0, 0, 1]                          │
├────────────────────────────────────────────────────────────┤
│ dictionary = StringArray                                  │
│   dict[0] = "red"                                         │
│   dict[1] = "blue"                                        │
│   logical values -> ["blue", null, "red", "blue"]         │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- index buffer 宽度由 dictionary index type 决定，不要默认 int32
- dictionary values 的生命周期要与 parent 绑好
- IPC 里还要处理 dictionary batch、replacement、delta 和随机访问时的状态重放

**C++ / Rust 对照**：
- C++: `arrow::DictionaryArray` / dictionary-capable builders（常见配合 `DictionaryBuilder` 或 compute encode）/ `arrow::ArrayData`
- Rust: `arrow_array::DictionaryArray<K>` / `arrow_array::builder::StringDictionaryBuilder`、`PrimitiveDictionaryBuilder` 等 / `arrow_data::ArrayData`

### 3.19 RunEndEncoded

**解释**：REE 通过 run_ends 和 values 两个 child 表达压缩后的逻辑序列。

**实现**：读取时需要先定位当前逻辑索引属于哪个 run，再去 values child 取值。

| 字段 | 内容 |
|---|---|
| `TypeID` | `RunEndEncoded` |
| `物理 buffer` | 无直接 buffers |
| `children` | `child[0]` run_ends；`child[1]` values |
| `字节宽度` | run_ends 由 run-end type 决定，常见为 4 或 8；values 由 value type 决定 |

**访问公式**：

```text
logical_idx = offset + i
run_idx = lower_bound(run_ends, logical_idx + 1)
value = values[run_idx]
```

**细节与约束**：
- run_ends 必须单调递增
- 最后一个 run_end 应覆盖逻辑长度
- 这是读取模型最不像普通数组的一类
- REE 父节点没有直接 buffers，所有数据都在两个 child 中
- `run_ends` child 与 `values` child 的长度应一致

**内存图**：

```text
┌──────────────────── RunEndEncodedArray ───────────────────┐
│ length = 5                                                │
├────────────────────────────────────────────────────────────┤
│ no direct buffers                                         │
├────────────────────────────────────────────────────────────┤
│ child[0] run_ends = Int32Array [2, 5]                     │
│ child[1] values   = Int32Array [100, 200]                 │
│ [0,2) -> 100, [2,5) -> 200                                │
│ logical values -> [100, 100, 200, 200, 200]               │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- `run_ends` 查找通常需要二分，不要按线性扫实现热点路径
- 最后一个 run_end 必须覆盖整个逻辑长度
- writer 构造时要确保 run 合并规则正确

**C++ / Rust 对照**：
- C++: `arrow::RunEndEncodedArray` / run-end-encoded builder / `arrow::ArrayData`
- Rust: `arrow_array::RunArray<RunEndType>` / 对应 run-array construction helpers / `arrow_data::ArrayData`

### 3.20 Extension

**解释**：Extension 不是新的物理布局，而是在已有 storage type 上叠加自定义语义。

**实现**：layout 校验走 storage type；扩展名和 metadata 负责语义识别。

| 字段 | 内容 |
|---|---|
| `TypeID` | `Extension` |
| `物理 buffer` | 继承 storage type |
| `children` | 继承 storage type |
| `字节宽度` | 继承 storage type |

**访问公式**：

```text
value(i) = interpret_as_extension(storage_value(i), extension_name, metadata)
```

**细节与约束**：
- extension name 决定类型身份
- metadata 可承载版本或额外参数
- IPC / FFI 中通常通过 metadata 传递扩展信息
- extension 的物理布局始终与 storage type 完全一致
- 在 IPC 中通常通过 schema / field metadata 传输扩展信息
- 在 C Data Interface 中也通常通过 schema metadata 传输扩展信息

**内存图**：

```text
┌──────────────────────── ExtensionArray ───────────────────┐
│ extension name = "com.example.int32_ext"                  │
│ storage type   = Int32                                    │
│ length = 3, null_count = 1                                │
├────────────────────────────────────────────────────────────┤
│ physical layout is identical to Int32Array                │
├────────────────────────────────────────────────────────────┤
│ buffer[0] validity bitmap                                 │
│   slot:  0  1  2                                          │
│   bit :  1  0  1                                          │
│ buffer[1] values = [7, ?, 11]                             │
│ logical values -> [ext(7), null, ext(11)]                 │
└────────────────────────────────────────────────────────────┘
```

**实现要点与常见坑**：
- extension 不是新布局，先把 storage type 实现好最重要
- identity 依赖 extension name 与 metadata，不要只看 storage type
- IPC / FFI 往返时要保住扩展元信息

**C++ / Rust 对照**：
- C++: storage 对应普通 `arrow::*Array`，扩展语义常落在 `arrow::ExtensionType` / `Field` metadata / `arrow::ArrayData`
- Rust: storage 对应普通 `arrow_array::*Array`，扩展语义常落在 `arrow_schema::DataType` metadata 或扩展封装 / `arrow_data::ArrayData`

### 3.21 总表

| 类型组 | TypeID | buffers | children | 典型字节宽度 |
|---|---|---:|---:|---|
| Null | `Null` | 0 | 0 | 无 |
| Boolean | `Bool` | 2 | 0 | bit-packed |
| Fixed-width primitive | `Int*` `UInt*` `Float*` | 2 | 0 | 1/2/4/8 |
| Decimal | `Decimal*` | 2 | 0 | 4/8/16/32 |
| Date/Time/Timestamp/Duration | temporal | 2 | 0 | 4 或 8 |
| Interval | interval | 2 | 0 | 4/8/16 |
| FixedSizeBinary | `FixedSizeBinary` | 2 | 0 | schema 指定 |
| String/Binary | `String` `Binary` | 3 | 0 | offsets 4 |
| LargeString/LargeBinary | `LargeString` `LargeBinary` | 3 | 0 | offsets 8 |
| StringView/BinaryView | `StringView` `BinaryView` | 2+ | 0 | view record 固定 |
| List/LargeList | `List` `LargeList` | 2 | 1 | offsets 4/8 |
| FixedSizeList | `FixedSizeList` | 1 | 1 | list_size 决定逻辑宽度 |
| ListView/LargeListView | `ListView` `LargeListView` | 3 | 1 | offsets/sizes 4/8 |
| Struct | `Struct` | 1 | N | 由 child 决定 |
| Map | `Map` | 2 | 1 | offsets 4 |
| SparseUnion | `SparseUnion` | 1 | N | type_ids 1 |
| DenseUnion | `DenseUnion` | 2 | N | type_ids 1, offsets 4 |
| Dictionary | `Dictionary` | 2 + dictionary | 0 | indices 1/2/4/8 |
| RunEndEncoded | `RunEndEncoded` | 0 | 2 | run_end 4/8 |
| Extension | `Extension` | inherited | inherited | inherited |

---

## 4. Builder 与内存管理

Builder 的职责是安全地产生合法的 `ArrayData`，而不是简单镜像最终数组类型。

通常要负责：

- 分配和扩容 buffer
- 写入 values / offsets / validity
- 管理 child builder
- finish 时生成结构正确的 array

常见优化：

- validity bitmap 延迟分配
- shared buffer 引用计数
- slice 不复制，只调整 `offset` 与 `length`

---

## 5. Validation 清单

实现中至少要验证：

- buffer 数量是否符合类型要求
- child 数量是否符合类型要求
- validity bitmap 是否足够覆盖 `[offset, offset + length)`
- fixed-width values buffer 是否足够大
- offsets 数组是否单调且不越界
- list/map/struct/union child 边界是否正确
- dictionary 索引是否越界
- REE 的 run_ends 是否单调递增

---

## 6. IPC Stream

IPC stream 适合顺序传输，结构大致为：

```text
[message length / continuation]
[flatbuffer metadata]
[body]
...
[EOS]
```

核心消息：

- `Schema`
- `DictionaryBatch`
- `RecordBatch`

实现重点：

- continuation marker
- metadata / body 分离
- body padding
- EOS 处理

---

## 7. IPC File

IPC file 在 stream 风格消息区之外，增加了支持随机访问的 footer：

```text
magic
messages...
footer
footer_length
magic
```

实现重点：

- header / trailer magic 校验
- footer block index 校验
- block offset / metadata length / body length 检查
- `recordBatchCount`
- 随机读取与 dictionary 状态重建

---

## 8. Dictionary Batch 与 Delta

字典编码在 IPC 中会引入额外状态：

- schema 中 field 绑定 dictionary id
- dictionary values 单独通过 dictionary batch 发送
- record batch 中只存 indices

reader 需要维护：

- `dictionary_id -> dictionary_values`
- delta merge
- file 随机访问时的前置状态回放

---

## 9. C Data Interface

FFI 的核心结构：

- `ArrowSchema`
- `ArrowArray`
- `ArrowArrayStream`

含义：

- `ArrowSchema` 表达逻辑类型
- `ArrowArray` 表达物理布局
- `release callback` 管理生命周期

实现关键点：

- format string 映射
- private_data 持有
- children / dictionary 级联释放
- borrowed 与 owned 的区分

---

## 10. 实现建议

推荐实现顺序：

1. aligned buffer 与 bitmap 工具
2. `DataType / Field / Schema`
3. `ArrayData`
4. primitive / string / list / struct
5. record batch
6. IPC stream
7. IPC file
8. dictionary / union / REE / views / extension
9. FFI

最常见的坑：

- `offset` 处理不完整
- 把 null 当成 values bytes 的一部分
- varlen offsets 校验不够严
- file reader 只支持顺序读取，不支持随机读取
- FFI release 语义混乱

---

## 11. 最终结论

Arrow 的核心并不是某个语言里的对象模型，而是这套跨语言共享的物理布局约定。

如果只能记住一件事，那就是：

先把 `ArrayData`、buffer 规则、offset 规则和 layout validation 做对，再去实现更高层的 API、IPC、FFI 和优化。
