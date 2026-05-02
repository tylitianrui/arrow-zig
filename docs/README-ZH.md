# zarrow

[English](../README.md) | [中文](README-ZH.md)

**为 Zig 原生构建的 Apache Arrow。**

`zarrow` 是 Apache Arrow 的 Zig 原生实现，面向零拷贝列式数据交换。它提供 Arrow 兼容的数组布局、Builder、布局校验、IPC Stream 支持，以及 Arrow C Data / C Stream ABI 互操作能力，不依赖 C++ 或 Rust 包装层。

项目目标是让 Zig 成为构建 Arrow 系统、分析工具、存储引擎、查询引擎与数据互操作工具链的严肃选项。

---

## 为什么是 zarrow？

Apache Arrow 是现代数据基础设施的核心基石之一。它定义了与语言无关的列式内存格式，被分析引擎、数据库、DataFrame 库和存储系统广泛使用。

Zig 在这个领域有天然优势：显式内存管理、可预测性能、清晰的跨语言边界与系统级控制能力。`zarrow` 的定位是把 Arrow 直接带到 Zig，而不是包装其他生态。

当你希望：

- 在 Zig 中构建 Arrow 数组；
- 校验 Arrow 兼容内存布局；
- 读取或写入 Arrow IPC Stream；
- 通过 Arrow C Data / C Stream ABI 与其他运行时交换数据；
- 构建下游 Arrow 工具（如 compute kernel、IPC 工具、查询引擎或存储集成）；

可以使用 `zarrow`。

---

## 当前状态

`zarrow` 正在积极开发中。它已可用于实验、互操作验证和下游早期库开发，但公共 API 仍在演进。

| 领域 | 状态 |
| --- | --- |
| Arrow 核心内存布局 | 已支持 |
| 数组 Builder | 已支持 |
| 布局校验 | 已支持 |
| 零拷贝切片 | 已支持 |
| 基础类型数组 | 已支持 |
| 布尔 / 字符串 / 二进制数组 | 已支持 |
| List / Struct / Map / Union / Dictionary / REE 数组 | 已支持 / 持续演进 |
| 时间与 Decimal 类型 | 已支持 / 持续演进 |
| RecordBatch | 已支持 |
| Arrow IPC Stream 读写 | 已支持 |
| Arrow IPC 互操作 fixtures | 已支持 |
| Arrow C Data / C Stream ABI | 已支持 |
| 面向下游库的 `zarrow-core` 模块 | 已支持 |
| 完整 compute kernels | 下游项目：[zarrow-compute](https://github.com/tylitianrui/zarrow-compute) |
| Parquet | 下游项目：[zarrow-parquet](https://github.com/tylitianrui/zarrow-parquet) |

项目优先级是正确性、显式所有权、互操作验证与规范对齐，而不是过早冻结 API。

---

## 亮点

- Zig 原生 Apache Arrow 实现
- Arrow 兼容内存数组布局
- 覆盖 primitive/boolean/string/binary/nested/dictionary/temporal/decimal/advanced 的 Builder
- Null bitmap 处理与布局校验
- 零拷贝切片
- RecordBatch 支持
- Arrow IPC Stream 读写
- Arrow C Data / C Stream ABI 导入导出
- 共享 C ABI 库目标：`zarrow_c`
- 面向无需 IPC/FFI 的下游库模块：`zarrow-core`
- 针对 Arrow Go、Arrow C++、arrow-rs 的互操作 smoke 测试
- 面向下游 [zarrow-compute](https://github.com/tylitianrui/zarrow-compute) 的 compute 框架基础
- 示例、基准、fuzz corpus 与 CI 兼容性工具

---

## 版本要求

`master` 分支当前目标 Zig 版本为 `0.15.x`。

由于 Zig 编译器版本间存在较多 breaking changes，`zarrow` 使用带编译器后缀的标签来维护多版本兼容：

- `v<zarrow-version>` 表示 zarrow 的版本
- `zig0.15` 表示目标 Zig 语言（编译器）版本

例如：`v0.0.2-zig0.15` 表示：

- zarrow 发布版本是 `0.0.2`
- 兼容的 Zig 版本目标是 `0.15.x`

推荐对应关系：

| Zig 版本 | 使用标签 |
| --- | --- |
| `0.15.x` | `v<zarrow-version>-zig0.15` |
| `0.14.x` | `v<zarrow-version>-zig0.14` |
| `0.13.x` | `v<zarrow-version>-zig0.13` |

如果你使用较老 Zig 版本，请优先使用对应的 `-zig0.xx` 标签，而不是 `master`。

---

## 安装

将 `zarrow` 添加到你的 Zig 项目：

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

如果你希望固定版本，建议使用 release tag：

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#v<zarrow-version>-zig0.15"
```

然后在 `build.zig` 中接入模块：

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

如果你的下游库只需要核心数据结构与 compute 框架 API，可改用 `zarrow-core`：

```zig
exe.root_module.addImport("zarrow-core", zarrow_dep.module("zarrow-core"));
```

---

## 快速开始

创建一个可空 `Int32Array`：

```zig
const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 3);
    defer builder.deinit();

    try builder.append(10);
    try builder.appendNull();
    try builder.append(30);

    var arr_ref = try builder.finish();
    defer arr_ref.release();

    const arr = zarrow.Int32Array{ .data = arr_ref.data() };

    std.debug.print("len={d}, v0={d}, isNull1={any}, v2={d}\n", .{
        arr.len(),
        arr.value(0),
        arr.isNull(1),
        arr.value(2),
    });
}
```

预期输出：

```text
len=3, v0=10, isNull1=true, v2=30
```

---

## 公共模块

`zarrow` 当前暴露两个 Zig 模块：

| 模块 | 作用 |
| --- | --- |
| `zarrow` | 全量公共包接口，包含数组、schema、record batch、IPC 及 Arrow 数据结构。 |
| `zarrow-core` | 核心能力接口，适合不需要 IPC 或 FFI 依赖的下游库（如 [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)）。 |

构建系统还提供 C ABI 共享库：

| 目标 | 作用 |
| --- | --- |
| `zarrow_c` | Arrow C Data / C Stream 导入导出的 C ABI 共享库接口。 |

构建 C ABI 共享库：

```sh
zig build c-api-lib
```

---

## 功能导航

本 README 中的能力覆盖以“当前状态”表格为唯一摘要来源。

本节作为快速导航入口：

- 类型覆盖矩阵：
  - [type-coverage.md](type-coverage.md)
  - [type-coverage-zh.md](type-coverage-zh.md)
- IPC 互操作指南：
  - [interop-arrow-go.md](interop-arrow-go.md)
  - [interop-arrow-go-zh.md](interop-arrow-go-zh.md)
- Arrow C Data / C Stream ABI 指南：
  - [interop-c-abi.md](interop-c-abi.md)
  - [interop-c-abi-zh.md](interop-c-abi-zh.md)
- Compute 框架边界：
  - [compute-api.md](compute-api.md)
  - [compute-api-zh.md](compute-api-zh.md)
  - [ecosystem-readiness.md](ecosystem-readiness.md)

常用命令：

```sh
zig build example-ipc_stream
zig build c-api-lib
zig build example-c_abi_roundtrip
zig build interop-fixture-writer -- .interop-fixtures/zarrow.arrow canonical stream
zig build interop-fixture-check -- .interop-fixtures/zarrow.arrow canonical stream
```

---

## Compute 框架

`zarrow` 提供面向下游（如 [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)）的 compute 框架基础。

核心库提供的框架级构件包括：

- `FunctionRegistry`
- `Kernel`
- `KernelSignature`
- `ExecContext`
- `Datum`
- 类型安全 options
- 结果类型推导
- `scalar / array / chunked` 三态输入表示
- null 传播 helper
- unary / binary / n-ary 执行 helper
- 面向 take/filter/permutation 路径的 chunk-aware helper

`zarrow` 在本仓库中**不会**直接承接完整 compute 函数目录。算术、比较、filter、sort、cast、aggregate 等具体 kernel 应在下游库（如 [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)）中实现。

下游 API 边界参考：

- [compute-api.md](compute-api.md)
- [compute-api-zh.md](compute-api-zh.md)
- [ecosystem-readiness.md](ecosystem-readiness.md)
- 示例：[`../examples/compute_lifecycle.zig`](../examples/compute_lifecycle.zig)

---

## 示例

运行默认示例：

```sh
zig build run
```

运行所有示例：

```sh
zig build examples
```

运行指定示例：

```sh
zig build example-primitive_builder
zig build example-string_builder
zig build example-struct_builder
zig build example-ipc_stream
zig build example-c_abi_roundtrip
zig build example-compute_lifecycle
```

当前示例覆盖 primitive/uint/boolean/string/large_string/binary/large_binary/decimal/dictionary/list/large_list/fixed_size_list/fixed_size_binary/map/struct/union/REE/temporal/interval/view/IPC/C ABI/compute lifecycle 等工作流。

---

## 开发

克隆仓库并运行测试：

```sh
git clone https://github.com/tylitianrui/zarrow.git
cd zarrow
zig build test
```

提交 PR 前运行格式化：

```sh
zig fmt src tools examples benchmarks build.zig
```

运行基准：

```sh
zig build benchmark-smoke
zig build benchmark
zig build benchmark-full
```

运行内置 fuzz corpus 回放：

```sh
zig build fuzz-corpus
```

手动运行 IPC fuzz harness：

```sh
zig build fuzz-array-layout
zig build fuzz-ipc-reader
```

---

## 互操作验证

`zarrow` 将互操作能力视为项目核心要求。

当前互操作覆盖：

| 领域 | 覆盖 |
| --- | --- |
| Arrow Go IPC | 双向 fixture 生成与校验 |
| Arrow C Data ABI | import/export smoke checks |
| Arrow C Stream ABI | 保留流语义的 import/export 路径 |
| Arrow C++ | C ABI smoke 工具链 |
| arrow-rs | C ABI smoke 工具链 |
| PyArrow compute | compute 对齐用兼容性检查工具 |

常用命令：

```sh
zig build c-api-lib
zig build example-c_abi_roundtrip
zig build compute-compat-check -- numeric
```

完整本地验证矩阵见：

- [interop-arrow-go.md](interop-arrow-go.md)
- [interop-c-abi.md](interop-c-abi.md)

---

## API 稳定性

公共 API 正在演进。

项目使用三档稳定性级别：

| 级别 | 含义 |
| --- | --- |
| Stable | 面向外部使用，补丁/小版本中尽量保持兼容。 |
| Experimental | 已公开但仍在演进，可能随 release notes 调整。 |
| Internal | 不提供兼容性保证。 |

参考：

- [api-stability.md](api-stability.md)
- [../CHANGELOG.md](../CHANGELOG.md)
- [release-checklist.md](release-checklist.md)
- [release-notes-next.md](release-notes-next.md)

---

## 生态

`zarrow` 的定位是 Zig Arrow 生态的基础层。

当前下游 / 相关项目：

- [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)：基于 `zarrow` / `zarrow-core` 的 Arrow compute kernels。
- [zarrow-parquet](https://github.com/tylitianrui/zarrow-parquet)：作为下游项目提供 Parquet 读写集成。

规划中或自然延伸方向：

- compute kernels；
- dataframe / query engine 集成；
- Parquet 集成；
- Arrow Flight / IPC 工具；
- PyArrow / Arrow C++ / arrow-rs 互操作示例；
- 更多基于 `zarrow-core` 的下游样例。

如果你的项目正在使用 `zarrow`，欢迎提交 PR 将项目加入本节。

---

## 路线图

近期重点：

- 保持 Arrow 布局行为与规范对齐；
- 扩展类型覆盖测试与示例；
- 强化 IPC 与 C ABI 互操作覆盖；
- 提升下游 `zarrow-core` 使用体验；
- 以稳定 compute 框架边界支撑 [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)；
- 增加真实互操作 demo；
- 持续完善所有权、内存布局、生命周期文档。

本仓库非目标：

- 成为完整查询引擎；
- 在 `zarrow` 内直接承载所有 compute kernel；
- 在核心仓库内直接实现 Parquet；
- 通过隐式运行时行为隐藏 Arrow 所有权语义。

---

## 贡献

欢迎贡献，尤其是：

- 正确性测试；
- 错误路径测试；
- Arrow IPC 边界场景；
- C ABI 生命周期覆盖；
- 示例与文档；
- 基准；
- 下游集成反馈；
- 类型覆盖提升。

提交 PR 前建议运行：

```sh
zig fmt src tools examples benchmarks build.zig
zig build test
zig build benchmark-smoke
```

请保持改动聚焦，并说明对公共 API、IPC 行为或互操作行为的兼容性影响。

详情见 [../CONTRIBUTING.md](../CONTRIBUTING.md)。

---

## 许可证

本项目基于 Apache License, Version 2.0 许可。

见 [../LICENSE](../LICENSE)。
