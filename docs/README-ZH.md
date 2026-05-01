# zarrow

[English](../README.md) | [中文](README-ZH.md)

一个面向零拷贝列式数据交换的 Apache Arrow Zig 实现。

`zarrow` 将 Arrow 的内存格式和 IPC 工作流带到 Zig，支持核心内存布局、数组构建、布局校验和高效切片。

它面向希望在系统语言中使用 Arrow 的开发者，强调显式控制、可预测性能和清晰的互操作边界。

## 亮点

- Zig 实现的 Apache Arrow 核心内存模型
- 用于构建 Arrow 数组的 Builders
- 布局校验，提升数据处理安全性
- 零拷贝切片
- Arrow IPC stream 读写
- 支持 Arrow C Data / C Stream ABI 导入导出
- 面向与其他 Arrow 实现互操作而设计

## 项目缘起

Apache Arrow 是现代分析与数据基础设施中最重要的基础组件之一，但 Zig 生态仍缺少成熟的原生实现。

`zarrow` 的目标是补齐这个空白：不是包装其他生态，而是直接在 Zig 中实现 Arrow。

## 项目状态

`zarrow` 正在积极开发中，已可用于实验、互操作测试以及构建基于 Arrow 的 Zig 工具。

项目希望让 Zig 在系统、分析与数据基础设施场景中，成为处理 Arrow 数据的可靠选项。

项目仍在持续演进，面向探索 Arrow 工具链、互操作和系统集成的开发者。

欢迎提交贡献、问题反馈与互操作测试结果。

## 使用

### 1. 添加依赖

在项目根目录运行：

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

### 2. 配置 `build.zig`

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

### 3. 示例

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

运行 `zig build run` 可看到输出：

```
len=3, v0=10, isNull1=true, v2=30
```

## Compute API

如果你要开发 `zarrow-compute`，请看：

- [Compute API（zarrow-core）使用说明](compute-api-zh.md)
- 示例：`../examples/compute_lifecycle.zig`

## 类型覆盖

如果你想查看当前数据类型支持范围（已声明/已实现/已测试/有示例），请看：

- [类型覆盖矩阵（中文）](type-coverage-zh.md)
- [Type Coverage Matrix (English)](type-coverage.md)

## 互操作

关于 Arrow IPC 与 C ABI 互操作覆盖范围和本地验证命令，请看：

- [Arrow Go IPC 互操作指南](interop-arrow-go-zh.md)
- [Arrow C ABI 互操作指南](interop-c-abi-zh.md)
- C ABI Zig 示例：`zig build example-c_abi_roundtrip`

## 贡献与发布

- [贡献指南](../CONTRIBUTING.md)
- [变更日志](../CHANGELOG.md)
- [API 稳定性策略](api-stability.md)
- [发布检查清单](release-checklist.md)
- [发布说明草案](release-notes-next.md)
