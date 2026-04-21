# zarrow

[English](../README.md) | [中文](README-ZH.md)

Apache Arrow 的 Zig 实现。

## 元数据

| 项目 | 值 |
|---|---|
| 名称 | `zarrow` |
| 当前版本 | `0.0.1` |
| 最低 Zig 版本 | `0.15.1` |
| 最高 Zig 版本 | `0.15.x`（暂不支持 Zig 0.16+） |
| 依赖是否清晰 | 是（`build.zig.zon` 中声明） |
| 当前直接依赖 | `flatbufferz` |

说明：版本、最低 Zig 版本与依赖来源于 [build.zig.zon](../build.zig.zon)。

## 已支持

- Arrow 核心内存模型与数组构建（Builders）
- 布局校验（ArrayData validate）
- 零拷贝切片与共享只读 buffer
- IPC Stream Reader：Schema / RecordBatch / DictionaryBatch
- IPC Stream Writer：Schema / RecordBatch / DictionaryBatch（含 REE、dictionary delta 场景）
- 互操作矩阵：PyArrow、arrow-rs、Arrow C++（双向读写验证）

## 使用方法

### 1. 添加依赖

在项目根目录下运行：

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

### 2. 配置 `build.zig`

`zarrow` 现在采用“`arrow_fbs` 预生成并提交仓库 + 保留 `flatbufferz` 运行时依赖”的模式。使用方只需正常引入模块，不需要再做 flatc 预生成步骤：

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

执行 `zig build run` 即可看到输出：

```
len=3, v0=10, isNull1=true, v2=30
```

## Compute API

如果你要开发 `zarrow-compute`，请看：

- [Compute API（zarrow-core）使用说明](compute-api-zh.md)
- 示例：`examples/compute_lifecycle.zig`

## 分库扩展

如果你要把 `zarrow-compute`、`zarrow-flight`、`zarrow-parquet` 拆成独立仓库并依赖 `zarrow`，请看：

- [分库方案与依赖规范](multi-repo-split-zh.md)
- 脚手架：`tools/scaffold_downstream_repo.sh`

## 贡献与发布

- [贡献指南](../CONTRIBUTING.md)
- [API 稳定性策略](api-stability.md)
- [发布检查清单](release-checklist.md)
