# zarrow

[English](../README.md) | [中文](README-ZH.md)

Apache Arrow 的 Zig 实现。

## 状态

- 开发状态：Active development（持续迭代中）
- 稳定性：核心功能可用，但 API 与覆盖范围仍在完善
- 测试基线：提交前应保证 `zig build test` 通过

## 元数据

| 项目 | 值 |
|---|---|
| 名称 | `zarrow` |
| 当前版本 | `0.0.1` |
| 最低 Zig 版本 | `0.15.2` |
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

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#5ede2689d054cbcf0d29c45c196d1aae344a50ae"
```

### 2. 配置 `build.zig`

两种方式任选其一。

**方式一（推荐）** — 添加预生成 `FlatBuffers` 代码的步骤，首次构建自动触发，后续无额外开销：

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});
const zarrow_path = zarrow_dep.builder.build_root.path.?;
const lib_zig_path = std.fs.path.join(b.allocator, &.{
    zarrow_path, ".zig-cache", "flatc-zig", "lib.zig",
}) catch @panic("OOM");
std.fs.accessAbsolute(lib_zig_path, .{}) catch {
    var child = std.process.Child.init(
        &.{ b.graph.zig_exe, "build", "test" },
        b.allocator,
    );
    child.cwd = zarrow_path;
    _ = child.spawnAndWait() catch @panic("zarrow: failed to pre-generate FlatBuffers code");
};
exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

**方式二（简易）** — 跳过预生成步骤，首次编译报错时手动触发一次：

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

```sh
# 首次编译报错时，在依赖目录下执行一次即可
cd ~/.cache/zig/p/zarrow-<version>-<hash>/
zig build test
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

