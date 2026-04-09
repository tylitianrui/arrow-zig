# zarrow

[English](README.md) | [中文](docs/README-ZH.md)

A Zig implementation of Apache Arrow.

## Status

- Development: Active development (continuously evolving)
- Stability: Core functionality is usable; API and coverage are still being refined
- Test baseline: `zig build test` must pass before committing

## Metadata

| Field | Value |
|---|---|
| Name | `zarrow` |
| Current version | `0.0.1` |
| Minimum Zig version | `0.15.2` |
| Dependencies declared | Yes (`build.zig.zon`) |
| Direct dependencies | `flatbufferz` |

Note: version, minimum Zig version, and dependencies are sourced from [build.zig.zon](build.zig.zon).

## Supported

- Arrow core memory model and array builders
- Layout validation (ArrayData validate)
- Zero-copy slicing and shared read-only buffers
- IPC Stream Reader: Schema / RecordBatch / DictionaryBatch
- IPC Stream Writer: Schema / RecordBatch / DictionaryBatch (including REE and dictionary delta)
- Interop matrix: PyArrow, arrow-rs, Arrow C++ (bidirectional read/write verification)

## Usage

### 1. Add dependency

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#5ede2689d054cbcf0d29c45c196d1aae344a50ae"
```

### 2. Configure `build.zig`

Choose one of the two options below.

**Option 1 (recommended)** — Add a pre-generation step for FlatBuffers code. Triggers automatically on the first build; no overhead afterward:

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

**Option 2 (simple)** — Skip the pre-generation step; run it manually once if the first build fails:

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

```sh
# Run once in the dependency directory if the first build fails
cd ~/.cache/zig/p/zarrow-<version>-<hash>/
zig build test
```

### 3. Example

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

