# zarrow

[English](README.md) | [中文](docs/README-ZH.md)

A Zig implementation of Apache Arrow.

## Metadata

| Field | Value |
|---|---|
| Name | `zarrow` |
| Current version | `0.0.1` |
| Minimum Zig version | `0.15.1` |
| Maximum Zig version | `0.15.x` (Zig 0.16+ not yet supported) |
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

Run the following in your project root:

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

### 2. Configure `build.zig`

`zarrow` now uses pre-generated `arrow_fbs` sources committed in the repository, while keeping `flatbufferz` as the FlatBuffers runtime dependency. Consumers only need to add the module import:

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
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

Run `zig build run` to see the output:

```
len=3, v0=10, isNull1=true, v2=30
```

## Compute API

For compute framework APIs used by downstream `zarrow-compute`, see:

- [docs/compute-api-zh.md](docs/compute-api-zh.md)
- Example: `examples/compute_lifecycle.zig`

## Downstream Repos

For splitting `zarrow-compute`, `zarrow-flight`, and `zarrow-parquet` into separate repos while depending on `zarrow`, see:

- [docs/multi-repo-split-zh.md](docs/multi-repo-split-zh.md)
- Scaffold script: `tools/scaffold_downstream_repo.sh`

## Contributing

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [API Stability Policy](docs/api-stability.md)
- [Release Checklist](docs/release-checklist.md)
