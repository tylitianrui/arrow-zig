# zarrow

[English](README.md) | [中文](docs/README-ZH.md)

A Zig implementation of Apache Arrow for zero-copy, columnar data interchange.

`zarrow` brings Arrow’s in-memory format and IPC workflows into Zig, with support for core memory layouts, array builders, validation, and efficient slicing.

Built for people who want Arrow in a systems language with explicit control, predictable performance, and clean interoperability boundaries.

## Highlights


- Core Apache Arrow memory model in Zig
- Builders for constructing Arrow arrays
- Layout validation for safer data handling
- Zero-copy slicing
- Arrow IPC stream read/write
- Arrow C Data/C Stream ABI import/export
- Designed for interoperability with other Arrow implementations

## Why this project exists

Apache Arrow is one of the most important foundations for modern analytics and data infrastructure, but Zig still lacks a mature native implementation.

`zarrow` exists to close that gap: not by wrapping another ecosystem, but by building Arrow in Zig directly.


## Project status

`zarrow` is under active development and already useful for experimentation, interoperability testing, and building Arrow-based tooling in Zig.

The project aims to make Zig a serious option for working with Arrow data in systems, analytics, and data infrastructure use cases.

The project is actively evolving and intended for developers exploring Arrow-based tooling, interoperability, and systems integration in Zig.

Contributions, bug reports, and interoperability feedback are welcome.


## Usage

### 1. Add dependency

Run the following in your project root:

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

### 2. Configure `build.zig`

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

## Interop

For Arrow IPC and C ABI interop coverage and local verification commands, see:

- [docs/interop-arrow-go.md](docs/interop-arrow-go.md)
- [docs/interop-c-abi.md](docs/interop-c-abi.md)
- C ABI Zig example: `zig build example-c_abi_roundtrip`

## Contributing

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [API Stability Policy](docs/api-stability.md)
- [Release Checklist](docs/release-checklist.md)
- [Release Notes Draft](docs/release-notes-next.md)
