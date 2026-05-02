# zarrow

[English](README.md) | [中文](docs/README-ZH.md)

**Apache Arrow, built natively for Zig.**

`zarrow` is a Zig-native implementation of Apache Arrow for zero-copy, columnar data interchange. It provides Arrow-compatible array layouts, builders, validation, IPC stream support, and Arrow C Data / C Stream ABI interoperability without wrapping C++ or Rust.

The goal is to make Zig a serious option for Arrow-based systems, analytics tooling, storage engines, query engines, and data interoperability work.

---

## Why zarrow?

Apache Arrow is one of the core foundations of modern data infrastructure. It defines a language-independent columnar memory format used across analytics engines, databases, dataframe libraries, and storage systems.

Zig has strong properties for this space: explicit memory management, predictable performance, simple cross-language boundaries, and excellent systems-level control. `zarrow` exists to bring Arrow into Zig directly, as a native library rather than as a wrapper around another ecosystem.

Use `zarrow` when you want to:

- construct Arrow arrays in Zig;
- validate Arrow-compatible memory layouts;
- read or write Arrow IPC streams;
- exchange Arrow data with other runtimes through the Arrow C Data / C Stream ABI;
- build downstream Arrow tooling such as compute kernels, IPC utilities, query engines, or storage integrations.

---

## Status

`zarrow` is under active development. It is already useful for experimentation, interoperability testing, and early downstream libraries, but the public API is still evolving.

| Area | Status |
| --- | --- |
| Core Arrow memory layout | Supported |
| Array builders | Supported |
| Layout validation | Supported |
| Zero-copy slicing | Supported |
| Primitive arrays | Supported |
| Boolean, string, binary arrays | Supported |
| List, struct, map, union, dictionary, run-end encoded arrays | Supported / evolving |
| Temporal and decimal types | Supported / evolving |
| Record batches | Supported |
| Arrow IPC stream read/write | Supported |
| Arrow IPC interop fixtures | Supported |
| Arrow C Data / C Stream ABI | Supported |
| `zarrow-core` module for downstream libraries | Supported |
| Full compute kernels | Downstream project: [zarrow-compute](https://github.com/tylitianrui/zarrow-compute) |
| Parquet | Downstream project: [zarrow-parquet](https://github.com/tylitianrui/zarrow-parquet) |

The project favors correctness, explicit ownership, interop testing, and spec-aligned behavior over premature API stabilization.

---

## Highlights

- Zig-native Apache Arrow implementation
- Arrow-compatible in-memory array layouts
- Builders for primitive, boolean, string, binary, nested, dictionary, temporal, decimal, and advanced array types
- Null bitmap handling and layout validation
- Zero-copy slicing
- Record batch support
- Arrow IPC stream read/write
- Arrow C Data / C Stream ABI import/export
- Shared C ABI library surface: `zarrow_c`
- `zarrow-core` module for downstream libraries that do not need IPC/FFI
- Interop smoke tests against Arrow Go, Arrow C++, and arrow-rs tooling
- Compute framework foundation for downstream [zarrow-compute](https://github.com/tylitianrui/zarrow-compute)
- Examples, benchmarks, fuzz corpus, and CI-oriented compatibility tools

---

## Requirements

`master` currently targets Zig `0.15.x`.

Because Zig has frequent breaking changes between compiler versions, `zarrow` maintains versioned tags that encode both:

- `v<zarrow-version>` = zarrow release version
- `zig0.15` = Zig compiler major/minor compatibility target

For example, `v0.0.2-zig0.15` means:

- zarrow release version `0.0.2`
- Zig compatibility target `0.15.x`

Recommended tag mapping:

| Zig version | Use tag |
| --- | --- |
| `0.15.x` | `v<zarrow-version>-zig0.15` |
| `0.14.x` | `v<zarrow-version>-zig0.14` |
| `0.13.x` | `v<zarrow-version>-zig0.13` |

If you are using older Zig versions, use the matching `-zig0.xx` tag instead of `master`.

---

## Installation

Add `zarrow` to your Zig project:

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#master"
```

For release-pinned usage, prefer a release tag:

```sh
zig fetch --save "git+https://github.com/tylitianrui/zarrow#v<zarrow-version>-zig0.15"
```

Then wire the module in your `build.zig`:

```zig
const zarrow_dep = b.dependency("zarrow", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("zarrow", zarrow_dep.module("zarrow"));
```

If your downstream library only needs core data structures and compute framework APIs, import `zarrow-core` instead:

```zig
exe.root_module.addImport("zarrow-core", zarrow_dep.module("zarrow-core"));
```

---

## Quick Start

Create a nullable `Int32Array`:

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

Expected output:

```text
len=3, v0=10, isNull1=true, v2=30
```

---

## Public Modules

`zarrow` exposes two Zig modules:

| Module | Purpose |
| --- | --- |
| `zarrow` | Full public package surface, including arrays, schemas, record batches, IPC, and public Arrow data structures. |
| `zarrow-core` | Core-only surface for downstream libraries that do not require IPC or FFI dependencies. Useful for compute libraries such as [zarrow-compute](https://github.com/tylitianrui/zarrow-compute). |

The build also provides a C ABI shared library:

| Target | Purpose |
| --- | --- |
| `zarrow_c` | Shared C ABI surface for Arrow C Data / C Stream import and export. |

Build the C ABI library with:

```sh
zig build c-api-lib
```

---

## Feature Overview

The **Status** table above is the single capability summary source in this README.

Use this section as a quick navigation map:

- Type coverage matrix:
  - [docs/type-coverage.md](docs/type-coverage.md)
  - [docs/type-coverage-zh.md](docs/type-coverage-zh.md)
- IPC interop guide:
  - [docs/interop-arrow-go.md](docs/interop-arrow-go.md)
  - [docs/interop-arrow-go-zh.md](docs/interop-arrow-go-zh.md)
- Arrow C Data / C Stream ABI guide:
  - [docs/interop-c-abi.md](docs/interop-c-abi.md)
  - [docs/interop-c-abi-zh.md](docs/interop-c-abi-zh.md)
- Compute framework boundary:
  - [docs/compute-api.md](docs/compute-api.md)
  - [docs/compute-api-zh.md](docs/compute-api-zh.md)
  - [docs/ecosystem-readiness.md](docs/ecosystem-readiness.md)

Common commands:

```sh
zig build example-ipc_stream
zig build c-api-lib
zig build example-c_abi_roundtrip
zig build interop-fixture-writer -- .interop-fixtures/zarrow.arrow canonical stream
zig build interop-fixture-check -- .interop-fixtures/zarrow.arrow canonical stream
```

---

## Compute Framework

`zarrow` includes a compute framework foundation for downstream projects such as [zarrow-compute](https://github.com/tylitianrui/zarrow-compute).

The core library provides framework-level building blocks:

- `FunctionRegistry`
- `Kernel`
- `KernelSignature`
- `ExecContext`
- `Datum`
- typed options
- result type inference
- scalar / array / chunked input representation
- null propagation helpers
- unary, binary, and n-ary execution helpers
- chunk-aware helpers for take, filter, and permutation-style operations

`zarrow` intentionally does **not** try to own the full compute function catalog in this repository. Concrete kernels such as arithmetic, comparison, filter, sort, cast, and aggregate functions should live in downstream libraries such as [zarrow-compute](https://github.com/tylitianrui/zarrow-compute).

For the downstream API boundary, see:

- [docs/compute-api.md](docs/compute-api.md)
- [docs/compute-api-zh.md](docs/compute-api-zh.md)
- [docs/ecosystem-readiness.md](docs/ecosystem-readiness.md)
- Example: [`examples/compute_lifecycle.zig`](examples/compute_lifecycle.zig)

---

## Examples

Run the default example:

```sh
zig build run
```

Run all examples:

```sh
zig build examples
```

Run a specific example:

```sh
zig build example-primitive_builder
zig build example-string_builder
zig build example-struct_builder
zig build example-ipc_stream
zig build example-c_abi_roundtrip
zig build example-compute_lifecycle
```

Available examples include builders for primitive, unsigned integer, boolean, string, large string, binary, large binary, decimal, dictionary, list, large list, fixed-size list, fixed-size binary, map, struct, union, run-end encoded, temporal, interval, view, IPC, C ABI, and compute lifecycle workflows.

---

## Development

Clone the repository and run tests:

```sh
git clone https://github.com/tylitianrui/zarrow.git
cd zarrow
zig build test
```

Run formatting before opening a PR:

```sh
zig fmt src tools examples benchmarks build.zig
```

Run benchmarks:

```sh
zig build benchmark-smoke
zig build benchmark
zig build benchmark-full
```

Run built-in fuzz corpus replay:

```sh
zig build fuzz-corpus
```

Run IPC fuzz harnesses manually:

```sh
zig build fuzz-array-layout
zig build fuzz-ipc-reader
```

---

## Interoperability Verification

`zarrow` treats interoperability as a core project requirement.

Current interop areas include:

| Area | Coverage |
| --- | --- |
| Arrow Go IPC | Fixture generation and validation in both directions |
| Arrow C Data ABI | Import/export smoke checks |
| Arrow C Stream ABI | Streaming-preserving import/export path |
| Arrow C++ | C ABI smoke tooling |
| arrow-rs | C ABI smoke tooling |
| PyArrow compute | Compatibility check tooling for compute-alignment cases |

Useful commands:

```sh
zig build c-api-lib
zig build example-c_abi_roundtrip
zig build compute-compat-check -- numeric
```

See the interop docs for the full local verification matrix:

- [docs/interop-arrow-go.md](docs/interop-arrow-go.md)
- [docs/interop-c-abi.md](docs/interop-c-abi.md)

---

## API Stability

The public API is evolving.

The project uses three stability levels:

| Level | Meaning |
| --- | --- |
| Stable | Intended for external use, with compatibility preserved across patch/minor releases. |
| Experimental | Public but still evolving; may change with release notes. |
| Internal | No compatibility guarantees. |

See:

- [docs/api-stability.md](docs/api-stability.md)
- [CHANGELOG.md](CHANGELOG.md)
- [docs/release-checklist.md](docs/release-checklist.md)
- [docs/release-notes-next.md](docs/release-notes-next.md)

---

## Ecosystem

`zarrow` is intended to be the base layer for a Zig Arrow ecosystem.

Downstream and related projects:

- [zarrow-compute](https://github.com/tylitianrui/zarrow-compute) — Arrow compute kernels in Zig, built on top of `zarrow` / `zarrow-core`.
- [zarrow-parquet](https://github.com/tylitianrui/zarrow-parquet) — Parquet read/write integration built as a downstream project.

Planned or natural ecosystem directions:

- compute kernels;
- dataframe/query-engine integration;
- Parquet integration;
- Arrow Flight / IPC tooling;
- PyArrow / Arrow C++ / arrow-rs interoperability examples;
- more downstream examples using `zarrow-core`.

If your project uses `zarrow`, feel free to open a PR adding it to this section.

---

## Roadmap

Near-term priorities:

- keep Arrow layout behavior spec-aligned;
- expand type coverage tests and examples;
- strengthen IPC and C ABI interop coverage;
- improve downstream `zarrow-core` ergonomics;
- support [zarrow-compute](https://github.com/tylitianrui/zarrow-compute) with stable compute framework boundaries;
- add more real-world interop demos;
- improve documentation for ownership, memory layout, and lifecycle rules.

Non-goals for this repository:

- becoming a full query engine;
- owning every compute kernel directly inside `zarrow`;
- implementing Parquet in the core repository;
- hiding Arrow ownership semantics behind implicit runtime behavior.

---

## Contributing

Contributions are welcome, especially in these areas:

- correctness tests;
- error-path tests;
- Arrow IPC edge cases;
- C ABI lifecycle coverage;
- examples and documentation;
- benchmarks;
- downstream integration feedback;
- type coverage improvements.

Before opening a PR:

```sh
zig fmt src tools examples benchmarks build.zig
zig build test
zig build benchmark-smoke
```

Please keep changes focused and mention any compatibility impact on public API, IPC behavior, or interop behavior.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Licensed under the Apache License, Version 2.0.

See [LICENSE](LICENSE).
