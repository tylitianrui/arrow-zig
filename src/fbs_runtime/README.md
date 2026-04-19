# fbs_runtime

Vendored FlatBuffers runtime for Zig, originally derived from
[archaistvolts/flatbufferz](https://github.com/archaistvolts/flatbufferz) @ commit `315ed95`.

## Background

zarrow previously depended on `flatbufferz` as an external package declared in
`build.zig.zon`.  The dependency was vendored here (as `fbs_runtime`) so that:

- zarrow has **zero external dependencies** — no network fetch required to build.
- The runtime can be maintained independently alongside the rest of zarrow,
  including forward-porting to new Zig versions without waiting on upstream.

## What is included

Only the runtime subset required by the pre-generated Arrow IPC schema code in
`src/ipc_schema/` is kept.  Each generated file references this module via
`@import("fbs_runtime")`.  Code-generation helpers (`codegen`, `idl`,
`reflection`, `binary_tools`) are intentionally omitted.

| File | Purpose |
|---|---|
| `lib.zig` | Module entry point — re-exports all public symbols |
| `Builder.zig` | FlatBuffers encoder (write path) |
| `Table.zig` | FlatBuffers table decoder (read path) |
| `encode.zig` | Low-level little-endian byte read/write helpers |
| `common.zig` | Shared types: `PackOptions`, `PackError`, `BuilderError` |
| `flatbuffers.zig` | Top-level helpers: `GetRootAs`, `BufferHasIdentifier`, etc. |

## Updating

If a bug fix or Zig compatibility patch is needed, edit the files in this
directory directly.  When pulling in changes from upstream flatbufferz, apply
the diff only to the files listed above and verify with `zig build test`.

The module is wired up in `build.zig` as a local module named `fbs_runtime`
(replacing the former `flatbufferz` external dependency).
