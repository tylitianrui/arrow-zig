# Release Notes Draft (v0.0.2)

## Summary

`v0.0.2` focuses on interoperability completeness and C ABI robustness:

- expanded Arrow Go IPC interop coverage and docs;
- introduced and documented C ABI smoke/readiness workflow;
- moved C Stream interop to true streaming transfer (no full-stream materialization);
- improved interop error handling and cross-ecosystem smoke reliability.

## New Features

- Added `zarrow.c_api` export for direct C ABI surface access from package root.
- Added `examples/c_abi_roundtrip.zig` for end-to-end stream roundtrip via C ABI.
- Added C ABI interop guides:
  - `docs/interop-c-abi.md`
  - `docs/interop-c-abi-zh.md`

## Bug Fixes and Reliability

- Fixed C ABI smoke integration with Arrow C++.
- Fixed arrow-rs interop binary invocation ambiguity (`cargo run --bin ...`).
- Improved Go/Rust interop tooling and CI smoke consistency.
- Strengthened error-path handling in array/value/interop validation paths.

## Breaking Changes

- `zarrow_c_export_stream` now requires mutable handle:
  - from: `const zarrow_c_stream_handle*`
  - to: `zarrow_c_stream_handle*`

## Migration Guidance

- Regenerate/update downstream FFI bindings for `zarrow_c_export_stream`.
- Update callers to pass mutable stream handles.
- Do not reuse a stream handle after successful export:
  - ownership is transferred;
  - repeated export returns `ZARROW_C_STATUS_RELEASED`.

## Compatibility Notes

- Minimum Zig: `0.14.1` (release/zig-0.14 line).
- Arrow Go IPC:
  - main lane targets Go `1.24.x`;
  - version-smoke lane validates Go `1.23.x` and `1.24.x`.
- C ABI smoke lanes:
  - `interop-c-abi-cpp-smoke`
  - `interop-c-abi-rs-smoke`
