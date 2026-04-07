# zarrow

Zig implementation of Apache Arrow.

## Goals

zarrow aims to provide a Zig implementation of the Apache Arrow in-memory model, with a strong focus on:

- clear ownership and lifetime semantics
- correct Arrow physical layout representation
- explicit validation of array structure and buffer layout
- zero-copy slicing and shared read-only buffers
- ergonomic builders for constructing Arrow arrays in Zig

## Project Status

zarrow is currently in active development.

The ownership model, public APIs, and supported Arrow types are still evolving.  
At this stage, the goal is to make the core memory model, validation rules, and array construction semantics explicit and correct before expanding broader format and ecosystem support.

## Benchmark

- Run all benchmarks (default): `zig build benchmark -Doptimize=ReleaseFast`
- Run all benchmarks (smoke): `zig build benchmark-smoke -Doptimize=ReleaseFast`
- Run all benchmarks (full): `zig build benchmark-full -Doptimize=ReleaseFast`
- Run all benchmarks (matrix CSV): `zig build benchmark-matrix -Doptimize=ReleaseFast`
- Run all benchmarks (CI CSV): `zig build benchmark-ci -Doptimize=ReleaseFast`
- Run one benchmark: `zig build benchmark-primitive_builder_benchmark -Doptimize=ReleaseFast`
- Run one benchmark in mode: `zig build benchmark-primitive_builder_benchmark -Doptimize=ReleaseFast -- matrix`

Benchmarks live under `benchmarks/` and are kept separate from examples.
