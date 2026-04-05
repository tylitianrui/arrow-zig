# zarrow TODO

Goal: Build a Zig ecosystem for Apache Arrow (core + IPC + tooling + integrations).

## Phase 0 - Project basics
- Define project scope and non-goals for the first release.
- Expand README with usage, build/test commands, and module overview.
- Establish semantic versioning and release process.
- Add a CONTRIBUTING guide and code style notes.

## Phase 1 - Core data model
- Finish ArrayData validation rules and tests for all supported types.
- Add large types: LargeString, LargeBinary, LargeList (+ builders).
- Add fixed-size types: FixedSizeBinary, FixedSizeList (+ builders).
- Add Date/Time/Timestamp/Duration/Interval arrays and builders.
- Add Decimal (32/64/128/256) arrays and builders.
- Add Dictionary arrays (index + values) and builders.
- Add Struct, List, Map, Union, RunEndEncoded arrays.

## Phase 2 - IPC and memory format
- Implement Schema/Field metadata serialization.
- Implement Arrow IPC: stream + file format (writer).
- Implement IPC reader with zero-copy buffers.
- Add buffer alignment and size validation for IPC payloads.

## Phase 3 - Compute + kernels
- Implement basic compute kernels (filter, take, cast, compare).
- Add arithmetic kernels for primitive types.
- Add string/binary kernels (concat, length, slice).
- Add null handling utilities and bitmap ops.

## Phase 4 - Interop + storage
- Add C Data Interface (FFI) support.
- Add Parquet read/write (optional, if scope allows).
- Add CSV/JSON adapters for quick import/export.

## Phase 5 - UX and tooling
- Add examples for all array types/builders.
- Add benchmarks for builders + IPC read/write.
- Add fuzz tests for ArrayData validation and IPC parsing.

## Near-term milestones (suggested)
1. Complete validation and tests for current arrays.
2. Add LargeString/LargeBinary/FixedSizeBinary + builders.
3. Add IPC writer for Schema + RecordBatch (no dictionary).
4. Add IPC reader for same subset.
