# zarrow TODO

Goal: Build a production-usable Zig implementation of Apache Arrow core memory model, then incrementally add IPC/interop/compute.

## Done (Code-Verified)

- [x] D1. Primitive numeric arrays/builders implemented.
- [x] D2. Boolean array/builder implemented.
- [x] D3. String/Binary arrays/builders implemented.
- [x] D4. List/LargeList arrays/builders implemented.
- [x] D5. Struct array/builder implemented (including child-builder coordination).
- [x] D6. Shared ownership model implemented (SharedBuffer/OwnedBuffer + ArrayRef retain/release).
- [x] D7. Array slicing path implemented and tested.
- [x] D8. Schema/DataType surface is exposed from root.
- [x] D9. RecordBatch implemented (init/deinit/column/numRows/numColumns/slice).
- [x] D10. RecordBatchBuilder implemented (set/setByName/finish/reset/clear).
- [x] D11. Benchmarks framework implemented (benchmark/smoke/full/matrix/ci).

## Phase A - Core Type Coverage Completion

### A1. LargeString / LargeBinary

- [x] A1.1 Add LargeStringArray view type.
- [x] A1.2 Add LargeBinaryArray view type.
- [x] A1.3 Add LargeStringBuilder (i64 offsets).
- [x] A1.4 Add LargeBinaryBuilder (i64 offsets).
- [x] A1.5 Add finishReset/finishClear parity for large builders.
- [x] A1.6 Export new types from src/array/array.zig.
- [x] A1.7 Export new types from src/root.zig.
- [x] A1.8 Add unit tests: append/appendNull/finish/slice.

### A2. FixedSizeBinary / FixedSizeList

- [x] A2.1 Add FixedSizeBinaryArray view type.
- [x] A2.2 Add FixedSizeBinaryBuilder with width checks.
- [x] A2.3 Add FixedSizeListArray view type.
- [x] A2.4 Add FixedSizeListBuilder with list_size invariants.
- [x] A2.5 Add unit tests for width/list_size mismatch errors.
- [x] A2.6 Add slice behavior tests for both types.
- [x] A2.7 Export from src/array/array.zig and src/root.zig.

### A3. Dictionary

- [x] A3.1 Add DictionaryArray view (indices + dictionary values).
- [x] A3.2 Add DictionaryBuilder for index types (int8/int16/int32/int64 at minimum).
- [x] A3.3 Enforce dictionary presence/type invariants at finish time.
- [x] A3.4 Add tests for invalid dictionary/missing dictionary.
- [x] A3.5 Add tests for slicing dictionary arrays.

### A4. Map / Union / RunEndEncoded

- [x] A4.1 Add MapArray view (offsets + struct child).
- [x] A4.2 Add SparseUnionArray and DenseUnionArray views.
- [x] A4.3 Add RunEndEncodedArray view.
- [x] A4.4 Add minimal builders for above or explicit constructor helpers.
- [x] A4.5 Add validation-focused tests for child counts and offsets.

### A5. Validation & Regression Suite Completion

- [x] A5.1 Add nullability invariant tests across all newly added builders.
- [x] A5.2 Add length/type mismatch tests for RecordBatch with new types.
- [x] A5.3 Add stress tests for retain/release balance in nested arrays.
- [x] A5.4 Add slice consistency tests (offset + length + null_count semantics).
- [x] A5.5 Ensure zig build test remains green for all targets in CI matrix.

## Phase B - IPC MVP (Schema + RecordBatch)

### B1. Metadata Serialization

- [x] B1.1 Implement Field metadata serialization.
- [x] B1.2 Implement Schema metadata serialization.
- [x] B1.3 Add deterministic ordering tests for metadata output.

### B2. IPC Writer (MVP Subset)

- [x] B2.1 Implement stream header/footer writer.
- [x] B2.2 Write RecordBatch for primitive/boolean/string/binary/list/struct.
- [x] B2.3 Add alignment/padding logic for body buffers.
- [x] B2.4 Add writer golden tests for small fixtures.

### B3. IPC Reader (MVP Subset)

- [x] B3.1 Implement stream message parser.
- [x] B3.2 Reconstruct Schema + RecordBatch from stream.
- [x] B3.3 Implement zero-copy path for compatible payloads.
- [x] B3.4 Add reader roundtrip tests vs writer output.

### B4. IPC Robustness

- [x] B4.1 Add malformed header/message length tests.
- [x] B4.2 Add invalid offset/length payload tests.
- [x] B4.3 Add deterministic error mapping for parse failures.

### B5. IPC Type Coverage Expansion

- [x] B5.1 Add Date/Time/Timestamp/Duration IPC read+write support.
- [x] B5.2 Add Decimal32/64/128/256 IPC read+write support.
- [x] B5.3 Add DictionaryBatch write and dictionary array read reconstruction.
- [x] B5.4 Add Map IPC read+write support.
- [x] B5.5 Add Union IPC read+write support.
- [x] B5.6 Add RunEndEncoded IPC read+write support.

## Phase C - Interop and Hardening

### C1. Arrow C Data Interface (FFI)

- [ ] C1.1 Export Schema to C Data Interface.
- [ ] C1.2 Export Array to C Data Interface.
- [ ] C1.3 Import Schema from C Data Interface.
- [ ] C1.4 Import Array from C Data Interface.
- [ ] C1.5 Add smoke roundtrip tests.

### C2. Fuzzing

- [ ] C2.1 Add fuzz target for ArrayData.validateLayout.
- [ ] C2.2 Add fuzz target for IPC reader message parsing.
- [ ] C2.3 Seed corpus with malformed and edge payloads.

### C3. Benchmark Reliability

- [ ] C3.1 Add git_sha field to CSV benchmark output.
- [ ] C3.2 Add timestamp field to CSV benchmark output.
- [ ] C3.3 Add benchmark-ci parser sanity check script/test.

### C4. Project Governance

- [ ] C4.1 Add CONTRIBUTING.md.
- [ ] C4.2 Add API stability policy (experimental vs stable sections).
- [ ] C4.3 Define release checklist and semver rules.

## Immediate Next Sprint (Recommended)

- [x] S1. Complete A1.1-A1.8 (LargeString/LargeBinary full path).
- [x] S2. Complete A2.1-A2.7 (FixedSizeBinary/FixedSizeList full path).
- [x] S3. Start B1.1-B1.3 (Schema/Field metadata serialization scaffold).

## Definition of Done (Per Task)

- [ ] T1. Code merged with unit tests.
- [ ] T2. Public exports updated in src/root.zig.
- [ ] T3. zig build test passes.
- [ ] T4. If perf-sensitive, benchmark-smoke run attached.
