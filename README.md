# zarrow
Zig implementation of Apache Arrow.

## Goals
Build a Zig ecosystem for Apache Arrow with core data structures, IPC support, compute kernels, and integrations with other formats. Focus on correctness, performance, and usability for Zig developers working with columnar data.

## Ownership and ArrayRef
- `ArrayData` is a read-only layout description.
- `ArrayRef` owns the layout and releases shared buffers on `release()`.
- Builders return `ArrayRef` and transfer ownership by default.
- Use `ArrayRef.fromBorrowed()` for borrowed layouts.
- Use `ArrayRef.fromOwnedUnsafe()` only when the layout is allocator-owned.

## Buffer model
- `OwnedBuffer` is the mutable, uniquely owned buffer used during building.
- `SharedBuffer` is the published, read-only buffer with ref-counted storage.
- `SharedBuffer.slice()` is zero-copy and retains the underlying storage.

## Slicing semantics
- `ArrayRef.slice()` is currently shallow: it adjusts top-level `offset/length` and
	retains buffers, children, and dictionary without deeper semantic slicing.
- This is correct for primitive, boolean, string, and binary arrays, but nested
	types (list/struct/dictionary/run-end-encoded) will need type-aware slicing.

