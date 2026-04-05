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

## Ownership / Lifetime

zarrow separates **layout**, **buffer ownership**, and **array lifetime**.

- `ArrayData` is a read-only description of Arrow layout.
- `SharedBuffer` is a read-only buffer view. It may either:
  - borrow existing memory, or
  - retain ref-counted shared storage.
- `OwnedBuffer` is a uniquely owned, mutable buffer used during array construction.
- `ArrayRef` is the owning handle for arrays. It is responsible for retaining and releasing shared buffers and referenced child arrays.

### Mental model

There are two phases:

1. **Build phase**
   - Builders write into `OwnedBuffer`
   - buffers are mutable and uniquely owned

2. **Published phase**
   - builders convert `OwnedBuffer` into `SharedBuffer`
   - the resulting layout is wrapped in `ArrayRef`
   - published array data is treated as read-only and shareable

In short:

`OwnedBuffer -> SharedBuffer -> ArrayData -> ArrayRef`

### Who owns what

`ArrayData` does **not** own memory by itself.

`ArrayRef` is the object that manages lifetime. When released, it is responsible for releasing:

- shared buffers in `ArrayData.buffers`
- child `ArrayRef`s
- dictionary `ArrayRef`, if present

Call `release()` on `ArrayRef` when you are done with it.

### Builder semantics

Builders return `ArrayRef` by default.

`finish()` transfers ownership out of the builder:
- the builder no longer owns the published buffers
- the returned `ArrayRef` becomes responsible for lifetime management

### Construction APIs

zarrow provides three array construction paths:

#### `ArrayRef.fromOwned()`

Use this when the layout should become owned by the returned `ArrayRef`.

This is the normal constructor for allocator-owned layouts. It normalizes empty container slices so they can be safely released later.

#### `ArrayRef.fromBorrowed()`

Use this when the input layout borrows memory or uses stack/static slices.

This function retains shared buffers and child references, copies the top-level container slices, and returns an owning `ArrayRef`.

#### `ArrayRef.fromOwnedUnsafe()`

Use this only when you have already guaranteed that the layout satisfies the ownership contract expected by `ArrayRef.release()`.

This is an advanced API. If unsure, prefer `fromOwned()` or `fromBorrowed()`.

### Slicing

`ArrayRef.slice()` is zero-copy.

- buffers are retained, not copied
- child refs are retained
- dictionary refs are retained

The returned value is another `ArrayRef` and must also be released.

## Current Limitations

The current implementation is still incomplete and some semantics are intentionally conservative.

Known limitations currently include:

- `ArrayRef.slice()` is currently a shallow slice for nested/reference-based structures
- high-level array/builders are currently focused on primitive, boolean, string, and binary paths
- some Arrow physical layouts are validated before full high-level array APIs exist for them
- unsafe construction paths rely on caller correctness

These limitations are expected during development and will be reduced as more array types and stronger semantic validation are added.

## Example

```zig
var builder = try zarrow.StringBuilder.init(allocator);
defer builder.deinit();

try builder.append("hello");
try builder.append("world");

var array_ref = try builder.finish();
defer array_ref.release();

const array = zarrow.StringArray{ .data = array_ref.data() };
try std.testing.expectEqualStrings("hello", array.value(0));