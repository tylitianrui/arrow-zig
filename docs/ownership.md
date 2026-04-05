## Ownership / Lifetime

zarrow separates **layout**, **buffer ownership**, and **array lifetime**:

- `ArrayData` is a read-only Arrow layout description.
- `SharedBuffer` is a read-only buffer view. It may either:
  - borrow existing memory, or
  - retain ref-counted storage.
- `OwnedBuffer` is a uniquely owned, mutable buffer used during building.
- `ArrayRef` is the owning handle for arrays. It retains/releases shared buffers and child arrays.

### Mental model

There are two phases:

1. **Build phase**
   - Builders write into `OwnedBuffer`.
   - Buffers are mutable and uniquely owned.

2. **Published phase**
   - Builders convert `OwnedBuffer` into `SharedBuffer`.
   - The resulting `ArrayData` is wrapped in `ArrayRef`.
   - After this point, data is treated as read-only and shared.

In other words:

`OwnedBuffer -> SharedBuffer -> ArrayData -> ArrayRef`

### Who owns what

- `ArrayData` does **not** own memory by itself.
- `ArrayRef` owns the array layout and is responsible for releasing:
  - shared buffers in `ArrayData.buffers`
  - child `ArrayRef`s
  - dictionary `ArrayRef`, if present

Call `release()` on `ArrayRef` when you are done with it.

### Builder semantics

Builders return `ArrayRef` by default.

That means `finish()` transfers ownership out of the builder:
- the builder no longer owns the published buffers
- the returned `ArrayRef` becomes responsible for lifetime management

### Borrowed vs owned layouts

zarrow provides three construction paths:

#### `ArrayRef.fromOwned()`
Use this when the layout should become owned by the returned `ArrayRef`.

This is the normal constructor for allocator-owned layouts. It also normalizes empty slices so they can be safely released later.

#### `ArrayRef.fromBorrowed()`
Use this when the input layout borrows memory or uses stack/static slices.

This function retains shared buffers and child refs, copies the top-level container slices, and returns an owning `ArrayRef`.

#### `ArrayRef.fromOwnedUnsafe()`
Use this only when you have already guaranteed that:
- `buffers`
- `children`
- `dictionary`

all satisfy the ownership contract expected by `ArrayRef.release()`.

This is an advanced API. If you are unsure, use `fromOwned()` or `fromBorrowed()` instead.

### Slicing

`ArrayRef.slice()` is zero-copy:
- buffers are retained, not copied
- child refs are retained
- dictionary refs are retained

The returned slice is another `ArrayRef` and must also be released.

### Common mistakes

#### Forgetting to call `release()`
`ArrayRef` is an owning handle. If you finish a builder or create an `ArrayRef`, release it when done.

#### Passing borrowed layouts to `fromOwnedUnsafe()`
Do not pass stack-backed, static, or otherwise borrowed container slices to `fromOwnedUnsafe()`. That API assumes the layout can be safely released later.

#### Assuming `ArrayData` owns memory
`ArrayData` only describes Arrow layout. Lifetime is managed by `ArrayRef`.

### Example

```zig
var builder = try zarrow.StringBuilder.init(allocator);
defer builder.deinit();

try builder.append("hello");
try builder.append("world");

var array_ref = try builder.finish();
defer array_ref.release();

const array = zarrow.StringArray{ .data = array_ref.data() };
try std.testing.expectEqualStrings("hello", array.value(0));
try std.testing.expectEqualStrings("world", array.value(1));
```