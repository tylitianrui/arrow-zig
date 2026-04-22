const std = @import("std");

/// Default byte alignment for Arrow-compatible buffer allocations (64-byte SIMD-friendly).
pub const ALIGNMENT: usize = 64;

const empty_storage: [0]u8 align(ALIGNMENT) = .{};

/// Round `size` up to the next `ALIGNMENT`-byte boundary.
/// Use this to determine the minimum capacity for a buffer storing `size` bytes.
///
/// Demo (`ALIGNMENT = 64`):
/// - `alignedSize(0) == (0 + 64 - 1) & ~(64 - 1) == 63 & ~63 == 0`
/// - `alignedSize(1) == (1 + 64 - 1) & ~(64 - 1) == 64 & ~63 == 64`
/// - `alignedSize(64) == (64 + 64 - 1) & ~(64 - 1) == 127 & ~63 == 64`
/// - `alignedSize(65) == (65 + 64 - 1) & ~(64 - 1) == 128 & ~63 == 128`
pub inline fn alignedSize(size: usize) usize {
    return (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
}

// Internal ref-counted storage backing shared buffers.
const BufferStorage = struct {
    // Allocator used to create and free this storage.
    allocator: std.mem.Allocator,
    // Aligned buffer bytes owned by this storage.
    data: []align(ALIGNMENT) u8,
    // Shared reference count for all SharedBuffer views.
    ref_count: std.atomic.Value(u32),
    // Custom release hook for external/FFI-managed memory.
    release_fn: *const fn (*BufferStorage) void,

    // Default release: free the data and destroy the control block.
    fn releaseDefault(storage: *BufferStorage) void {
        storage.allocator.free(storage.data);
        storage.allocator.destroy(storage);
    }
};

/// Reference-counted, read-only view over a contiguous byte region.
///
/// Ownership model:
/// - When `storage` is non-null the view is *owning*: `retain`/`release` manage
///   the shared reference count and free the backing allocation on last release.
/// - When `storage` is null the view is *borrowed*: callers must ensure the
///   pointed-to memory outlives all `SharedBuffer` handles derived from it.
///
/// Slicing (`slice`) produces a new owning handle sharing the same storage.
pub const SharedBuffer = struct {
    /// Shared control block. `null` for borrowed (non-owning) views.
    storage: ?*BufferStorage,
    /// Logical byte window for this handle; may be a sub-slice of `storage.data`.
    data: []const u8,

    const Self = @This();

    /// Recoverable errors returned by `slice` and `typedSlice`.
    pub const Error = error{
        /// Invalid slice range: `start > end` or `end > self.data.len`.
        SliceOutOfBounds,
        /// `typedSlice(T)` requires pointer alignment compatible with `@alignOf(T)`.
        MisalignedPointer,
        /// `typedSlice(T)` requires byte length to be a multiple of `@sizeOf(T)`.
        LengthNotMultipleOfTypeSize,
    };

    /// Zero-length borrowed buffer that can be used as a sentinel or placeholder.
    pub const empty: SharedBuffer = .{ .storage = null, .data = &.{} };

    /// Build a borrowed non-owning shared buffer view from an existing slice.
    pub fn fromSlice(data: []const u8) SharedBuffer {
        return .{ .storage = null, .data = data };
    }

    /// Increment shared ownership and return another handle.
    pub fn retain(self: Self) Self {
        if (self.storage) |storage| {
            _ = storage.ref_count.fetchAdd(1, .monotonic);
        }
        return self;
    }

    /// Release one ownership reference and cleanup if this is the last handle.
    pub fn release(self: *Self) void {
        if (self.storage) |storage| {
            if (storage.ref_count.fetchSub(1, .acq_rel) == 1) {
                storage.release_fn(storage);
            }
        }
        self.storage = null;
        self.data = &.{};
    }

    /// Return the logical length.
    pub fn len(self: Self) usize {
        return self.data.len;
    }

    /// Return `true` when the logical length is zero.
    pub inline fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

    /// Create a logical slice view over the current value.
    pub fn slice(self: Self, start: usize, end: usize) Error!Self {
        if (start > end or end > self.data.len) {
            return Error.SliceOutOfBounds;
        }
        const out = Self{ .storage = self.storage, .data = self.data[start..end] };

        if (out.storage) |storage| {
            _ = storage.ref_count.fetchAdd(1, .monotonic);
        }
        return out;
    }

    /// Reinterpret raw bytes as a typed immutable slice.
    /// Returns an error when pointer alignment or byte-length constraints are not met.
    pub fn typedSlice(self: Self, comptime T: type) Error![]const T {
        comptime {
            if (@sizeOf(T) == 0) @compileError("typedSlice does not support zero-sized types");
        }
        if (!std.mem.isAligned(@intFromPtr(self.data.ptr), @alignOf(T))) {
            return Error.MisalignedPointer;
        }
        if (self.data.len % @sizeOf(T) != 0) {
            return Error.LengthNotMultipleOfTypeSize;
        }
        const aligned: []align(@alignOf(T)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(T, aligned);
    }
};

/// Mutable, uniquely owned buffer used by array builders.
///
/// Capacity is always a multiple of `ALIGNMENT` bytes and the backing memory
/// is zero-initialised on allocation and resize.  Call `toShared` to transfer
/// ownership to an immutable `SharedBuffer` — the `OwnedBuffer` is emptied in
/// the process and must not be written to afterwards.
pub const OwnedBuffer = struct {
    /// Aligned mutable backing storage.
    data: []align(ALIGNMENT) u8,
    /// Allocator used for all (re)allocations.
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Allocate a zeroed buffer with at least `capacity` usable bytes.
    ///
    /// The actual allocation is rounded up to the nearest `ALIGNMENT` boundary.
    /// A capacity of 0 still allocates one alignment-unit so that the pointer
    /// is always valid and correctly aligned.
    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
        // Ensure a minimum of ALIGNMENT bytes so the pointer is never null.
        const actualCapacity = alignedSize(if (capacity == 0) ALIGNMENT else capacity);

        const data = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(ALIGNMENT), actualCapacity);
        // Zero-initialise the buffer so that uninitialised bytes are always valid to read.
        @memset(data, 0);
        return .{ .data = data, .allocator = allocator };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *Self) void {
        if (self.data.len == 0) return;
        self.allocator.free(self.data);
        self.data = empty_storage[0..];
    }

    /// Return the logical length.
    pub fn len(self: Self) usize {
        return self.data.len;
    }

    /// Return `true` when the logical length is zero.
    pub inline fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

    /// Transfer ownership to a new `SharedBuffer` and invalidate this buffer.
    ///
    /// `used` is the number of bytes to expose through the returned view;
    /// it must be ≤ `self.data.len`.  After the call `self.data` is reset to
    /// the empty sentinel — any previously held pointer into `self.data` must
    /// not be dereferenced.
    pub fn toShared(self: *Self, used: usize) !SharedBuffer {
        const storage = try self.allocator.create(BufferStorage);
        storage.* = .{
            .allocator = self.allocator,
            .data = self.data,
            .ref_count = std.atomic.Value(u32).init(1),
            .release_fn = BufferStorage.releaseDefault,
        };
        const shared = SharedBuffer{ .storage = storage, .data = self.data[0..used] };
        self.data = empty_storage[0..];
        return shared;
    }

    /// Reinterpret the backing bytes as a mutable slice of `T`.
    ///
    /// The buffer's alignment (`ALIGNMENT` ≥ 64) satisfies any primitive type,
    /// so this never fails at runtime.  The returned slice covers the full
    /// allocated capacity, not just the logically written portion.
    pub fn typedSlice(self: Self, comptime T: type) []T {
        return std.mem.bytesAsSlice(T, self.data);
    }

    /// Grow or shrink the buffer to hold at least `newSize` bytes.
    ///
    /// - Existing bytes in `[0, min(oldLen, newSize))` are preserved.
    /// - Any newly allocated bytes beyond the old length are zeroed.
    /// - `newSize` is rounded up to the next `ALIGNMENT` boundary before
    ///   allocating, so `self.data.len` may exceed `newSize` after the call.
    pub fn resize(self: *Self, newSize: usize) !void {
        const actualSize = alignedSize(newSize);
        const newData = try self.allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(ALIGNMENT), actualSize);
        const copySize = @min(self.data.len, actualSize);
        @memcpy(newData[0..copySize], self.data[0..copySize]);
        if (actualSize > copySize) @memset(newData[copySize..], 0);

        if (self.data.len > 0) {
            self.allocator.free(self.data);
        }
        self.data = newData;
    }
};

test "buffer exposes immutable view" {
    const data = "arrow";
    const buffer = SharedBuffer.fromSlice(data);

    try std.testing.expectEqual(@as(usize, 5), buffer.len());
    try std.testing.expect(!buffer.isEmpty());
    try std.testing.expectEqualStrings(data, buffer.data[0..buffer.len()]);
}

test "buffer empty constant is reusable" {
    const buffer = SharedBuffer.empty;

    try std.testing.expectEqual(@as(usize, 0), buffer.len());
    try std.testing.expect(buffer.isEmpty());
    try std.testing.expectEqualStrings("", buffer.data[0..buffer.len()]);
}

test "mutable buffer exposes initialized data" {
    var buffer = try OwnedBuffer.init(std.testing.allocator, 5);
    defer buffer.deinit();

    try std.testing.expectEqual(alignedSize(@as(usize, 5)), buffer.len());
    try std.testing.expect(!buffer.isEmpty());

    @memcpy(buffer.data[0..5], "arrow");
    var shared = try buffer.toShared(5);
    defer shared.release();
    try std.testing.expectEqualStrings("arrow", shared.data[0..5]);
}

test "shared buffer view can be smaller than owned storage" {
    var owned = try OwnedBuffer.init(std.testing.allocator, 32);
    defer owned.deinit();

    @memset(owned.data, 0xAB);
    var shared = try owned.toShared(8);
    defer shared.release();

    try std.testing.expect(shared.storage != null);
    try std.testing.expectEqual(@as(usize, 8), shared.data.len);
    try std.testing.expect(shared.storage.?.data.len > shared.data.len);
}

test "shared buffer slice keeps owner but narrows logical data window" {
    var owned = try OwnedBuffer.init(std.testing.allocator, 8);
    defer owned.deinit();
    @memcpy(owned.data[0..8], "abcdefgh");

    var whole = try owned.toShared(8);
    defer whole.release();

    var mid = try whole.slice(2, 6);
    defer mid.release();

    try std.testing.expect(whole.storage == mid.storage);
    try std.testing.expectEqualStrings("cdef", mid.data);

    whole.release();
    try std.testing.expectEqualStrings("cdef", mid.data);
}

test "borrowed shared buffer slice stays non-owning" {
    const raw = "abcdef";
    var borrowed = SharedBuffer.fromSlice(raw);
    defer borrowed.release();

    var part = try borrowed.slice(1, 4);
    defer part.release();

    try std.testing.expect(part.storage == null);
    try std.testing.expectEqualStrings("bcd", part.data);
}

test "shared buffer slice validates bounds in all build modes" {
    const raw = "abcdef";
    const borrowed = SharedBuffer.fromSlice(raw);

    try std.testing.expectError(SharedBuffer.Error.SliceOutOfBounds, borrowed.slice(5, 2));
    try std.testing.expectError(SharedBuffer.Error.SliceOutOfBounds, borrowed.slice(0, 7));
}

test "shared buffer typedSlice returns error on misaligned pointer" {
    var owned = try OwnedBuffer.init(std.testing.allocator, 8);
    defer owned.deinit();
    @memcpy(owned.data[0..8], "abcdefgh");

    var whole = try owned.toShared(8);
    defer whole.release();
    var misaligned = try whole.slice(1, 5);
    defer misaligned.release();

    try std.testing.expectError(SharedBuffer.Error.MisalignedPointer, misaligned.typedSlice(u16));
}

test "shared buffer typedSlice returns error on invalid byte length" {
    var owned = try OwnedBuffer.init(std.testing.allocator, 8);
    defer owned.deinit();
    @memcpy(owned.data[0..8], "abcdefgh");

    var shared = try owned.toShared(3);
    defer shared.release();

    try std.testing.expectError(SharedBuffer.Error.LengthNotMultipleOfTypeSize, shared.typedSlice(u16));
}

test "alignedSize demo values" {
    try std.testing.expectEqual(@as(usize, 0), alignedSize(0));
    try std.testing.expectEqual(@as(usize, 64), alignedSize(1));
    try std.testing.expectEqual(@as(usize, 64), alignedSize(64));
    try std.testing.expectEqual(@as(usize, 128), alignedSize(65));
}
