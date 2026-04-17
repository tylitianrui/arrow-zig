const std = @import("std");

// Default byte alignment used for Arrow-compatible buffer allocations.
pub const ALIGNMENT: usize = 64; // 64-byte alignment for buffers

const empty_storage: [0]u8 align(ALIGNMENT) = .{};

// Round a requested size up to the next buffer alignment boundary.
pub fn alignedSize(size: usize) usize {
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

// SharedBuffer is a read-only view with shared ownership of its storage.
pub const SharedBuffer = struct {
    // Shared owner/control block. When null, this is a borrowed non-owning view.
    storage: ?*BufferStorage,
    // Logical byte view for this handle. May be a sub-slice of storage.data.
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

    pub fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

    /// Create a logical slice view over the current value.
    pub fn slice(self: Self, start: usize, end: usize) Error!Self {
        if (start > end or end > self.data.len) {
            return error.SliceOutOfBounds;
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
            return error.MisalignedPointer;
        }
        if (self.data.len % @sizeOf(T) != 0) {
            return error.LengthNotMultipleOfTypeSize;
        }
        const aligned: []align(@alignOf(T)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(T, aligned);
    }
};

// OwnedBuffer is a mutable, uniquely owned buffer for builders.
pub const OwnedBuffer = struct {
    data: []align(ALIGNMENT) u8,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
        const actualCapacity = alignedSize(if (capacity == 0) ALIGNMENT else capacity);

        const data = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(ALIGNMENT), actualCapacity);
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

    pub fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

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

    pub fn typedSlice(self: Self, comptime T: type) []T {
        return std.mem.bytesAsSlice(T, self.data);
    }

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
