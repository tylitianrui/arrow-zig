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

    pub const empty: SharedBuffer = .{ .storage = null, .data = &.{} };

    /// Initialize and return a new instance.
    pub fn init(data: []const u8) SharedBuffer {
        return .{ .storage = null, .data = data };
    }

    /// Execute fromSlice logic for this type.
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

    /// Execute isEmpty logic for this type.
    pub fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

    /// Create a logical slice view over the current value.
    pub fn slice(self: Self, start: usize, end: usize) Self {
        std.debug.assert(start <= end);
        std.debug.assert(end <= self.data.len);
        const out = Self{ .storage = self.storage, .data = self.data[start..end] };
        if (out.storage) |storage| {
            _ = storage.ref_count.fetchAdd(1, .monotonic);
        }
        return out;
    }

    /// Execute typedSlice logic for this type.
    pub fn typedSlice(self: Self, comptime T: type) []const T {
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

    /// Execute isEmpty logic for this type.
    pub fn isEmpty(self: Self) bool {
        return self.data.len == 0;
    }

    /// Execute toShared logic for this type.
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

    /// Execute typedSlice logic for this type.
    pub fn typedSlice(self: Self, comptime T: type) []T {
        return std.mem.bytesAsSlice(T, self.data);
    }

    /// Execute resize logic for this type.
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
    const buffer = SharedBuffer.init(data);

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

    var mid = whole.slice(2, 6);
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

    var part = borrowed.slice(1, 4);
    defer part.release();

    try std.testing.expect(part.storage == null);
    try std.testing.expectEqualStrings("bcd", part.data);
}
