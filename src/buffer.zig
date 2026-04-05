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
    storage: ?*BufferStorage,
    data: []const u8,

    pub const empty: SharedBuffer = .{ .storage = null, .data = &.{} };

    pub fn init(data: []const u8) SharedBuffer {
        return .{ .storage = null, .data = data };
    }

    pub fn fromSlice(data: []const u8) SharedBuffer {
        return .{ .storage = null, .data = data };
    }

    pub fn retain(self: SharedBuffer) SharedBuffer {
        if (self.storage) |storage| {
            _ = storage.ref_count.fetchAdd(1, .monotonic);
        }
        return self;
    }

    pub fn release(self: *SharedBuffer) void {
        if (self.storage) |storage| {
            if (storage.ref_count.fetchSub(1, .acq_rel) == 1) {
                storage.release_fn(storage);
            }
        }
        self.storage = null;
        self.data = &.{};
    }

    pub fn len(self: SharedBuffer) usize {
        return self.data.len;
    }

    pub fn isEmpty(self: SharedBuffer) bool {
        return self.data.len == 0;
    }

    pub fn slice(self: SharedBuffer, start: usize, end: usize) SharedBuffer {
        std.debug.assert(start <= end);
        std.debug.assert(end <= self.data.len);
        const out = SharedBuffer{ .storage = self.storage, .data = self.data[start..end] };
        if (out.storage) |storage| {
            _ = storage.ref_count.fetchAdd(1, .monotonic);
        }
        return out;
    }

    pub fn typedSlice(self: SharedBuffer, comptime T: type) []const T {
        const aligned: []align(ALIGNMENT) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(T, aligned);
    }
};

// OwnedBuffer is a mutable, uniquely owned buffer for builders.
pub const OwnedBuffer = struct {
    data: []align(ALIGNMENT) u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !OwnedBuffer {
        const actualCapacity = alignedSize(if (capacity == 0) ALIGNMENT else capacity);
        const data = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(ALIGNMENT), actualCapacity);
        @memset(data, 0);
        return .{ .data = data, .allocator = allocator };
    }

    pub fn deinit(self: *OwnedBuffer) void {
        if (self.data.len == 0) return;
        self.allocator.free(self.data);
        self.data = empty_storage[0..];
    }

    pub fn len(self: OwnedBuffer) usize {
        return self.data.len;
    }

    pub fn isEmpty(self: OwnedBuffer) bool {
        return self.data.len == 0;
    }

    pub fn toShared(self: *OwnedBuffer, used: usize) !SharedBuffer {
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

    pub fn typedSlice(self: OwnedBuffer, comptime T: type) []T {
        return std.mem.bytesAsSlice(T, self.data);
    }

    pub fn resize(self: *OwnedBuffer, newSize: usize) !void {
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
