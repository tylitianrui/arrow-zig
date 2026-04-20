const std = @import("std");
const datatype = @import("datatype.zig");
const array = @import("array/array.zig");

pub const DataType = datatype.DataType;
pub const ArrayRef = array.ArrayRef;

pub const Error = error{
    OutOfMemory,
    InvalidChunkType,
    LengthOverflow,
    SliceOutOfBounds,
};

const ChunkedArrayNode = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    data_type: DataType,
    chunks: []ArrayRef,
    total_len: usize,
};

fn releaseOwnedChunks(allocator: std.mem.Allocator, chunks: []ArrayRef) void {
    for (chunks) |*chunk| {
        chunk.release();
    }
    allocator.free(chunks);
}

pub const ChunkedArray = struct {
    node: *ChunkedArrayNode,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, data_type: DataType, chunk_refs: []const ArrayRef) Error!Self {
        const owned_chunks = try allocator.alloc(ArrayRef, chunk_refs.len);
        var owned_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < owned_count) : (i += 1) {
                owned_chunks[i].release();
            }
            allocator.free(owned_chunks);
        }

        var total_len: usize = 0;
        for (chunk_refs, 0..) |chunk_ref, i| {
            const chunk_dt = chunk_ref.data().data_type;
            if (!chunk_dt.eql(data_type)) return error.InvalidChunkType;
            total_len = std.math.add(usize, total_len, chunk_ref.data().length) catch return error.LengthOverflow;
            owned_chunks[i] = chunk_ref.retain();
            owned_count += 1;
        }

        return fromOwnedChunks(allocator, data_type, owned_chunks, total_len);
    }

    pub fn fromSingle(allocator: std.mem.Allocator, array_ref: ArrayRef) Error!Self {
        return init(allocator, array_ref.data().data_type, &[_]ArrayRef{array_ref});
    }

    fn fromOwnedChunks(allocator: std.mem.Allocator, data_type: DataType, owned_chunks: []ArrayRef, total_len: usize) Error!Self {
        const node = try allocator.create(ChunkedArrayNode);
        errdefer allocator.destroy(node);

        node.* = .{
            .allocator = allocator,
            .ref_count = std.atomic.Value(u32).init(1),
            .data_type = data_type,
            .chunks = owned_chunks,
            .total_len = total_len,
        };
        return .{ .node = node };
    }

    pub fn retain(self: Self) Self {
        _ = self.node.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn release(self: *Self) void {
        if (self.node.ref_count.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.node.allocator;
        releaseOwnedChunks(allocator, self.node.chunks);
        allocator.destroy(self.node);
    }

    pub fn dataType(self: Self) DataType {
        return self.node.data_type;
    }

    pub fn len(self: Self) usize {
        return self.node.total_len;
    }

    pub fn numChunks(self: Self) usize {
        return self.node.chunks.len;
    }

    pub fn chunk(self: Self, index: usize) *const ArrayRef {
        std.debug.assert(index < self.node.chunks.len);
        return &self.node.chunks[index];
    }

    pub fn chunks(self: Self) []const ArrayRef {
        return self.node.chunks;
    }

    pub fn slice(self: Self, allocator: std.mem.Allocator, offset: usize, length: usize) Error!Self {
        if (offset > self.node.total_len) return error.SliceOutOfBounds;
        const end = std.math.add(usize, offset, length) catch return error.SliceOutOfBounds;
        if (end > self.node.total_len) return error.SliceOutOfBounds;

        var out_chunks: std.ArrayList(ArrayRef) = .{};
        defer {
            for (out_chunks.items) |*chunk_ref| {
                chunk_ref.release();
            }
            out_chunks.deinit(allocator);
        }
        try out_chunks.ensureUnusedCapacity(allocator, self.node.chunks.len);

        var global_index: usize = 0;
        var remaining_skip = offset;
        var remaining_take = length;
        while (global_index < self.node.chunks.len and remaining_take > 0) : (global_index += 1) {
            const current = self.node.chunks[global_index];
            const current_len = current.data().length;
            if (remaining_skip >= current_len) {
                remaining_skip -= current_len;
                continue;
            }

            const local_offset = remaining_skip;
            const available = current_len - local_offset;
            const local_take = @min(available, remaining_take);
            const sliced = try current.slice(local_offset, local_take);
            out_chunks.append(allocator, sliced) catch return error.OutOfMemory;

            remaining_skip = 0;
            remaining_take -= local_take;
        }

        const owned = try out_chunks.toOwnedSlice(allocator);
        errdefer releaseOwnedChunks(allocator, owned);
        out_chunks = .{};

        return fromOwnedChunks(allocator, self.node.data_type, owned, length);
    }
};

test "chunked array init retains all chunks and validates type" {
    const allocator = std.testing.allocator;

    var b1 = try array.Int32Builder.init(allocator, 2);
    defer b1.deinit();
    try b1.append(1);
    try b1.append(2);
    var c1 = try b1.finish();
    defer c1.release();

    var b2 = try array.Int32Builder.init(allocator, 2);
    defer b2.deinit();
    try b2.append(3);
    try b2.append(4);
    var c2 = try b2.finish();
    defer c2.release();

    var ca = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c1, c2 });
    defer ca.release();

    try std.testing.expectEqual(@as(usize, 2), ca.numChunks());
    try std.testing.expectEqual(@as(usize, 4), ca.len());
    try std.testing.expect(ca.dataType().eql(.{ .int32 = {} }));
}

test "chunked array slice spans chunk boundary" {
    const allocator = std.testing.allocator;

    var b1 = try array.Int32Builder.init(allocator, 3);
    defer b1.deinit();
    try b1.append(10);
    try b1.append(11);
    try b1.append(12);
    var c1 = try b1.finish();
    defer c1.release();

    var b2 = try array.Int32Builder.init(allocator, 3);
    defer b2.deinit();
    try b2.append(20);
    try b2.append(21);
    try b2.append(22);
    var c2 = try b2.finish();
    defer c2.release();

    var ca = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c1, c2 });
    defer ca.release();

    var sliced = try ca.slice(allocator, 2, 3);
    defer sliced.release();

    try std.testing.expectEqual(@as(usize, 3), sliced.len());
    try std.testing.expectEqual(@as(usize, 2), sliced.numChunks());

    const first = array.Int32Array{ .data = sliced.chunk(0).data() };
    const second = array.Int32Array{ .data = sliced.chunk(1).data() };
    try std.testing.expectEqual(@as(usize, 1), first.len());
    try std.testing.expectEqual(@as(usize, 2), second.len());
    try std.testing.expectEqual(@as(i32, 12), first.value(0));
    try std.testing.expectEqual(@as(i32, 20), second.value(0));
    try std.testing.expectEqual(@as(i32, 21), second.value(1));
}
