const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

// Binary and LargeBinary array views/builders backed by offset buffers.

const SharedBuffer = buffer.SharedBuffer;
const OwnedBuffer = buffer.OwnedBuffer;
const ArrayData = array_data.ArrayData;
const DataType = datatype.DataType;
const ArrayRef = array_ref.ArrayRef;
const BuilderState = builder_state.BuilderState;

const BINARY_TYPE = DataType{ .binary = {} };
const LARGE_BINARY_TYPE = DataType{ .large_binary = {} };

pub const BinaryArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: BinaryArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: BinaryArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: BinaryArray, i: usize) []const u8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const start = offsets[self.data.offset + i];
        const end = offsets[self.data.offset + i + 1];
        return self.data.buffers[2].data[@intCast(start)..@intCast(end)];
    }
};

pub const LargeBinaryArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: LargeBinaryArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: LargeBinaryArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: LargeBinaryArray, i: usize) []const u8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);

        const offsets = self.data.buffers[1].typedSlice(i64);
        const start = offsets[self.data.offset + i];
        const end = offsets[self.data.offset + i + 1];
        return self.data.buffers[2].data[@intCast(start)..@intCast(end)];
    }
};

/// Builder for variable-length binary arrays.
pub const BinaryBuilder = struct {
    allocator: std.mem.Allocator,
    offsets: OwnedBuffer,
    data: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [3]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    data_len: usize = 0,
    state: BuilderState = .ready,

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, data_capacity: usize) !BinaryBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i32));
        const offsets_slice = std.mem.bytesAsSlice(i32, offsets.data);
        offsets_slice[0] = 0;
        return .{
            .allocator = allocator,
            .offsets = offsets,
            .data = try OwnedBuffer.init(allocator, data_capacity),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *BinaryBuilder) void {
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *BinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (!self.offsets.isEmpty()) {
            const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
            offsets_slice[0] = 0;
        }
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *BinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    fn ensureOffsetsCapacity(self: *BinaryBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i32);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i32));
        if (was_empty) {
            const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
            offsets_slice[0] = 0;
        }
    }

    fn ensureDataCapacity(self: *BinaryBuilder, needed_len: usize) !void {
        if (needed_len <= self.data.len()) return;
        try self.data.resize(needed_len);
    }

    const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow };

    /// Append one logical value into the builder.
    pub fn append(self: *BinaryBuilder, value: []const u8) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        try self.ensureDataCapacity(self.data_len + value.len);
        @memcpy(self.data.data[self.data_len .. self.data_len + value.len], value);
        self.data_len += value.len;

        const cast_offset = std.math.cast(i32, self.data_len) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = cast_offset;
        try array_utils.setValidBit(&self.validity, self.len);
        self.len = next_len;
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *BinaryBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const cast_offset = std.math.cast(i32, self.data_len) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = cast_offset;
        try array_utils.ensureValidityForNull(self.allocator, &self.validity, &self.null_count, next_len);
        self.len = next_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *BinaryBuilder) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(i32));
        self.buffers[2] = try self.data.toShared(self.data_len);

        const buffers = try self.allocator.alloc(SharedBuffer, 3);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];
        buffers[2] = self.buffers[2];

        const data = ArrayData{
            .data_type = BINARY_TYPE,
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *BinaryBuilder) !ArrayRef {
        const finished_ref = try self.finish();
        try self.reset();
        return finished_ref;
    }

    /// Finalize output and then clear builder state and buffers.
    pub fn finishClear(self: *BinaryBuilder) !ArrayRef {
        const finished_ref = try self.finish();
        try self.clear();
        return finished_ref;
    }
};

pub const LargeBinaryBuilder = struct {
    allocator: std.mem.Allocator,
    offsets: OwnedBuffer,
    data: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [3]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    data_len: usize = 0,
    state: BuilderState = .ready,

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, data_capacity: usize) !LargeBinaryBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i64));
        const offsets_slice = std.mem.bytesAsSlice(i64, offsets.data);
        offsets_slice[0] = 0;
        return .{
            .allocator = allocator,
            .offsets = offsets,
            .data = try OwnedBuffer.init(allocator, data_capacity),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *LargeBinaryBuilder) void {
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *LargeBinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (!self.offsets.isEmpty()) {
            const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
            offsets_slice[0] = 0;
        }
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *LargeBinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    fn ensureOffsetsCapacity(self: *LargeBinaryBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i64);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i64));
        if (was_empty) {
            const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
            offsets_slice[0] = 0;
        }
    }

    fn ensureDataCapacity(self: *LargeBinaryBuilder, needed_len: usize) !void {
        if (needed_len <= self.data.len()) return;
        try self.data.resize(needed_len);
    }

    const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow };

    /// Append one logical value into the builder.
    pub fn append(self: *LargeBinaryBuilder, value: []const u8) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        try self.ensureDataCapacity(self.data_len + value.len);
        @memcpy(self.data.data[self.data_len .. self.data_len + value.len], value);
        self.data_len += value.len;

        const cast_offset = std.math.cast(i64, self.data_len) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
        offsets_slice[next_len] = cast_offset;
        try array_utils.setValidBit(&self.validity, self.len);
        self.len = next_len;
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *LargeBinaryBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const cast_offset = std.math.cast(i64, self.data_len) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
        offsets_slice[next_len] = cast_offset;
        try array_utils.ensureValidityForNull(self.allocator, &self.validity, &self.null_count, next_len);
        self.len = next_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *LargeBinaryBuilder) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(i64));
        self.buffers[2] = try self.data.toShared(self.data_len);

        const buffers = try self.allocator.alloc(SharedBuffer, 3);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];
        buffers[2] = self.buffers[2];

        const data = ArrayData{
            .data_type = LARGE_BINARY_TYPE,
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *LargeBinaryBuilder) !ArrayRef {
        const finished_ref = try self.finish();
        try self.reset();
        return finished_ref;
    }

    /// Finalize output and then clear builder state and buffers.
    pub fn finishClear(self: *LargeBinaryBuilder) !ArrayRef {
        const finished_ref = try self.finish();
        try self.clear();
        return finished_ref;
    }
};

test "binary array reads slices" {
    const dtype = DataType{ .binary = {} };
    const offsets = [_]i32{ 0, 2, 5 };
    const data_bytes = "ziggy";
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(offset_bytes[0..]),
            SharedBuffer.fromSlice(data_bytes),
        },
    };

    const array = BinaryArray{ .data = &data };
    try std.testing.expectEqualStrings("zi", array.value(0));
    try std.testing.expectEqualStrings("ggy", array.value(1));
}

test "binary builder appends slices" {
    var builder = try BinaryBuilder.init(std.testing.allocator, 2, 8);
    defer builder.deinit();

    try builder.append("zi");
    try builder.appendNull();
    try builder.append("ggy");

    var array_handle = try builder.finish();
    defer array_handle.release();
    const array = BinaryArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), array.len());
    try std.testing.expectEqualStrings("zi", array.value(0));
    try std.testing.expect(array.isNull(1));
    try std.testing.expectEqualStrings("ggy", array.value(2));
}

test "binary builder returns offset overflow for 32-bit offsets" {
    var builder = try BinaryBuilder.init(std.testing.allocator, 1, 0);
    defer builder.deinit();

    builder.data_len = @as(usize, @intCast(std.math.maxInt(i32))) + 1;
    try std.testing.expectError(BinaryBuilder.BuilderError.OffsetOverflow, builder.appendNull());
}

test "large binary array reads slices" {
    const dtype = DataType{ .large_binary = {} };
    const offsets = [_]i64{ 0, 2, 5 };
    const data_bytes = "ziggy";
    var offset_bytes: [offsets.len * @sizeOf(i64)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(offset_bytes[0..]),
            SharedBuffer.fromSlice(data_bytes),
        },
    };

    const array = LargeBinaryArray{ .data = &data };
    try std.testing.expectEqualStrings("zi", array.value(0));
    try std.testing.expectEqualStrings("ggy", array.value(1));
}

test "large binary builder appends slices and supports slice" {
    var builder = try LargeBinaryBuilder.init(std.testing.allocator, 2, 8);
    defer builder.deinit();

    try builder.append("zi");
    try builder.appendNull();
    try builder.append("ggy");

    var array_handle = try builder.finishReset();
    defer array_handle.release();
    const array = LargeBinaryArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), array.len());
    try std.testing.expectEqualStrings("zi", array.value(0));
    try std.testing.expect(array.isNull(1));
    try std.testing.expectEqualStrings("ggy", array.value(2));

    var sliced = try array_handle.slice(1, 2);
    defer sliced.release();
    const sliced_view = LargeBinaryArray{ .data = sliced.data() };
    try std.testing.expect(sliced_view.isNull(0));
    try std.testing.expectEqualStrings("ggy", sliced_view.value(1));

    try builder.append("xy");
    var second = try builder.finishClear();
    defer second.release();
    const second_view = LargeBinaryArray{ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 1), second_view.len());
    try std.testing.expectEqualStrings("xy", second_view.value(0));
}
