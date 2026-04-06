const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;

const BINARY_TYPE = DataType{ .binary = {} };

pub const BinaryArray = struct {
    data: *const ArrayData,

    pub fn len(self: BinaryArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: BinaryArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn value(self: BinaryArray, i: usize) []const u8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const start = offsets[self.data.offset + i];
        const end = offsets[self.data.offset + i + 1];
        return self.data.buffers[2].data[@intCast(start)..@intCast(end)];
    }
};

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

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

    pub fn deinit(self: *BinaryBuilder) void {
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

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

    fn ensureValidityForNull(self: *BinaryBuilder, new_len: usize) !void {
        if (self.validity == null) {
            var buf = try initValidityAllValid(self.allocator, new_len);
            bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
            self.validity = buf;
            self.null_count += 1;
            return;
        }
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, new_len);
        bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
        self.null_count += 1;
    }

    fn setValidBit(self: *BinaryBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    const BuilderError = error{ AlreadyFinished, NotFinished };

    pub fn append(self: *BinaryBuilder, value: []const u8) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        try self.ensureDataCapacity(self.data_len + value.len);
        @memcpy(self.data.data[self.data_len .. self.data_len + value.len], value);
        self.data_len += value.len;

        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = @intCast(self.data_len);
        try self.setValidBit(self.len);
        self.len = next_len;
    }

    pub fn appendNull(self: *BinaryBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = @intCast(self.data_len);
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

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

    pub fn finishReset(self: *BinaryBuilder) !ArrayRef {
        const finished_ref = try self.finish();
        try self.reset();
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
