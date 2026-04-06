const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
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

const BOOL_TYPE = DataType{ .bool = {} };

/// Bit-packed boolean array view.
pub const BooleanArray = struct {
    data: *const ArrayData,
    const Self = @This();

    pub fn len(self: Self) usize {
        return self.data.length;
    }

    pub fn isNull(self: Self, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn value(self: Self, i: usize) bool {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        return bitmap.bitIsSet(self.data.buffers[1].data, self.data.offset + i);
    }
};

fn initValidityAllValid(allocator: std.mem.Allocator, bit_len: usize) !OwnedBuffer {
    const used_bytes = bitmap.byteLength(bit_len);
    var buf = try OwnedBuffer.init(allocator, used_bytes);
    if (used_bytes > 0) {
        @memset(buf.data[0..used_bytes], 0xFF);
        const remainder = bit_len & 7;
        if (remainder != 0) {
            const keep_mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
            buf.data[used_bytes - 1] &= keep_mask;
        }
    }
    return buf;
}

fn ensureBitmapCapacity(buf: *OwnedBuffer, bit_len: usize) !void {
    const needed = bitmap.byteLength(bit_len);
    if (needed <= buf.len()) return;
    try buf.resize(needed);
}

/// Builder for bit-packed boolean arrays with optional validity.
pub const BooleanBuilder = struct {
    allocator: std.mem.Allocator,
    values: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    state: BuilderState = .ready,

    const Self = @This();

    const BuilderError = error{ AlreadyFinished, NotFinished };

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
        return .{
            .allocator = allocator,
            .values = try OwnedBuffer.init(allocator, bitmap.byteLength(capacity)),
        };
    }

    pub fn deinit(self: *Self) void {
        self.values.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    pub fn clear(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.values.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    fn ensureValuesCapacity(self: *Self, new_len: usize) !void {
        const needed = bitmap.byteLength(new_len);
        if (needed <= self.values.len()) return;
        try self.values.resize(needed);
    }

    fn ensureValidityForNull(self: *Self, new_len: usize) !void {
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

    fn setValidBit(self: *Self, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn append(self: *Self, value: bool) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureValuesCapacity(next_len);
        if (value) {
            bitmap.setBit(self.values.data[0..bitmap.byteLength(next_len)], self.len);
        } else {
            bitmap.clearBit(self.values.data[0..bitmap.byteLength(next_len)], self.len);
        }
        try self.setValidBit(self.len);
        self.len = next_len;
    }

    pub fn appendNull(self: *Self) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureValuesCapacity(next_len);
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *Self) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.values.toShared(bitmap.byteLength(self.len));

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];

        const data = ArrayData{
            .data_type = BOOL_TYPE,
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    pub fn finishReset(self: *Self) !ArrayRef {
        const finished_ref = try self.finish();
        try self.reset();
        return finished_ref;
    }
};

test "boolean builder appends values" {
    var builder = try BooleanBuilder.init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(true);
    try builder.append(false);
    try builder.appendNull();

    var array_handle = try builder.finish();
    defer array_handle.release();
    const built = BooleanArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.value(0));
    try std.testing.expect(!built.value(1));
    try std.testing.expect(built.isNull(2));
}
