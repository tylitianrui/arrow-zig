const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const Buffer = buffer.Buffer;
pub const MutableBuffer = buffer.MutableBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;

const BOOL_TYPE = DataType{ .bool = {} };

/// Bit-packed boolean array view.
pub const BooleanArray = struct {
    data: ArrayData,
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

fn initValidityAllValid(allocator: std.mem.Allocator, bit_len: usize) !MutableBuffer {
    const used_bytes = bitmap.byteLength(bit_len);
    var buf = try MutableBuffer.init(allocator, used_bytes);
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

fn ensureBitmapCapacity(buf: *MutableBuffer, bit_len: usize) !void {
    const needed = bitmap.byteLength(bit_len);
    if (needed <= buf.len()) return;
    try buf.resize(needed);
}

/// Builder for bit-packed boolean arrays with optional validity.
pub const BooleanBuilder = struct {
    allocator: std.mem.Allocator,
    values: MutableBuffer,
    validity: ?MutableBuffer = null,
    buffers: [2]Buffer = undefined,
    len: usize = 0,
    null_count: isize = 0,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
        return .{
            .allocator = allocator,
            .values = try MutableBuffer.init(allocator, bitmap.byteLength(capacity)),
        };
    }

    pub fn deinit(self: *Self) void {
        self.values.deinit();
        if (self.validity) |*valid| valid.deinit();
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
        const next_len = self.len + 1;
        try self.ensureValuesCapacity(next_len);
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *Self) BooleanArray {
        const validity_buf = if (self.validity) |buf| buf.toBuffer(bitmap.byteLength(self.len)) else Buffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = self.values.toBuffer(bitmap.byteLength(self.len));
        return BooleanArray{
            .data = ArrayData{
                .data_type = &BOOL_TYPE,
                .length = self.len,
                .null_count = self.null_count,
                .buffers = self.buffers[0..],
            },
        };
    }
};

test "boolean builder appends values" {
    var builder = try BooleanBuilder.init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(true);
    try builder.append(false);
    try builder.appendNull();

    const built = builder.finish();
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.value(0));
    try std.testing.expect(!built.value(1));
    try std.testing.expect(built.isNull(2));
}
