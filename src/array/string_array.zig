const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const Buffer = buffer.Buffer;
pub const MutableBuffer = buffer.MutableBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;

const STRING_TYPE = DataType{ .string = {} };

/// Variable-length UTF-8 array view.
pub const StringArray = struct {
    data: ArrayData,

    pub fn len(self: StringArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: StringArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn value(self: StringArray, i: usize) []const u8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const start = offsets[self.data.offset + i];
        const end = offsets[self.data.offset + i + 1];
        return self.data.buffers[2].slice(@intCast(start), @intCast(end));
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

/// Builder for variable-length UTF-8 arrays.
pub const StringBuilder = struct {
    allocator: std.mem.Allocator,
    offsets: MutableBuffer,
    data: MutableBuffer,
    validity: ?MutableBuffer = null,
    buffers: [3]Buffer = undefined,
    len: usize = 0,
    null_count: isize = 0,
    data_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator, capacity: usize, data_capacity: usize) !StringBuilder {
        const offsets = try MutableBuffer.init(allocator, (capacity + 1) * @sizeOf(i32));
        const offsets_slice = std.mem.bytesAsSlice(i32, offsets.data);
        offsets_slice[0] = 0;
        return .{
            .allocator = allocator,
            .offsets = offsets,
            .data = try MutableBuffer.init(allocator, data_capacity),
        };
    }

    pub fn deinit(self: *StringBuilder) void {
        self.offsets.deinit();
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    fn ensureOffsetsCapacity(self: *StringBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i32);
        if (needed_len <= capacity) return;
        try self.offsets.resize(needed_len * @sizeOf(i32));
    }

    fn ensureDataCapacity(self: *StringBuilder, needed_len: usize) !void {
        if (needed_len <= self.data.len()) return;
        try self.data.resize(needed_len);
    }

    fn ensureValidityForNull(self: *StringBuilder, new_len: usize) !void {
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

    fn setValidBit(self: *StringBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn append(self: *StringBuilder, value: []const u8) !void {
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

    pub fn appendNull(self: *StringBuilder) !void {
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = @intCast(self.data_len);
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *StringBuilder) StringArray {
        const validity_buf = if (self.validity) |buf| buf.toBuffer(bitmap.byteLength(self.len)) else Buffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = self.offsets.toBuffer((self.len + 1) * @sizeOf(i32));
        self.buffers[2] = self.data.toBuffer(self.data_len);
        return StringArray{
            .data = ArrayData{
                .data_type = &STRING_TYPE,
                .length = self.len,
                .null_count = self.null_count,
                .buffers = self.buffers[0..],
            },
        };
    }
};

test "string array reads slices" {
    const dtype = DataType{ .string = {} };
    const offsets = [_]i32{ 0, 3, 7 };
    const data_bytes = "ziglang";
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    const data = ArrayData{
        .data_type = &dtype,
        .length = 2,
        .buffers = &[_]Buffer{
            Buffer.empty,
            Buffer.fromSlice(offset_bytes[0..]),
            Buffer.fromSlice(data_bytes),
        },
    };

    const array = StringArray{ .data = data };
    try std.testing.expectEqualStrings("zig", array.value(0));
    try std.testing.expectEqualStrings("lang", array.value(1));
}

test "string builder appends slices" {
    var builder = try StringBuilder.init(std.testing.allocator, 2, 8);
    defer builder.deinit();

    try builder.append("zig");
    try builder.appendNull();
    try builder.append("lang");

    const built = builder.finish();
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expectEqualStrings("zig", built.value(0));
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqualStrings("lang", built.value(2));
}
