const std = @import("std");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const Buffer = buffer.Buffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;

pub const BinaryArray = struct {
    data: ArrayData,

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
        return self.data.buffers[2].slice(@intCast(start), @intCast(end));
    }
};

test "binary array reads slices" {
    const dtype = DataType{ .binary = {} };
    const offsets = [_]i32{ 0, 2, 5 };
    const data_bytes = "ziggy";
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

    const array = BinaryArray{ .data = data };
    try std.testing.expectEqualStrings("zi", array.value(0));
    try std.testing.expectEqualStrings("ggy", array.value(1));
}
