const std = @import("std");
const buffer = @import("../buffer.zig");
const array_ref = @import("array_ref.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const ArrayRef = array_ref.ArrayRef;
pub const DataType = datatype.DataType;

pub const StructArray = struct {
    data: *const ArrayData,

    pub fn len(self: StructArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: StructArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn fieldCount(self: StructArray) usize {
        return self.data.children.len;
    }

    pub fn fieldRef(self: StructArray, index: usize) *const ArrayRef {
        std.debug.assert(index < self.data.children.len);
        return &self.data.children[index];
    }

    pub fn field(self: StructArray, index: usize) !ArrayRef {
        std.debug.assert(index < self.data.children.len);
        const child = self.data.children[index];
        const child_data = child.data();
        if (child_data.length == self.data.length and child_data.offset == self.data.offset) {
            return child.retain();
        }
        return child.slice(self.data.offset, self.data.length);
    }
};

test "struct array fields follow parent slice" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };

    var values_bytes: [4 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4 }));

    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 4,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = SharedBuffer.empty;

    const struct_layout = ArrayData{
        .data_type = DataType{ .struct_ = .{ .fields = &[_]datatype.Field{.{ .name = "a", .data_type = &value_type, .nullable = true }} } },
        .length = 2,
        .offset = 1,
        .buffers = buffers,
        .children = children,
    };

    var struct_ref = try ArrayRef.fromOwnedUnsafe(allocator, struct_layout);
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    var sliced_child = try struct_array.field(0);
    defer sliced_child.release();

    const child_view = @import("primitive_array.zig").PrimitiveArray(i32){ .data = sliced_child.data() };
    try std.testing.expectEqual(@as(usize, 2), child_view.len());
    try std.testing.expectEqual(@as(i32, 2), child_view.value(0));
    try std.testing.expectEqual(@as(i32, 3), child_view.value(1));
}

test "struct array fieldRef returns child" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };

    var values_bytes: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 9, 11 }));

    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = SharedBuffer.empty;

    const struct_layout = ArrayData{
        .data_type = DataType{ .struct_ = .{ .fields = &[_]datatype.Field{.{ .name = "a", .data_type = &value_type, .nullable = true }} } },
        .length = 2,
        .buffers = buffers,
        .children = children,
    };

    var struct_ref = try ArrayRef.fromOwnedUnsafe(allocator, struct_layout);
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    const child_view = @import("primitive_array.zig").PrimitiveArray(i32){ .data = struct_array.fieldRef(0).data() };
    try std.testing.expectEqual(@as(usize, 2), child_view.len());
    try std.testing.expectEqual(@as(i32, 9), child_view.value(0));
    try std.testing.expectEqual(@as(i32, 11), child_view.value(1));
}
