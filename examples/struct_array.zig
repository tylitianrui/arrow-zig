const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const value_type = zarrow.DataType{ .int32 = {} };
    const field = zarrow.Field{ .name = "a", .data_type = &value_type, .nullable = true };

    var values_builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(7);
    try values_builder.append(8);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    const buffers = try std.heap.page_allocator.alloc(zarrow.SharedBuffer, 1);
    buffers[0] = zarrow.SharedBuffer.empty;

    const children = try std.heap.page_allocator.alloc(zarrow.ArrayRef, 1);
    children[0] = values_ref.retain();

    const layout = zarrow.ArrayData{
        .data_type = zarrow.DataType{ .struct_ = .{ .fields = &[_]zarrow.Field{field} } },
        .length = 2,
        .buffers = buffers,
        .children = children,
    };

    var struct_ref = try zarrow.ArrayRef.fromOwnedUnsafe(std.heap.page_allocator, layout);
    defer struct_ref.release();

    const struct_array = zarrow.StructArray{ .data = struct_ref.data() };
    const child = struct_array.fieldRef(0);
    const child_array = zarrow.Int32Array{ .data = child.data() };

    std.debug.print("examples/struct_array.zig | length={d}, field0_value0={d}\n", .{
        struct_array.len(),
        try child_array.value(0),
    });
}
