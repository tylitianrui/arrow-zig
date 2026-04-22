const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const value_type = zarrow.DataType{ .int32 = {} };
    const fields = &[_]zarrow.Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    var values_builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(1);
    try values_builder.append(2);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = zarrow.StructBuilder.init(std.heap.page_allocator, fields);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();

    var struct_ref = try builder.finish(&[_]zarrow.ArrayRef{values_ref});
    defer struct_ref.release();

    const struct_array = zarrow.StructArray{ .data = struct_ref.data() };
    const child = struct_array.fieldRef(0);
    const child_array = zarrow.Int32Array{ .data = child.data() };

    std.debug.print("examples/struct_builder.zig | length={d}, field0_value0={d}\n", .{
        struct_array.len(),
        try child_array.value(0),
    });
}
