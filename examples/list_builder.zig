const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const value_type = zarrow.DataType{ .int32 = {} };
    const field = zarrow.Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(1);
    try values_builder.append(2);
    try values_builder.append(3);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try zarrow.ListBuilder.init(std.heap.page_allocator, 2, field);
    defer builder.deinit();
    try builder.appendLen(2);
    try builder.appendLen(1);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = zarrow.ListArray{ .data = list_ref.data() };

    std.debug.print("examples/list_builder.zig | length={d}\n", .{list.len()});

    var first = try list.value(0);
    defer first.release();
    const first_values = zarrow.Int32Array{ .data = first.data() };
    const v0 = try first_values.value(0);
    const v1 = try first_values.value(1);
    std.debug.print("first list len={d}, v0={d}, v1={d}\n", .{ first_values.len(), v0, v1 });
}
