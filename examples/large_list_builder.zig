const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const value_type = zarrow.DataType{ .int32 = {} };
    const field = zarrow.Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 4);
    defer values_builder.deinit();
    try values_builder.append(10);
    try values_builder.append(20);
    try values_builder.append(30);
    try values_builder.append(40);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try zarrow.LargeListBuilder.init(std.heap.page_allocator, 2, field);
    defer builder.deinit();
    try builder.appendLen(1);
    try builder.appendLen(3);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = zarrow.LargeListArray{ .data = list_ref.data() };

    std.debug.print("examples/large_list_builder.zig | length={d}\n", .{list.len()});

    var second = try list.value(1);
    defer second.release();
    const second_values = zarrow.Int32Array{ .data = second.data() };
    const v0 = try second_values.value(0);
    const v1 = try second_values.value(1);
    const v2 = try second_values.value(2);
    std.debug.print("second list len={d}, v0={d}, v1={d}, v2={d}\n", .{ second_values.len(), v0, v1, v2 });
}
