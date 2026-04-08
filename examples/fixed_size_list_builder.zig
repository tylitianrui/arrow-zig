const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Each row is a fixed-size list of 3 i32 values (e.g., RGB triples).
    const item_type = zarrow.DataType{ .int32 = {} };
    const value_field = zarrow.Field{ .name = "item", .data_type = &item_type, .nullable = false };

    // Build the flat values array: 4 rows × 3 elements = 12 values.
    // Row 0: [10, 20, 30], Row 1: [40, 50, 60], Row 2: null (placeholder zeros),
    // Row 3: [70, 80, 90]
    var values_builder = try zarrow.Int32Builder.init(allocator, 12);
    defer values_builder.deinit();
    try values_builder.append(10);
    try values_builder.append(20);
    try values_builder.append(30);
    try values_builder.append(40);
    try values_builder.append(50);
    try values_builder.append(60);
    try values_builder.append(0); // placeholder for null row
    try values_builder.append(0);
    try values_builder.append(0);
    try values_builder.append(70);
    try values_builder.append(80);
    try values_builder.append(90);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try zarrow.FixedSizeListBuilder.init(allocator, value_field, 3);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();
    try builder.appendNull(); // row 2 is null
    try builder.appendValid();

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();

    const arr = zarrow.FixedSizeListArray{ .data = list_ref.data() };
    std.debug.print("examples/fixed_size_list_builder.zig | length={d}, list_size={d}\n", .{
        arr.len(), arr.listSize(),
    });

    for (0..arr.len()) |i| {
        if (arr.isNull(i)) {
            std.debug.print("  [{d}] null\n", .{i});
        } else {
            var row_ref = try arr.value(i);
            defer row_ref.release();
            const row = zarrow.Int32Array{ .data = row_ref.data() };
            std.debug.print("  [{d}] [{d}, {d}, {d}]\n", .{
                i, row.value(0), row.value(1), row.value(2),
            });
        }
    }
}
