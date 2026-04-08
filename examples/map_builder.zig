const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Map from string key to i32 value.
    // Row 0: {"a": 1, "b": 2}
    // Row 1: null
    // Row 2: {"c": 3}
    const key_type = zarrow.DataType{ .string = {} };
    const item_type = zarrow.DataType{ .int32 = {} };
    const key_field = zarrow.Field{ .name = "key", .data_type = &key_type, .nullable = false };
    const item_field = zarrow.Field{ .name = "value", .data_type = &item_type, .nullable = true };

    // Build the flat entries: 3 key-value pairs total.
    var keys_builder = try zarrow.StringBuilder.init(allocator, 3, 6);
    defer keys_builder.deinit();
    try keys_builder.append("a");
    try keys_builder.append("b");
    try keys_builder.append("c");
    var keys_ref = try keys_builder.finish();
    defer keys_ref.release();

    var items_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer items_builder.deinit();
    try items_builder.append(1);
    try items_builder.append(2);
    try items_builder.append(3);
    var items_ref = try items_builder.finish();
    defer items_ref.release();

    // Wrap keys + items into a struct array (the entries child).
    var entries_builder = zarrow.StructBuilder.init(allocator, &[_]zarrow.Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]zarrow.ArrayRef{ keys_ref, items_ref });
    defer entries_ref.release();

    // Build the map array: 3 rows, row 1 is null.
    var map_builder = try zarrow.MapBuilder.init(allocator, 3, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2); // row 0 has 2 entries
    try map_builder.appendNull(); // row 1 is null (0 entries)
    try map_builder.appendLen(1); // row 2 has 1 entry

    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    const arr = zarrow.MapArray{ .data = map_ref.data() };
    std.debug.print("examples/map_builder.zig | length={d}\n", .{arr.len()});

    for (0..arr.len()) |i| {
        if (arr.isNull(i)) {
            std.debug.print("  [{d}] null\n", .{i});
            continue;
        }
        var row_ref = try arr.value(i);
        defer row_ref.release();
        const n = row_ref.data().length;
        const row_keys = zarrow.StringArray{ .data = row_ref.data().children[0].data() };
        const row_vals = zarrow.Int32Array{ .data = row_ref.data().children[1].data() };
        std.debug.print("  [{d}]", .{i});
        for (0..n) |j| {
            std.debug.print(" {s}:{d}", .{ row_keys.value(j), row_vals.value(j) });
        }
        std.debug.print("\n", .{});
    }
}
