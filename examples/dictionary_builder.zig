const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Build the dictionary values: ["red", "green", "blue"]
    const value_type = zarrow.DataType{ .string = {} };
    var dict_builder = try zarrow.StringBuilder.init(allocator, 3, 13);
    defer dict_builder.deinit();
    try dict_builder.append("red");
    try dict_builder.append("green");
    try dict_builder.append("blue");
    var dict_values = try dict_builder.finish();
    defer dict_values.release();

    // Build the index array: [0, 2, 1, null, 0]
    var idx_builder = try zarrow.DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        5,
    );
    defer idx_builder.deinit();
    try idx_builder.appendIndex(0);
    try idx_builder.appendIndex(2);
    try idx_builder.appendIndex(1);
    try idx_builder.appendNull();
    try idx_builder.appendIndex(0);
    var dict_ref = try idx_builder.finish(dict_values);
    defer dict_ref.release();

    const arr = zarrow.DictionaryArray{ .data = dict_ref.data() };
    std.debug.print("examples/dictionary_builder.zig | length={d}\n", .{arr.len()});

    const values = zarrow.StringArray{ .data = arr.dictionaryRef().data() };
    for (0..arr.len()) |i| {
        if (arr.isNull(i)) {
            std.debug.print("  [{d}] null\n", .{i});
        } else {
            const idx: usize = @intCast(arr.index(i));
            std.debug.print("  [{d}] {s}\n", .{ i, values.value(idx) });
        }
    }
}
