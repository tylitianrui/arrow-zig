const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Run-end-encoded string array.
    // Logical sequence: ["apple","apple","banana","banana","banana","cherry"]
    //   Run 0: run_end=2  → "apple"  (indices 0..1)
    //   Run 1: run_end=5  → "banana" (indices 2..4)
    //   Run 2: run_end=6  → "cherry" (index 5)
    const value_type = zarrow.DataType{ .string = {} };

    var values_builder = try zarrow.StringBuilder.init(allocator, 3, 18);
    defer values_builder.deinit();
    try values_builder.append("apple");
    try values_builder.append("banana");
    try values_builder.append("cherry");
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var ree_builder = try zarrow.RunEndEncodedBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        3,
    );
    defer ree_builder.deinit();
    try ree_builder.appendRunEnd(2); // first run ends before logical index 2
    try ree_builder.appendRunEnd(5); // second run ends before logical index 5
    try ree_builder.appendRunEnd(6); // third run ends before logical index 6
    var ree_ref = try ree_builder.finish(values_ref);
    defer ree_ref.release();

    const arr = zarrow.RunEndEncodedArray{ .data = ree_ref.data() };
    std.debug.print("examples/run_end_encoded_builder.zig | logical_length={d}\n", .{arr.len()});

    for (0..arr.len()) |i| {
        var v = try arr.value(i);
        defer v.release();
        const s = zarrow.StringArray{ .data = v.data() };
        std.debug.print("  [{d}] {s}\n", .{ i, s.value(0) });
    }
}
