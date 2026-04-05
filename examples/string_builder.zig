const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.StringBuilder.init(std.heap.page_allocator, 3, 16);
    defer builder.deinit();

    try builder.append("zig");
    try builder.appendNull();
    try builder.append("arrow");

    var array_ref = try builder.finishReset();
    defer array_ref.release();
    const array = zarrow.StringArray{ .data = array_ref.data() };

    std.debug.assert(array.len() == 3);
    std.debug.assert(std.mem.eql(u8, array.value(0), "zig"));
    std.debug.assert(array.isNull(1));
    std.debug.assert(std.mem.eql(u8, array.value(2), "arrow"));

    try builder.append("hello");
    try builder.append("world");

    var array_ref2 = try builder.finish();
    defer array_ref2.release();
    const array2 = zarrow.StringArray{ .data = array_ref2.data() };

    std.debug.assert(array2.len() == 2);
    std.debug.assert(std.mem.eql(u8, array2.value(0), "hello"));
    std.debug.assert(std.mem.eql(u8, array2.value(1), "world"));

    std.debug.print("examples/string_builder.zig | type=StringBuilder | length={d}, value_index_0={s}, isNull_index_1={any}, value_index_2={s}, length2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
        array2.len(),
    });
}
