const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.BinaryBuilder.init(std.heap.page_allocator, 3, 16);
    defer builder.deinit();

    try builder.append("zi");
    try builder.appendNull();
    try builder.append("ggy");

    var array_ref = try builder.finishReset();
    defer array_ref.release();
    const array = zarrow.BinaryArray{ .data = array_ref.data() };

    std.debug.assert(array.len() == 3);
    std.debug.assert(std.mem.eql(u8, array.value(0), "zi"));
    std.debug.assert(array.isNull(1));
    std.debug.assert(std.mem.eql(u8, array.value(2), "ggy"));

    try builder.append("again");
    try builder.appendNull();

    var array_ref2 = try builder.finish();
    defer array_ref2.release();
    const array2 = zarrow.BinaryArray{ .data = array_ref2.data() };

    std.debug.assert(array2.len() == 2);
    std.debug.assert(std.mem.eql(u8, array2.value(0), "again"));
    std.debug.assert(array2.isNull(1));

    std.debug.print("examples/binary_builder.zig | type=BinaryBuilder | length={d}, value_index_0={s}, isNull_index_1={any}, value_index_2={s}, length2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
        array2.len(),
    });
}
