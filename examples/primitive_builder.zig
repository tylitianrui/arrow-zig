const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 3);
    defer builder.deinit();

    try builder.append(10);
    try builder.appendNull();
    try builder.append(30);

    var array_ref = try builder.finishReset();
    defer array_ref.release();
    const array = zarrow.Int32Array{ .data = array_ref.data() };

    try builder.append(7);
    try builder.append(8);

    var array_ref2 = try builder.finish();
    defer array_ref2.release();
    const array2 = zarrow.Int32Array{ .data = array_ref2.data() };

    std.debug.assert(array2.len() == 2);
    std.debug.assert(array2.value(0) == 7);
    std.debug.assert(array2.value(1) == 8);

    std.debug.print("examples/primitive_builder.zig | type=Int32Builder | length={d}, value0={d}, isNull1={any}, value2={d}, length2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
        array2.len(),
    });
}
