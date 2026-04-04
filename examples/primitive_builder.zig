const std = @import("std");
const root = @import("zarrow");

pub fn main() !void {
    var builder = try root.Int32Builder.init(std.heap.page_allocator, 3);
    defer builder.deinit();

    try builder.append(10);
    try builder.appendNull();
    try builder.append(30);

    const array = builder.finish();

    std.debug.print("len={d}, value_0={d}, isNull_1={any}, value_2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
    });
}
