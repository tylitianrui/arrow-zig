const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var builder = try zarrow.NullBuilder.init(allocator, 4);
    defer builder.deinit();

    try builder.appendNull();
    try builder.appendNulls(2);

    var first_ref = try builder.finishReset();
    defer first_ref.release();
    const first = zarrow.NullArray{ .data = first_ref.data() };
    std.debug.assert(first_ref.data().data_type == .null);

    try builder.appendNulls(2);
    var second_ref = try builder.finish();
    defer second_ref.release();
    const second = zarrow.NullArray{ .data = second_ref.data() };
    std.debug.assert(second_ref.data().data_type == .null);

    std.debug.print(
        "examples/null_builder.zig | first_len={d}, first_isNull0={any}, second_len={d}, second_isNull1={any}\n",
        .{ first.len(), first.isNull(0), second.len(), second.isNull(1) },
    );
}
