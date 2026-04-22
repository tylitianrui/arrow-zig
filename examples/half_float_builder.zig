const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var builder = try zarrow.HalfFloatBuilder.init(allocator, 4);
    defer builder.deinit();

    try builder.append(@as(f16, 1.5));
    try builder.append(@as(f16, -2.25));
    try builder.appendNull();
    try builder.append(@as(f16, 0.75));

    var ref = try builder.finish();
    defer ref.release();

    const arr = zarrow.HalfFloatArray{ .data = ref.data() };
    std.debug.assert(ref.data().data_type == .half_float);

    std.debug.print(
        "examples/half_float_builder.zig | length={d}, v0={d:.2}, v1={d:.2}, isNull2={any}, v3={d:.2}\n",
        .{
            arr.len(),
            @as(f32, try arr.value(0)),
            @as(f32, try arr.value(1)),
            arr.isNull(2),
            @as(f32, try arr.value(3)),
        },
    );
}
