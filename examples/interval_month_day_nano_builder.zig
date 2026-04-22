const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // month_day_nano is currently exposed as a packed i128 scalar payload.
    const interval_type = zarrow.IntervalType{ .unit = .month_day_nano };
    const Builder = zarrow.IntervalMonthDayNanoBuilder(interval_type);
    var builder = try Builder.init(allocator, 3);
    defer builder.deinit();

    try builder.append(@as(i128, 123_456_789_012_345_678));
    try builder.appendNull();
    try builder.append(@as(i128, -123_456_789_012_345_678));

    var ref = try builder.finish();
    defer ref.release();

    const arr = zarrow.IntervalMonthDayNanoArray{ .data = ref.data() };
    std.debug.assert(ref.data().data_type == .interval_month_day_nano);
    std.debug.assert(ref.data().data_type.interval_month_day_nano.unit == .month_day_nano);

    std.debug.print(
        "examples/interval_month_day_nano_builder.zig | length={d}, v0={d}, isNull1={any}, v2={d}\n",
        .{ arr.len(), try arr.value(0), arr.isNull(1), try arr.value(2) },
    );
}
