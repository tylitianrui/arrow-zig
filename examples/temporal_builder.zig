const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var date32_builder = try zarrow.Date32Builder.init(allocator, 3);
    defer date32_builder.deinit();
    try date32_builder.append(18_628);
    try date32_builder.appendNull();
    try date32_builder.append(18_630);
    var date32_ref = try date32_builder.finish();
    defer date32_ref.release();
    const date32 = zarrow.Date32Array{ .data = date32_ref.data() };
    std.debug.assert(date32_ref.data().data_type == .date32);

    var date64_builder = try zarrow.Date64Builder.init(allocator, 2);
    defer date64_builder.deinit();
    try date64_builder.append(1_609_459_200_000);
    try date64_builder.append(1_609_545_600_000);
    var date64_ref = try date64_builder.finish();
    defer date64_ref.release();
    const date64 = zarrow.Date64Array{ .data = date64_ref.data() };
    std.debug.assert(date64_ref.data().data_type == .date64);

    const Time32MsBuilder = zarrow.Time32Builder(.millisecond);
    var time32_builder = try Time32MsBuilder.init(allocator, 2);
    defer time32_builder.deinit();
    try time32_builder.append(1000);
    try time32_builder.append(2500);
    var time32_ref = try time32_builder.finish();
    defer time32_ref.release();
    const time32 = zarrow.Time32Array{ .data = time32_ref.data() };
    std.debug.assert(time32_ref.data().data_type == .time32);
    std.debug.assert(time32_ref.data().data_type.time32.unit == .millisecond);

    const Time64NsBuilder = zarrow.Time64Builder(.nanosecond);
    var time64_builder = try Time64NsBuilder.init(allocator, 2);
    defer time64_builder.deinit();
    try time64_builder.append(1_000_000);
    try time64_builder.append(2_500_000);
    var time64_ref = try time64_builder.finish();
    defer time64_ref.release();
    const time64 = zarrow.Time64Array{ .data = time64_ref.data() };
    std.debug.assert(time64_ref.data().data_type == .time64);
    std.debug.assert(time64_ref.data().data_type.time64.unit == .nanosecond);

    const TimestampUsUtcBuilder = zarrow.TimestampBuilder(.microsecond, "UTC");
    var timestamp_builder = try TimestampUsUtcBuilder.init(allocator, 2);
    defer timestamp_builder.deinit();
    try timestamp_builder.append(1_700_000_000_000_000);
    try timestamp_builder.append(1_700_000_000_123_456);
    var timestamp_ref = try timestamp_builder.finish();
    defer timestamp_ref.release();
    const timestamp = zarrow.TimestampArray{ .data = timestamp_ref.data() };
    std.debug.assert(timestamp_ref.data().data_type == .timestamp);
    std.debug.assert(timestamp_ref.data().data_type.timestamp.unit == .microsecond);
    std.debug.assert(std.mem.eql(u8, timestamp_ref.data().data_type.timestamp.timezone.?, "UTC"));

    const DurationMsBuilder = zarrow.DurationBuilder(.millisecond);
    var duration_builder = try DurationMsBuilder.init(allocator, 3);
    defer duration_builder.deinit();
    try duration_builder.append(50);
    try duration_builder.appendNull();
    try duration_builder.append(125);
    var duration_ref = try duration_builder.finish();
    defer duration_ref.release();
    const duration = zarrow.DurationArray{ .data = duration_ref.data() };
    std.debug.assert(duration_ref.data().data_type == .duration);
    std.debug.assert(duration_ref.data().data_type.duration.unit == .millisecond);

    std.debug.print(
        "examples/temporal_builder.zig | date32={d} date64={d} time32={d} time64={d} ts={d} duration={d}\n",
        .{ date32.len(), date64.len(), time32.len(), time64.len(), timestamp.len(), duration.len() },
    );
}
