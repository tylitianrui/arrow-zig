const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const d32_params = zarrow.DecimalParams{ .precision = 9, .scale = 2 };
    const Decimal32BuilderT = zarrow.Decimal32Builder(d32_params);
    var d32_builder = try Decimal32BuilderT.init(allocator, 3);
    defer d32_builder.deinit();
    try d32_builder.append(12_345);
    try d32_builder.appendNull();
    try d32_builder.append(-67_890);
    var d32_ref = try d32_builder.finish();
    defer d32_ref.release();
    const d32 = zarrow.Decimal32Array{ .data = d32_ref.data() };
    std.debug.assert(d32_ref.data().data_type == .decimal32);
    std.debug.assert(d32_ref.data().data_type.decimal32.precision == 9);
    std.debug.assert(d32_ref.data().data_type.decimal32.scale == 2);

    const d64_params = zarrow.DecimalParams{ .precision = 18, .scale = 4 };
    const Decimal64BuilderT = zarrow.Decimal64Builder(d64_params);
    var d64_builder = try Decimal64BuilderT.init(allocator, 3);
    defer d64_builder.deinit();
    try d64_builder.append(1_234_567_890_123);
    try d64_builder.appendNull();
    try d64_builder.append(-9_876_543_210_987);
    var d64_ref = try d64_builder.finish();
    defer d64_ref.release();
    const d64 = zarrow.Decimal64Array{ .data = d64_ref.data() };
    std.debug.assert(d64_ref.data().data_type == .decimal64);
    std.debug.assert(d64_ref.data().data_type.decimal64.precision == 18);
    std.debug.assert(d64_ref.data().data_type.decimal64.scale == 4);

    const d128_params = zarrow.DecimalParams{ .precision = 38, .scale = 10 };
    const Decimal128BuilderT = zarrow.Decimal128Builder(d128_params);
    var d128_builder = try Decimal128BuilderT.init(allocator, 2);
    defer d128_builder.deinit();
    try d128_builder.append(@as(i128, 123_456_789_012_345_678));
    try d128_builder.append(@as(i128, -987_654_321_098_765_432));
    var d128_ref = try d128_builder.finish();
    defer d128_ref.release();
    const d128 = zarrow.Decimal128Array{ .data = d128_ref.data() };
    std.debug.assert(d128_ref.data().data_type == .decimal128);
    std.debug.assert(d128_ref.data().data_type.decimal128.precision == 38);
    std.debug.assert(d128_ref.data().data_type.decimal128.scale == 10);

    const d256_params = zarrow.DecimalParams{ .precision = 76, .scale = 20 };
    const Decimal256BuilderT = zarrow.Decimal256Builder(d256_params);
    var d256_builder = try Decimal256BuilderT.init(allocator, 2);
    defer d256_builder.deinit();
    try d256_builder.append(@as(i256, 123_456_789_012_345_678_901_234_567_890));
    try d256_builder.append(@as(i256, -987_654_321_098_765_432_109_876_543_210));
    var d256_ref = try d256_builder.finish();
    defer d256_ref.release();
    const d256 = zarrow.Decimal256Array{ .data = d256_ref.data() };
    std.debug.assert(d256_ref.data().data_type == .decimal256);
    std.debug.assert(d256_ref.data().data_type.decimal256.precision == 76);
    std.debug.assert(d256_ref.data().data_type.decimal256.scale == 20);

    std.debug.print(
        "examples/decimal_builder.zig | d32={d} d64={d} d128={d} d256={d} v32={d} v64={d}\n",
        .{ d32.len(), d64.len(), d128.len(), d256.len(), try d32.value(0), try d64.value(0) },
    );
}
