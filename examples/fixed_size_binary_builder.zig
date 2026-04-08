const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Build a fixed-size-binary array with byte_width=4 (IPv4 addresses).
    var builder = try zarrow.FixedSizeBinaryBuilder.init(allocator, 4, 4);
    defer builder.deinit();
    try builder.append(&[4]u8{ 192, 168, 1, 1 });
    try builder.append(&[4]u8{ 10, 0, 0, 1 });
    try builder.appendNull();
    try builder.append(&[4]u8{ 172, 16, 0, 1 });
    var ref = try builder.finish();
    defer ref.release();

    const arr = zarrow.FixedSizeBinaryArray{ .data = ref.data() };
    std.debug.print("examples/fixed_size_binary_builder.zig | length={d}, byte_width={d}\n", .{
        arr.len(), arr.byteWidth(),
    });

    for (0..arr.len()) |i| {
        if (arr.isNull(i)) {
            std.debug.print("  [{d}] null\n", .{i});
        } else {
            const v = arr.value(i);
            std.debug.print("  [{d}] {d}.{d}.{d}.{d}\n", .{ i, v[0], v[1], v[2], v[3] });
        }
    }
}
