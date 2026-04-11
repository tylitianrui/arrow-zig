const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var sv_builder = try zarrow.StringViewBuilder.init(std.heap.page_allocator, 4, 32);
    defer sv_builder.deinit();

    try sv_builder.append("short");
    try sv_builder.appendNull();
    try sv_builder.append("tiny");
    try sv_builder.append("this string is longer than twelve");

    var sv_ref = try sv_builder.finish();
    defer sv_ref.release();
    const sv = zarrow.StringViewArray{ .data = sv_ref.data() };

    std.debug.assert(sv.len() == 4);
    std.debug.assert(std.mem.eql(u8, sv.value(0), "short"));
    std.debug.assert(sv.isNull(1));
    std.debug.assert(std.mem.eql(u8, sv.value(2), "tiny"));
    std.debug.assert(std.mem.eql(u8, sv.value(3), "this string is longer than twelve"));

    var bv_builder = try zarrow.BinaryViewBuilder.init(std.heap.page_allocator, 3, 24);
    defer bv_builder.deinit();

    try bv_builder.append("ab");
    try bv_builder.append("this-binary-view-is-long");
    try bv_builder.appendNull();

    var bv_ref = try bv_builder.finish();
    defer bv_ref.release();
    const bv = zarrow.BinaryViewArray{ .data = bv_ref.data() };

    std.debug.assert(bv.len() == 3);
    std.debug.assert(std.mem.eql(u8, bv.value(0), "ab"));
    std.debug.assert(std.mem.eql(u8, bv.value(1), "this-binary-view-is-long"));
    std.debug.assert(bv.isNull(2));

    std.debug.print(
        "examples/view_builder.zig | sv_len={d}, sv_v3={s}, bv_len={d}, bv_v1={s}\n",
        .{ sv.len(), sv.value(3), bv.len(), bv.value(1) },
    );
}
