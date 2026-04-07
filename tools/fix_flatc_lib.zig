const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // executable name
    const path = args.next() orelse return error.MissingPathArgument;

    const max_bytes: usize = 16 * 1024 * 1024;
    const original = try std.fs.cwd().readFileAlloc(allocator, path, max_bytes);
    defer allocator.free(original);

    const fixed = try allocator.dupe(u8, original);
    defer allocator.free(fixed);

    var changed = false;
    for (fixed) |*c| {
        if (c.* == '\\') {
            c.* = '/';
            changed = true;
        }
    }

    if (!changed) return;
    try std.fs.cwd().writeFile(.{ .sub_path = path, .data = fixed });
}
