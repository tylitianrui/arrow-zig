const std = @import("std");
const zarrow = @import("zarrow");

const MAX_INPUT_BYTES: usize = 4 << 20;
const MAX_BATCHES: usize = 16;

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    const bytes = try readInput(allocator);
    defer allocator.free(bytes);

    try runOne(allocator, bytes);
}

fn readInput(allocator: std.mem.Allocator) ![]u8 {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    if (args.next()) |path| {
        return try std.fs.cwd().readFileAlloc(allocator, path, MAX_INPUT_BYTES);
    }
    return error.MissingInputPath;
}

fn runOne(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var stream = std.io.fixedBufferStream(bytes);
    var reader = zarrow.IpcStreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = reader.readSchema() catch return;

    var seen: usize = 0;
    while (seen < MAX_BATCHES) : (seen += 1) {
        const maybe_batch = reader.nextRecordBatch() catch return;
        if (maybe_batch == null) return;

        var batch = maybe_batch.?;
        batch.deinit();
    }
}
