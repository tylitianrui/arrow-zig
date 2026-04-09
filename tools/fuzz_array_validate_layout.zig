const std = @import("std");
const zarrow = @import("zarrow");

const MAX_INPUT_BYTES: usize = 1 << 20;
const MAX_BUFFER_BYTES: usize = 128;
const MAX_BUFFERS: usize = 5;

const Input = struct {
    bytes: []const u8,
    idx: usize = 0,

    fn nextByte(self: *Input) u8 {
        if (self.bytes.len == 0) return 0;
        const b = self.bytes[self.idx % self.bytes.len];
        self.idx += 1;
        return b;
    }

    fn nextUsize(self: *Input, limit: usize) usize {
        if (limit == 0) return 0;
        return @as(usize, self.nextByte()) % limit;
    }
};

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
    var input = Input{ .bytes = bytes };

    const dt = randomDataType(&input);
    const length = input.nextUsize(64);
    const offset = input.nextUsize(64);
    const null_count_mode = input.nextByte() % 4;
    const null_count: ?usize = switch (null_count_mode) {
        0 => null,
        1 => 0,
        2 => length,
        else => input.nextUsize(128),
    };

    const buffer_count = input.nextUsize(MAX_BUFFERS + 1);
    const buffers = try allocator.alloc(zarrow.SharedBuffer, buffer_count);
    defer {
        for (buffers) |buf| {
            var owned = buf;
            owned.release();
        }
        allocator.free(buffers);
    }

    for (buffers) |*buf| {
        const len = input.nextUsize(MAX_BUFFER_BYTES + 1);
        buf.* = try randomSharedBuffer(allocator, &input, len);
    }

    const data = zarrow.ArrayData{
        .data_type = dt,
        .length = length,
        .offset = offset,
        .null_count = null_count,
        .buffers = buffers,
        .children = &[_]zarrow.ArrayRef{},
        .dictionary = null,
    };

    _ = data.validateLayout() catch {};
}

fn randomSharedBuffer(allocator: std.mem.Allocator, input: *Input, len: usize) !zarrow.SharedBuffer {
    if (len == 0) return zarrow.SharedBuffer.empty;

    var owned = try zarrow.OwnedBuffer.init(allocator, len);
    errdefer owned.deinit();

    var i: usize = 0;
    while (i < len) : (i += 1) {
        owned.data[i] = input.nextByte();
    }

    return try owned.toShared(len);
}

fn randomDataType(input: *Input) zarrow.DataType {
    return switch (input.nextByte() % 8) {
        0 => zarrow.DataType{ .null = {} },
        1 => zarrow.DataType{ .bool = {} },
        2 => zarrow.DataType{ .int32 = {} },
        3 => zarrow.DataType{ .string = {} },
        4 => zarrow.DataType{ .large_string = {} },
        5 => zarrow.DataType{ .fixed_size_binary = .{ .byte_width = @as(i32, @intCast((input.nextByte() % 16) + 1)) } },
        6 => zarrow.DataType{ .decimal128 = .{ .precision = @as(u8, @intCast((input.nextByte() % 38) + 1)), .scale = @as(i32, @intCast(input.nextByte() % 20)) } },
        else => zarrow.DataType{ .duration = .{ .unit = switch (input.nextByte() % 4) {
            0 => .second,
            1 => .millisecond,
            2 => .microsecond,
            else => .nanosecond,
        } } },
    };
}
