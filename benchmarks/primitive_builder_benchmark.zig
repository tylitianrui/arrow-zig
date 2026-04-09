const std = @import("std");
const zarrow = @import("zarrow");
const csv_meta = @import("csv_meta.zig");

const Mode = enum { default, smoke, full, matrix, matrix_no_header, ci, ci_no_header };

const BenchConfig = struct {
    rows: usize,
    iterations: usize,
};

fn parseMode(allocator: std.mem.Allocator) !Mode {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const mode_str = args.next() orelse return .default;

    if (std.mem.eql(u8, mode_str, "smoke")) return .smoke;
    if (std.mem.eql(u8, mode_str, "full")) return .full;
    if (std.mem.eql(u8, mode_str, "matrix")) return .matrix;
    if (std.mem.eql(u8, mode_str, "matrix-no-header")) return .matrix_no_header;
    if (std.mem.eql(u8, mode_str, "ci")) return .ci;
    if (std.mem.eql(u8, mode_str, "ci-no-header")) return .ci_no_header;
    return .default;
}

fn printCsvHeader() void {
    std.debug.print("benchmark,rows,iterations,elapsed_ns,rows_per_sec,ns_per_row,checksum,git_sha,timestamp\n", .{});
}

fn runBenchmark(allocator: std.mem.Allocator, cfg: BenchConfig, emit_csv: bool, meta: csv_meta.CsvMeta) !void {
    var builder = try zarrow.Int32Builder.init(allocator, cfg.rows);
    defer builder.deinit();

    var checksum: i64 = 0;
    var timer = try std.time.Timer.start();

    var iter: usize = 0;
    while (iter < cfg.iterations) : (iter += 1) {
        var i: usize = 0;
        while (i < cfg.rows) : (i += 1) {
            const v: i32 = @intCast((i + iter) % 1024);
            try builder.append(v);
            checksum += v;
        }

        var arr = try builder.finishReset();
        defer arr.release();
    }

    const elapsed_ns = timer.read();
    const total_rows = cfg.rows * cfg.iterations;
    const rows_per_sec = (@as(f64, @floatFromInt(total_rows)) * 1_000_000_000.0) / @as(f64, @floatFromInt(elapsed_ns));
    const ns_per_row = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(total_rows));

    if (emit_csv) {
        std.debug.print("primitive_builder,{d},{d},{d},{d:.2},{d:.2},{d},{s},{d}\n", .{
            cfg.rows,
            cfg.iterations,
            elapsed_ns,
            rows_per_sec,
            ns_per_row,
            checksum,
            meta.git_sha,
            meta.timestamp,
        });
        return;
    }

    std.debug.print("benchmark=primitive_builder rows={d} iterations={d} elapsed_ns={d} rows_per_sec={d:.2} ns_per_row={d:.2} checksum={d}\n", .{
        cfg.rows,
        cfg.iterations,
        elapsed_ns,
        rows_per_sec,
        ns_per_row,
        checksum,
    });
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const mode = try parseMode(allocator);
    const meta = try csv_meta.resolve(allocator);
    defer meta.deinit(allocator);

    switch (mode) {
        .smoke => try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 20 }, false, meta),
        .full => try runBenchmark(allocator, .{ .rows = 500_000, .iterations = 120 }, false, meta),
        .matrix => {
            printCsvHeader();
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 500 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 120 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 20 }, true, meta);
        },
        .matrix_no_header => {
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 500 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 120 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 20 }, true, meta);
        },
        .ci => {
            printCsvHeader();
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 80 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 20 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 4 }, true, meta);
        },
        .ci_no_header => {
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 80 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 20 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 4 }, true, meta);
        },
        .default => try runBenchmark(allocator, .{ .rows = 200_000, .iterations = 80 }, false, meta),
    }
}
