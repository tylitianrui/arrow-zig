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

fn runOnce(allocator: std.mem.Allocator, batch_builder: *zarrow.RecordBatchBuilder, rows: usize, checksum: *usize) !void {
    var id_builder = try zarrow.Int32Builder.init(allocator, rows);
    defer id_builder.deinit();

    var ok_builder = try zarrow.BooleanBuilder.init(allocator, rows);
    defer ok_builder.deinit();

    var i: usize = 0;
    while (i < rows) : (i += 1) {
        try id_builder.append(@intCast(i));
        try ok_builder.append((i & 1) == 0);
    }

    var id_ref = try id_builder.finish();
    defer id_ref.release();

    var ok_ref = try ok_builder.finish();
    defer ok_ref.release();

    try batch_builder.setColumnByName("id", id_ref);
    try batch_builder.setColumnByName("ok", ok_ref);

    var batch = try batch_builder.finish();
    defer batch.deinit();

    var half = try batch.slice(rows / 4, rows / 2);
    defer half.deinit();

    checksum.* += half.numRows();

    try batch_builder.reset();
}

fn runBenchmark(allocator: std.mem.Allocator, cfg: BenchConfig, emit_csv: bool, meta: csv_meta.CsvMeta) !void {
    const int_type = zarrow.DataType{ .int32 = {} };
    const bool_type = zarrow.DataType{ .bool = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "ok", .data_type = &bool_type, .nullable = false },
    };

    var batch_builder = try zarrow.RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer batch_builder.deinit();

    var warmup: usize = 0;
    try runOnce(allocator, &batch_builder, cfg.rows, &warmup);

    var checksum: usize = 0;
    var timer = try std.time.Timer.start();

    var iter: usize = 0;
    while (iter < cfg.iterations) : (iter += 1) {
        try runOnce(allocator, &batch_builder, cfg.rows, &checksum);
    }

    const elapsed_ns = timer.read();
    const total_rows = cfg.rows * cfg.iterations;
    const rows_per_sec = (@as(f64, @floatFromInt(total_rows)) * 1_000_000_000.0) / @as(f64, @floatFromInt(elapsed_ns));
    const ns_per_row = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(total_rows));
    const final_checksum = checksum + warmup;

    if (emit_csv) {
        std.debug.print("record_batch_builder,{d},{d},{d},{d:.2},{d:.2},{d},{s},{d}\n", .{
            cfg.rows,
            cfg.iterations,
            elapsed_ns,
            rows_per_sec,
            ns_per_row,
            final_checksum,
            meta.git_sha,
            meta.timestamp,
        });
        return;
    }

    const ns_per_iter = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(cfg.iterations));
    std.debug.print("benchmark=record_batch_builder rows={d} iterations={d} elapsed_ns={d} ns_per_iter={d:.0} rows_per_sec={d:.2} checksum={d}\n", .{
        cfg.rows,
        cfg.iterations,
        elapsed_ns,
        ns_per_iter,
        rows_per_sec,
        final_checksum,
    });
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const mode = try parseMode(allocator);
    const meta = try csv_meta.resolve(allocator);
    defer meta.deinit(allocator);

    switch (mode) {
        .smoke => try runBenchmark(allocator, .{ .rows = 5_000, .iterations = 30 }, false, meta),
        .full => try runBenchmark(allocator, .{ .rows = 80_000, .iterations = 180 }, false, meta),
        .matrix => {
            printCsvHeader();
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 300 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 120 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 24 }, true, meta);
        },
        .matrix_no_header => {
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 300 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 120 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 24 }, true, meta);
        },
        .ci => {
            printCsvHeader();
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 80 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 24 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 5 }, true, meta);
        },
        .ci_no_header => {
            try runBenchmark(allocator, .{ .rows = 1_000, .iterations = 80 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 10_000, .iterations = 24 }, true, meta);
            try runBenchmark(allocator, .{ .rows = 100_000, .iterations = 5 }, true, meta);
        },
        .default => try runBenchmark(allocator, .{ .rows = 20_000, .iterations = 120 }, false, meta),
    }
}
