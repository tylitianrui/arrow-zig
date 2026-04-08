const std = @import("std");
const zarrow = @import("zarrow");

const ReaderAdapter = struct {
    inner: *std.Io.Reader,
    pub const Error = anyerror;

    pub fn readNoEof(self: @This(), dest: []u8) Error!void {
        try self.inner.readSliceAll(dest);
    }
};

const FixtureCase = enum {
    canonical,
    dict_delta,
    ree,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // exe
    const in_path = args.next() orelse {
        std.log.err("usage: interop-fixture-check <in.arrow> [canonical|dict-delta|ree]", .{});
        return error.InvalidArgs;
    };
    const fixture_case: FixtureCase = blk: {
        const mode = args.next() orelse break :blk .canonical;
        if (std.mem.eql(u8, mode, "canonical")) break :blk .canonical;
        if (std.mem.eql(u8, mode, "dict-delta")) break :blk .dict_delta;
        if (std.mem.eql(u8, mode, "ree")) break :blk .ree;
        std.log.err("unknown fixture mode: {s}", .{mode});
        return error.InvalidArgs;
    };

    const file = try std.fs.cwd().openFile(in_path, .{});
    defer file.close();
    var io_buf: [4096]u8 = undefined;
    const fr = file.reader(&io_buf);
    const reader_adapter = ReaderAdapter{ .inner = @constCast(&fr.interface) };
    var reader = zarrow.IpcStreamReader(ReaderAdapter).init(allocator, reader_adapter);
    defer reader.deinit();

    switch (fixture_case) {
        .canonical => try checkCanonical(&reader),
        .dict_delta => try checkDictionaryDelta(&reader),
        .ree => try checkRee(&reader),
    }
}

fn checkRee(reader: *zarrow.IpcStreamReader(ReaderAdapter)) !void {
    const schema = try reader.readSchema();
    if (schema.fields.len != 1) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "ree")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .run_end_encoded) return error.InvalidSchema;
    const ree_dt = schema.fields[0].data_type.run_end_encoded;
    if (ree_dt.run_end_type.bit_width != 32 or !ree_dt.run_end_type.signed) return error.InvalidSchema;
    if (ree_dt.value_type.* != .int32) return error.InvalidSchema;

    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.MissingBatch;
    var batch = batch_opt.?;
    defer batch.deinit();
    if (batch.numRows() != 5) return error.InvalidBatch;

    const ree = zarrow.RunEndEncodedArray{ .data = batch.columns[0].data() };
    const expected = [_]i32{ 100, 100, 200, 200, 200 };
    for (expected, 0..) |want, i| {
        var one = try ree.value(i);
        defer one.release();
        const ints = zarrow.Int32Array{ .data = one.data() };
        if (ints.value(0) != want) return error.InvalidBatch;
    }

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}

fn checkCanonical(reader: *zarrow.IpcStreamReader(ReaderAdapter)) !void {
    const schema = try reader.readSchema();
    if (schema.fields.len != 2) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "id")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[1].name, "name")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .int32) return error.InvalidSchema;
    if (schema.fields[1].data_type.* != .string) return error.InvalidSchema;

    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.MissingBatch;
    var batch = batch_opt.?;
    defer batch.deinit();
    if (batch.numRows() != 3) return error.InvalidBatch;

    const ids = zarrow.Int32Array{ .data = batch.columns[0].data() };
    if (ids.value(0) != 1 or ids.value(1) != 2 or ids.value(2) != 3) return error.InvalidBatch;

    const names = zarrow.StringArray{ .data = batch.columns[1].data() };
    if (!std.mem.eql(u8, names.value(0), "alice")) return error.InvalidBatch;
    if (!names.isNull(1)) return error.InvalidBatch;
    if (!std.mem.eql(u8, names.value(2), "bob")) return error.InvalidBatch;

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}

fn checkDictionaryDelta(reader: *zarrow.IpcStreamReader(ReaderAdapter)) !void {
    const schema = try reader.readSchema();
    if (schema.fields.len != 1) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "color")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .dictionary) return error.InvalidSchema;

    const first_opt = try reader.nextRecordBatch();
    if (first_opt == null) return error.MissingBatch;
    var first = first_opt.?;
    defer first.deinit();
    if (first.numRows() != 2) return error.InvalidBatch;

    const first_dict = zarrow.DictionaryArray{ .data = first.columns[0].data() };
    const first_values = zarrow.StringArray{ .data = first_dict.dictionaryRef().data() };
    if (!std.mem.eql(u8, first_values.value(@intCast(first_dict.index(0))), "red")) return error.InvalidBatch;
    if (!std.mem.eql(u8, first_values.value(@intCast(first_dict.index(1))), "blue")) return error.InvalidBatch;

    const second_opt = try reader.nextRecordBatch();
    if (second_opt == null) return error.MissingBatch;
    var second = second_opt.?;
    defer second.deinit();
    if (second.numRows() != 1) return error.InvalidBatch;

    const second_dict = zarrow.DictionaryArray{ .data = second.columns[0].data() };
    const second_values = zarrow.StringArray{ .data = second_dict.dictionaryRef().data() };
    if (!std.mem.eql(u8, second_values.value(@intCast(second_dict.index(0))), "green")) return error.InvalidBatch;

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}
