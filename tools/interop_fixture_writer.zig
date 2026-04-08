const std = @import("std");
const zarrow = @import("zarrow");

const WriterAdapter = struct {
    inner: *std.Io.Writer,
    pub const Error = anyerror;

    pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
        try self.inner.writeAll(bytes);
    }

    pub fn flush(self: @This()) Error!void {
        try self.inner.flush();
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
    const out_path = args.next() orelse {
        std.log.err("usage: interop-fixture-writer <out.arrow> [canonical|dict-delta|ree]", .{});
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

    const file = try std.fs.cwd().createFile(out_path, .{ .truncate = true });
    defer file.close();
    var io_buf: [4096]u8 = undefined;
    const fw = file.writer(&io_buf);
    var writer_adapter = WriterAdapter{ .inner = @constCast(&fw.interface) };
    var writer = zarrow.IpcStreamWriter(WriterAdapter).init(allocator, writer_adapter);
    defer writer.deinit();

    switch (fixture_case) {
        .canonical => try writeCanonicalFixture(allocator, &writer),
        .dict_delta => try writeDictionaryDeltaFixture(allocator, &writer),
        .ree => try writeReeFixture(allocator, &writer),
    }
    try writer_adapter.flush();
}

fn writeReeFixture(allocator: std.mem.Allocator, writer: *zarrow.IpcStreamWriter(WriterAdapter)) !void {
    // Writes one stream with:
    // - schema: ree: run_end_encoded<int32, int32>
    // - one record batch (5 rows)
    //   run_ends=[2, 5], values=[100, 200]
    //   decoded logical values=[100, 100, 200, 200, 200]
    const value_type = zarrow.DataType{ .int32 = {} };
    const ree_type = zarrow.DataType{
        .run_end_encoded = .{
            .run_end_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
        },
    };
    const fields = [_]zarrow.Field{
        .{ .name = "ree", .data_type = &ree_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var values_builder = try zarrow.Int32Builder.init(allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(100);
    try values_builder.append(200);
    var values = try values_builder.finish();
    defer values.release();

    var ree_builder = try zarrow.RunEndEncodedBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        2,
    );
    defer ree_builder.deinit();
    try ree_builder.appendRunEnd(2);
    try ree_builder.appendRunEnd(5);
    var ree = try ree_builder.finish(values);
    defer ree.release();

    var batch = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{ree});
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}

fn writeCanonicalFixture(allocator: std.mem.Allocator, writer: *zarrow.IpcStreamWriter(WriterAdapter)) !void {
    // Writes one stream with:
    // - schema: id: int32 (non-null), name: utf8 (nullable)
    // - one record batch (3 rows)
    //   id=[1, 2, 3]
    //   name=["alice", null, "bob"]
    const id_type = zarrow.DataType{ .int32 = {} };
    const name_type = zarrow.DataType{ .string = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var ids_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer ids_builder.deinit();
    try ids_builder.append(1);
    try ids_builder.append(2);
    try ids_builder.append(3);
    var ids = try ids_builder.finish();
    defer ids.release();

    var names_builder = try zarrow.StringBuilder.init(allocator, 3, 12);
    defer names_builder.deinit();
    try names_builder.append("alice");
    try names_builder.appendNull();
    try names_builder.append("bob");
    var names = try names_builder.finish();
    defer names.release();

    var batch = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{ ids, names });
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}

fn writeDictionaryDeltaFixture(allocator: std.mem.Allocator, writer: *zarrow.IpcStreamWriter(WriterAdapter)) !void {
    // Writes one stream with dictionary-encoded column "color":
    // - schema: color: dictionary<int32, utf8>
    // - two record batches to exercise dictionary delta behavior.
    const value_type = zarrow.DataType{ .string = {} };
    const dict_type = zarrow.DataType{
        .dictionary = .{
            .id = null,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
            .ordered = false,
        },
    };
    const fields = [_]zarrow.Field{
        .{ .name = "color", .data_type = &dict_type, .nullable = false },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var dict_values_builder_1 = try zarrow.StringBuilder.init(allocator, 2, 7);
    defer dict_values_builder_1.deinit();
    try dict_values_builder_1.append("red");
    try dict_values_builder_1.append("blue");
    var dict_values_1 = try dict_values_builder_1.finish();
    defer dict_values_1.release();

    var dict_builder_1 = try zarrow.DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        2,
    );
    defer dict_builder_1.deinit();
    // Batch 1:
    // - dictionary values=["red", "blue"]
    // - indices=[0, 1]
    // - decoded values=["red", "blue"]
    try dict_builder_1.appendIndex(0);
    try dict_builder_1.appendIndex(1);
    var dict_col_1 = try dict_builder_1.finish(dict_values_1);
    defer dict_col_1.release();
    var batch_1 = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{dict_col_1});
    defer batch_1.deinit();

    var dict_values_builder_2 = try zarrow.StringBuilder.init(allocator, 1, 5);
    defer dict_values_builder_2.deinit();
    try dict_values_builder_2.append("green");
    var dict_values_2 = try dict_values_builder_2.finish();
    defer dict_values_2.release();

    var dict_builder_2 = try zarrow.DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        1,
    );
    defer dict_builder_2.deinit();
    // Batch 2:
    // - dictionary values=["green"]
    // - indices=[0]
    // - decoded values=["green"]
    try dict_builder_2.appendIndex(0);
    var dict_col_2 = try dict_builder_2.finish(dict_values_2);
    defer dict_col_2.release();
    var batch_2 = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{dict_col_2});
    defer batch_2.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch_1);
    try writer.writeRecordBatch(batch_2);
    try writer.writeEnd();
}
