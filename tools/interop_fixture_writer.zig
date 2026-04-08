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
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // exe
    const out_path = args.next() orelse {
        std.log.err("usage: interop-fixture-writer <out.arrow> [canonical|dict-delta]", .{});
        return error.InvalidArgs;
    };
    const fixture_case: FixtureCase = blk: {
        const mode = args.next() orelse break :blk .canonical;
        if (std.mem.eql(u8, mode, "canonical")) break :blk .canonical;
        if (std.mem.eql(u8, mode, "dict-delta")) break :blk .dict_delta;
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
    }
    try writer_adapter.flush();
}

fn writeCanonicalFixture(allocator: std.mem.Allocator, writer: *zarrow.IpcStreamWriter(WriterAdapter)) !void {
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
    try dict_builder_1.appendIndex(0);
    try dict_builder_1.appendIndex(1);
    var dict_col_1 = try dict_builder_1.finish(dict_values_1);
    defer dict_col_1.release();
    var batch_1 = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{dict_col_1});
    defer batch_1.deinit();

    var dict_values_builder_2 = try zarrow.StringBuilder.init(allocator, 3, 12);
    defer dict_values_builder_2.deinit();
    try dict_values_builder_2.append("red");
    try dict_values_builder_2.append("blue");
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
    try dict_builder_2.appendIndex(2);
    var dict_col_2 = try dict_builder_2.finish(dict_values_2);
    defer dict_col_2.release();
    var batch_2 = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{dict_col_2});
    defer batch_2.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch_1);
    try writer.writeRecordBatch(batch_2);
    try writer.writeEnd();
}
