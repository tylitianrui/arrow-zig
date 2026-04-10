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
    complex,
    extension,
};

const ContainerMode = enum {
    stream,
    file,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // exe
    const out_path = args.next() orelse {
        std.log.err("usage: interop-fixture-writer <out.arrow> [canonical|dict-delta|ree|complex|extension] [stream|file]", .{});
        return error.InvalidArgs;
    };
    const fixture_case: FixtureCase = blk: {
        const mode = args.next() orelse break :blk .canonical;
        if (std.mem.eql(u8, mode, "canonical")) break :blk .canonical;
        if (std.mem.eql(u8, mode, "dict-delta")) break :blk .dict_delta;
        if (std.mem.eql(u8, mode, "ree")) break :blk .ree;
        if (std.mem.eql(u8, mode, "complex")) break :blk .complex;
        if (std.mem.eql(u8, mode, "extension")) break :blk .extension;
        std.log.err("unknown fixture mode: {s}", .{mode});
        return error.InvalidArgs;
    };
    const container_mode: ContainerMode = blk: {
        const mode = args.next() orelse break :blk .stream;
        if (std.mem.eql(u8, mode, "stream")) break :blk .stream;
        if (std.mem.eql(u8, mode, "file")) break :blk .file;
        std.log.err("unknown container mode: {s}", .{mode});
        return error.InvalidArgs;
    };
    if (args.next() != null) {
        std.log.err("usage: interop-fixture-writer <out.arrow> [canonical|dict-delta|ree|complex|extension] [stream|file]", .{});
        return error.InvalidArgs;
    }
    if (container_mode == .file and fixture_case == .dict_delta) {
        std.log.err("dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches", .{});
        return error.InvalidArgs;
    }

    const file = try std.fs.cwd().createFile(out_path, .{ .truncate = true });
    defer file.close();
    var io_buf: [4096]u8 = undefined;
    const fw = file.writer(&io_buf);
    var writer_adapter = WriterAdapter{ .inner = @constCast(&fw.interface) };
    switch (container_mode) {
        .stream => {
            var writer = zarrow.IpcStreamWriter(WriterAdapter).init(allocator, writer_adapter);
            defer writer.deinit();
            try writeFixture(allocator, &writer, fixture_case);
        },
        .file => {
            var writer = try zarrow.IpcFileWriter(WriterAdapter).init(allocator, writer_adapter);
            defer writer.deinit();
            try writeFixture(allocator, &writer, fixture_case);
        },
    }
    try writer_adapter.flush();
}

fn writeFixture(allocator: std.mem.Allocator, writer: anytype, fixture_case: FixtureCase) !void {
    switch (fixture_case) {
        .canonical => try writeCanonicalFixture(allocator, writer),
        .dict_delta => try writeDictionaryDeltaFixture(allocator, writer),
        .ree => try writeReeFixture(allocator, writer),
        .complex => try writeComplexFixture(allocator, writer),
        .extension => try writeExtensionFixture(allocator, writer),
    }
}

fn writeExtensionFixture(allocator: std.mem.Allocator, writer: anytype) !void {
    const storage_type = zarrow.DataType{ .int32 = {} };
    const ext_type = zarrow.DataType{
        .extension = .{
            .name = "com.example.int32_ext",
            .storage_type = &storage_type,
            .metadata = "v1",
        },
    };
    const field_metadata = [_]zarrow.KeyValue{
        .{ .key = "owner", .value = "interop" },
    };
    const fields = [_]zarrow.Field{
        .{ .name = "ext_i32", .data_type = &ext_type, .nullable = true, .metadata = field_metadata[0..] },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var values_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(7);
    try values_builder.appendNull();
    try values_builder.append(11);
    var values = try values_builder.finish();
    defer values.release();

    var ext_builder = try zarrow.ExtensionBuilder.init(allocator, ext_type.extension);
    defer ext_builder.deinit();
    var ext_values = try ext_builder.finish(values);
    defer ext_values.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ext_values});
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}

fn writeReeFixture(allocator: std.mem.Allocator, writer: anytype) !void {
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

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ree});
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}

fn writeCanonicalFixture(allocator: std.mem.Allocator, writer: anytype) !void {
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

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ ids, names });
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}

fn writeDictionaryDeltaFixture(allocator: std.mem.Allocator, writer: anytype) !void {
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
    var batch_1 = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{dict_col_1});
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
    var batch_2 = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{dict_col_2});
    defer batch_2.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch_1);
    try writer.writeRecordBatch(batch_2);
    try writer.writeEnd();
}

fn writeComplexFixture(allocator: std.mem.Allocator, writer: anytype) !void {
    // Writes one fixture with:
    // - list<int32>, struct<id:int32,name:utf8>, map<int32,int32>, dense_union<int32,bool>,
    //   decimal128(10,2), timestamp(ms, UTC)
    // - one record batch (3 rows)
    const int_type = zarrow.DataType{ .int32 = {} };
    const bool_type = zarrow.DataType{ .bool = {} };
    const string_type = zarrow.DataType{ .string = {} };

    const list_value_field = zarrow.Field{ .name = "item", .data_type = &int_type, .nullable = true };
    const list_type = zarrow.DataType{ .list = .{ .value_field = list_value_field } };

    const struct_fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "name", .data_type = &string_type, .nullable = true },
    };
    const struct_type = zarrow.DataType{ .struct_ = .{ .fields = struct_fields[0..] } };

    const map_key_field = zarrow.Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const map_value_field = zarrow.Field{ .name = "value", .data_type = &int_type, .nullable = true };
    const map_type = zarrow.DataType{
        .map = .{
            .key_field = map_key_field,
            .item_field = map_value_field,
            .keys_sorted = false,
        },
    };

    const union_fields = [_]zarrow.Field{
        .{ .name = "i", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const union_type = zarrow.UnionType{
        .type_ids = union_type_ids[0..],
        .fields = union_fields[0..],
        .mode = .dense,
    };
    const dense_union_type = zarrow.DataType{ .dense_union = union_type };

    const decimal_type = zarrow.DataType{ .decimal128 = .{ .precision = 10, .scale = 2 } };
    const ts_type = zarrow.DataType{ .timestamp = .{ .unit = .millisecond, .timezone = "UTC" } };

    const schema_fields = [_]zarrow.Field{
        .{ .name = "list_i32", .data_type = &list_type, .nullable = true },
        .{ .name = "struct_pair", .data_type = &struct_type, .nullable = true },
        .{ .name = "map_i32_i32", .data_type = &map_type, .nullable = true },
        .{ .name = "u_dense", .data_type = &dense_union_type, .nullable = true },
        .{ .name = "dec", .data_type = &decimal_type, .nullable = false },
        .{ .name = "ts", .data_type = &ts_type, .nullable = false },
    };
    const schema = zarrow.Schema{ .fields = schema_fields[0..] };

    var list_values_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer list_values_builder.deinit();
    try list_values_builder.append(1);
    try list_values_builder.append(2);
    try list_values_builder.append(3);
    var list_values = try list_values_builder.finish();
    defer list_values.release();

    var list_builder = try zarrow.ListBuilder.init(allocator, 3, list_value_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2);
    try list_builder.appendNull();
    try list_builder.appendLen(1);
    var list_col = try list_builder.finish(list_values);
    defer list_col.release();

    var struct_ids_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer struct_ids_builder.deinit();
    try struct_ids_builder.append(10);
    try struct_ids_builder.append(20);
    try struct_ids_builder.append(30);
    var struct_ids = try struct_ids_builder.finish();
    defer struct_ids.release();

    var struct_names_builder = try zarrow.StringBuilder.init(allocator, 3, 6);
    defer struct_names_builder.deinit();
    try struct_names_builder.append("aa");
    try struct_names_builder.appendNull();
    try struct_names_builder.append("cc");
    var struct_names = try struct_names_builder.finish();
    defer struct_names.release();

    var struct_builder = zarrow.StructBuilder.init(allocator, struct_fields[0..]);
    defer struct_builder.deinit();
    try struct_builder.appendValid();
    try struct_builder.appendNull();
    try struct_builder.appendValid();
    var struct_col = try struct_builder.finish(&[_]zarrow.ArrayRef{ struct_ids, struct_names });
    defer struct_col.release();

    var map_keys_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer map_keys_builder.deinit();
    try map_keys_builder.append(1);
    try map_keys_builder.append(2);
    try map_keys_builder.append(3);
    var map_keys = try map_keys_builder.finish();
    defer map_keys.release();

    var map_values_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer map_values_builder.deinit();
    try map_values_builder.append(10);
    try map_values_builder.append(20);
    try map_values_builder.append(30);
    var map_values = try map_values_builder.finish();
    defer map_values.release();

    var entries_builder = zarrow.StructBuilder.init(allocator, &[_]zarrow.Field{ map_key_field, map_value_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var map_entries = try entries_builder.finish(&[_]zarrow.ArrayRef{ map_keys, map_values });
    defer map_entries.release();

    var map_builder = try zarrow.MapBuilder.init(allocator, 3, map_key_field, map_value_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendNull();
    try map_builder.appendLen(1);
    var map_col = try map_builder.finish(map_entries);
    defer map_col.release();

    var union_i_builder = try zarrow.Int32Builder.init(allocator, 2);
    defer union_i_builder.deinit();
    try union_i_builder.append(100);
    try union_i_builder.append(200);
    var union_i = try union_i_builder.finish();
    defer union_i.release();

    var union_b_builder = try zarrow.BooleanBuilder.init(allocator, 1);
    defer union_b_builder.deinit();
    try union_b_builder.append(true);
    var union_b = try union_b_builder.finish();
    defer union_b.release();

    var dense_union_builder = try zarrow.DenseUnionBuilder.init(allocator, union_type, 3);
    defer dense_union_builder.deinit();
    try dense_union_builder.append(5, 0);
    try dense_union_builder.append(7, 0);
    try dense_union_builder.append(5, 1);
    var union_col = try dense_union_builder.finish(&[_]zarrow.ArrayRef{ union_i, union_b });
    defer union_col.release();

    var decimal_builder = try zarrow.PrimitiveBuilder(i128, zarrow.DataType{ .decimal128 = .{ .precision = 10, .scale = 2 } }).init(allocator, 3);
    defer decimal_builder.deinit();
    try decimal_builder.append(12345);
    try decimal_builder.append(-42);
    try decimal_builder.append(0);
    var decimal_col = try decimal_builder.finish();
    defer decimal_col.release();

    var ts_builder = try zarrow.PrimitiveBuilder(i64, zarrow.DataType{ .timestamp = .{ .unit = .millisecond, .timezone = "UTC" } }).init(allocator, 3);
    defer ts_builder.deinit();
    try ts_builder.append(1700000000000);
    try ts_builder.append(1700000001000);
    try ts_builder.append(1700000002000);
    var ts_col = try ts_builder.finish();
    defer ts_col.release();

    var batch = try zarrow.RecordBatch.initBorrowed(
        allocator,
        schema,
        &[_]zarrow.ArrayRef{ list_col, struct_col, map_col, union_col, decimal_col, ts_col },
    );
    defer batch.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
}
