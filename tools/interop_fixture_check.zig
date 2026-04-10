const std = @import("std");
const zarrow = @import("zarrow");

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
    const in_path = args.next() orelse {
        std.log.err("usage: interop-fixture-check <in.arrow> [canonical|dict-delta|ree|complex|extension] [stream|file]", .{});
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
        std.log.err("usage: interop-fixture-check <in.arrow> [canonical|dict-delta|ree|complex|extension] [stream|file]", .{});
        return error.InvalidArgs;
    }
    if (container_mode == .file and fixture_case == .dict_delta) {
        std.log.err("dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches", .{});
        return error.InvalidArgs;
    }

    const bytes = try std.fs.cwd().readFileAlloc(allocator, in_path, std.math.maxInt(usize));
    defer allocator.free(bytes);
    var fixed = std.io.fixedBufferStream(bytes);

    switch (container_mode) {
        .stream => {
            var reader = zarrow.IpcStreamReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
            defer reader.deinit();
            try checkFixture(&reader, fixture_case);
        },
        .file => {
            var reader = zarrow.IpcFileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
            defer reader.deinit();
            try checkFixture(&reader, fixture_case);
        },
    }
}

fn checkFixture(reader: anytype, fixture_case: FixtureCase) !void {
    switch (fixture_case) {
        .canonical => try checkCanonical(reader),
        .dict_delta => try checkDictionaryDelta(reader),
        .ree => try checkRee(reader),
        .complex => try checkComplex(reader),
        .extension => try checkExtension(reader),
    }
}

fn checkExtension(reader: anytype) !void {
    const schema = try reader.readSchema();
    if (schema.fields.len != 1) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "ext_i32")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .extension) return error.InvalidSchema;
    const ext = schema.fields[0].data_type.extension;
    if (!std.mem.eql(u8, ext.name, "com.example.int32_ext")) return error.InvalidSchema;
    if (ext.metadata == null or !std.mem.eql(u8, ext.metadata.?, "v1")) return error.InvalidSchema;
    if (ext.storage_type.* != .int32) return error.InvalidSchema;
    if (schema.fields[0].metadata == null) return error.InvalidSchema;
    if (schema.fields[0].metadata.?.len != 1) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].metadata.?[0].key, "owner")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].metadata.?[0].value, "interop")) return error.InvalidSchema;

    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.MissingBatch;
    var batch = batch_opt.?;
    defer batch.deinit();
    if (batch.numRows() != 3) return error.InvalidBatch;

    if (batch.columns[0].data().data_type != .extension) return error.InvalidBatch;
    const values = zarrow.PrimitiveArray(i32){ .data = batch.columns[0].data() };
    if (values.value(0) != 7) return error.InvalidBatch;
    if (!values.isNull(1)) return error.InvalidBatch;
    if (values.value(2) != 11) return error.InvalidBatch;

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}

fn checkRee(reader: anytype) !void {
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

fn checkCanonical(reader: anytype) !void {
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

fn checkDictionaryDelta(reader: anytype) !void {
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

fn checkComplex(reader: anytype) !void {
    const schema = try reader.readSchema();
    if (schema.fields.len != 6) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "list_i32")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[1].name, "struct_pair")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[2].name, "map_i32_i32")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[3].name, "u_dense")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[4].name, "dec")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[5].name, "ts")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .list) return error.InvalidSchema;
    if (schema.fields[1].data_type.* != .struct_) return error.InvalidSchema;
    if (schema.fields[2].data_type.* != .map) return error.InvalidSchema;
    if (schema.fields[3].data_type.* != .dense_union) return error.InvalidSchema;
    if (schema.fields[4].data_type.* != .decimal128) return error.InvalidSchema;
    if (schema.fields[5].data_type.* != .timestamp) return error.InvalidSchema;

    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.MissingBatch;
    var batch = batch_opt.?;
    defer batch.deinit();
    if (batch.numRows() != 3) return error.InvalidBatch;

    const list_col = zarrow.ListArray{ .data = batch.columns[0].data() };
    if (list_col.len() != 3) return error.InvalidBatch;
    if (!list_col.isNull(1)) return error.InvalidBatch;
    var l0 = try list_col.value(0);
    defer l0.release();
    var l2 = try list_col.value(2);
    defer l2.release();
    const l0_values = zarrow.Int32Array{ .data = l0.data() };
    const l2_values = zarrow.Int32Array{ .data = l2.data() };
    if (l0.data().length != 2 or l2.data().length != 1) return error.InvalidBatch;
    if (l0_values.value(0) != 1 or l0_values.value(1) != 2 or l2_values.value(0) != 3) return error.InvalidBatch;

    const struct_col = zarrow.StructArray{ .data = batch.columns[1].data() };
    if (!struct_col.isNull(1)) return error.InvalidBatch;
    const struct_ids = zarrow.Int32Array{ .data = batch.columns[1].data().children[0].data() };
    const struct_names = zarrow.StringArray{ .data = batch.columns[1].data().children[1].data() };
    if (struct_ids.value(0) != 10 or struct_ids.value(2) != 30) return error.InvalidBatch;
    if (!std.mem.eql(u8, struct_names.value(0), "aa")) return error.InvalidBatch;
    if (!std.mem.eql(u8, struct_names.value(2), "cc")) return error.InvalidBatch;

    const map_col = zarrow.MapArray{ .data = batch.columns[2].data() };
    if (!map_col.isNull(1)) return error.InvalidBatch;
    var m0 = try map_col.value(0);
    defer m0.release();
    var m2 = try map_col.value(2);
    defer m2.release();
    const m0_keys = zarrow.Int32Array{ .data = m0.data().children[0].data() };
    const m0_vals = zarrow.Int32Array{ .data = m0.data().children[1].data() };
    const m2_keys = zarrow.Int32Array{ .data = m2.data().children[0].data() };
    const m2_vals = zarrow.Int32Array{ .data = m2.data().children[1].data() };
    if (m0.data().length != 2 or m2.data().length != 1) return error.InvalidBatch;
    if (m0_keys.value(0) != 1 or m0_keys.value(1) != 2) return error.InvalidBatch;
    if (m0_vals.value(0) != 10 or m0_vals.value(1) != 20) return error.InvalidBatch;
    if (m2_keys.value(0) != 3 or m2_vals.value(0) != 30) return error.InvalidBatch;

    const union_col = zarrow.DenseUnionArray{ .data = batch.columns[3].data() };
    if (union_col.typeId(0) != 5 or union_col.childOffset(0) != 0) return error.InvalidBatch;
    if (union_col.typeId(1) != 7 or union_col.childOffset(1) != 0) return error.InvalidBatch;
    if (union_col.typeId(2) != 5 or union_col.childOffset(2) != 1) return error.InvalidBatch;
    var uv0 = try union_col.value(0);
    defer uv0.release();
    var uv1 = try union_col.value(1);
    defer uv1.release();
    var uv2 = try union_col.value(2);
    defer uv2.release();
    const union_i_first = zarrow.Int32Array{ .data = uv0.data() };
    const union_b_middle = zarrow.BooleanArray{ .data = uv1.data() };
    const union_i_last = zarrow.Int32Array{ .data = uv2.data() };
    if (union_i_first.value(0) != 100 or !union_b_middle.value(0) or union_i_last.value(0) != 200) {
        return error.InvalidBatch;
    }

    const dec = zarrow.PrimitiveArray(i128){ .data = batch.columns[4].data() };
    if (dec.value(0) != 12345 or dec.value(1) != -42 or dec.value(2) != 0) return error.InvalidBatch;

    const ts = zarrow.PrimitiveArray(i64){ .data = batch.columns[5].data() };
    if (ts.value(0) != 1700000000000 or ts.value(1) != 1700000001000 or ts.value(2) != 1700000002000) {
        return error.InvalidBatch;
    }

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}
