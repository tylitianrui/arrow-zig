const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const int_type = zarrow.DataType{ .int32 = {} };
    const bool_type = zarrow.DataType{ .bool = {} };
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
    const schema_fields = [_]zarrow.Field{
        .{ .name = "u", .data_type = &dense_union_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = schema_fields[0..] };

    var int_builder = try zarrow.Int32Builder.init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(10);
    try int_builder.append(20);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try zarrow.BooleanBuilder.init(allocator, 1);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var union_builder = try zarrow.DenseUnionBuilder.init(allocator, union_type, 3);
    defer union_builder.deinit();
    try union_builder.append(5, 0);
    try union_builder.append(7, 0);
    try union_builder.append(5, 1);
    var union_ref = try union_builder.finish(&[_]zarrow.ArrayRef{ int_ref, bool_ref });
    defer union_ref.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{union_ref});
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = zarrow.IpcStreamWriter(@TypeOf(out.writer())).initWithOptions(
        allocator,
        out.writer(),
        .{
            .metadata_version = .v4,
        },
    );
    defer writer.deinit();

    std.debug.print("examples/ipc_metadata_v4_union.zig | writer metadata version={s}\n", .{@tagName(writer.metadataVersion())});

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    std.debug.print("wrote {d} bytes\n", .{out.items.len});

    var stream = std.io.fixedBufferStream(out.items);
    var reader = zarrow.IpcStreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.InvalidMessage;
    var out_batch = batch_opt.?;
    defer out_batch.deinit();

    const out_union = zarrow.DenseUnionArray{ .data = out_batch.columns[0].data() };
    std.debug.print("roundtrip rows={d}\n", .{out_union.len()});
    for (0..out_union.len()) |i| {
        const type_id = out_union.typeId(i);
        if (type_id == 5) {
            var value = try out_union.value(i);
            defer value.release();
            const arr = zarrow.Int32Array{ .data = value.data() };
            std.debug.print("  row[{d}] type_id={d} int32={d}\n", .{ i, type_id, try arr.value(0) });
        } else {
            var value = try out_union.value(i);
            defer value.release();
            const arr = zarrow.BooleanArray{ .data = value.data() };
            std.debug.print("  row[{d}] type_id={d} bool={}\n", .{ i, type_id, arr.value(0) });
        }
    }
}
