const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Schema: id (int32, non-null), name (string, nullable)
    const id_type = zarrow.DataType{ .int32 = {} };
    const name_type = zarrow.DataType{ .string = {} };
    const schema_fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = schema_fields[0..] };

    // Build columns for two batches.
    var id_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(1);
    try id_builder.append(2);
    try id_builder.append(3);
    var id_ref = try id_builder.finish();
    defer id_ref.release();

    var name_builder = try zarrow.StringBuilder.init(allocator, 3, 12);
    defer name_builder.deinit();
    try name_builder.append("alice");
    try name_builder.appendNull();
    try name_builder.append("bob");
    var name_ref = try name_builder.finish();
    defer name_ref.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ id_ref, name_ref });
    defer batch.deinit();

    // ── Write IPC stream to an in-memory buffer ───────────────────────────────
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    var writer = zarrow.IpcStreamWriter(@TypeOf(buf.writer())).init(allocator, buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    std.debug.print("examples/ipc_stream.zig | wrote {d} bytes\n", .{buf.items.len});

    // ── Read back from the buffer ─────────────────────────────────────────────
    var stream = std.io.fixedBufferStream(buf.items);
    var reader = zarrow.IpcStreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const owned_schema = try reader.readSchema();
    std.debug.print("schema fields: {d}\n", .{owned_schema.fields.len});
    for (owned_schema.fields) |f| {
        std.debug.print("  {s} nullable={}\n", .{ f.name, f.nullable });
    }

    while (try reader.nextRecordBatch()) |*rb| {
        var out_batch = rb.*;
        defer out_batch.deinit();
        std.debug.print("batch rows={d}\n", .{out_batch.numRows()});
        const ids = zarrow.Int32Array{ .data = out_batch.columns[0].data() };
        const names = zarrow.StringArray{ .data = out_batch.columns[1].data() };
        for (0..out_batch.numRows()) |i| {
            const id = try ids.value(i);
            if (names.isNull(i)) {
                std.debug.print("  id={d} name=null\n", .{id});
            } else {
                std.debug.print("  id={d} name={s}\n", .{ id, names.value(i) });
            }
        }
    }
}
