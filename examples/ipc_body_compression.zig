const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const value_type = zarrow.DataType{ .int32 = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "value", .data_type = &value_type, .nullable = false },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var builder = try zarrow.Int32Builder.init(allocator, 16);
    defer builder.deinit();
    for (0..16) |_| try builder.append(7);
    var col = try builder.finish();
    defer col.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{col});
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = zarrow.IpcStreamWriter(@TypeOf(out.writer())).initWithBodyCompression(
        allocator,
        out.writer(),
        zarrow.IpcBodyCompressionCodec.zstd,
    );
    defer writer.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    std.debug.print("examples/ipc_body_compression.zig | wrote {d} bytes\n", .{out.items.len});

    var stream = std.io.fixedBufferStream(out.items);
    var reader = zarrow.IpcStreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const read_schema = try reader.readSchema();
    std.debug.print("schema fields: {d}\n", .{read_schema.fields.len});

    while (try reader.nextRecordBatch()) |*rb| {
        var read_batch = rb.*;
        defer read_batch.deinit();
        const values = zarrow.Int32Array{ .data = read_batch.columns[0].data() };
        std.debug.print("rows={d}\n", .{read_batch.numRows()});
        for (0..read_batch.numRows()) |i| {
            std.debug.print("  value={d}\n", .{try values.value(i)});
        }
    }
}
