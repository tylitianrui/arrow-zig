const std = @import("std");
const zarrow = @import("zarrow");

fn writeI64Le(dst: []u8, value: i64) void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(i64, &tmp, value, .little);
    @memcpy(dst[0..8], tmp[0..]);
}

fn writeI32Le(dst: []u8, value: i32) void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(i32, &tmp, value, .little);
    @memcpy(dst[0..4], tmp[0..]);
}

fn printI32Slice(prefix: []const u8, bytes: []const u8) void {
    const count = bytes.len / 4;
    std.debug.print("{s}", .{prefix});
    for (0..count) |i| {
        const start = i * 4;
        var tmp: [4]u8 = undefined;
        @memcpy(tmp[0..], bytes[start .. start + 4]);
        const v = std.mem.readInt(i32, &tmp, .little);
        std.debug.print("{d}{s}", .{ v, if (i + 1 == count) "" else ", " });
    }
    std.debug.print("\n", .{});
}

fn buildTensorBody() [24]u8 {
    var body: [24]u8 = undefined;
    for (0..6) |i| {
        writeI32Le(body[i * 4 ..][0..4], @intCast(@as(i32, @intCast(i)) + 1));
    }
    return body;
}

fn buildSparseBody() [40]u8 {
    // COO indices (i64, row-major pairs): (0,1), (1,2)
    // Data values (i32): 10, 20
    var body: [40]u8 = undefined;
    writeI64Le(body[0..8], 0);
    writeI64Le(body[8..16], 1);
    writeI64Le(body[16..24], 1);
    writeI64Le(body[24..32], 2);
    writeI32Le(body[32..36], 10);
    writeI32Le(body[36..40], 20);
    return body;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const id_type = zarrow.DataType{ .int32 = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = zarrow.Schema{
        .fields = fields[0..],
        .endianness = .big, // demo normalize_to_little mode
    };

    const dims = [_]zarrow.IpcTensorDim{
        .{ .size = 2, .name = "rows" },
        .{ .size = 3, .name = "cols" },
    };
    const tensor_strides = [_]i64{ 12, 4 };

    const tensor_body = buildTensorBody();
    const tensor_meta = zarrow.IpcTensorLikeMetadata{
        .tensor = .{
            .value_type = zarrow.DataType{ .int32 = {} },
            .shape = dims[0..],
            .strides = tensor_strides[0..],
            .data = .{ .offset = 0, .length = tensor_body.len },
        },
    };

    const sparse_body = buildSparseBody();
    const sparse_meta = zarrow.IpcTensorLikeMetadata{
        .sparse_tensor = .{
            .value_type = zarrow.DataType{ .int32 = {} },
            .shape = dims[0..],
            .non_zero_length = 2,
            .sparse_index = .{
                .coo = .{
                    .indices_type = .{ .signed = true, .bit_width = 64 },
                    .indices_strides = null,
                    .indices = .{ .offset = 0, .length = 32 },
                    .is_canonical = true,
                },
            },
            .data = .{ .offset = 32, .length = 8 },
        },
    };

    // Stream: write schema + tensor + sparse tensor, then read them back.
    var out = std.ArrayList(u8).init(allocator);
    defer out.deinit();

    var stream_writer = zarrow.IpcStreamWriter(@TypeOf(out.writer())).initWithOptions(
        allocator,
        out.writer(),
        .{ .endianness_mode = .normalize_to_little },
    );
    defer stream_writer.deinit();

    try stream_writer.writeSchema(schema);
    try stream_writer.writeTensorLikeMessage(tensor_meta, tensor_body[0..]);
    try stream_writer.writeTensorLikeMessage(sparse_meta, sparse_body[0..]);
    try stream_writer.writeEnd();

    std.debug.print("examples/ipc_tensor_sparse_roundtrip.zig | stream wrote {d} bytes\n", .{out.items.len});

    var stream = std.io.fixedBufferStream(out.items);
    var stream_reader = zarrow.IpcStreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer stream_reader.deinit();

    const out_schema = try stream_reader.readSchema();
    std.debug.print("stream schema endianness={s}\n", .{@tagName(out_schema.endianness)});

    var tensor_like_count: usize = 0;
    while (try stream_reader.nextTensorLikeMessage()) |*msg_ptr| {
        var msg = msg_ptr.*;
        defer msg.deinit();
        tensor_like_count += 1;

        switch (msg.metadata) {
            .tensor => |meta| {
                std.debug.print("stream tensor: dims={d} data_len={d}\n", .{ meta.shape.len, meta.data.length });
                const tensor_bytes = meta.data.bytes(msg.body.data);
                printI32Slice("  values: ", tensor_bytes);
            },
            .sparse_tensor => |meta| {
                std.debug.print("stream sparse tensor: nnz={d} data_len={d}\n", .{ meta.non_zero_length, meta.data.length });
                const sparse_values = meta.data.bytes(msg.body.data);
                printI32Slice("  sparse values: ", sparse_values);
            },
        }
    }
    std.debug.print("stream tensor-like message count={d}\n", .{tensor_like_count});

    // File: write same schema/tensor messages, verify file reader can load schema.
    var file_out = std.ArrayList(u8).init(allocator);
    defer file_out.deinit();

    var file_writer = try zarrow.IpcFileWriter(@TypeOf(file_out.writer())).initWithOptions(
        allocator,
        file_out.writer(),
        .{ .endianness_mode = .normalize_to_little },
    );
    defer file_writer.deinit();

    try file_writer.writeSchema(schema);
    try file_writer.writeTensorLikeMessage(tensor_meta, tensor_body[0..]);
    try file_writer.writeTensorLikeMessage(sparse_meta, sparse_body[0..]);
    try file_writer.writeEnd();
    std.debug.print("file wrote {d} bytes\n", .{file_out.items.len});

    var file_stream = std.io.fixedBufferStream(file_out.items);
    var file_reader = zarrow.IpcFileReader(@TypeOf(file_stream.reader())).init(allocator, file_stream.reader());
    defer file_reader.deinit();

    const file_schema = try file_reader.readSchema();
    const file_batch_count = try file_reader.recordBatchCount();
    const tensor_count = try file_reader.tensorCount();
    const sparse_count = try file_reader.sparseTensorCount();
    std.debug.print("file schema endianness={s}, record_batches={d}, tensors={d}, sparse_tensors={d}\n", .{
        @tagName(file_schema.endianness),
        file_batch_count,
        tensor_count,
        sparse_count,
    });

    // Read tensor by index.
    if (tensor_count > 0) {
        var t_msg = try file_reader.readTensorAt(0);
        defer t_msg.deinit();
        const t_meta = t_msg.metadata.tensor;
        std.debug.print("file tensor[0]: dims={d} data_len={d}\n", .{ t_meta.shape.len, t_meta.data.length });
        const t_bytes = t_meta.data.bytes(t_msg.body.data);
        printI32Slice("  values: ", t_bytes);
    }

    // Read sparse tensor by index.
    if (sparse_count > 0) {
        var s_msg = try file_reader.readSparseTensorAt(0);
        defer s_msg.deinit();
        const s_meta = s_msg.metadata.sparse_tensor;
        std.debug.print("file sparse_tensor[0]: nnz={d} data_len={d}\n", .{ s_meta.non_zero_length, s_meta.data.length });
        const s_bytes = s_meta.data.bytes(s_msg.body.data);
        printI32Slice("  sparse values: ", s_bytes);
    }
}
