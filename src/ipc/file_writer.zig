const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const stream_writer = @import("stream_writer.zig");
const fbs_lite_reader = @import("fbs_lite/reader.zig");
const fbs_lite_builder = @import("fbs_lite/builder.zig");
const fbs_lite_verify = @import("fbs_lite/verify.zig");
const format = @import("format.zig");
const arrow_fbs = @import("ipc_schema");

pub const FileMagic = "ARROW1";

pub const Schema = schema_mod.Schema;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const RecordBatch = record_batch.RecordBatch;
pub const TensorLikeMetadata = stream_writer.TensorLikeMetadata;
pub const WriterOptions = stream_writer.WriterOptions;

pub const FileError = stream_writer.StreamError || fbs_lite_builder.PackError || error{
    OutOfMemory,
    AlreadyFinished,
    InvalidFile,
    InvalidMessage,
    MissingSchema,
    UnsupportedMessage,
};

const fbs = struct {
    const Message = arrow_fbs.org_apache_arrow_flatbuf_Message.Message;
    const MessageT = arrow_fbs.org_apache_arrow_flatbuf_Message.MessageT;
    const Footer = arrow_fbs.org_apache_arrow_flatbuf_Footer.Footer;
    const FooterT = arrow_fbs.org_apache_arrow_flatbuf_Footer.FooterT;
    const BlockT = arrow_fbs.org_apache_arrow_flatbuf_Block.BlockT;
    const KeyValueT = arrow_fbs.org_apache_arrow_flatbuf_KeyValue.KeyValueT;
    const MetadataVersion = arrow_fbs.org_apache_arrow_flatbuf_MetadataVersion.MetadataVersion;
    const Schema = arrow_fbs.org_apache_arrow_flatbuf_Schema.Schema;
    const SchemaT = arrow_fbs.org_apache_arrow_flatbuf_Schema.SchemaT;
};

const CollectWriter = struct {
    allocator: std.mem.Allocator,
    bytes: *std.ArrayListUnmanaged(u8),

    pub const Error = error{OutOfMemory};

    pub fn writeAll(self: @This(), data: []const u8) Error!void {
        try self.bytes.appendSlice(self.allocator, data);
    }
};

pub fn FileWriter(comptime WriterType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        writer: WriterType,
        stream_bytes: *std.ArrayListUnmanaged(u8),
        stream: stream_writer.StreamWriter(CollectWriter),
        schema_msg: ?*fbs.MessageT = null,
        schema_metadata_bytes: ?[]u8 = null,
        dictionary_blocks: std.ArrayListUnmanaged(fbs.BlockT),
        record_batch_blocks: std.ArrayListUnmanaged(fbs.BlockT),
        tensor_blocks: std.ArrayListUnmanaged(fbs.BlockT),
        sparse_tensor_blocks: std.ArrayListUnmanaged(fbs.BlockT),
        stream_offset: usize = 0,
        header_written: bool = false,
        saw_stream_end: bool = false,
        finished: bool = false,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, writer: WriterType) FileError!Self {
            return initWithOptions(allocator, writer, .{});
        }

        pub fn initWithOptions(allocator: std.mem.Allocator, writer: WriterType, options: WriterOptions) FileError!Self {
            const stream_bytes = try allocator.create(std.ArrayListUnmanaged(u8));
            stream_bytes.* = std.ArrayListUnmanaged(u8){};

            const collect_writer = CollectWriter{
                .allocator = allocator,
                .bytes = stream_bytes,
            };
            return .{
                .allocator = allocator,
                .writer = writer,
                .stream_bytes = stream_bytes,
                .stream = stream_writer.StreamWriter(CollectWriter).initWithOptions(allocator, collect_writer, options),
                .dictionary_blocks = try std.ArrayListUnmanaged(fbs.BlockT).initCapacity(allocator, 0),
                .record_batch_blocks = try std.ArrayListUnmanaged(fbs.BlockT).initCapacity(allocator, 0),
                .tensor_blocks = try std.ArrayListUnmanaged(fbs.BlockT).initCapacity(allocator, 0),
                .sparse_tensor_blocks = try std.ArrayListUnmanaged(fbs.BlockT).initCapacity(allocator, 0),
            };
        }

        pub fn deinit(self: *Self) void {
            self.stream.deinit();
            if (self.schema_msg) |msg| {
                msg.deinit(self.allocator);
                self.allocator.destroy(msg);
            }
            if (self.schema_metadata_bytes) |bytes| self.allocator.free(bytes);
            self.dictionary_blocks.deinit(self.allocator);
            self.record_batch_blocks.deinit(self.allocator);
            self.tensor_blocks.deinit(self.allocator);
            self.sparse_tensor_blocks.deinit(self.allocator);
            self.stream_bytes.deinit(self.allocator);
            self.allocator.destroy(self.stream_bytes);
        }

        pub fn writeSchema(self: *Self, schema: Schema) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeSchema(schema);
            try self.flushPendingMessages(false);
        }

        pub fn writeRecordBatch(self: *Self, batch: RecordBatch) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeRecordBatch(batch);
            try self.flushPendingMessages(false);
        }

        pub fn writeTensorLikeMessage(self: *Self, metadata: TensorLikeMetadata, body: []const u8) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeTensorLikeMessage(metadata, body);
            try self.flushPendingMessages(false);
        }

        pub fn writeEnd(self: *Self) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeEnd();
            try self.flushPendingMessages(true);
            if (!self.saw_stream_end) return error.InvalidMessage;
            if (self.schema_msg == null) return error.MissingSchema;

            const footer_bytes = try buildFooterBytes(
                self.allocator,
                self.schema_msg.?.header.Schema.?,
                self.dictionary_blocks,
                self.record_batch_blocks,
                self.tensor_blocks,
                self.sparse_tensor_blocks,
                self.stream.flatbufMetadataVersion(),
            );
            defer self.allocator.free(footer_bytes);

            // Align footer to 8-byte boundary relative to file start (header is 8 bytes)
            const footer_pad = format.padLen(self.stream_offset);
            try format.writePadding(self.writer, footer_pad);
            try self.writer.writeAll(footer_bytes);
            const footer_len = std.math.cast(u32, footer_bytes.len) orelse return FileError.InvalidMessage;
            try writeU32Le(self.writer, footer_len);
            try self.writer.writeAll(FileMagic);

            self.finished = true;
        }

        fn ensureHeaderWritten(self: *Self) (@TypeOf(self.writer).Error || FileError)!void {
            if (self.header_written) return;
            try self.writer.writeAll(FileMagic);
            try self.writer.writeAll("\x00\x00");
            self.header_written = true;
        }

        fn consumeStreamPrefix(self: *Self, n: usize) void {
            if (n >= self.stream_bytes.items.len) {
                self.stream_bytes.clearRetainingCapacity();
                return;
            }
            const remain = self.stream_bytes.items.len - n;
            std.mem.copyForwards(u8, self.stream_bytes.items[0..remain], self.stream_bytes.items[n..]);
            self.stream_bytes.items.len = remain;
        }

        fn flushPendingMessages(self: *Self, final: bool) (FileError || @TypeOf(self.writer).Error)!void {
            while (self.stream_bytes.items.len > 0) {
                if (self.stream_bytes.items.len < 4) {
                    if (final) return error.InvalidMessage;
                    return error.InvalidMessage;
                }

                const first = readU32Le(self.stream_bytes.items[0..4]);
                var prefix_len: usize = 4;
                var metadata_len_u32 = first;
                if (first == format.ContinuationMarker) {
                    if (self.stream_bytes.items.len < 8) {
                        if (final) return error.InvalidMessage;
                        return error.InvalidMessage;
                    }
                    metadata_len_u32 = readU32Le(self.stream_bytes.items[4..8]);
                    prefix_len = 8;
                    if (metadata_len_u32 == 0) {
                        if (!final) return error.InvalidMessage;
                        self.saw_stream_end = true;
                        self.consumeStreamPrefix(8);
                        continue;
                    }
                } else if (first == 0) {
                    if (!final) return error.InvalidMessage;
                    self.saw_stream_end = true;
                    self.consumeStreamPrefix(4);
                    continue;
                }

                const metadata_len = std.math.cast(usize, metadata_len_u32) orelse return error.InvalidMessage;
                const needed_metadata = std.math.add(usize, prefix_len, metadata_len) catch return error.InvalidMessage;
                if (self.stream_bytes.items.len < needed_metadata) {
                    if (final) return error.InvalidMessage;
                    return error.InvalidMessage;
                }

                const metadata = self.stream_bytes.items[prefix_len..needed_metadata];
                const envelope = fbs_lite_verify.parseArrowMessageEnvelope(metadata) catch return error.InvalidMessage;
                if (envelope.body_length < 0) return error.InvalidMessage;
                const body_len = std.math.cast(usize, envelope.body_length) orelse return error.InvalidMessage;
                var msg_t = try fbs_lite_builder.unpackMessage(self.allocator, metadata);
                errdefer msg_t.deinit(self.allocator);

                if (msg_t.bodyLength != envelope.body_length) return error.InvalidMessage;
                const body_pad = format.padLen(body_len);
                const body_total = std.math.add(usize, body_len, body_pad) catch return error.InvalidMessage;
                const message_total = std.math.add(usize, needed_metadata, body_total) catch return error.InvalidMessage;
                if (self.stream_bytes.items.len < message_total) {
                    if (final) return error.InvalidMessage;
                    return error.InvalidMessage;
                }

                switch (msg_t.header) {
                    .Schema => {
                        if (self.schema_msg != null) return error.InvalidMessage;
                        const metadata_copy = try self.allocator.dupe(u8, metadata);
                        errdefer self.allocator.free(metadata_copy);
                        var schema_msg_t = try fbs_lite_builder.unpackMessage(self.allocator, metadata_copy);
                        errdefer schema_msg_t.deinit(self.allocator);
                        if (schema_msg_t.header != .Schema) return error.InvalidMessage;
                        const owned_msg = try self.allocator.create(fbs.MessageT);
                        owned_msg.* = schema_msg_t;
                        self.schema_msg = owned_msg;
                        self.schema_metadata_bytes = metadata_copy;
                        msg_t.deinit(self.allocator);
                    },
                    .DictionaryBatch, .RecordBatch => {
                        const file_offset = std.math.cast(i64, FileMagic.len + 2 + self.stream_offset) orelse return error.InvalidMessage;
                        const meta_data_length = std.math.cast(i32, prefix_len + format.paddedLen(metadata_len)) orelse return error.InvalidMessage;
                        const block = fbs.BlockT{
                            .offset = file_offset,
                            .metaDataLength = meta_data_length,
                            .bodyLength = envelope.body_length,
                        };
                        if (msg_t.header == .DictionaryBatch) {
                            try self.dictionary_blocks.append(self.allocator, block);
                        } else {
                            try self.record_batch_blocks.append(self.allocator, block);
                        }
                        msg_t.deinit(self.allocator);
                    },
                    .Tensor, .SparseTensor => {
                        const file_offset = std.math.cast(i64, FileMagic.len + 2 + self.stream_offset) orelse return error.InvalidMessage;
                        const meta_data_length = std.math.cast(i32, prefix_len + format.paddedLen(metadata_len)) orelse return error.InvalidMessage;
                        const block = fbs.BlockT{
                            .offset = file_offset,
                            .metaDataLength = meta_data_length,
                            .bodyLength = envelope.body_length,
                        };
                        if (msg_t.header == .Tensor) {
                            try self.tensor_blocks.append(self.allocator, block);
                        } else {
                            try self.sparse_tensor_blocks.append(self.allocator, block);
                        }
                        msg_t.deinit(self.allocator);
                    },
                    else => {
                        msg_t.deinit(self.allocator);
                        return error.UnsupportedMessage;
                    },
                }

                try self.ensureHeaderWritten();
                try self.writer.writeAll(self.stream_bytes.items[0..message_total]);
                self.stream_offset = std.math.add(usize, self.stream_offset, message_total) catch return error.InvalidMessage;
                self.consumeStreamPrefix(message_total);
            }
        }
    };
}

fn writeU32Le(writer: anytype, value: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], value, .little);
    try writer.writeAll(bytes[0..4]);
}

fn encodeBlocksToString(allocator: std.mem.Allocator, blocks: []const fbs.BlockT) error{OutOfMemory}![]u8 {
    var buf = std.ArrayListUnmanaged(u8){};
    defer buf.deinit(allocator);
    for (blocks, 0..) |blk, i| {
        if (i > 0) try buf.append(allocator, ',');
        const entry = try std.fmt.allocPrint(allocator, "{d}:{d}:{d}", .{ blk.offset, blk.metaDataLength, blk.bodyLength });
        defer allocator.free(entry);
        try buf.appendSlice(allocator, entry);
    }
    return buf.toOwnedSlice(allocator);
}

fn buildFooterBytes(
    allocator: std.mem.Allocator,
    schema: *fbs.SchemaT,
    dictionary_blocks: std.ArrayListUnmanaged(fbs.BlockT),
    record_batch_blocks: std.ArrayListUnmanaged(fbs.BlockT),
    tensor_blocks: std.ArrayListUnmanaged(fbs.BlockT),
    sparse_tensor_blocks: std.ArrayListUnmanaged(fbs.BlockT),
    metadata_version: fbs.MetadataVersion,
) FileError![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var custom_metadata = std.ArrayListUnmanaged(fbs.KeyValueT){};

    if (tensor_blocks.items.len > 0) {
        const encoded = try encodeBlocksToString(aa, tensor_blocks.items);
        try custom_metadata.append(aa, fbs.KeyValueT{ .key = "zarrow:tensor_blocks", .value = encoded });
    }
    if (sparse_tensor_blocks.items.len > 0) {
        const encoded = try encodeBlocksToString(aa, sparse_tensor_blocks.items);
        try custom_metadata.append(aa, fbs.KeyValueT{ .key = "zarrow:sparse_tensor_blocks", .value = encoded });
    }

    const footer = fbs.FooterT{
        .version = metadata_version,
        .schema = schema,
        .dictionaries = dictionary_blocks,
        .recordBatches = record_batch_blocks,
        .custom_metadata = custom_metadata,
    };

    return try fbs_lite_builder.packFooterBytes(allocator, footer);
}

fn readU32Le(bytes: []const u8) u32 {
    var buf: [4]u8 = undefined;
    @memcpy(buf[0..], bytes[0..4]);
    return std.mem.readInt(u32, &buf, .little);
}

fn firstMessageMetadataForTest(bytes: []u8) error{InvalidFixture}![]u8 {
    if (bytes.len < 4) return error.InvalidFixture;
    const first = readU32Le(bytes[0..4]);
    var prefix_len: usize = 4;
    var metadata_len_u32 = first;
    if (first == format.ContinuationMarker) {
        if (bytes.len < 8) return error.InvalidFixture;
        metadata_len_u32 = readU32Le(bytes[4..8]);
        prefix_len = 8;
    }
    if (metadata_len_u32 == 0) return error.InvalidFixture;
    const metadata_len = std.math.cast(usize, metadata_len_u32) orelse return error.InvalidFixture;
    const message_prefix = std.math.add(usize, prefix_len, metadata_len) catch return error.InvalidFixture;
    if (bytes.len < message_prefix) return error.InvalidFixture;
    return bytes[prefix_len..message_prefix];
}

test "ipc file writer emits arrow file magic and footer" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(1);
    try id_builder.append(2);
    try id_builder.append(3);
    var id_ref = try id_builder.finish();
    defer id_ref.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]@import("../array/array_ref.zig").ArrayRef{id_ref});
    defer batch.deinit();

    var out = std.ArrayListUnmanaged(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var writer = try FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &out });
    defer writer.deinit();

    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    try std.testing.expect(out.items.len > (FileMagic.len * 2 + 4));
    try std.testing.expectEqualSlices(u8, FileMagic, out.items[0..FileMagic.len]);
    try std.testing.expectEqualSlices(
        u8,
        FileMagic,
        out.items[out.items.len - FileMagic.len .. out.items.len],
    );
}

test "ipc file writer can emit footer metadata version V4" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.ArrayListUnmanaged(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var writer = try FileWriter(Sink).initWithOptions(allocator, .{ .allocator = allocator, .out = &out }, .{
        .metadata_version = .v4,
    });
    defer writer.deinit();

    try writer.writeSchema(schema);
    try writer.writeEnd();

    const trailer_len = 4 + FileMagic.len;
    try std.testing.expect(out.items.len > trailer_len + FileMagic.len);
    const trailer = out.items[out.items.len - trailer_len .. out.items.len];
    try std.testing.expectEqualSlices(u8, FileMagic, trailer[4..]);

    const footer_len_u32 = readU32Le(trailer[0..4]);
    const footer_len = std.math.cast(usize, footer_len_u32) orelse return error.InvalidMessage;
    const footer_start = out.items.len - trailer_len - footer_len;
    const footer_bytes = out.items[footer_start .. out.items.len - trailer_len];

    var footer_t = try fbs_lite_builder.unpackFooter(allocator, footer_bytes);
    defer footer_t.deinit(allocator);

    try std.testing.expectEqual(fbs.MetadataVersion.V4, footer_t.version);
}

test "ipc file writer streams incrementally and accepts tensor-like messages" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(1);
    try id_builder.append(2);
    try id_builder.append(3);
    var id_ref = try id_builder.finish();
    defer id_ref.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]@import("../array/array_ref.zig").ArrayRef{id_ref});
    defer batch.deinit();

    var tensor_body: [24]u8 = undefined;
    for (0..6) |i| {
        const value: i32 = @intCast(i + 10);
        var encoded: [4]u8 = undefined;
        std.mem.writeInt(i32, &encoded, value, .little);
        @memcpy(tensor_body[i * 4 .. i * 4 + 4], encoded[0..]);
    }
    const tensor_shape = [_]stream_writer.TensorDim{
        .{ .size = 2, .name = "rows" },
        .{ .size = 3, .name = "cols" },
    };
    const tensor_strides = [_]i64{ 12, 4 };
    const tensor_meta = stream_writer.TensorLikeMetadata{
        .tensor = .{
            .value_type = DataType{ .int32 = {} },
            .shape = tensor_shape[0..],
            .strides = tensor_strides[0..],
            .data = .{ .offset = 0, .length = tensor_body.len },
        },
    };

    var out = std.ArrayListUnmanaged(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var writer = try FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &out });
    defer writer.deinit();

    try writer.writeSchema(schema);
    try std.testing.expectEqual(@as(usize, 0), writer.stream_bytes.items.len);
    try writer.writeTensorLikeMessage(tensor_meta, tensor_body[0..]);
    try std.testing.expectEqual(@as(usize, 0), writer.stream_bytes.items.len);
    try writer.writeRecordBatch(batch);
    try std.testing.expectEqual(@as(usize, 0), writer.stream_bytes.items.len);
    try writer.writeEnd();
    try std.testing.expectEqual(@as(usize, 0), writer.stream_bytes.items.len);

    var file_stream = std.io.fixedBufferStream(out.items);
    var reader = @import("file_reader.zig").FileReader(@TypeOf(file_stream.reader())).init(allocator, file_stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expectEqual(@as(usize, 1), out_schema.fields.len);
    try std.testing.expectEqualStrings("id", out_schema.fields[0].name);

    const maybe_batch = try reader.nextRecordBatch();
    try std.testing.expect(maybe_batch != null);
    var out_batch = maybe_batch.?;
    defer out_batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), out_batch.numRows());
}

test "ipc file writer rejects damaged metadata root offset" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.ArrayListUnmanaged(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var writer = try FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &out });
    defer writer.deinit();

    // Bypass file_writer flush and corrupt the pending stream metadata in place.
    try writer.stream.writeSchema(schema);
    const metadata = try firstMessageMetadataForTest(writer.stream_bytes.items);
    @memset(metadata[0..@sizeOf(u32)], 0xFF); // root offset now out of range

    try std.testing.expectError(error.InvalidMessage, writer.flushPendingMessages(true));
}

test "ipc file writer rejects damaged metadata header type" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.ArrayListUnmanaged(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var writer = try FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &out });
    defer writer.deinit();

    // Corrupt Message.header_type to NONE(0); parseArrowMessageEnvelope must reject.
    try writer.stream.writeSchema(schema);
    const metadata = try firstMessageMetadataForTest(writer.stream_bytes.items);
    const table = try fbs_lite_reader.rootTable(metadata);
    const header_type_abs = table.fieldAbsOffset(1) orelse return error.InvalidFixture;
    metadata[header_type_abs] = 0;

    try std.testing.expectError(error.InvalidMessage, writer.flushPendingMessages(true));
}
