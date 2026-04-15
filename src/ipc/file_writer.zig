const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const stream_writer = @import("stream_writer.zig");
const format = @import("format.zig");
const fb = @import("flatbufferz");
const arrow_fbs = @import("arrow_fbs");

pub const FileMagic = "ARROW1";

pub const Schema = schema_mod.Schema;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const RecordBatch = record_batch.RecordBatch;

pub const FileError = stream_writer.StreamError || fb.common.PackError || error{
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
    bytes: *std.ArrayList(u8),

    pub const Error = error{OutOfMemory};

    pub fn writeAll(self: @This(), data: []const u8) Error!void {
        try self.bytes.appendSlice(self.allocator, data);
    }
};

const ParsedStream = struct {
    schema_msg: *fbs.MessageT,
    dictionary_blocks: std.ArrayList(fbs.BlockT),
    record_batch_blocks: std.ArrayList(fbs.BlockT),
    stream_end: usize,

    fn deinit(self: *ParsedStream, allocator: std.mem.Allocator) void {
        self.schema_msg.deinit(allocator);
        allocator.destroy(self.schema_msg);
        self.dictionary_blocks.deinit(allocator);
        self.record_batch_blocks.deinit(allocator);
    }
};

pub fn FileWriter(comptime WriterType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        writer: WriterType,
        stream_bytes: *std.ArrayList(u8),
        stream: stream_writer.StreamWriter(CollectWriter),
        finished: bool = false,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, writer: WriterType) FileError!Self {
            const stream_bytes = try allocator.create(std.ArrayList(u8));
            stream_bytes.* = std.ArrayList(u8){};

            const collect_writer = CollectWriter{
                .allocator = allocator,
                .bytes = stream_bytes,
            };
            return .{
                .allocator = allocator,
                .writer = writer,
                .stream_bytes = stream_bytes,
                .stream = stream_writer.StreamWriter(CollectWriter).init(allocator, collect_writer),
            };
        }

        pub fn deinit(self: *Self) void {
            self.stream.deinit();
            self.stream_bytes.deinit(self.allocator);
            self.allocator.destroy(self.stream_bytes);
        }

        pub fn writeSchema(self: *Self, schema: Schema) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeSchema(schema);
        }

        pub fn writeRecordBatch(self: *Self, batch: RecordBatch) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeRecordBatch(batch);
        }

        pub fn writeEnd(self: *Self) (FileError || @TypeOf(self.writer).Error)!void {
            if (self.finished) return FileError.AlreadyFinished;
            try self.stream.writeEnd();

            var parsed = try parseStreamForFooter(self.allocator, self.stream_bytes.items);
            defer parsed.deinit(self.allocator);

            const footer_bytes = try buildFooterBytes(
                self.allocator,
                parsed.schema_msg.header.Schema.?,
                parsed.dictionary_blocks,
                parsed.record_batch_blocks,
            );
            defer self.allocator.free(footer_bytes);

            // Arrow IPC file format: magic (6) + 2 padding bytes = 8-byte aligned header
            try self.writer.writeAll(FileMagic);
            try self.writer.writeAll("\x00\x00");
            try self.writer.writeAll(self.stream_bytes.items[0..parsed.stream_end]);
            // Align footer to 8-byte boundary relative to file start (header is 8 bytes)
            const footer_pad = format.padLen(parsed.stream_end);
            try format.writePadding(self.writer, footer_pad);
            try self.writer.writeAll(footer_bytes);
            const footer_len = std.math.cast(u32, footer_bytes.len) orelse return FileError.InvalidMessage;
            try writeU32Le(self.writer, footer_len);
            try self.writer.writeAll(FileMagic);

            self.finished = true;
        }
    };
}

fn writeU32Le(writer: anytype, value: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], value, .little);
    try writer.writeAll(bytes[0..4]);
}

fn parseStreamForFooter(allocator: std.mem.Allocator, stream_bytes: []const u8) FileError!ParsedStream {
    var dict_blocks = try std.ArrayList(fbs.BlockT).initCapacity(allocator, 0);
    errdefer dict_blocks.deinit(allocator);
    var rb_blocks = try std.ArrayList(fbs.BlockT).initCapacity(allocator, 0);
    errdefer rb_blocks.deinit(allocator);

    var schema_msg: ?*fbs.MessageT = null;
    errdefer if (schema_msg) |msg_ptr| {
        msg_ptr.deinit(allocator);
        allocator.destroy(msg_ptr);
    };

    var cursor: usize = 0;
    var stream_end: ?usize = null;
    while (cursor < stream_bytes.len) {
        const message_offset = cursor;
        if (stream_bytes.len - cursor < 4) return error.InvalidMessage;
        const first = readU32Le(stream_bytes[cursor .. cursor + 4]);

        var prefix_len: usize = 4;
        var metadata_len_u32 = first;
        if (first == format.ContinuationMarker) {
            if (stream_bytes.len - cursor < 8) return error.InvalidMessage;
            metadata_len_u32 = readU32Le(stream_bytes[cursor + 4 .. cursor + 8]);
            prefix_len = 8;
            if (metadata_len_u32 == 0) {
                stream_end = message_offset;
                cursor += 8;
                break;
            }
        } else if (first == 0) {
            stream_end = message_offset;
            cursor += 4;
            break;
        }

        const metadata_len = std.math.cast(usize, metadata_len_u32) orelse return error.InvalidMessage;
        cursor += prefix_len;
        const metadata_end = std.math.add(usize, cursor, metadata_len) catch return error.InvalidMessage;
        if (metadata_end > stream_bytes.len) return error.InvalidMessage;
        const metadata = stream_bytes[cursor..metadata_end];
        cursor = metadata_end;

        if (!isSaneFlatbufferTable(metadata)) return error.InvalidMessage;
        const msg = fbs.Message.GetRootAs(@constCast(metadata), 0);
        const opts: fb.common.PackOptions = .{ .allocator = allocator };
        var msg_t = try fbs.MessageT.Unpack(msg, opts);

        if (msg_t.bodyLength < 0) {
            msg_t.deinit(allocator);
            return error.InvalidMessage;
        }
        const body_len = std.math.cast(usize, msg_t.bodyLength) orelse {
            msg_t.deinit(allocator);
            return error.InvalidMessage;
        };
        const body_pad = format.padLen(body_len);
        const body_total = std.math.add(usize, body_len, body_pad) catch {
            msg_t.deinit(allocator);
            return error.InvalidMessage;
        };
        const body_end = std.math.add(usize, cursor, body_total) catch {
            msg_t.deinit(allocator);
            return error.InvalidMessage;
        };
        if (body_end > stream_bytes.len) {
            msg_t.deinit(allocator);
            return error.InvalidMessage;
        }

        switch (msg_t.header) {
            .Schema => {
                if (schema_msg != null) {
                    msg_t.deinit(allocator);
                    return error.InvalidMessage;
                }
                const owned_msg = try allocator.create(fbs.MessageT);
                owned_msg.* = msg_t;
                schema_msg = owned_msg;
            },
            .DictionaryBatch, .RecordBatch => {
                // Arrow file header = magic (6) + padding (2) = 8 bytes before stream data.
                // Block.offset = absolute file offset of the message preamble.
                // Block.metaDataLength = prefix (8) + metadata + metadata padding per spec.
                const file_offset = std.math.cast(i64, FileMagic.len + 2 + message_offset) orelse {
                    msg_t.deinit(allocator);
                    return error.InvalidMessage;
                };
                const meta_data_length = std.math.cast(i32, prefix_len + format.paddedLen(metadata_len)) orelse {
                    msg_t.deinit(allocator);
                    return error.InvalidMessage;
                };
                const block = fbs.BlockT{
                    .offset = file_offset,
                    .metaDataLength = meta_data_length,
                    .bodyLength = msg_t.bodyLength,
                };
                if (msg_t.header == .DictionaryBatch) {
                    try dict_blocks.append(allocator, block);
                } else {
                    try rb_blocks.append(allocator, block);
                }
                msg_t.deinit(allocator);
            },
            // Tensor/SparseTensor are valid IPC message headers. Arrow file
            // footer only indexes dictionary/record blocks, so keep bytes but
            // don't index these messages.
            .Tensor, .SparseTensor => {
                msg_t.deinit(allocator);
            },
            else => {
                msg_t.deinit(allocator);
                return error.UnsupportedMessage;
            },
        }

        cursor = body_end;
    }

    if (stream_end == null or cursor != stream_bytes.len) return error.InvalidMessage;
    if (schema_msg == null) return error.MissingSchema;

    return .{
        .schema_msg = schema_msg.?,
        .dictionary_blocks = dict_blocks,
        .record_batch_blocks = rb_blocks,
        .stream_end = stream_end.?,
    };
}

fn buildFooterBytes(
    allocator: std.mem.Allocator,
    schema: *fbs.SchemaT,
    dictionary_blocks: std.ArrayList(fbs.BlockT),
    record_batch_blocks: std.ArrayList(fbs.BlockT),
) FileError![]u8 {
    var custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0);
    defer custom_metadata.deinit(allocator);

    const footer = fbs.FooterT{
        .version = .V5,
        .schema = schema,
        .dictionaries = dictionary_blocks,
        .recordBatches = record_batch_blocks,
        .custom_metadata = custom_metadata,
    };

    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    const footer_off = try fbs.FooterT.Pack(footer, &builder, opts);
    try fbs.Footer.FinishBuffer(&builder, footer_off);

    const footer_bytes = try builder.finishedBytes();
    return try allocator.dupe(u8, footer_bytes);
}

fn isSaneFlatbufferTable(buf: []const u8) bool {
    if (buf.len < 8) return false;

    const root_u32 = std.mem.readInt(u32, @ptrCast(buf[0..4]), .little);
    const root = std.math.cast(usize, root_u32) orelse return false;
    if (root > buf.len - 4) return false;

    const rel = std.mem.readInt(i32, @ptrCast(buf[root .. root + 4]), .little);
    if (rel <= 0) return false;
    const rel_usize = std.math.cast(usize, rel) orelse return false;
    if (rel_usize > root) return false;

    const vtable = root - rel_usize;
    if (vtable > buf.len - 4) return false;

    const vtable_len = std.mem.readInt(u16, @ptrCast(buf[vtable .. vtable + 2]), .little);
    const object_len = std.mem.readInt(u16, @ptrCast(buf[vtable + 2 .. vtable + 4]), .little);
    if (vtable_len < 4) return false;

    const vtable_len_usize = @as(usize, vtable_len);
    const object_len_usize = @as(usize, object_len);
    if (vtable + vtable_len_usize > buf.len) return false;
    if (root + object_len_usize > buf.len) return false;

    return true;
}

fn readU32Le(bytes: []const u8) u32 {
    var buf: [4]u8 = undefined;
    @memcpy(buf[0..], bytes[0..4]);
    return std.mem.readInt(u32, &buf, .little);
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

    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
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
