const std = @import("std");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const stream_reader = @import("stream_reader.zig");
const format = @import("format.zig");
const file_writer = @import("file_writer.zig");
const array_data = @import("../array/array_data.zig");
const fb = @import("flatbufferz");
const arrow_fbs = @import("arrow_fbs");

pub const FileMagic = file_writer.FileMagic;

pub const Schema = schema_mod.Schema;
pub const RecordBatch = record_batch.RecordBatch;

pub const FileError = stream_reader.StreamError || array_data.ValidationError || record_batch.RecordBatchError || fb.common.PackError || error{
    OutOfMemory,
    InvalidFile,
    FooterTooLarge,
};

const fbs = struct {
    const Footer = arrow_fbs.org_apache_arrow_flatbuf_Footer.Footer;
    const FooterT = arrow_fbs.org_apache_arrow_flatbuf_Footer.FooterT;
    const BlockT = arrow_fbs.org_apache_arrow_flatbuf_Block.BlockT;
    const Message = arrow_fbs.org_apache_arrow_flatbuf_Message.Message;
    const MessageT = arrow_fbs.org_apache_arrow_flatbuf_Message.MessageT;
};

const SliceState = struct {
    data: []const u8,
    cursor: usize = 0,
};

const SliceReader = struct {
    state: *SliceState,

    pub const Error = error{EndOfStream};

    pub fn readNoEof(self: @This(), dest: []u8) Error!void {
        const end = std.math.add(usize, self.state.cursor, dest.len) catch return error.EndOfStream;
        if (end > self.state.data.len) return error.EndOfStream;
        @memcpy(dest, self.state.data[self.state.cursor..end]);
        self.state.cursor = end;
    }
};

pub fn FileReader(comptime ReaderType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        loaded: bool = false,
        backing_bytes: []u8 = &.{},
        stream_bytes: []u8 = &.{},
        slice_state: ?*SliceState = null,
        stream: ?stream_reader.StreamReader(SliceReader) = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.stream) |*s| s.deinit();
            self.stream = null;
            if (self.slice_state) |state| self.allocator.destroy(state);
            self.slice_state = null;
            if (self.stream_bytes.len > 0) self.allocator.free(self.stream_bytes);
            self.stream_bytes = &.{};
            if (self.backing_bytes.len > 0) self.allocator.free(self.backing_bytes);
            self.backing_bytes = &.{};
        }

        pub fn readSchema(self: *Self) (FileError || @TypeOf(self.reader).Error || SliceReader.Error)!Schema {
            try self.ensureLoaded();
            const s = &self.stream.?;
            return try s.readSchema();
        }

        pub fn nextRecordBatch(self: *Self) (FileError || @TypeOf(self.reader).Error || SliceReader.Error)!?RecordBatch {
            try self.ensureLoaded();
            const s = &self.stream.?;
            return try s.nextRecordBatch();
        }

        fn ensureLoaded(self: *Self) (FileError || @TypeOf(self.reader).Error)!void {
            if (self.loaded) return;

            var all = std.ArrayList(u8){};
            defer all.deinit(self.allocator);

            var chunk: [4096]u8 = undefined;
            while (true) {
                const read_n = try self.reader.read(&chunk);
                if (read_n == 0) break;
                try all.appendSlice(self.allocator, chunk[0..read_n]);
            }
            self.backing_bytes = try all.toOwnedSlice(self.allocator);

            self.stream_bytes = try buildIndexedStreamFromFile(self.allocator, self.backing_bytes);

            const state = try self.allocator.create(SliceState);
            state.* = .{ .data = self.stream_bytes };
            self.slice_state = state;

            const reader = SliceReader{ .state = state };
            self.stream = stream_reader.StreamReader(SliceReader).init(self.allocator, reader);
            self.loaded = true;
        }
    };
}

const MessageHeaderKind = enum {
    schema,
    dictionary_batch,
    record_batch,
    other,
};

const ParsedMessage = struct {
    meta_len: usize,
    body_len: usize,
    total_len: usize,
    header: MessageHeaderKind,
};

const IndexedBlock = struct {
    offset: usize,
    total_len: usize,
};

const FoundSchema = struct {
    offset: usize,
    parsed: ParsedMessage,
};

fn buildIndexedStreamFromFile(allocator: std.mem.Allocator, bytes: []const u8) FileError![]u8 {
    // Arrow IPC file header: magic (6 bytes) + 2 padding bytes = 8 bytes total.
    const header_len = FileMagic.len + 2;
    const trailer_len = 4 + FileMagic.len;
    if (bytes.len < header_len + trailer_len) return error.InvalidFile;
    if (!std.mem.eql(u8, bytes[0..FileMagic.len], FileMagic)) return error.InvalidFile;
    if (!std.mem.eql(u8, bytes[bytes.len - FileMagic.len .. bytes.len], FileMagic)) return error.InvalidFile;

    const footer_len_pos = bytes.len - trailer_len;
    const footer_len_u32 = readU32Le(bytes[footer_len_pos .. footer_len_pos + 4]);
    const footer_len = std.math.cast(usize, footer_len_u32) orelse return error.FooterTooLarge;
    const footer_end = footer_len_pos;
    if (footer_len > footer_end - header_len) return error.InvalidFile;
    const footer_start = footer_end - footer_len;
    if (footer_start < header_len) return error.InvalidFile;

    const footer_bytes = bytes[footer_start..footer_end];
    if (!isSaneFlatbufferTable(footer_bytes)) return error.InvalidFile;

    const footer = fbs.Footer.GetRootAs(@constCast(footer_bytes), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var footer_t = try fbs.FooterT.Unpack(footer, opts);
    defer footer_t.deinit(allocator);
    if (footer_t.schema == null) return error.InvalidFile;

    const schema_search_limit = try findSchemaSearchLimit(footer_t, header_len, footer_start);
    const found_schema = try findSchemaMessage(allocator, bytes, header_len, schema_search_limit);
    const schema_offset = found_schema.offset;
    const schema_msg = found_schema.parsed;

    // Validate every block's absolute file offsets against [header_len, footer_start).
    // block.offset           = absolute file offset of the message preamble
    // block.metaDataLength   = prefix(8) + paddedLen(metadata)
    // block.bodyLength       = sum of paddedLen(each_buffer)  [includes per-buffer padding]
    // Together they must fit entirely within the message region.
    var indexed_blocks = try collectIndexedBlocks(allocator, bytes, footer_t, header_len, footer_start);
    defer indexed_blocks.deinit(allocator);

    const schema_bytes_len = schema_msg.total_len;
    var total_len: usize = 0;
    total_len = std.math.add(usize, total_len, schema_bytes_len) catch return error.InvalidFile;
    for (indexed_blocks.items) |blk| {
        total_len = std.math.add(usize, total_len, blk.total_len) catch return error.InvalidFile;
    }
    // Append synthetic EOS for StreamReader.
    total_len = std.math.add(usize, total_len, 8) catch return error.InvalidFile;

    const out = try allocator.alloc(u8, total_len);
    var cursor: usize = 0;

    @memcpy(out[cursor .. cursor + schema_bytes_len], bytes[schema_offset .. schema_offset + schema_bytes_len]);
    cursor += schema_bytes_len;
    for (indexed_blocks.items) |blk| {
        @memcpy(out[cursor .. cursor + blk.total_len], bytes[blk.offset .. blk.offset + blk.total_len]);
        cursor += blk.total_len;
    }
    var eos: [8]u8 = undefined;
    std.mem.writeInt(u32, eos[0..4], format.ContinuationMarker, .little);
    std.mem.writeInt(u32, eos[4..8], 0, .little);
    @memcpy(out[cursor .. cursor + eos.len], eos[0..]);

    return out;
}

fn findSchemaSearchLimit(footer_t: fbs.FooterT, header_len: usize, footer_start: usize) FileError!usize {
    var limit = footer_start;
    for (footer_t.dictionaries.items) |block| {
        if (block.offset < 0) return error.InvalidFile;
        const off = std.math.cast(usize, block.offset) orelse return error.InvalidFile;
        if (off < header_len or off >= footer_start) return error.InvalidFile;
        if (off < limit) limit = off;
    }
    for (footer_t.recordBatches.items) |block| {
        if (block.offset < 0) return error.InvalidFile;
        const off = std.math.cast(usize, block.offset) orelse return error.InvalidFile;
        if (off < header_len or off >= footer_start) return error.InvalidFile;
        if (off < limit) limit = off;
    }
    if (limit <= header_len) return error.InvalidFile;
    return limit;
}

fn findSchemaMessage(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    search_start: usize,
    search_limit: usize,
) FileError!FoundSchema {
    if (search_start >= search_limit) return error.InvalidFile;
    if (search_limit > bytes.len) return error.InvalidFile;

    var off = search_start;
    while (off + 4 <= search_limit) : (off += 1) {
        const first = readU32Le(bytes[off .. off + 4]);
        if (first == 0) continue;

        const parsed = parseMessageAt(allocator, bytes, off, search_limit) catch |err| switch (err) {
            error.InvalidFile => continue,
            else => return err,
        };
        if (parsed.header != .schema) return error.InvalidFile;
        return .{
            .offset = off,
            .parsed = parsed,
        };
    }
    return error.InvalidFile;
}

fn collectIndexedBlocks(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    footer_t: fbs.FooterT,
    header_len: usize,
    footer_start: usize,
) FileError!std.ArrayList(IndexedBlock) {
    const total_blocks = std.math.add(usize, footer_t.dictionaries.items.len, footer_t.recordBatches.items.len) catch return error.InvalidFile;
    var blocks = try std.ArrayList(IndexedBlock).initCapacity(allocator, total_blocks);
    errdefer blocks.deinit(allocator);

    for (footer_t.dictionaries.items) |block| {
        try appendCheckedBlock(allocator, bytes, &blocks, block, .dictionary_batch, header_len, footer_start);
    }
    for (footer_t.recordBatches.items) |block| {
        try appendCheckedBlock(allocator, bytes, &blocks, block, .record_batch, header_len, footer_start);
    }

    std.mem.sort(IndexedBlock, blocks.items, {}, struct {
        fn lessThan(_: void, lhs: IndexedBlock, rhs: IndexedBlock) bool {
            return lhs.offset < rhs.offset;
        }
    }.lessThan);

    // Footer blocks must not overlap each other.
    var prev_end: usize = 0;
    var have_prev = false;
    for (blocks.items) |blk| {
        if (have_prev and blk.offset < prev_end) return error.InvalidFile;
        prev_end = std.math.add(usize, blk.offset, blk.total_len) catch return error.InvalidFile;
        have_prev = true;
    }

    return blocks;
}

fn appendCheckedBlock(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    blocks: *std.ArrayList(IndexedBlock),
    block: fbs.BlockT,
    expected_header: MessageHeaderKind,
    header_len: usize,
    footer_start: usize,
) FileError!void {
    if (block.offset < 0) return error.InvalidFile;
    const offset = std.math.cast(usize, block.offset) orelse return error.InvalidFile;
    if (offset < header_len or offset >= footer_start) return error.InvalidFile;

    if (block.metaDataLength <= 0) return error.InvalidFile;
    const expected_meta_len = std.math.cast(usize, block.metaDataLength) orelse return error.InvalidFile;
    if (block.bodyLength < 0) return error.InvalidFile;
    const expected_body_len = std.math.cast(usize, block.bodyLength) orelse return error.InvalidFile;

    const parsed = try parseMessageAt(allocator, bytes, offset, footer_start);
    if (parsed.meta_len != expected_meta_len) return error.InvalidFile;
    if (parsed.body_len != expected_body_len) return error.InvalidFile;
    if (parsed.header != expected_header) return error.InvalidFile;

    try blocks.append(allocator, .{
        .offset = offset,
        .total_len = parsed.total_len,
    });
}

fn parseMessageAt(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    offset: usize,
    limit: usize,
) FileError!ParsedMessage {
    if (offset >= limit) return error.InvalidFile;
    if (limit > bytes.len) return error.InvalidFile;

    if (limit - offset < 4) return error.InvalidFile;
    const first = readU32Le(bytes[offset .. offset + 4]);

    var prefix_len: usize = 4;
    var metadata_len_u32 = first;
    if (first == format.ContinuationMarker) {
        if (limit - offset < 8) return error.InvalidFile;
        metadata_len_u32 = readU32Le(bytes[offset + 4 .. offset + 8]);
        prefix_len = 8;
        if (metadata_len_u32 == 0) return error.InvalidFile;
    } else if (first == 0) {
        return error.InvalidFile;
    }

    const metadata_len = std.math.cast(usize, metadata_len_u32) orelse return error.InvalidFile;
    const metadata_start = std.math.add(usize, offset, prefix_len) catch return error.InvalidFile;
    const metadata_end = std.math.add(usize, metadata_start, metadata_len) catch return error.InvalidFile;
    if (metadata_end > limit) return error.InvalidFile;
    const metadata = bytes[metadata_start..metadata_end];
    if (!isSaneFlatbufferTable(metadata)) return error.InvalidFile;

    const msg = fbs.Message.GetRootAs(@constCast(metadata), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var msg_t = try fbs.MessageT.Unpack(msg, opts);
    defer msg_t.deinit(allocator);

    if (msg_t.bodyLength < 0) return error.InvalidFile;
    const body_len = std.math.cast(usize, msg_t.bodyLength) orelse return error.InvalidFile;
    const total_len = std.math.add(usize, prefix_len + metadata_len, body_len) catch return error.InvalidFile;
    const end = std.math.add(usize, offset, total_len) catch return error.InvalidFile;
    if (end > limit) return error.InvalidFile;

    const header: MessageHeaderKind = switch (msg_t.header) {
        .Schema => .schema,
        .DictionaryBatch => .dictionary_batch,
        .RecordBatch => .record_batch,
        else => .other,
    };
    return .{
        .meta_len = prefix_len + metadata_len,
        .body_len = body_len,
        .total_len = total_len,
        .header = header,
    };
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

test "ipc file reader roundtrips batches via stream reader" {
    const allocator = std.testing.allocator;

    const zarray = @import("../array/array_ref.zig");
    const prim = @import("../array/primitive_array.zig");
    const str = @import("../array/string_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    const id_type = DataType{ .int32 = {} };
    const name_type = DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(10);
    try id_builder.append(20);
    try id_builder.append(30);
    var ids = try id_builder.finish();
    defer ids.release();

    var name_builder = try str.StringBuilder.init(allocator, 3, 16);
    defer name_builder.deinit();
    try name_builder.append("aa");
    try name_builder.appendNull();
    try name_builder.append("cc");
    var names = try name_builder.finish();
    defer names.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]zarray.ArrayRef{ ids, names });
    defer batch.deinit();

    var file_bytes = std.ArrayList(u8){};
    defer file_bytes.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };

    var fw = try file_writer.FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &file_bytes });
    defer fw.deinit();
    try fw.writeSchema(schema);
    try fw.writeRecordBatch(batch);
    try fw.writeEnd();

    var fixed = std.io.fixedBufferStream(file_bytes.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    const out_schema = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 2), out_schema.fields.len);
    try std.testing.expectEqualStrings("id", out_schema.fields[0].name);
    try std.testing.expectEqualStrings("name", out_schema.fields[1].name);

    const out_batch_opt = try fr.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), out_batch.numRows());

    const id_arr = prim.PrimitiveArray(i32){ .data = out_batch.columns[0].data() };
    const name_arr = str.StringArray{ .data = out_batch.columns[1].data() };
    try std.testing.expectEqual(@as(i32, 10), id_arr.value(0));
    try std.testing.expectEqual(@as(i32, 20), id_arr.value(1));
    try std.testing.expectEqual(@as(i32, 30), id_arr.value(2));
    try std.testing.expectEqualStrings("aa", name_arr.value(0));
    try std.testing.expect(name_arr.isNull(1));
    try std.testing.expectEqualStrings("cc", name_arr.value(2));

    try std.testing.expect((try fr.nextRecordBatch()) == null);
}

test "ipc file reader rejects footer with out-of-bounds block offset" {
    const allocator = std.testing.allocator;

    const prim = @import("../array/primitive_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    // Build a valid file first.
    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &id_type, .nullable = false }};
    const schema = Schema{ .fields = fields[0..] };
    var id_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer id_builder.deinit();
    try id_builder.append(42);
    var ids = try id_builder.finish();
    defer ids.release();

    var batch = try record_batch.RecordBatch.init(allocator, schema, &[_]@import("../array/array_ref.zig").ArrayRef{ids});
    defer batch.deinit();

    var file_bytes = std.ArrayList(u8){};
    defer file_bytes.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };
    var fw = try file_writer.FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &file_bytes });
    defer fw.deinit();
    try fw.writeSchema(schema);
    try fw.writeRecordBatch(batch);
    try fw.writeEnd();

    // Locate the footer: last 6 bytes = magic, preceding 4 bytes = footer_len.
    const trailer_len = 4 + FileMagic.len;
    const footer_len_pos = file_bytes.items.len - trailer_len;
    const footer_len_u32 = std.mem.readInt(u32, file_bytes.items[footer_len_pos..][0..4], .little);
    const footer_end = footer_len_pos;
    const footer_start = footer_end - @as(usize, footer_len_u32);

    // Parse the real footer, corrupt a block offset, re-serialize, and patch back into file bytes.
    const footer_bytes_orig = file_bytes.items[footer_start..footer_end];
    const footer_fb = fbs.Footer.GetRootAs(@constCast(footer_bytes_orig), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var footer_t = try fbs.FooterT.Unpack(footer_fb, opts);
    defer footer_t.deinit(allocator);

    // Set the first recordBatch block offset to a value beyond the file end.
    try std.testing.expect(footer_t.recordBatches.items.len > 0);
    footer_t.recordBatches.items[0].offset = @intCast(file_bytes.items.len + 9999);

    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();
    const footer_off = try fbs.FooterT.Pack(footer_t, &builder, opts);
    try fbs.Footer.FinishBuffer(&builder, footer_off);
    const new_footer_bytes = try builder.finishedBytes();

    // Rebuild file bytes with the corrupted footer.
    var corrupted = std.ArrayList(u8){};
    defer corrupted.deinit(allocator);
    try corrupted.appendSlice(allocator, file_bytes.items[0..footer_start]);
    try corrupted.appendSlice(allocator, new_footer_bytes);
    var new_footer_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &new_footer_len, @intCast(new_footer_bytes.len), .little);
    try corrupted.appendSlice(allocator, new_footer_len[0..]);
    try corrupted.appendSlice(allocator, FileMagic);

    var fixed = std.io.fixedBufferStream(corrupted.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();
    try std.testing.expectError(error.InvalidFile, fr.readSchema());
}

test "ipc file reader reconstructs stream from footer block index" {
    const allocator = std.testing.allocator;

    const zarray = @import("../array/array_ref.zig");
    const prim = @import("../array/primitive_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(7);
    try id_builder.append(8);
    try id_builder.append(9);
    var ids = try id_builder.finish();
    defer ids.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]zarray.ArrayRef{ids});
    defer batch.deinit();

    var file_bytes = std.ArrayList(u8){};
    defer file_bytes.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), data: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, data);
        }
    };

    var fw = try file_writer.FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &file_bytes });
    defer fw.deinit();
    try fw.writeSchema(schema);
    try fw.writeRecordBatch(batch);
    try fw.writeEnd();

    // Locate footer.
    const trailer_len = 4 + FileMagic.len;
    const footer_len_pos = file_bytes.items.len - trailer_len;
    const footer_len_u32 = std.mem.readInt(u32, file_bytes.items[footer_len_pos..][0..4], .little);
    const footer_end = footer_len_pos;
    const footer_start = footer_end - @as(usize, footer_len_u32);
    const footer_bytes_orig = file_bytes.items[footer_start..footer_end];

    // Insert non-message bytes in the message region and shift indexed block offsets.
    const footer_fb = fbs.Footer.GetRootAs(@constCast(footer_bytes_orig), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var footer_t = try fbs.FooterT.Unpack(footer_fb, opts);
    defer footer_t.deinit(allocator);
    try std.testing.expect(footer_t.recordBatches.items.len > 0);
    const insert_pos = std.math.cast(usize, footer_t.recordBatches.items[0].offset) orelse return error.InvalidFile;
    const junk = [_]u8{ 0x13, 0x37, 0xAA, 0x55, 0x99 };

    for (footer_t.dictionaries.items) |*blk| {
        if (blk.offset >= @as(i64, @intCast(insert_pos))) blk.offset += junk.len;
    }
    for (footer_t.recordBatches.items) |*blk| {
        if (blk.offset >= @as(i64, @intCast(insert_pos))) blk.offset += junk.len;
    }

    var shifted_prefix = std.ArrayList(u8){};
    defer shifted_prefix.deinit(allocator);
    try shifted_prefix.appendSlice(allocator, file_bytes.items[0..insert_pos]);
    try shifted_prefix.appendSlice(allocator, junk[0..]);
    try shifted_prefix.appendSlice(allocator, file_bytes.items[insert_pos..footer_start]);

    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();
    const footer_off = try fbs.FooterT.Pack(footer_t, &builder, opts);
    try fbs.Footer.FinishBuffer(&builder, footer_off);
    const footer_bytes_new = try builder.finishedBytes();

    var rewritten = std.ArrayList(u8){};
    defer rewritten.deinit(allocator);
    try rewritten.appendSlice(allocator, shifted_prefix.items);
    try rewritten.appendSlice(allocator, footer_bytes_new);
    var new_footer_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &new_footer_len, @intCast(footer_bytes_new.len), .little);
    try rewritten.appendSlice(allocator, new_footer_len[0..]);
    try rewritten.appendSlice(allocator, FileMagic);

    // Reader should succeed because it reconstructs stream bytes from footer indexes.
    var fixed = std.io.fixedBufferStream(rewritten.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    const out_schema = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 1), out_schema.fields.len);
    const out_batch_opt = try fr.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    const id_arr = prim.PrimitiveArray(i32){ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i32, 7), id_arr.value(0));
    try std.testing.expectEqual(@as(i32, 8), id_arr.value(1));
    try std.testing.expectEqual(@as(i32, 9), id_arr.value(2));
}

test "ipc file reader accepts file with leading padding before schema message" {
    const allocator = std.testing.allocator;

    const zarray = @import("../array/array_ref.zig");
    const prim = @import("../array/primitive_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer id_builder.deinit();
    try id_builder.append(11);
    try id_builder.append(22);
    var ids = try id_builder.finish();
    defer ids.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]zarray.ArrayRef{ids});
    defer batch.deinit();

    var file_bytes = std.ArrayList(u8){};
    defer file_bytes.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), data: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, data);
        }
    };
    var fw = try file_writer.FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &file_bytes });
    defer fw.deinit();
    try fw.writeSchema(schema);
    try fw.writeRecordBatch(batch);
    try fw.writeEnd();

    const trailer_len = 4 + FileMagic.len;
    const footer_len_pos = file_bytes.items.len - trailer_len;
    const footer_len_u32 = std.mem.readInt(u32, file_bytes.items[footer_len_pos..][0..4], .little);
    const footer_end = footer_len_pos;
    const footer_start = footer_end - @as(usize, footer_len_u32);
    const footer_bytes_orig = file_bytes.items[footer_start..footer_end];

    const footer_fb = fbs.Footer.GetRootAs(@constCast(footer_bytes_orig), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var footer_t = try fbs.FooterT.Unpack(footer_fb, opts);
    defer footer_t.deinit(allocator);

    const header_len = FileMagic.len + 2;
    const lead_pad_len: i64 = 56;
    for (footer_t.dictionaries.items) |*blk| blk.offset += lead_pad_len;
    for (footer_t.recordBatches.items) |*blk| blk.offset += lead_pad_len;

    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();
    const footer_off = try fbs.FooterT.Pack(footer_t, &builder, opts);
    try fbs.Footer.FinishBuffer(&builder, footer_off);
    const footer_bytes_new = try builder.finishedBytes();

    var rewritten = std.ArrayList(u8){};
    defer rewritten.deinit(allocator);
    try rewritten.appendSlice(allocator, file_bytes.items[0..header_len]);
    try rewritten.appendNTimes(allocator, 0, @intCast(lead_pad_len));
    try rewritten.appendSlice(allocator, file_bytes.items[header_len..footer_start]);
    try rewritten.appendSlice(allocator, footer_bytes_new);
    var new_footer_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &new_footer_len, @intCast(footer_bytes_new.len), .little);
    try rewritten.appendSlice(allocator, new_footer_len[0..]);
    try rewritten.appendSlice(allocator, FileMagic);

    var fixed = std.io.fixedBufferStream(rewritten.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    const out_schema = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 1), out_schema.fields.len);
    const out_batch_opt = try fr.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    const id_arr = prim.PrimitiveArray(i32){ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i32, 11), id_arr.value(0));
    try std.testing.expectEqual(@as(i32, 22), id_arr.value(1));
}
