const std = @import("std");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const stream_reader = @import("stream_reader.zig");
const fbs_lite_builder = @import("fbs_lite/builder.zig");
const fbs_lite_verify = @import("fbs_lite/verify.zig");
const format = @import("format.zig");
const file_writer = @import("file_writer.zig");
const array_data = @import("../array/array_data.zig");
const buffer = @import("../buffer.zig");
const arrow_fbs = @import("ipc_schema");

pub const FileMagic = file_writer.FileMagic;

pub const Schema = schema_mod.Schema;
pub const SchemaRef = schema_mod.SchemaRef;
pub const RecordBatch = record_batch.RecordBatch;
pub const EndiannessMode = stream_reader.EndiannessMode;
pub const ReaderOptions = struct {
    endianness_mode: EndiannessMode = .strict,
};

pub const FileError = stream_reader.StreamError || array_data.ValidationError || record_batch.RecordBatchError || fbs_lite_builder.PackError || error{
    OutOfMemory,
    InvalidFile,
    FooterTooLarge,
};

const fbs = struct {
    const Footer = arrow_fbs.org_apache_arrow_flatbuf_Footer.Footer;
    const FooterT = arrow_fbs.org_apache_arrow_flatbuf_Footer.FooterT;
    const BlockT = arrow_fbs.org_apache_arrow_flatbuf_Block.BlockT;
    const MessageHeader = arrow_fbs.org_apache_arrow_flatbuf_MessageHeader.MessageHeader;
};

const IndexedFile = struct {
    dictionaries: []IndexedBlock,
    record_batches: []IndexedBlock,
    tensors: []IndexedBlock,
    sparse_tensors: []IndexedBlock,
    message_order: []IndexedBlock, // dictionaries + record batches sorted by offset
    record_message_positions: []usize, // record index -> message_order position

    fn deinit(self: *IndexedFile, allocator: std.mem.Allocator) void {
        allocator.free(self.record_message_positions);
        allocator.free(self.message_order);
        allocator.free(self.record_batches);
        allocator.free(self.dictionaries);
        allocator.free(self.sparse_tensors);
        allocator.free(self.tensors);
        self.* = undefined;
    }
};

pub fn FileReader(comptime ReaderType: type) type {
    const ReaderReadError = @TypeOf(@as(ReaderType, undefined)).Error;
    const ReaderContextPtr = @TypeOf(@as(ReaderType, undefined).context);
    const ReaderContext = switch (@typeInfo(ReaderContextPtr)) {
        .pointer => |p| p.child,
        else => ReaderContextPtr,
    };
    const ReaderSeekToReturn = @typeInfo(@TypeOf(ReaderContext.seekTo)).@"fn".return_type.?;
    const ReaderSeekError = @typeInfo(ReaderSeekToReturn).error_union.error_set;
    const ReaderGetEndPosReturn = @typeInfo(@TypeOf(ReaderContext.getEndPos)).@"fn".return_type.?;
    const ReaderGetEndPosError = @typeInfo(ReaderGetEndPosReturn).error_union.error_set;
    const ReaderIoError = ReaderReadError || ReaderSeekError || ReaderGetEndPosError || error{EndOfStream};

    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        options: ReaderOptions,
        loaded: bool = false,
        indexed: ?IndexedFile = null,
        schema_ref: ?SchemaRef = null,
        dictionary_values: std.AutoHashMap(i64, array_data.ArrayRef),
        next_record_index: usize = 0,
        decode_message_cursor: usize = 0, // points into indexed.message_order

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return initWithOptions(allocator, reader, .{});
        }

        pub fn initWithOptions(allocator: std.mem.Allocator, reader: ReaderType, options: ReaderOptions) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
                .options = options,
                .dictionary_values = std.AutoHashMap(i64, array_data.ArrayRef).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.clearDictionaryValues();
            self.dictionary_values.deinit();
            if (self.schema_ref) |*ref| ref.release();
            self.schema_ref = null;
            if (self.indexed) |*idx| idx.deinit(self.allocator);
            self.indexed = null;
        }

        pub fn readSchema(self: *Self) (FileError || ReaderIoError)!Schema {
            try self.ensureLoaded();
            return self.schema_ref.?.schema().*;
        }

        pub fn recordBatchCount(self: *Self) (FileError || ReaderIoError)!usize {
            try self.ensureLoaded();
            return self.indexed.?.record_batches.len;
        }

        pub fn readRecordBatchAt(self: *Self, index: usize) (FileError || ReaderIoError)!RecordBatch {
            try self.ensureLoaded();
            const idx = self.indexed.?;
            if (index >= idx.record_batches.len) return error.InvalidFile;
            try self.ensureDictionaryStateForRecord(index);

            const message_pos = idx.record_message_positions[index];
            const block = idx.message_order[message_pos];
            if (block.header != .record_batch) return error.InvalidFile;

            var decoded_block = try self.readDecodedBlock(block);
            defer decoded_block.deinit(self.allocator);
            const batch = try stream_reader.buildRecordBatchFromMessageMetadata(
                self.allocator,
                self.schema_ref.?.retain(),
                &self.dictionary_values,
                decoded_block.metadata,
                decoded_block.body,
            );

            self.decode_message_cursor = message_pos + 1;
            self.next_record_index = index + 1;
            return batch;
        }

        pub fn nextRecordBatch(self: *Self) (FileError || ReaderIoError)!?RecordBatch {
            try self.ensureLoaded();
            const idx = self.indexed.?;
            if (self.next_record_index >= idx.record_batches.len) return null;
            return try self.readRecordBatchAt(self.next_record_index);
        }

        pub fn tensorCount(self: *Self) (FileError || ReaderIoError)!usize {
            try self.ensureLoaded();
            return self.indexed.?.tensors.len;
        }

        pub fn readTensorAt(self: *Self, index: usize) (FileError || ReaderIoError)!stream_reader.OwnedTensorLikeMessage {
            try self.ensureLoaded();
            const idx = self.indexed.?;
            if (index >= idx.tensors.len) return error.InvalidFile;
            return try self.readTensorLikeBlock(idx.tensors[index]);
        }

        pub fn sparseTensorCount(self: *Self) (FileError || ReaderIoError)!usize {
            try self.ensureLoaded();
            return self.indexed.?.sparse_tensors.len;
        }

        pub fn readSparseTensorAt(self: *Self, index: usize) (FileError || ReaderIoError)!stream_reader.OwnedTensorLikeMessage {
            try self.ensureLoaded();
            const idx = self.indexed.?;
            if (index >= idx.sparse_tensors.len) return error.InvalidFile;
            return try self.readTensorLikeBlock(idx.sparse_tensors[index]);
        }

        fn readTensorLikeBlock(self: *Self, block: IndexedBlock) (FileError || ReaderIoError)!stream_reader.OwnedTensorLikeMessage {
            var decoded_block = try self.readDecodedBlock(block);
            const retained_body = decoded_block.body.retain();
            defer decoded_block.deinit(self.allocator);

            var arena = std.heap.ArenaAllocator.init(self.allocator);
            errdefer arena.deinit();
            var body_for_result = retained_body;
            errdefer body_for_result.release();

            const meta = switch (block.header) {
                .tensor => stream_reader.TensorLikeMetadata{
                    .tensor = try stream_reader.decodeTensorFromMessageMetadata(
                        arena.allocator(),
                        decoded_block.metadata,
                        retained_body,
                    ),
                },
                .sparse_tensor => stream_reader.TensorLikeMetadata{
                    .sparse_tensor = try stream_reader.decodeSparseTensorFromMessageMetadata(
                        arena.allocator(),
                        decoded_block.metadata,
                        retained_body,
                    ),
                },
                else => return error.InvalidFile,
            };

            return stream_reader.OwnedTensorLikeMessage{
                .arena = arena,
                .metadata = meta,
                .body = body_for_result,
            };
        }

        fn ensureLoaded(self: *Self) (FileError || ReaderIoError)!void {
            if (self.loaded) return;

            const header_len = FileMagic.len + 2;
            const trailer_len = 4 + FileMagic.len;
            const end_pos = try self.reader.context.getEndPos();
            const file_len = std.math.cast(usize, end_pos) orelse return error.FooterTooLarge;
            if (file_len < header_len + trailer_len) return error.InvalidFile;

            var header_magic: [FileMagic.len]u8 = undefined;
            try self.readAtNoEof(0, header_magic[0..]);
            if (!std.mem.eql(u8, header_magic[0..], FileMagic)) return error.InvalidFile;

            var trailer: [4 + FileMagic.len]u8 = undefined;
            const footer_len_pos = file_len - trailer_len;
            try self.readAtNoEof(footer_len_pos, trailer[0..]);
            if (!std.mem.eql(u8, trailer[4..], FileMagic)) return error.InvalidFile;

            const footer_len_u32 = readU32Le(trailer[0..4]);
            const footer_len = std.math.cast(usize, footer_len_u32) orelse return error.FooterTooLarge;
            const footer_end = footer_len_pos;
            if (footer_len > footer_end - header_len) return error.InvalidFile;
            const footer_start = footer_end - footer_len;
            if (footer_start < header_len) return error.InvalidFile;

            const footer_bytes = try self.allocator.alloc(u8, footer_len);
            defer self.allocator.free(footer_bytes);
            try self.readAtNoEof(footer_start, footer_bytes);
            if (!fbs_lite_verify.isSaneTable(footer_bytes)) return error.InvalidFile;

            var footer_t = try fbs_lite_builder.unpackFooter(self.allocator, footer_bytes);
            defer footer_t.deinit(self.allocator);
            if (footer_t.schema == null) return error.InvalidFile;

            self.indexed = try self.buildIndexedFromFooter(footer_t, header_len, footer_start);

            var arena = std.heap.ArenaAllocator.init(self.allocator);
            errdefer arena.deinit();
            const schema = try stream_reader.buildSchemaFromFlatbufWithEndiannessMode(
                arena.allocator(),
                footer_t.schema.?,
                self.options.endianness_mode,
            );
            self.schema_ref = try SchemaRef.fromArena(self.allocator, arena, schema);

            self.resetDecodeState();
            self.loaded = true;
        }

        fn clearDictionaryValues(self: *Self) void {
            var it = self.dictionary_values.iterator();
            while (it.next()) |entry| {
                var dict = entry.value_ptr.*;
                dict.release();
            }
            self.dictionary_values.clearRetainingCapacity();
        }

        fn resetDecodeState(self: *Self) void {
            self.clearDictionaryValues();
            self.decode_message_cursor = 0;
            self.next_record_index = 0;
        }

        fn ensureDictionaryStateForRecord(self: *Self, record_index: usize) (FileError || ReaderIoError)!void {
            const idx = self.indexed orelse return error.InvalidFile;
            const target_pos = idx.record_message_positions[record_index];

            if (self.decode_message_cursor > target_pos) {
                self.resetDecodeState();
            }

            while (self.decode_message_cursor < target_pos) : (self.decode_message_cursor += 1) {
                const block = idx.message_order[self.decode_message_cursor];
                if (block.header != .dictionary_batch) continue;
                var decoded_block = try self.readDecodedBlock(block);
                defer decoded_block.deinit(self.allocator);
                try stream_reader.ingestDictionaryBatchFromMessageMetadata(
                    self.allocator,
                    self.schema_ref.?.schema().*,
                    &self.dictionary_values,
                    decoded_block.metadata,
                    decoded_block.body,
                );
            }
        }

        const DecodedBlock = struct {
            metadata: []u8,
            body: array_data.SharedBuffer,

            fn deinit(self: *DecodedBlock, allocator: std.mem.Allocator) void {
                allocator.free(self.metadata);
                self.body.release();
            }
        };

        fn readDecodedBlock(self: *Self, block: IndexedBlock) (FileError || ReaderIoError)!DecodedBlock {
            const metadata_len = block.parsed.metadata_end - block.parsed.metadata_start;
            const metadata = try self.allocator.alloc(u8, metadata_len);
            errdefer self.allocator.free(metadata);
            try self.readAtNoEof(block.parsed.metadata_start, metadata);

            var body = try self.readSharedAt(block.parsed.body_start, block.parsed.body_len);
            errdefer body.release();

            return .{
                .metadata = metadata,
                .body = body,
            };
        }

        fn readSharedAt(self: *Self, offset: usize, len: usize) (FileError || ReaderIoError)!array_data.SharedBuffer {
            if (len == 0) return array_data.SharedBuffer.empty;
            var owned = try buffer.OwnedBuffer.init(self.allocator, len);
            errdefer owned.deinit();
            try self.readAtNoEof(offset, owned.data[0..len]);
            return try owned.toShared(len);
        }

        fn readAtNoEof(self: *Self, offset: usize, out: []u8) ReaderIoError!void {
            try self.reader.context.seekTo(@intCast(offset));
            if (out.len == 0) return;
            try self.reader.readNoEof(out);
        }

        fn buildIndexedFromFooter(
            self: *Self,
            footer_t: fbs.FooterT,
            header_len: usize,
            footer_start: usize,
        ) (FileError || ReaderIoError)!IndexedFile {
            var indexed_blocks = try self.collectIndexedBlocks(footer_t, header_len, footer_start);
            defer indexed_blocks.deinit(self.allocator);

            const message_order = try indexed_blocks.toOwnedSlice(self.allocator);
            errdefer self.allocator.free(message_order);

            var dictionary_count: usize = 0;
            var record_count: usize = 0;
            for (message_order) |blk| {
                switch (blk.header) {
                    .dictionary_batch => dictionary_count += 1,
                    .record_batch => record_count += 1,
                    else => return error.InvalidFile,
                }
            }

            const dictionaries = try self.allocator.alloc(IndexedBlock, dictionary_count);
            errdefer self.allocator.free(dictionaries);
            const record_batches = try self.allocator.alloc(IndexedBlock, record_count);
            errdefer self.allocator.free(record_batches);
            const record_message_positions = try self.allocator.alloc(usize, record_count);
            errdefer self.allocator.free(record_message_positions);

            const tensors = try self.collectTensorBlocksFromCustomMetadata(
                footer_t,
                header_len,
                footer_start,
                .tensor,
                "zarrow:tensor_blocks",
            );
            errdefer self.allocator.free(tensors);

            const sparse_tensors = try self.collectTensorBlocksFromCustomMetadata(
                footer_t,
                header_len,
                footer_start,
                .sparse_tensor,
                "zarrow:sparse_tensor_blocks",
            );
            errdefer self.allocator.free(sparse_tensors);

            var d_i: usize = 0;
            var r_i: usize = 0;
            for (message_order, 0..) |blk, pos| {
                switch (blk.header) {
                    .dictionary_batch => {
                        dictionaries[d_i] = blk;
                        d_i += 1;
                    },
                    .record_batch => {
                        record_batches[r_i] = blk;
                        record_message_positions[r_i] = pos;
                        r_i += 1;
                    },
                    else => return error.InvalidFile,
                }
            }

            return .{
                .dictionaries = dictionaries,
                .record_batches = record_batches,
                .tensors = tensors,
                .sparse_tensors = sparse_tensors,
                .message_order = message_order,
                .record_message_positions = record_message_positions,
            };
        }

        fn collectTensorBlocksFromCustomMetadata(
            self: *Self,
            footer_t: fbs.FooterT,
            header_len: usize,
            footer_start: usize,
            expected_header: MessageHeaderKind,
            meta_key: []const u8,
        ) (FileError || ReaderIoError)![]IndexedBlock {
            var blocks = std.ArrayList(IndexedBlock){};
            errdefer blocks.deinit(self.allocator);

            for (footer_t.custom_metadata.items) |kv| {
                if (!std.mem.eql(u8, kv.key, meta_key)) continue;
                const value = kv.value;

                var it = std.mem.splitScalar(u8, value, ',');
                while (it.next()) |entry| {
                    if (entry.len == 0) continue;
                    var parts = std.mem.splitScalar(u8, entry, ':');
                    const off_str = parts.next() orelse return error.InvalidFile;
                    const meta_str = parts.next() orelse return error.InvalidFile;
                    const body_str = parts.next() orelse return error.InvalidFile;
                    if (parts.next() != null) return error.InvalidFile;

                    const fb_block = fbs.BlockT{
                        .offset = std.fmt.parseInt(i64, off_str, 10) catch return error.InvalidFile,
                        .metaDataLength = std.fmt.parseInt(i32, meta_str, 10) catch return error.InvalidFile,
                        .bodyLength = std.fmt.parseInt(i64, body_str, 10) catch return error.InvalidFile,
                    };
                    try self.appendCheckedBlock(&blocks, fb_block, expected_header, header_len, footer_start);
                }
                break;
            }

            return try blocks.toOwnedSlice(self.allocator);
        }

        fn collectIndexedBlocks(
            self: *Self,
            footer_t: fbs.FooterT,
            header_len: usize,
            footer_start: usize,
        ) (FileError || ReaderIoError)!std.ArrayList(IndexedBlock) {
            const total_blocks = std.math.add(usize, footer_t.dictionaries.items.len, footer_t.recordBatches.items.len) catch return error.InvalidFile;
            var blocks = try std.ArrayList(IndexedBlock).initCapacity(self.allocator, total_blocks);
            errdefer blocks.deinit(self.allocator);

            for (footer_t.dictionaries.items) |block| {
                try self.appendCheckedBlock(&blocks, block, .dictionary_batch, header_len, footer_start);
            }
            for (footer_t.recordBatches.items) |block| {
                try self.appendCheckedBlock(&blocks, block, .record_batch, header_len, footer_start);
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
                prev_end = std.math.add(usize, blk.offset, blk.parsed.total_len) catch return error.InvalidFile;
                have_prev = true;
            }

            return blocks;
        }

        fn appendCheckedBlock(
            self: *Self,
            blocks: *std.ArrayList(IndexedBlock),
            block: fbs.BlockT,
            expected_header: MessageHeaderKind,
            header_len: usize,
            footer_start: usize,
        ) (FileError || ReaderIoError)!void {
            if (block.offset < 0) return error.InvalidFile;
            const offset = std.math.cast(usize, block.offset) orelse return error.InvalidFile;
            if (offset < header_len or offset >= footer_start) return error.InvalidFile;

            if (block.metaDataLength <= 0) return error.InvalidFile;
            const expected_meta_len = std.math.cast(usize, block.metaDataLength) orelse return error.InvalidFile;
            if (block.bodyLength < 0) return error.InvalidFile;
            const expected_body_len = std.math.cast(usize, block.bodyLength) orelse return error.InvalidFile;

            const parsed = try self.parseMessageAt(offset, footer_start);
            if (parsed.meta_len != expected_meta_len) return error.InvalidFile;
            if (parsed.body_len != expected_body_len) return error.InvalidFile;
            if (parsed.header != expected_header) return error.InvalidFile;

            try blocks.append(self.allocator, .{
                .offset = offset,
                .parsed = parsed,
                .header = expected_header,
            });
        }

        fn parseMessageAt(self: *Self, offset: usize, limit: usize) (FileError || ReaderIoError)!ParsedMessage {
            if (offset >= limit) return error.InvalidFile;

            if (limit - offset < 4) return error.InvalidFile;
            var first_buf: [4]u8 = undefined;
            try self.readAtNoEof(offset, first_buf[0..]);
            const first = readU32Le(first_buf[0..]);

            var prefix_len: usize = 4;
            var metadata_len_u32 = first;
            if (first == format.ContinuationMarker) {
                if (limit - offset < 8) return error.InvalidFile;
                var second_buf: [4]u8 = undefined;
                try self.readAtNoEof(offset + 4, second_buf[0..]);
                metadata_len_u32 = readU32Le(second_buf[0..]);
                prefix_len = 8;
                if (metadata_len_u32 == 0) return error.InvalidFile;
            } else if (first == 0) {
                return error.InvalidFile;
            }

            const metadata_len = std.math.cast(usize, metadata_len_u32) orelse return error.InvalidFile;
            const metadata_start = std.math.add(usize, offset, prefix_len) catch return error.InvalidFile;
            const metadata_end = std.math.add(usize, metadata_start, metadata_len) catch return error.InvalidFile;
            if (metadata_end > limit) return error.InvalidFile;

            const metadata = try self.allocator.alloc(u8, metadata_len);
            defer self.allocator.free(metadata);
            try self.readAtNoEof(metadata_start, metadata);
            const envelope = fbs_lite_verify.parseArrowMessageEnvelope(metadata) catch return error.InvalidFile;
            if (envelope.body_length < 0) return error.InvalidFile;
            const body_len = std.math.cast(usize, envelope.body_length) orelse return error.InvalidFile;
            const total_len = std.math.add(usize, prefix_len + metadata_len, body_len) catch return error.InvalidFile;
            const end = std.math.add(usize, offset, total_len) catch return error.InvalidFile;
            if (end > limit) return error.InvalidFile;

            const header = messageHeaderFromTypeTag(envelope.header_type);
            return .{
                .meta_len = prefix_len + metadata_len,
                .body_len = body_len,
                .total_len = total_len,
                .metadata_start = metadata_start,
                .metadata_end = metadata_end,
                .body_start = metadata_end,
                .body_end = end,
                .header = header,
            };
        }
    };
}

const MessageHeaderKind = enum {
    schema,
    dictionary_batch,
    record_batch,
    tensor,
    sparse_tensor,
    other,
};

const ParsedMessage = struct {
    meta_len: usize,
    body_len: usize,
    total_len: usize,
    metadata_start: usize,
    metadata_end: usize,
    body_start: usize,
    body_end: usize,
    header: MessageHeaderKind,
};

const IndexedBlock = struct {
    offset: usize,
    parsed: ParsedMessage,
    header: MessageHeaderKind,
};

fn messageHeaderFromTypeTag(header_type: u8) MessageHeaderKind {
    if (header_type == @intFromEnum(fbs.MessageHeader.Schema)) return .schema;
    if (header_type == @intFromEnum(fbs.MessageHeader.DictionaryBatch)) return .dictionary_batch;
    if (header_type == @intFromEnum(fbs.MessageHeader.RecordBatch)) return .record_batch;
    if (header_type == @intFromEnum(fbs.MessageHeader.Tensor)) return .tensor;
    if (header_type == @intFromEnum(fbs.MessageHeader.SparseTensor)) return .sparse_tensor;
    return .other;
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

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{ ids, names });
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

test "ipc file reader supports indexed record batch access" {
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

    var b1_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer b1_builder.deinit();
    try b1_builder.append(1);
    try b1_builder.append(2);
    var b1_ids = try b1_builder.finish();
    defer b1_ids.release();
    var batch1 = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{b1_ids});
    defer batch1.deinit();

    var b2_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer b2_builder.deinit();
    try b2_builder.append(3);
    try b2_builder.append(4);
    var b2_ids = try b2_builder.finish();
    defer b2_ids.release();
    var batch2 = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{b2_ids});
    defer batch2.deinit();

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
    try fw.writeRecordBatch(batch1);
    try fw.writeRecordBatch(batch2);
    try fw.writeEnd();

    var fixed = std.io.fixedBufferStream(file_bytes.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    _ = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 2), try fr.recordBatchCount());

    var second = try fr.readRecordBatchAt(1);
    defer second.deinit();
    const second_ids = prim.PrimitiveArray(i32){ .data = second.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 2), second.numRows());
    try std.testing.expectEqual(@as(i32, 3), second_ids.value(0));
    try std.testing.expectEqual(@as(i32, 4), second_ids.value(1));

    var first = try fr.readRecordBatchAt(0);
    defer first.deinit();
    const first_ids = prim.PrimitiveArray(i32){ .data = first.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 2), first.numRows());
    try std.testing.expectEqual(@as(i32, 1), first_ids.value(0));
    try std.testing.expectEqual(@as(i32, 2), first_ids.value(1));
}

test "ipc file reader rebuilds dictionary state for indexed reads" {
    const allocator = std.testing.allocator;

    const zarray = @import("../array/array_ref.zig");
    const dict_arr = @import("../array/dictionary_array.zig");
    const str = @import("../array/string_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    const value_type = DataType{ .string = {} };
    const dict_type = DataType{
        .dictionary = .{
            .id = null,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
            .ordered = false,
        },
    };
    const fields = [_]Field{
        .{ .name = "color", .data_type = &dict_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var dict_values_builder_1 = try str.StringBuilder.init(allocator, 2, 8);
    defer dict_values_builder_1.deinit();
    try dict_values_builder_1.append("red");
    try dict_values_builder_1.append("blue");
    var dict_values_1 = try dict_values_builder_1.finish();
    defer dict_values_1.release();

    var dict_builder_1 = try dict_arr.DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        2,
    );
    defer dict_builder_1.deinit();
    try dict_builder_1.appendIndex(1);
    try dict_builder_1.appendIndex(0);
    var dict_col_1 = try dict_builder_1.finish(dict_values_1);
    defer dict_col_1.release();
    var batch_1 = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{dict_col_1});
    defer batch_1.deinit();

    var dict_values_builder_2 = try str.StringBuilder.init(allocator, 2, 8);
    defer dict_values_builder_2.deinit();
    try dict_values_builder_2.append("red");
    try dict_values_builder_2.append("blue");
    var dict_values_2 = try dict_values_builder_2.finish();
    defer dict_values_2.release();

    var dict_builder_2 = try dict_arr.DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        1,
    );
    defer dict_builder_2.deinit();
    try dict_builder_2.appendIndex(0);
    var dict_col_2 = try dict_builder_2.finish(dict_values_2);
    defer dict_col_2.release();
    var batch_2 = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{dict_col_2});
    defer batch_2.deinit();

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
    try fw.writeRecordBatch(batch_1);
    try fw.writeRecordBatch(batch_2);
    try fw.writeEnd();

    var fixed = std.io.fixedBufferStream(file_bytes.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    _ = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 2), try fr.recordBatchCount());

    var second = try fr.readRecordBatchAt(1);
    defer second.deinit();
    const second_dict = dict_arr.DictionaryArray{ .data = second.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 1), second_dict.len());
    try std.testing.expectEqual(@as(i64, 0), second_dict.index(0));
    const second_values = str.StringArray{ .data = second_dict.dictionaryRef().data() };
    try std.testing.expectEqualStrings("red", second_values.value(0));
    try std.testing.expectEqualStrings("blue", second_values.value(1));

    var first = try fr.readRecordBatchAt(0);
    defer first.deinit();
    const first_dict = dict_arr.DictionaryArray{ .data = first.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 2), first_dict.len());
    try std.testing.expectEqual(@as(i64, 1), first_dict.index(0));
    try std.testing.expectEqual(@as(i64, 0), first_dict.index(1));
    const first_values = str.StringArray{ .data = first_dict.dictionaryRef().data() };
    try std.testing.expectEqualStrings("red", first_values.value(0));
    try std.testing.expectEqualStrings("blue", first_values.value(1));
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

    var batch = try record_batch.RecordBatch.initBorrowed(allocator, schema, &[_]@import("../array/array_ref.zig").ArrayRef{ids});
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
    var footer_t = try fbs_lite_builder.unpackFooter(allocator, footer_bytes_orig);
    defer footer_t.deinit(allocator);

    // Set the first recordBatch block offset to a value beyond the file end.
    try std.testing.expect(footer_t.recordBatches.items.len > 0);
    footer_t.recordBatches.items[0].offset = @intCast(file_bytes.items.len + 9999);

    const new_footer_bytes = try fbs_lite_builder.packFooterBytes(allocator, footer_t);
    defer allocator.free(new_footer_bytes);

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

test "ipc file reader decodes using footer block index without stream reconstruction" {
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

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{ids});
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
    var footer_t = try fbs_lite_builder.unpackFooter(allocator, footer_bytes_orig);
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

    const footer_bytes_new = try fbs_lite_builder.packFooterBytes(allocator, footer_t);
    defer allocator.free(footer_bytes_new);

    var rewritten = std.ArrayList(u8){};
    defer rewritten.deinit(allocator);
    try rewritten.appendSlice(allocator, shifted_prefix.items);
    try rewritten.appendSlice(allocator, footer_bytes_new);
    var new_footer_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &new_footer_len, @intCast(footer_bytes_new.len), .little);
    try rewritten.appendSlice(allocator, new_footer_len[0..]);
    try rewritten.appendSlice(allocator, FileMagic);

    // Reader should succeed because it decodes directly from footer-indexed blocks.
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

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]zarray.ArrayRef{ids});
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

    var footer_t = try fbs_lite_builder.unpackFooter(allocator, footer_bytes_orig);
    defer footer_t.deinit(allocator);

    const header_len = FileMagic.len + 2;
    const lead_pad_len: i64 = 56;
    for (footer_t.dictionaries.items) |*blk| blk.offset += lead_pad_len;
    for (footer_t.recordBatches.items) |*blk| blk.offset += lead_pad_len;

    const footer_bytes_new = try fbs_lite_builder.packFooterBytes(allocator, footer_t);
    defer allocator.free(footer_bytes_new);

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

test "ipc file roundtrips tensor and sparse tensor messages" {
    const allocator = std.testing.allocator;
    const DataType = @import("../datatype.zig").DataType;

    // Build tensor body: 2×3 i32 matrix [1,2,3,4,5,6] row-major.
    var tensor_body: [24]u8 = undefined;
    for (0..6) |i| {
        std.mem.writeInt(i32, tensor_body[i * 4 ..][0..4], @intCast(i + 1), .little);
    }
    const tensor_meta = stream_reader.TensorLikeMetadata{
        .tensor = .{
            .value_type = DataType{ .int32 = {} },
            .shape = &.{
                .{ .size = 2, .name = "rows" },
                .{ .size = 3, .name = "cols" },
            },
            .strides = &.{ 12, 4 },
            .data = .{ .offset = 0, .length = tensor_body.len },
        },
    };

    // Build sparse COO body: indices (0,1),(1,2) then values 10,20.
    var sparse_body: [40]u8 = undefined;
    std.mem.writeInt(i64, sparse_body[0..8], 0, .little);
    std.mem.writeInt(i64, sparse_body[8..16], 1, .little);
    std.mem.writeInt(i64, sparse_body[16..24], 1, .little);
    std.mem.writeInt(i64, sparse_body[24..32], 2, .little);
    std.mem.writeInt(i32, sparse_body[32..36], 10, .little);
    std.mem.writeInt(i32, sparse_body[36..40], 20, .little);
    const sparse_meta = stream_reader.TensorLikeMetadata{
        .sparse_tensor = .{
            .value_type = DataType{ .int32 = {} },
            .shape = &.{
                .{ .size = 2, .name = "rows" },
                .{ .size = 3, .name = "cols" },
            },
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

    const id_type = @import("../datatype.zig").DataType{ .int32 = {} };
    const fields = [_]@import("../datatype.zig").Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema_val = Schema{ .fields = fields[0..] };

    // Write file.
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
    try fw.writeSchema(schema_val);
    try fw.writeTensorLikeMessage(tensor_meta, tensor_body[0..]);
    try fw.writeTensorLikeMessage(sparse_meta, sparse_body[0..]);
    try fw.writeEnd();

    // Read back.
    var fixed = std.io.fixedBufferStream(file_bytes.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    _ = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 0), try fr.recordBatchCount());
    try std.testing.expectEqual(@as(usize, 1), try fr.tensorCount());
    try std.testing.expectEqual(@as(usize, 1), try fr.sparseTensorCount());

    // Verify tensor.
    var t_msg = try fr.readTensorAt(0);
    defer t_msg.deinit();
    const t_meta = t_msg.metadata.tensor;
    try std.testing.expectEqual(@as(usize, 2), t_meta.shape.len);
    try std.testing.expectEqual(@as(i64, 2), t_meta.shape[0].size);
    try std.testing.expectEqual(@as(i64, 3), t_meta.shape[1].size);
    try std.testing.expectEqual(@as(usize, 24), t_meta.data.length);
    const t_bytes = t_meta.data.bytes(t_msg.body.data);
    try std.testing.expectEqual(@as(i32, 1), std.mem.readInt(i32, t_bytes[0..4], .little));
    try std.testing.expectEqual(@as(i32, 6), std.mem.readInt(i32, t_bytes[20..24], .little));

    // Verify sparse tensor.
    var s_msg = try fr.readSparseTensorAt(0);
    defer s_msg.deinit();
    const s_meta = s_msg.metadata.sparse_tensor;
    try std.testing.expectEqual(@as(usize, 2), s_meta.non_zero_length);
    const s_bytes = s_meta.data.bytes(s_msg.body.data);
    try std.testing.expectEqual(@as(i32, 10), std.mem.readInt(i32, s_bytes[0..4], .little));
    try std.testing.expectEqual(@as(i32, 20), std.mem.readInt(i32, s_bytes[4..8], .little));
}
