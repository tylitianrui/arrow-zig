const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("../array/array_ref.zig");
const array_data = @import("../array/array_data.zig");
const format = @import("format.zig");
const fb = @import("flatbufferz");
const arrow_fbs = @import("arrow_fbs");

pub const StreamError = format.StreamError;

pub const Schema = schema_mod.Schema;
pub const Field = datatype.Field;
pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref.ArrayRef;
pub const ArrayData = array_data.ArrayData;
pub const RecordBatch = record_batch.RecordBatch;
pub const OwnedBuffer = buffer.OwnedBuffer;

const fbs = struct {
    const Message = arrow_fbs.org_apache_arrow_flatbuf_Message.Message;
    const MessageT = arrow_fbs.org_apache_arrow_flatbuf_Message.MessageT;
    const MessageHeader = arrow_fbs.org_apache_arrow_flatbuf_MessageHeader.MessageHeader;
    const MessageHeaderT = arrow_fbs.org_apache_arrow_flatbuf_MessageHeader.MessageHeaderT;
    const MetadataVersion = arrow_fbs.org_apache_arrow_flatbuf_MetadataVersion.MetadataVersion;
    const SchemaT = arrow_fbs.org_apache_arrow_flatbuf_Schema.SchemaT;
    const FieldT = arrow_fbs.org_apache_arrow_flatbuf_Field.FieldT;
    const KeyValueT = arrow_fbs.org_apache_arrow_flatbuf_KeyValue.KeyValueT;
    const TypeT = arrow_fbs.org_apache_arrow_flatbuf_Type.TypeT;
    const DictionaryEncodingT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryEncoding.DictionaryEncodingT;
    const Endianness = arrow_fbs.org_apache_arrow_flatbuf_Endianness.Endianness;
    const RecordBatchT = arrow_fbs.org_apache_arrow_flatbuf_RecordBatch.RecordBatchT;
    const FieldNodeT = arrow_fbs.org_apache_arrow_flatbuf_FieldNode.FieldNodeT;
    const BufferT = arrow_fbs.org_apache_arrow_flatbuf_Buffer.BufferT;
    const IntT = arrow_fbs.org_apache_arrow_flatbuf_Int.IntT;
    const FloatingPointT = arrow_fbs.org_apache_arrow_flatbuf_FloatingPoint.FloatingPointT;
    const Precision = arrow_fbs.org_apache_arrow_flatbuf_Precision.Precision;
    const FixedSizeBinaryT = arrow_fbs.org_apache_arrow_flatbuf_FixedSizeBinary.FixedSizeBinaryT;
    const FixedSizeListT = arrow_fbs.org_apache_arrow_flatbuf_FixedSizeList.FixedSizeListT;
    const MapT = arrow_fbs.org_apache_arrow_flatbuf_Map.MapT;
    const BoolT = arrow_fbs.org_apache_arrow_flatbuf_Bool.BoolT;
    const BinaryT = arrow_fbs.org_apache_arrow_flatbuf_Binary.BinaryT;
    const Utf8T = arrow_fbs.org_apache_arrow_flatbuf_Utf8.Utf8T;
    const LargeBinaryT = arrow_fbs.org_apache_arrow_flatbuf_LargeBinary.LargeBinaryT;
    const LargeUtf8T = arrow_fbs.org_apache_arrow_flatbuf_LargeUtf8.LargeUtf8T;
    const ListT = arrow_fbs.org_apache_arrow_flatbuf_List.ListT;
    const LargeListT = arrow_fbs.org_apache_arrow_flatbuf_LargeList.LargeListT;
    const Struct_T = arrow_fbs.org_apache_arrow_flatbuf_Struct_.Struct_T;
    const NullT = arrow_fbs.org_apache_arrow_flatbuf_Null.NullT;
};

pub const OwnedSchema = struct {
    arena: std.heap.ArenaAllocator,
    schema: Schema,

    pub fn deinit(self: *OwnedSchema) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *OwnedSchema) std.mem.Allocator {
        return self.arena.allocator();
    }
};

pub fn StreamReader(comptime ReaderType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        schema_owned: ?OwnedSchema = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return .{ .allocator = allocator, .reader = reader };
        }

        pub fn deinit(self: *Self) void {
            if (self.schema_owned) |*owned| owned.deinit();
            self.schema_owned = null;
        }

        pub fn readSchema(self: *Self) (StreamError || array_data.ValidationError || record_batch.RecordBatchError || fb.common.PackError || @TypeOf(self.reader).Error || error{ EndOfStream, OutOfMemory })!Schema {
            var msg = try readMessage(self.*);
            defer msg.deinit(self.allocator);
            if (msg.msg.header != .Schema) return StreamError.InvalidMessage;

            var arena = std.heap.ArenaAllocator.init(self.allocator);
            errdefer arena.deinit();
            const schema = try buildSchemaFromFlatbuf(arena.allocator(), msg.msg.header.Schema.?);

            if (self.schema_owned) |*owned| owned.deinit();
            self.schema_owned = .{ .arena = arena, .schema = schema };
            return schema;
        }

        pub fn nextRecordBatch(self: *Self) (StreamError || array_data.ValidationError || record_batch.RecordBatchError || fb.common.PackError || @TypeOf(self.reader).Error || error{ EndOfStream, OutOfMemory })!?RecordBatch {
            if (self.schema_owned == null) return StreamError.SchemaNotRead;

            const maybe_msg = try readMessageOptional(self.*);
            if (maybe_msg == null) return null;
            var msg = maybe_msg.?;
            defer msg.deinit(self.allocator);

            if (msg.msg.header != .RecordBatch) return StreamError.InvalidMessage;
            if (msg.body == null) return StreamError.InvalidBody;

            const schema = self.schema_owned.?.schema;
            const batch = try buildRecordBatchFromFlatbuf(self.allocator, schema, msg.msg.header.RecordBatch.?, msg.body.?);
            return batch;
        }
    };
}

const MessageWithBody = struct {
    metadata: array_data.SharedBuffer,
    msg: fbs.MessageT,
    body_len: usize,
    body: ?array_data.SharedBuffer,

    fn deinit(self: *MessageWithBody, allocator: std.mem.Allocator) void {
        self.msg.deinit(allocator);
        self.metadata.release();
        if (self.body) |*buf| buf.release();
    }
};

fn readMessageOptional(self: anytype) (StreamError || fb.common.PackError || @TypeOf(self.reader).Error || error{ EndOfStream, OutOfMemory })!?MessageWithBody {
    const meta_len_opt = try format.readMessageLength(self.reader);
    if (meta_len_opt == null) return null;
    const meta_len = meta_len_opt.?;

    var meta_buf_owned = try OwnedBuffer.init(self.allocator, meta_len);
    errdefer meta_buf_owned.deinit();
    if (meta_len > 0) try self.reader.readNoEof(meta_buf_owned.data[0..meta_len]);
    var metadata = try meta_buf_owned.toShared(meta_len);
    errdefer metadata.release();
    try format.skipPadding(self.reader, format.padLen(meta_len));

    const msg = fbs.Message.GetRootAs(@constCast(metadata.data), 0);
    const opts: fb.common.PackOptions = .{ .allocator = self.allocator };
    const msg_t = try fbs.MessageT.Unpack(msg, opts);
    if (msg_t.version != .V5 and msg_t.version != .V4) {
        var tmp = msg_t;
        tmp.deinit(self.allocator);
        metadata.release();
        return StreamError.InvalidMetadata;
    }

    var body_shared: ?array_data.SharedBuffer = null;
    const body_len: usize = @intCast(msg_t.bodyLength);
    if (body_len > 0) {
        var body_buf = try OwnedBuffer.init(self.allocator, body_len);
        errdefer body_buf.deinit();
        try self.reader.readNoEof(body_buf.data[0..body_len]);
        body_shared = try body_buf.toShared(body_len);
    }
    try format.skipPadding(self.reader, format.padLen(body_len));

    return .{ .metadata = metadata, .msg = msg_t, .body_len = body_len, .body = body_shared };
}

fn readMessage(self: anytype) (StreamError || fb.common.PackError || @TypeOf(self.reader).Error || error{ EndOfStream, OutOfMemory })!MessageWithBody {
    const msg_opt = try readMessageOptional(self);
    if (msg_opt == null) return StreamError.InvalidMessage;
    return msg_opt.?;
}

fn buildSchemaFromFlatbuf(allocator: std.mem.Allocator, schema_t: *fbs.SchemaT) (StreamError || error{OutOfMemory})!Schema {
    if (schema_t.endianness != .Little) return StreamError.UnsupportedType;
    const fields = try allocator.alloc(Field, schema_t.fields.items.len);
    for (schema_t.fields.items, 0..) |field_t, i| {
        fields[i] = try buildFieldFromFlatbuf(allocator, field_t);
    }
    return .{
        .fields = fields,
        .endianness = .little,
        .metadata = try buildMetadataFromFlatbuf(allocator, schema_t.custom_metadata.items),
    };
}

fn buildFieldFromFlatbuf(allocator: std.mem.Allocator, field_t: fbs.FieldT) (StreamError || error{OutOfMemory})!Field {
    const name = try allocator.dupe(u8, field_t.name);
    const dtype = try buildDataTypeFromFlatbuf(allocator, field_t);
    const dtype_ptr = try allocator.create(DataType);
    dtype_ptr.* = dtype;
    return .{
        .name = name,
        .data_type = dtype_ptr,
        .nullable = field_t.nullable,
        .metadata = try buildMetadataFromFlatbuf(allocator, field_t.custom_metadata.items),
    };
}

fn buildMetadataFromFlatbuf(allocator: std.mem.Allocator, metadata_t: []const fbs.KeyValueT) error{OutOfMemory}!?[]const datatype.KeyValue {
    if (metadata_t.len == 0) return null;

    const out = try allocator.alloc(datatype.KeyValue, metadata_t.len);
    for (metadata_t, 0..) |entry, i| {
        out[i] = .{
            .key = try allocator.dupe(u8, entry.key),
            .value = try allocator.dupe(u8, entry.value),
        };
    }
    return out;
}

fn buildDataTypeFromFlatbuf(allocator: std.mem.Allocator, field_t: fbs.FieldT) (StreamError || error{OutOfMemory})!DataType {
    if (field_t.type == .NONE) return StreamError.UnsupportedType;

    const dtype = switch (field_t.type) {
        .NONE => return StreamError.UnsupportedType,
        .Null => DataType{ .null = {} },
        .Bool => DataType{ .bool = {} },
        .Int => blk: {
            const int_t = field_t.type.Int.?;
            const signed = int_t.is_signed;
            const width = int_t.bitWidth;
            break :blk switch (width) {
                8 => if (signed) DataType{ .int8 = {} } else DataType{ .uint8 = {} },
                16 => if (signed) DataType{ .int16 = {} } else DataType{ .uint16 = {} },
                32 => if (signed) DataType{ .int32 = {} } else DataType{ .uint32 = {} },
                64 => if (signed) DataType{ .int64 = {} } else DataType{ .uint64 = {} },
                else => return StreamError.UnsupportedType,
            };
        },
        .FloatingPoint => blk: {
            const fp = field_t.type.FloatingPoint.?;
            break :blk switch (fp.precision) {
                .HALF => DataType{ .half_float = {} },
                .SINGLE => DataType{ .float = {} },
                .DOUBLE => DataType{ .double = {} },
            };
        },
        .Utf8 => DataType{ .string = {} },
        .Binary => DataType{ .binary = {} },
        .LargeUtf8 => DataType{ .large_string = {} },
        .LargeBinary => DataType{ .large_binary = {} },
        .List => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .list = .{ .value_field = child_field } };
        },
        .LargeList => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .large_list = .{ .value_field = child_field } };
        },
        .FixedSizeList => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const list_size = field_t.type.FixedSizeList.?.listSize;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .fixed_size_list = .{ .value_field = child_field, .list_size = list_size } };
        },
        .FixedSizeBinary => DataType{ .fixed_size_binary = .{ .byte_width = field_t.type.FixedSizeBinary.?.byteWidth } },
        .Struct_ => blk: {
            const child_fields = try allocator.alloc(Field, field_t.children.items.len);
            for (field_t.children.items, 0..) |child, i| {
                child_fields[i] = try buildFieldFromFlatbuf(allocator, child);
            }
            break :blk DataType{ .struct_ = .{ .fields = child_fields } };
        },
        .Decimal,
        .Date,
        .Time,
        .Timestamp,
        .Interval,
        .Union,
        .Map,
        .Duration,
        .RunEndEncoded,
        .BinaryView,
        .Utf8View,
        .ListView,
        .LargeListView,
        => return StreamError.UnsupportedType,
    };

    if (field_t.dictionary) |dict_t| {
        const index_type = dict_t.indexType orelse return StreamError.UnsupportedType;
        const index = datatype.IntType{ .bit_width = @intCast(index_type.bitWidth), .signed = index_type.is_signed };
        const value_ptr = try allocator.create(DataType);
        value_ptr.* = dtype;
        return DataType{ .dictionary = .{ .index_type = index, .value_type = value_ptr, .ordered = dict_t.isOrdered } };
    }

    return dtype;
}

fn buildRecordBatchFromFlatbuf(
    allocator: std.mem.Allocator,
    schema: Schema,
    record_batch_t: *fbs.RecordBatchT,
    body: array_data.SharedBuffer,
) (StreamError || array_data.ValidationError || record_batch.RecordBatchError || error{OutOfMemory})!RecordBatch {
    if (record_batch_t.variadicBufferCounts.items.len != 0) return StreamError.UnsupportedType;

    const columns = try allocator.alloc(ArrayRef, schema.fields.len);
    var col_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < col_count) : (i += 1) columns[i].release();
        allocator.free(columns);
    }

    var node_index: usize = 0;
    var buffer_index: usize = 0;
    for (schema.fields, 0..) |field, i| {
        columns[i] = try readArrayFromMeta(
            allocator,
            field.data_type.*,
            record_batch_t.nodes.items,
            record_batch_t.buffers.items,
            body,
            &node_index,
            &buffer_index,
        );
        col_count += 1;
    }

    const batch = try RecordBatch.init(allocator, schema, columns);
    var i: usize = 0;
    while (i < col_count) : (i += 1) columns[i].release();
    allocator.free(columns);
    if (batch.numRows() != @as(usize, @intCast(record_batch_t.length))) return StreamError.InvalidMetadata;
    return batch;
}

fn readArrayFromMeta(
    allocator: std.mem.Allocator,
    dt: DataType,
    nodes: []const fbs.FieldNodeT,
    buffers_meta: []const fbs.BufferT,
    body: array_data.SharedBuffer,
    node_index: *usize,
    buffer_index: *usize,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    if (node_index.* >= nodes.len) return StreamError.InvalidMetadata;
    const node = nodes[node_index.*];
    node_index.* += 1;

    const buffer_count = try bufferCountForType(dt);
    const buffers = try allocator.alloc(array_data.SharedBuffer, buffer_count);
    var buf_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < buf_count) : (i += 1) {
            var owned = buffers[i];
            owned.release();
        }
        allocator.free(buffers);
    }

    var i: usize = 0;
    while (i < buffer_count) : (i += 1) {
        if (buffer_index.* >= buffers_meta.len) return StreamError.InvalidMetadata;
        const meta = buffers_meta[buffer_index.*];
        buffer_index.* += 1;
        const start = @as(usize, @intCast(meta.offset));
        const end = start + @as(usize, @intCast(meta.length));
        if (end > body.len()) return StreamError.InvalidBody;
        buffers[i] = if (meta.length == 0) array_data.SharedBuffer.empty else body.slice(start, end);
        buf_count += 1;
    }

    var children: []ArrayRef = &.{};
    if (dt == .list or dt == .large_list or dt == .fixed_size_list) {
        children = try allocator.alloc(ArrayRef, 1);
        errdefer allocator.free(children);
        children[0] = try readArrayFromMeta(allocator, childValueType(dt), nodes, buffers_meta, body, node_index, buffer_index);
    } else if (dt == .struct_) {
        const field_count = dt.struct_.fields.len;
        children = try allocator.alloc(ArrayRef, field_count);
        var filled: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < filled) : (j += 1) children[j].release();
            allocator.free(children);
        }
        var idx: usize = 0;
        while (idx < field_count) : (idx += 1) {
            children[idx] = try readArrayFromMeta(allocator, dt.struct_.fields[idx].data_type.*, nodes, buffers_meta, body, node_index, buffer_index);
            filled += 1;
        }
    } else if (dt == .dictionary) {
        return StreamError.UnsupportedType;
    }

    const data = ArrayData{
        .data_type = dt,
        .length = @intCast(node.length),
        .null_count = @intCast(node.null_count),
        .buffers = buffers,
        .children = children,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn childValueType(dt: DataType) DataType {
    return switch (dt) {
        .list => |lst| lst.value_field.data_type.*,
        .large_list => |lst| lst.value_field.data_type.*,
        .fixed_size_list => |lst| lst.value_field.data_type.*,
        else => dt,
    };
}

fn bufferCountForType(dt: DataType) StreamError!usize {
    return switch (dt) {
        .null => 0,
        .struct_, .fixed_size_list => 1,
        .list, .large_list, .map => 2,
        .string, .binary, .large_string, .large_binary => 3,
        .bool, .uint8, .int8, .uint16, .int16, .uint32, .int32, .uint64, .int64, .half_float, .float, .double, .fixed_size_binary => 2,
        .dictionary => StreamError.UnsupportedType,
        else => StreamError.UnsupportedType,
    };
}

fn expectMetadataEntry(metadata: []const datatype.KeyValue, key: []const u8, value: []const u8) !void {
    for (metadata) |entry| {
        if (std.mem.eql(u8, entry.key, key) and std.mem.eql(u8, entry.value, value)) return;
    }
    return error.MetadataEntryMissing;
}

test "ipc stream roundtrip schema and batch" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const str_type = DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "name", .data_type = &str_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var int_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(1);
    try int_builder.append(2);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var str_builder = try @import("../array/string_array.zig").StringBuilder.init(allocator, 2, 8);
    defer str_builder.deinit();
    try str_builder.append("a");
    try str_builder.appendNull();
    var str_ref = try str_builder.finish();
    defer str_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ int_ref, str_ref });
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expectEqual(@as(usize, 2), out_schema.fields.len);
    try std.testing.expect(std.mem.eql(u8, out_schema.fields[0].name, "id"));
    try std.testing.expect(std.mem.eql(u8, out_schema.fields[1].name, "name"));

    const batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(batch_opt != null);
    var out_batch = batch_opt.?;
    defer out_batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), out_batch.numRows());
    const names = @import("../array/string_array.zig").StringArray{ .data = out_batch.columns[1].data() };
    try std.testing.expect(names.isNull(1));
}

test "ipc stream roundtrip large string and binary" {
    const allocator = std.testing.allocator;

    const ls_type = DataType{ .large_string = {} };
    const lb_type = DataType{ .large_binary = {} };
    const fields = [_]Field{
        .{ .name = "ls", .data_type = &ls_type, .nullable = true },
        .{ .name = "lb", .data_type = &lb_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var ls_builder = try @import("../array/string_array.zig").LargeStringBuilder.init(allocator, 2, 8);
    defer ls_builder.deinit();
    try ls_builder.append("hi");
    try ls_builder.appendNull();
    var ls_ref = try ls_builder.finish();
    defer ls_ref.release();

    var lb_builder = try @import("../array/binary_array.zig").LargeBinaryBuilder.init(allocator, 2, 8);
    defer lb_builder.deinit();
    try lb_builder.append("zz");
    try lb_builder.appendNull();
    var lb_ref = try lb_builder.finish();
    defer lb_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ ls_ref, lb_ref });
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    const batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(batch_opt != null);
    var out_batch = batch_opt.?;
    defer out_batch.deinit();

    const ls_view = @import("../array/string_array.zig").LargeStringArray{ .data = out_batch.columns[0].data() };
    const lb_view = @import("../array/binary_array.zig").LargeBinaryArray{ .data = out_batch.columns[1].data() };
    try std.testing.expectEqualStrings("hi", ls_view.value(0));
    try std.testing.expect(ls_view.isNull(1));
    try std.testing.expectEqualStrings("zz", lb_view.value(0));
    try std.testing.expect(lb_view.isNull(1));
}

test "ipc schema roundtrip preserves field and schema metadata" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const field_md = [_]datatype.KeyValue{
        .{ .key = "z", .value = "9" },
        .{ .key = "a", .value = "1" },
    };
    const schema_md = [_]datatype.KeyValue{
        .{ .key = "owner", .value = "core" },
        .{ .key = "version", .value = "1" },
    };
    const fields = [_]Field{
        .{
            .name = "id",
            .data_type = &int_type,
            .nullable = false,
            .metadata = field_md[0..],
        },
    };
    const schema = Schema{
        .fields = fields[0..],
        .metadata = schema_md[0..],
    };

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();
    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    try writer.writeSchema(schema);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.metadata != null);
    try std.testing.expect(out_schema.fields[0].metadata != null);

    const out_schema_md = out_schema.metadata.?;
    try std.testing.expectEqual(@as(usize, 2), out_schema_md.len);
    try std.testing.expectEqualStrings("owner", out_schema_md[0].key);
    try std.testing.expectEqualStrings("core", out_schema_md[0].value);
    try std.testing.expectEqualStrings("version", out_schema_md[1].key);
    try std.testing.expectEqualStrings("1", out_schema_md[1].value);

    const out_field_md = out_schema.fields[0].metadata.?;
    try std.testing.expectEqual(@as(usize, 2), out_field_md.len);
    try std.testing.expectEqualStrings("a", out_field_md[0].key);
    try std.testing.expectEqualStrings("1", out_field_md[0].value);
    try std.testing.expectEqualStrings("z", out_field_md[1].key);
    try std.testing.expectEqualStrings("9", out_field_md[1].value);
}

test "ipc schema metadata serialization is deterministic" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };

    const field_md_a = [_]datatype.KeyValue{
        .{ .key = "beta", .value = "2" },
        .{ .key = "alpha", .value = "1" },
    };
    const field_md_b = [_]datatype.KeyValue{
        .{ .key = "alpha", .value = "1" },
        .{ .key = "beta", .value = "2" },
    };
    const schema_md_a = [_]datatype.KeyValue{
        .{ .key = "z", .value = "9" },
        .{ .key = "a", .value = "0" },
    };
    const schema_md_b = [_]datatype.KeyValue{
        .{ .key = "a", .value = "0" },
        .{ .key = "z", .value = "9" },
    };

    const fields_a = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false, .metadata = field_md_a[0..] },
    };
    const fields_b = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false, .metadata = field_md_b[0..] },
    };
    const schema_a = Schema{ .fields = fields_a[0..], .metadata = schema_md_a[0..] };
    const schema_b = Schema{ .fields = fields_b[0..], .metadata = schema_md_b[0..] };

    var out_a = std.array_list.Managed(u8).init(allocator);
    defer out_a.deinit();
    var out_b = std.array_list.Managed(u8).init(allocator);
    defer out_b.deinit();

    var writer_a = @import("stream_writer.zig").StreamWriter(@TypeOf(out_a.writer())).init(allocator, out_a.writer());
    var writer_b = @import("stream_writer.zig").StreamWriter(@TypeOf(out_b.writer())).init(allocator, out_b.writer());
    try writer_a.writeSchema(schema_a);
    try writer_b.writeSchema(schema_b);

    try std.testing.expectEqualSlices(u8, out_a.items, out_b.items);
}

test "ipc reader accepts pyarrow simple stream fixture" {
    const allocator = std.testing.allocator;
    const data = @embedFile("testdata/pyarrow_simple_stream.arrow");

    var stream = std.io.fixedBufferStream(data);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const schema = try reader.readSchema();
    try std.testing.expectEqual(@as(usize, 2), schema.fields.len);

    const batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(batch_opt != null);
    var batch = batch_opt.?;
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), batch.numRows());
    const names = @import("../array/string_array.zig").StringArray{ .data = batch.columns[1].data() };
    try std.testing.expectEqualStrings("a", names.value(0));
    try std.testing.expect(names.isNull(1));
}

test "ipc reader accepts pyarrow metadata stream fixture" {
    const allocator = std.testing.allocator;
    const data = @embedFile("testdata/pyarrow_metadata_stream.arrow");

    var stream = std.io.fixedBufferStream(data);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const schema = try reader.readSchema();
    try std.testing.expect(schema.metadata != null);
    try std.testing.expect(schema.fields[0].metadata != null);

    const schema_md = schema.metadata.?;
    try expectMetadataEntry(schema_md, "owner", "core");
    try expectMetadataEntry(schema_md, "version", "1");
    try expectMetadataEntry(schema_md, "pad", "padpadpad");

    const field_md = schema.fields[0].metadata.?;
    try expectMetadataEntry(field_md, "alpha", "1");
    try expectMetadataEntry(field_md, "z", "9");
}

test "ipc reader reads pyarrow multi-batch stream fixture" {
    const allocator = std.testing.allocator;
    const data = @embedFile("testdata/pyarrow_multi_batch_stream.arrow");

    var stream = std.io.fixedBufferStream(data);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();

    const first_opt = try reader.nextRecordBatch();
    try std.testing.expect(first_opt != null);
    var first = first_opt.?;
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 3), first.numRows());

    const second_opt = try reader.nextRecordBatch();
    try std.testing.expect(second_opt != null);
    var second = second_opt.?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 2), second.numRows());

    const done = try reader.nextRecordBatch();
    try std.testing.expect(done == null);
}

test "ipc reader handles non-8-aligned body and still reads next real writer message" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    // 1) Real writer output: schema message.
    var real_stream = std.array_list.Managed(u8).init(allocator);
    defer real_stream.deinit();
    var real_writer = @import("stream_writer.zig").StreamWriter(@TypeOf(real_stream.writer())).init(allocator, real_stream.writer());
    try real_writer.writeSchema(schema);
    try real_writer.writeEnd();

    // 2) Build a stream where a valid flatbuffer message with body_len=1 is inserted
    // before the real writer schema message.
    var combined = std.array_list.Managed(u8).init(allocator);
    defer combined.deinit();
    try appendValidTestMessageWithBody(allocator, combined.writer(), 1, 0xAB);
    // Explicitly place Arrow IPC body padding (8-byte alignment) before next message.
    try format.writePadding(combined.writer(), format.padLen(@as(usize, 1)));
    const next_real_message_offset = combined.items.len;
    try combined.appendSlice(real_stream.items);

    // 3) Reader must consume body + body padding and then parse the real schema message.
    var input = std.io.fixedBufferStream(combined.items);
    var reader = StreamReader(@TypeOf(input.reader())).init(allocator, input.reader());
    defer reader.deinit();

    const first_opt = try readMessageOptional(reader);
    try std.testing.expect(first_opt != null);
    var first = first_opt.?;
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), first.body_len);
    try std.testing.expectEqual(next_real_message_offset, input.pos);

    const second_opt = try readMessageOptional(reader);
    try std.testing.expect(second_opt != null);
    var second = second_opt.?;
    defer second.deinit(allocator);
    try std.testing.expect(second.msg.header == .Schema);
}

fn appendValidTestMessageWithBody(allocator: std.mem.Allocator, writer: anytype, body_len: usize, fill: u8) !void {
    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();

    const schema_ptr = try allocator.create(fbs.SchemaT);
    errdefer allocator.destroy(schema_ptr);
    schema_ptr.* = .{
        .endianness = .Little,
        .fields = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
        .features = try std.ArrayList(i64).initCapacity(allocator, 0),
    };

    var msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .Schema = schema_ptr },
        .bodyLength = @intCast(body_len),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg.deinit(allocator);

    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    const msg_off = try fbs.MessageT.Pack(msg, &builder, opts);
    try fbs.Message.FinishBuffer(&builder, msg_off);
    const metadata = try builder.finishedBytes();

    try format.writeMessageLength(writer, @intCast(metadata.len));
    try writer.writeAll(metadata);
    try format.writePadding(writer, format.padLen(metadata.len));

    if (body_len > 0) {
        const body = try allocator.alloc(u8, body_len);
        defer allocator.free(body);
        @memset(body, fill);
        try writer.writeAll(body);
    }
}
