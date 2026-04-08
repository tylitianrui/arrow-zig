const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("../array/array_ref.zig");
const array_data = @import("../array/array_data.zig");
const format = @import("format.zig");
const bitmap = @import("../bitmap.zig");
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
    const DateT = arrow_fbs.org_apache_arrow_flatbuf_Date.DateT;
    const DateUnit = arrow_fbs.org_apache_arrow_flatbuf_DateUnit.DateUnit;
    const TimeT = arrow_fbs.org_apache_arrow_flatbuf_Time.TimeT;
    const TimeUnit = arrow_fbs.org_apache_arrow_flatbuf_TimeUnit.TimeUnit;
    const TimestampT = arrow_fbs.org_apache_arrow_flatbuf_Timestamp.TimestampT;
    const DurationT = arrow_fbs.org_apache_arrow_flatbuf_Duration.DurationT;
    const DecimalT = arrow_fbs.org_apache_arrow_flatbuf_Decimal.DecimalT;
    const IntervalT = arrow_fbs.org_apache_arrow_flatbuf_Interval.IntervalT;
    const IntervalUnit = arrow_fbs.org_apache_arrow_flatbuf_IntervalUnit.IntervalUnit;
    const UnionT = arrow_fbs.org_apache_arrow_flatbuf_Union.UnionT;
    const UnionMode = arrow_fbs.org_apache_arrow_flatbuf_UnionMode.UnionMode;
    const RunEndEncodedT = arrow_fbs.org_apache_arrow_flatbuf_RunEndEncoded.RunEndEncodedT;
    const Endianness = arrow_fbs.org_apache_arrow_flatbuf_Endianness.Endianness;
    const RecordBatchT = arrow_fbs.org_apache_arrow_flatbuf_RecordBatch.RecordBatchT;
    const DictionaryBatchT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryBatch.DictionaryBatchT;
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
    const ListViewT = arrow_fbs.org_apache_arrow_flatbuf_ListView.ListViewT;
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
        dictionary_values: std.AutoHashMap(i64, ArrayRef),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
                .dictionary_values = std.AutoHashMap(i64, ArrayRef).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.dictionary_values.iterator();
            while (it.next()) |entry| {
                var dict = entry.value_ptr.*;
                dict.release();
            }
            self.dictionary_values.deinit();
            if (self.schema_owned) |*owned| owned.deinit();
            self.schema_owned = null;
        }

        fn clearDictionaryValues(self: *Self) void {
            var it = self.dictionary_values.iterator();
            while (it.next()) |entry| {
                var dict = entry.value_ptr.*;
                dict.release();
            }
            self.dictionary_values.clearRetainingCapacity();
        }

        pub fn readSchema(self: *Self) (StreamError || array_data.ValidationError || record_batch.RecordBatchError || fb.common.PackError || @TypeOf(self.reader).Error || error{ EndOfStream, OutOfMemory })!Schema {
            self.clearDictionaryValues();
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

            const schema = self.schema_owned.?.schema;
            while (true) {
                const maybe_msg = try readMessageOptional(self.*);
                if (maybe_msg == null) return null;
                var msg = maybe_msg.?;
                defer msg.deinit(self.allocator);

                switch (msg.msg.header) {
                    .DictionaryBatch => {
                        if (msg.body == null) return StreamError.InvalidBody;
                        try ingestDictionaryBatch(self, schema, msg.msg.header.DictionaryBatch.?, msg.body.?);
                    },
                    .RecordBatch => {
                        if (msg.body == null) return StreamError.InvalidBody;
                        return try buildRecordBatchFromFlatbuf(self.allocator, schema, msg.msg.header.RecordBatch.?, msg.body.?, &self.dictionary_values);
                    },
                    else => return StreamError.InvalidMessage,
                }
            }
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
    const body_len = std.math.cast(usize, msg_t.bodyLength) orelse return StreamError.InvalidBody;
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
            if (list_size < 0) return StreamError.InvalidMetadata;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .fixed_size_list = .{ .value_field = child_field, .list_size = list_size } };
        },
        .ListView => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .list_view = .{ .value_field = child_field } };
        },
        .LargeListView => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const child_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            break :blk DataType{ .large_list_view = .{ .value_field = child_field } };
        },
        .FixedSizeBinary => blk: {
            const byte_width = field_t.type.FixedSizeBinary.?.byteWidth;
            if (byte_width <= 0) return StreamError.InvalidMetadata;
            break :blk DataType{ .fixed_size_binary = .{ .byte_width = byte_width } };
        },
        .Struct_ => blk: {
            const child_fields = try allocator.alloc(Field, field_t.children.items.len);
            for (field_t.children.items, 0..) |child, i| {
                child_fields[i] = try buildFieldFromFlatbuf(allocator, child);
            }
            break :blk DataType{ .struct_ = .{ .fields = child_fields } };
        },
        .Map => blk: {
            if (field_t.children.items.len != 1) return StreamError.InvalidMetadata;
            const entries_meta = field_t.children.items[0];
            if (entries_meta.nullable) return StreamError.InvalidMetadata;
            if (entries_meta.type != .Struct_) return StreamError.InvalidMetadata;
            if (entries_meta.children.items.len != 2) return StreamError.InvalidMetadata;
            if (entries_meta.children.items[0].nullable) return StreamError.InvalidMetadata;
            const entries_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            if (entries_field.data_type.* != .struct_) return StreamError.InvalidMetadata;
            const entry_fields = entries_field.data_type.struct_.fields;
            break :blk DataType{
                .map = .{
                    .key_field = entry_fields[0],
                    .item_field = entry_fields[1],
                    .keys_sorted = field_t.type.Map.?.keysSorted,
                    .entries_type = entries_field.data_type,
                },
            };
        },
        .Union => blk: {
            const union_t = field_t.type.Union.?;
            const child_fields = try allocator.alloc(Field, field_t.children.items.len);
            for (field_t.children.items, 0..) |child, i| {
                child_fields[i] = try buildFieldFromFlatbuf(allocator, child);
            }

            const type_ids = blk_ids: {
                if (union_t.typeIds.items.len > 0) {
                    if (union_t.typeIds.items.len != child_fields.len) return StreamError.InvalidMetadata;
                    const ids = try allocator.alloc(i8, union_t.typeIds.items.len);
                    for (union_t.typeIds.items, 0..) |id, i| {
                        ids[i] = std.math.cast(i8, id) orelse return StreamError.InvalidMetadata;
                    }
                    break :blk_ids ids;
                }
                const ids = try allocator.alloc(i8, child_fields.len);
                for (ids, 0..) |*id, i| id.* = std.math.cast(i8, i) orelse return StreamError.InvalidMetadata;
                break :blk_ids ids;
            };

            const mode = switch (union_t.mode) {
                .Sparse => datatype.UnionMode.sparse,
                .Dense => datatype.UnionMode.dense,
            };
            const union_type = datatype.UnionType{
                .type_ids = type_ids,
                .fields = child_fields,
                .mode = mode,
            };
            break :blk switch (mode) {
                .sparse => DataType{ .sparse_union = union_type },
                .dense => DataType{ .dense_union = union_type },
            };
        },
        .RunEndEncoded => blk: {
            if (field_t.children.items.len != 2) return StreamError.InvalidMetadata;
            const run_end_meta = field_t.children.items[0];
            if (run_end_meta.type != .Int) return StreamError.InvalidMetadata;
            const run_end_int = run_end_meta.type.Int.?;
            if (!run_end_int.is_signed) return StreamError.InvalidMetadata;
            if (run_end_int.bitWidth != 16 and run_end_int.bitWidth != 32 and run_end_int.bitWidth != 64) {
                return StreamError.InvalidMetadata;
            }
            const run_end_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[0]);
            const value_field = try buildFieldFromFlatbuf(allocator, field_t.children.items[1]);
            const run_end_type = try intTypeFromDataType(run_end_field.data_type.*);
            const value_ptr = try allocator.create(DataType);
            value_ptr.* = value_field.data_type.*;
            break :blk DataType{
                .run_end_encoded = .{
                    .run_end_type = run_end_type,
                    .value_type = value_ptr,
                },
            };
        },
        .Date => blk: {
            const date_t = field_t.type.Date.?;
            break :blk switch (date_t.unit) {
                .DAY => DataType{ .date32 = {} },
                .MILLISECOND => DataType{ .date64 = {} },
            };
        },
        .Time => blk: {
            const time_t = field_t.type.Time.?;
            const unit = fromFbsTimeUnit(time_t.unit);
            break :blk switch (time_t.bitWidth) {
                32 => DataType{ .time32 = .{ .unit = unit } },
                64 => DataType{ .time64 = .{ .unit = unit } },
                else => return StreamError.UnsupportedType,
            };
        },
        .Timestamp => blk: {
            const ts_t = field_t.type.Timestamp.?;
            const tz = if (ts_t.timezone.len == 0) null else try allocator.dupe(u8, ts_t.timezone);
            break :blk DataType{
                .timestamp = .{
                    .unit = fromFbsTimeUnit(ts_t.unit),
                    .timezone = tz,
                },
            };
        },
        .Duration => DataType{ .duration = .{ .unit = fromFbsTimeUnit(field_t.type.Duration.?.unit) } },
        .Decimal => blk: {
            const dec_t = field_t.type.Decimal.?;
            const precision = std.math.cast(u8, dec_t.precision) orelse return StreamError.InvalidMetadata;
            const params = datatype.DecimalParams{
                .precision = precision,
                .scale = dec_t.scale,
            };
            break :blk switch (dec_t.bitWidth) {
                32 => DataType{ .decimal32 = params },
                64 => DataType{ .decimal64 = params },
                128 => DataType{ .decimal128 = params },
                256 => DataType{ .decimal256 = params },
                else => return StreamError.UnsupportedType,
            };
        },
        .Interval => blk: {
            const int_t = field_t.type.Interval.?;
            break :blk switch (int_t.unit) {
                .YEAR_MONTH => DataType{ .interval_months = .{ .unit = .months } },
                .DAY_TIME => DataType{ .interval_day_time = .{ .unit = .day_time } },
                .MONTH_DAY_NANO => DataType{ .interval_month_day_nano = .{ .unit = .month_day_nano } },
            };
        },
        .BinaryView => DataType{ .binary_view = {} },
        .Utf8View => DataType{ .string_view = {} },
    };

    if (field_t.dictionary) |dict_t| {
        const index_type = dict_t.indexType orelse return StreamError.UnsupportedType;
        const bit_width = std.math.cast(u8, index_type.bitWidth) orelse return StreamError.InvalidMetadata;
        if (bit_width != 8 and bit_width != 16 and bit_width != 32 and bit_width != 64) return StreamError.InvalidMetadata;
        const index = datatype.IntType{ .bit_width = bit_width, .signed = index_type.is_signed };
        const value_ptr = try allocator.create(DataType);
        value_ptr.* = dtype;
        return DataType{ .dictionary = .{ .id = dict_t.id, .index_type = index, .value_type = value_ptr, .ordered = dict_t.isOrdered } };
    }

    return dtype;
}

fn fromFbsTimeUnit(unit: fbs.TimeUnit) datatype.TimeUnit {
    return switch (unit) {
        .SECOND => .second,
        .MILLISECOND => .millisecond,
        .MICROSECOND => .microsecond,
        .NANOSECOND => .nanosecond,
    };
}

fn intTypeFromDataType(dt: DataType) StreamError!datatype.IntType {
    return switch (dt) {
        .int8 => .{ .bit_width = 8, .signed = true },
        .uint8 => .{ .bit_width = 8, .signed = false },
        .int16 => .{ .bit_width = 16, .signed = true },
        .uint16 => .{ .bit_width = 16, .signed = false },
        .int32 => .{ .bit_width = 32, .signed = true },
        .uint32 => .{ .bit_width = 32, .signed = false },
        .int64 => .{ .bit_width = 64, .signed = true },
        .uint64 => .{ .bit_width = 64, .signed = false },
        else => StreamError.UnsupportedType,
    };
}

fn dataTypeFromIntType(int_type: datatype.IntType) DataType {
    return switch (int_type.bit_width) {
        8 => if (int_type.signed) .{ .int8 = {} } else .{ .uint8 = {} },
        16 => if (int_type.signed) .{ .int16 = {} } else .{ .uint16 = {} },
        32 => if (int_type.signed) .{ .int32 = {} } else .{ .uint32 = {} },
        64 => if (int_type.signed) .{ .int64 = {} } else .{ .uint64 = {} },
        else => unreachable,
    };
}

fn buildRecordBatchFromFlatbuf(
    allocator: std.mem.Allocator,
    schema: Schema,
    record_batch_t: *fbs.RecordBatchT,
    body: array_data.SharedBuffer,
    dictionary_values: *const std.AutoHashMap(i64, ArrayRef),
) (StreamError || array_data.ValidationError || record_batch.RecordBatchError || error{OutOfMemory})!RecordBatch {
    const columns = try allocator.alloc(ArrayRef, schema.fields.len);
    var col_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < col_count) : (i += 1) columns[i].release();
        allocator.free(columns);
    }

    var node_index: usize = 0;
    var buffer_index: usize = 0;
    var variadic_index: usize = 0;
    for (schema.fields, 0..) |field, i| {
        columns[i] = try readArrayFromMeta(
            allocator,
            field.data_type.*,
            record_batch_t.nodes.items,
            record_batch_t.buffers.items,
            record_batch_t.variadicBufferCounts.items,
            body,
            &node_index,
            &buffer_index,
            &variadic_index,
            dictionary_values,
        );
        col_count += 1;
    }
    if (node_index != record_batch_t.nodes.items.len or buffer_index != record_batch_t.buffers.items.len) {
        return StreamError.InvalidMetadata;
    }
    if (variadic_index != record_batch_t.variadicBufferCounts.items.len) return StreamError.InvalidMetadata;

    const batch = try RecordBatch.init(allocator, schema, columns);
    var i: usize = 0;
    while (i < col_count) : (i += 1) columns[i].release();
    allocator.free(columns);
    const expected_rows = std.math.cast(usize, record_batch_t.length) orelse return StreamError.InvalidMetadata;
    if (batch.numRows() != expected_rows) return StreamError.InvalidMetadata;
    return batch;
}

fn readArrayFromMeta(
    allocator: std.mem.Allocator,
    dt: DataType,
    nodes: []const fbs.FieldNodeT,
    buffers_meta: []const fbs.BufferT,
    variadic_buffer_counts: []const i64,
    body: array_data.SharedBuffer,
    node_index: *usize,
    buffer_index: *usize,
    variadic_index: *usize,
    dictionary_values: *const std.AutoHashMap(i64, ArrayRef),
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    if (node_index.* >= nodes.len) return StreamError.InvalidMetadata;
    const node = nodes[node_index.*];
    node_index.* += 1;

    const buffer_count = try bufferCountForType(dt, variadic_buffer_counts, variadic_index);
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
        const start = std.math.cast(usize, meta.offset) orelse return StreamError.InvalidBody;
        const len = std.math.cast(usize, meta.length) orelse return StreamError.InvalidBody;
        const end = std.math.add(usize, start, len) catch return StreamError.InvalidBody;
        if (end > body.len()) return StreamError.InvalidBody;
        buffers[i] = if (meta.length == 0) array_data.SharedBuffer.empty else body.slice(start, end);
        buf_count += 1;
    }

    var children: []ArrayRef = &.{};
    var dictionary_ref: ?ArrayRef = null;
    errdefer if (dictionary_ref) |*dict| dict.release();
    if (dt == .list or dt == .large_list or dt == .fixed_size_list or dt == .map or dt == .list_view or dt == .large_list_view) {
        children = try allocator.alloc(ArrayRef, 1);
        errdefer allocator.free(children);
        const child_dt = if (dt == .map)
            (dt.map.entries_type orelse return StreamError.InvalidMetadata).*
        else
            childValueType(dt);
        children[0] = try readArrayFromMeta(allocator, child_dt, nodes, buffers_meta, variadic_buffer_counts, body, node_index, buffer_index, variadic_index, dictionary_values);
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
            children[idx] = try readArrayFromMeta(allocator, dt.struct_.fields[idx].data_type.*, nodes, buffers_meta, variadic_buffer_counts, body, node_index, buffer_index, variadic_index, dictionary_values);
            filled += 1;
        }
    } else if (dt == .sparse_union or dt == .dense_union) {
        const union_fields = switch (dt) {
            .sparse_union => dt.sparse_union.fields,
            .dense_union => dt.dense_union.fields,
            else => unreachable,
        };
        children = try allocator.alloc(ArrayRef, union_fields.len);
        var filled: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < filled) : (j += 1) children[j].release();
            allocator.free(children);
        }
        for (union_fields, 0..) |field, idx| {
            children[idx] = try readArrayFromMeta(allocator, field.data_type.*, nodes, buffers_meta, variadic_buffer_counts, body, node_index, buffer_index, variadic_index, dictionary_values);
            filled += 1;
        }
    } else if (dt == .run_end_encoded) {
        children = try allocator.alloc(ArrayRef, 2);
        var filled: usize = 0;
        errdefer {
            var j: usize = 0;
            while (j < filled) : (j += 1) children[j].release();
            allocator.free(children);
        }

        const run_end_dt = dataTypeFromIntType(dt.run_end_encoded.run_end_type);
        children[0] = try readArrayFromMeta(allocator, run_end_dt, nodes, buffers_meta, variadic_buffer_counts, body, node_index, buffer_index, variadic_index, dictionary_values);
        filled += 1;
        children[1] = try readArrayFromMeta(allocator, dt.run_end_encoded.value_type.*, nodes, buffers_meta, variadic_buffer_counts, body, node_index, buffer_index, variadic_index, dictionary_values);
        filled += 1;
    } else if (dt == .dictionary) {
        const dictionary_id = dt.dictionary.id orelse return StreamError.InvalidMetadata;
        const dict_ref = dictionary_values.get(dictionary_id) orelse return StreamError.InvalidMetadata;
        dictionary_ref = dict_ref.retain();
    }

    const array_len = std.math.cast(usize, node.length) orelse return StreamError.InvalidMetadata;
    const null_count = std.math.cast(usize, node.null_count) orelse return StreamError.InvalidMetadata;

    const data = ArrayData{
        .data_type = dt,
        .length = array_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = dictionary_ref,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn childValueType(dt: DataType) DataType {
    return switch (dt) {
        .list => |lst| lst.value_field.data_type.*,
        .large_list => |lst| lst.value_field.data_type.*,
        .fixed_size_list => |lst| lst.value_field.data_type.*,
        .list_view => |lst| lst.value_field.data_type.*,
        .large_list_view => |lst| lst.value_field.data_type.*,
        else => dt,
    };
}

fn bufferCountForType(dt: DataType, variadic_buffer_counts: []const i64, variadic_index: *usize) StreamError!usize {
    return switch (dt) {
        .null => 0,
        .struct_, .fixed_size_list => 1,
        .list, .large_list, .map => 2,
        .list_view, .large_list_view => 3,
        .string, .binary, .large_string, .large_binary => 3,
        .string_view, .binary_view => blk: {
            if (variadic_index.* >= variadic_buffer_counts.len) return StreamError.InvalidMetadata;
            const variadic_count = std.math.cast(usize, variadic_buffer_counts[variadic_index.*]) orelse return StreamError.InvalidMetadata;
            variadic_index.* += 1;
            break :blk std.math.add(usize, 2, variadic_count) catch return StreamError.InvalidMetadata;
        },
        .bool, .uint8, .int8, .uint16, .int16, .uint32, .int32, .uint64, .int64, .half_float, .float, .double, .fixed_size_binary, .date32, .date64, .time32, .time64, .timestamp, .duration, .interval_months, .interval_day_time, .interval_month_day_nano, .decimal32, .decimal64, .decimal128, .decimal256 => 2,
        .dictionary => 2,
        .sparse_union => 1,
        .dense_union => 2,
        .run_end_encoded => 0,
        else => StreamError.UnsupportedType,
    };
}

fn ingestDictionaryBatch(
    self: anytype,
    schema: Schema,
    dictionary_batch_t: *fbs.DictionaryBatchT,
    body: array_data.SharedBuffer,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!void {
    const record_batch_t = dictionary_batch_t.data orelse return StreamError.InvalidMetadata;
    const value_type = findDictionaryValueType(schema, dictionary_batch_t.id) orelse return StreamError.InvalidMetadata;

    var node_index: usize = 0;
    var buffer_index: usize = 0;
    var variadic_index: usize = 0;
    var dictionary = try readArrayFromMeta(
        self.allocator,
        value_type,
        record_batch_t.nodes.items,
        record_batch_t.buffers.items,
        record_batch_t.variadicBufferCounts.items,
        body,
        &node_index,
        &buffer_index,
        &variadic_index,
        &self.dictionary_values,
    );
    errdefer dictionary.release();

    if (node_index != record_batch_t.nodes.items.len or buffer_index != record_batch_t.buffers.items.len) {
        return StreamError.InvalidMetadata;
    }
    if (variadic_index != record_batch_t.variadicBufferCounts.items.len) return StreamError.InvalidMetadata;

    var incoming = dictionary;
    if (dictionary_batch_t.isDelta) {
        const previous_ref = self.dictionary_values.get(dictionary_batch_t.id) orelse return StreamError.InvalidMetadata;
        incoming = try mergeDictionaryValues(self.allocator, previous_ref, dictionary);
        dictionary.release();
    }

    const previous = try self.dictionary_values.fetchPut(dictionary_batch_t.id, incoming);
    if (previous) |entry| {
        var old = entry.value;
        old.release();
    }
}

fn mergeDictionaryValues(
    allocator: std.mem.Allocator,
    base: ArrayRef,
    delta: ArrayRef,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    if (!std.meta.eql(base.data().data_type, delta.data().data_type)) return StreamError.InvalidMetadata;

    const dt = base.data().data_type;
    return switch (dt) {
        .null => try concatNullArray(allocator, dt, base.data(), delta.data()),
        .bool => try concatBooleanArray(allocator, dt, base.data(), delta.data()),
        .uint8, .int8 => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 1),
        .uint16, .int16, .half_float => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 2),
        .uint32, .int32, .float, .date32, .time32, .interval_months, .decimal32 => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 4),
        .uint64, .int64, .double, .date64, .time64, .timestamp, .duration, .interval_day_time, .decimal64 => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 8),
        .decimal128, .interval_month_day_nano => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 16),
        .decimal256 => try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), 32),
        .fixed_size_binary => |fsb| blk: {
            const byte_width = std.math.cast(usize, fsb.byte_width) orelse return StreamError.InvalidMetadata;
            break :blk try concatFixedWidthArray(allocator, dt, base.data(), delta.data(), byte_width);
        },
        .string, .binary => try concatVariableBinaryArrayI32(allocator, dt, base.data(), delta.data()),
        .large_string, .large_binary => try concatVariableBinaryArrayI64(allocator, dt, base.data(), delta.data()),
        .list, .map => try concatListLikeArrayI32(allocator, dt, base.data(), delta.data()),
        .large_list => try concatListLikeArrayI64(allocator, dt, base.data(), delta.data()),
        .fixed_size_list => try concatFixedSizeListArray(allocator, dt, base.data(), delta.data()),
        .struct_ => try concatStructArray(allocator, dt, base.data(), delta.data()),
        else => StreamError.UnsupportedType,
    };
}

fn concatListLikeArrayI32(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);
    if (left.children.len != 1 or right.children.len != 1) return StreamError.InvalidMetadata;

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i32);
    const right_offsets = right.buffers[1].typedSlice(i32);
    const left_base = left_offsets[left.offset];
    const left_last = left_offsets[left.offset + left.length];
    const right_base = right_offsets[right.offset];
    const right_last = right_offsets[right.offset + right.length];
    if (left_base < 0 or left_last < left_base or right_base < 0 or right_last < right_base) return StreamError.InvalidMetadata;

    const offsets_len = std.math.add(usize, total_len, 1) catch return StreamError.InvalidMetadata;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i32));
    var out_offsets = offsets_owned.typedSlice(i32)[0..offsets_len];
    out_offsets[0] = 0;

    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const rel = std.math.sub(i32, left_offsets[left.offset + i + 1], left_base) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = rel;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_i = i - left.length;
        const rel = std.math.sub(i32, right_offsets[right.offset + right_i + 1], right_base) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = std.math.add(i32, left_prefix, rel) catch return StreamError.InvalidMetadata;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i32));
    errdefer offsets.release();

    const left_child_start = std.math.cast(usize, left_base) orelse return StreamError.InvalidMetadata;
    const left_child_end = std.math.cast(usize, left_last) orelse return StreamError.InvalidMetadata;
    const right_child_start = std.math.cast(usize, right_base) orelse return StreamError.InvalidMetadata;
    const right_child_end = std.math.cast(usize, right_last) orelse return StreamError.InvalidMetadata;
    const left_child_len = std.math.sub(usize, left_child_end, left_child_start) catch return StreamError.InvalidMetadata;
    const right_child_len = std.math.sub(usize, right_child_end, right_child_start) catch return StreamError.InvalidMetadata;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try mergeDictionaryValues(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatListLikeArrayI64(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);
    if (left.children.len != 1 or right.children.len != 1) return StreamError.InvalidMetadata;

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i64);
    const right_offsets = right.buffers[1].typedSlice(i64);
    const left_base = left_offsets[left.offset];
    const left_last = left_offsets[left.offset + left.length];
    const right_base = right_offsets[right.offset];
    const right_last = right_offsets[right.offset + right.length];
    if (left_base < 0 or left_last < left_base or right_base < 0 or right_last < right_base) return StreamError.InvalidMetadata;

    const offsets_len = std.math.add(usize, total_len, 1) catch return StreamError.InvalidMetadata;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i64));
    var out_offsets = offsets_owned.typedSlice(i64)[0..offsets_len];
    out_offsets[0] = 0;

    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const rel = std.math.sub(i64, left_offsets[left.offset + i + 1], left_base) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = rel;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_i = i - left.length;
        const rel = std.math.sub(i64, right_offsets[right.offset + right_i + 1], right_base) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = std.math.add(i64, left_prefix, rel) catch return StreamError.InvalidMetadata;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i64));
    errdefer offsets.release();

    const left_child_start = std.math.cast(usize, left_base) orelse return StreamError.InvalidMetadata;
    const left_child_end = std.math.cast(usize, left_last) orelse return StreamError.InvalidMetadata;
    const right_child_start = std.math.cast(usize, right_base) orelse return StreamError.InvalidMetadata;
    const right_child_end = std.math.cast(usize, right_last) orelse return StreamError.InvalidMetadata;
    const left_child_len = std.math.sub(usize, left_child_end, left_child_start) catch return StreamError.InvalidMetadata;
    const right_child_len = std.math.sub(usize, right_child_end, right_child_start) catch return StreamError.InvalidMetadata;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try mergeDictionaryValues(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatFixedSizeListArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 1);
    try requireBufferCount(right, 1);
    if (left.children.len != 1 or right.children.len != 1) return StreamError.InvalidMetadata;
    const list_size = std.math.cast(usize, dt.fixed_size_list.list_size) orelse return StreamError.InvalidMetadata;
    if (list_size == 0) return StreamError.InvalidMetadata;

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_child_start = std.math.mul(usize, left.offset, list_size) catch return StreamError.InvalidMetadata;
    const left_child_len = std.math.mul(usize, left.length, list_size) catch return StreamError.InvalidMetadata;
    const right_child_start = std.math.mul(usize, right.offset, list_size) catch return StreamError.InvalidMetadata;
    const right_child_len = std.math.mul(usize, right.length, list_size) catch return StreamError.InvalidMetadata;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try mergeDictionaryValues(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 1);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatStructArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 1);
    try requireBufferCount(right, 1);
    if (left.children.len != dt.struct_.fields.len or right.children.len != dt.struct_.fields.len) return StreamError.InvalidMetadata;

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const children = try allocator.alloc(ArrayRef, dt.struct_.fields.len);
    var child_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < child_count) : (i += 1) {
            var owned = children[i];
            owned.release();
        }
        allocator.free(children);
    }
    for (left.children, right.children, 0..) |left_child_ref, right_child_ref, idx| {
        var left_child = try left_child_ref.slice(left.offset, left.length);
        defer left_child.release();
        var right_child = try right_child_ref.slice(right.offset, right.length);
        defer right_child.release();
        children[idx] = try mergeDictionaryValues(allocator, left_child, right_child);
        child_count += 1;
    }

    const buffers = try allocator.alloc(array_data.SharedBuffer, 1);
    errdefer allocator.free(buffers);
    buffers[0] = validity;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatNullArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const buffers = try allocator.alloc(array_data.SharedBuffer, 0);
    const children = try allocator.alloc(ArrayRef, 0);
    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = total_len,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatBooleanArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    var values_owned = try buffer.OwnedBuffer.init(allocator, bitmap.byteLength(total_len));
    const values_bytes = values_owned.data[0..bitmap.byteLength(total_len)];
    @memset(values_bytes, 0);
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (bitAt(left.buffers[1], left.offset + i)) bitmap.setBit(values_bytes, i);
    }
    i = 0;
    while (i < right.length) : (i += 1) {
        if (bitAt(right.buffers[1], right.offset + i)) bitmap.setBit(values_bytes, left.length + i);
    }
    var values = try values_owned.toShared(values_bytes.len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatFixedWidthArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
    byte_width: usize,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_bytes = try dataBytesForFixedWidth(left, byte_width);
    const right_bytes = try dataBytesForFixedWidth(right, byte_width);
    const total_data_len = std.math.add(usize, left_bytes.len, right_bytes.len) catch return StreamError.InvalidMetadata;
    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    @memcpy(values_owned.data[0..left_bytes.len], left_bytes);
    @memcpy(values_owned.data[left_bytes.len .. left_bytes.len + right_bytes.len], right_bytes);
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatVariableBinaryArrayI32(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 3);
    try requireBufferCount(right, 3);

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i32);
    const right_offsets = right.buffers[1].typedSlice(i32);
    const left_start = std.math.cast(usize, left_offsets[left.offset]) orelse return StreamError.InvalidMetadata;
    const left_end = std.math.cast(usize, left_offsets[left.offset + left.length]) orelse return StreamError.InvalidMetadata;
    const right_start = std.math.cast(usize, right_offsets[right.offset]) orelse return StreamError.InvalidMetadata;
    const right_end = std.math.cast(usize, right_offsets[right.offset + right.length]) orelse return StreamError.InvalidMetadata;
    if (left_end < left_start or right_end < right_start) return StreamError.InvalidMetadata;

    const left_data_len = std.math.sub(usize, left_end, left_start) catch return StreamError.InvalidMetadata;
    const right_data_len = std.math.sub(usize, right_end, right_start) catch return StreamError.InvalidMetadata;
    const total_data_len = std.math.add(usize, left_data_len, right_data_len) catch return StreamError.InvalidMetadata;

    const offsets_len = std.math.add(usize, total_len, 1) catch return StreamError.InvalidMetadata;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i32));
    var out_offsets = offsets_owned.typedSlice(i32)[0..offsets_len];
    out_offsets[0] = 0;
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const cur = std.math.sub(i32, left_offsets[left.offset + i + 1], left_offsets[left.offset]) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = cur;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_idx = i - left.length;
        const delta_off = std.math.sub(i32, right_offsets[right.offset + right_idx + 1], right_offsets[right.offset]) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = std.math.add(i32, left_prefix, delta_off) catch return StreamError.InvalidMetadata;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i32));
    errdefer offsets.release();

    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    if (left_data_len > 0) {
        @memcpy(values_owned.data[0..left_data_len], left.buffers[2].data[left_start..left_end]);
    }
    if (right_data_len > 0) {
        @memcpy(values_owned.data[left_data_len .. left_data_len + right_data_len], right.buffers[2].data[right_start..right_end]);
    }
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    buffers[2] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatVariableBinaryArrayI64(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) (StreamError || array_data.ValidationError || error{OutOfMemory})!ArrayRef {
    try requireBufferCount(left, 3);
    try requireBufferCount(right, 3);

    const total_len = std.math.add(usize, left.length, right.length) catch return StreamError.InvalidMetadata;
    const null_count = std.math.add(usize, nullCountForArray(left), nullCountForArray(right)) catch return StreamError.InvalidMetadata;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i64);
    const right_offsets = right.buffers[1].typedSlice(i64);
    const left_start = std.math.cast(usize, left_offsets[left.offset]) orelse return StreamError.InvalidMetadata;
    const left_end = std.math.cast(usize, left_offsets[left.offset + left.length]) orelse return StreamError.InvalidMetadata;
    const right_start = std.math.cast(usize, right_offsets[right.offset]) orelse return StreamError.InvalidMetadata;
    const right_end = std.math.cast(usize, right_offsets[right.offset + right.length]) orelse return StreamError.InvalidMetadata;
    if (left_end < left_start or right_end < right_start) return StreamError.InvalidMetadata;

    const left_data_len = std.math.sub(usize, left_end, left_start) catch return StreamError.InvalidMetadata;
    const right_data_len = std.math.sub(usize, right_end, right_start) catch return StreamError.InvalidMetadata;
    const total_data_len = std.math.add(usize, left_data_len, right_data_len) catch return StreamError.InvalidMetadata;

    const offsets_len = std.math.add(usize, total_len, 1) catch return StreamError.InvalidMetadata;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i64));
    var out_offsets = offsets_owned.typedSlice(i64)[0..offsets_len];
    out_offsets[0] = 0;
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const cur = std.math.sub(i64, left_offsets[left.offset + i + 1], left_offsets[left.offset]) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = cur;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_idx = i - left.length;
        const delta_off = std.math.sub(i64, right_offsets[right.offset + right_idx + 1], right_offsets[right.offset]) catch return StreamError.InvalidMetadata;
        out_offsets[i + 1] = std.math.add(i64, left_prefix, delta_off) catch return StreamError.InvalidMetadata;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i64));
    errdefer offsets.release();

    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    if (left_data_len > 0) {
        @memcpy(values_owned.data[0..left_data_len], left.buffers[2].data[left_start..left_end]);
    }
    if (right_data_len > 0) {
        @memcpy(values_owned.data[left_data_len .. left_data_len + right_data_len], right.buffers[2].data[right_start..right_end]);
    }
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    buffers[2] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn nullCountForArray(data: *const ArrayData) usize {
    if (data.null_count) |count| return count;
    if (data.validity()) |v| return v.countNulls();
    return 0;
}

fn concatValidityBuffer(
    allocator: std.mem.Allocator,
    left: *const ArrayData,
    right: *const ArrayData,
    total_len: usize,
    total_nulls: usize,
) error{OutOfMemory}!array_data.SharedBuffer {
    if (total_nulls == 0) return array_data.SharedBuffer.empty;
    const used = bitmap.byteLength(total_len);
    var owned = try buffer.OwnedBuffer.init(allocator, used);
    const bytes = owned.data[0..used];
    @memset(bytes, 0);
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (isValidAt(left, i)) bitmap.setBit(bytes, i);
    }
    i = 0;
    while (i < right.length) : (i += 1) {
        if (isValidAt(right, i)) bitmap.setBit(bytes, left.length + i);
    }
    return owned.toShared(used);
}

fn isValidAt(data: *const ArrayData, index: usize) bool {
    if (data.null_count) |count| {
        if (count == 0) return true;
        if (count == data.length) return false;
    }
    const validity = data.validity() orelse return true;
    return validity.isValid(data.offset + index);
}

fn bitAt(buf: array_data.SharedBuffer, bit_index: usize) bool {
    return bitmap.bitIsSet(buf.data, bit_index);
}

fn dataBytesForFixedWidth(data: *const ArrayData, byte_width: usize) StreamError![]const u8 {
    const start = std.math.mul(usize, data.offset, byte_width) catch return StreamError.InvalidMetadata;
    const data_len = std.math.mul(usize, data.length, byte_width) catch return StreamError.InvalidMetadata;
    const end = std.math.add(usize, start, data_len) catch return StreamError.InvalidMetadata;
    if (end > data.buffers[1].len()) return StreamError.InvalidMetadata;
    return data.buffers[1].data[start..end];
}

fn requireBufferCount(data: *const ArrayData, min_count: usize) StreamError!void {
    if (data.buffers.len < min_count) return StreamError.InvalidMetadata;
}

fn findDictionaryValueType(schema: Schema, dictionary_id: i64) ?DataType {
    for (schema.fields) |field| {
        if (findDictionaryValueTypeInDataType(field.data_type.*, dictionary_id)) |value_type| return value_type;
    }
    return null;
}

fn findDictionaryValueTypeInDataType(dt: DataType, dictionary_id: i64) ?DataType {
    return switch (dt) {
        .dictionary => |dict| {
            if (dict.id) |id| {
                if (id == dictionary_id) return dict.value_type.*;
            }
            return null;
        },
        .list => |lst| findDictionaryValueTypeInDataType(lst.value_field.data_type.*, dictionary_id),
        .large_list => |lst| findDictionaryValueTypeInDataType(lst.value_field.data_type.*, dictionary_id),
        .list_view => |lst| findDictionaryValueTypeInDataType(lst.value_field.data_type.*, dictionary_id),
        .large_list_view => |lst| findDictionaryValueTypeInDataType(lst.value_field.data_type.*, dictionary_id),
        .fixed_size_list => |lst| findDictionaryValueTypeInDataType(lst.value_field.data_type.*, dictionary_id),
        .map => |map_t| blk: {
            if (findDictionaryValueTypeInDataType(map_t.key_field.data_type.*, dictionary_id)) |value_type| {
                break :blk value_type;
            }
            break :blk findDictionaryValueTypeInDataType(map_t.item_field.data_type.*, dictionary_id);
        },
        .struct_ => |st| blk: {
            for (st.fields) |field| {
                if (findDictionaryValueTypeInDataType(field.data_type.*, dictionary_id)) |value_type| {
                    break :blk value_type;
                }
            }
            break :blk null;
        },
        .sparse_union, .dense_union => |uni| blk: {
            for (uni.fields) |field| {
                if (findDictionaryValueTypeInDataType(field.data_type.*, dictionary_id)) |value_type| {
                    break :blk value_type;
                }
            }
            break :blk null;
        },
        .run_end_encoded => |ree| findDictionaryValueTypeInDataType(ree.value_type.*, dictionary_id),
        .extension => |ext| findDictionaryValueTypeInDataType(ext.storage_type.*, dictionary_id),
        else => null,
    };
}

test "ipc reader finds dictionary value type in nested metadata layouts" {
    const value_type = DataType{ .string = {} };
    const int32_type = DataType{ .int32 = {} };
    const dict_type = DataType{
        .dictionary = .{
            .id = 99,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
            .ordered = false,
        },
    };
    const dict_field = Field{ .name = "dict", .data_type = &dict_type, .nullable = true };
    const int_field = Field{ .name = "item", .data_type = &int32_type, .nullable = true };

    const list_view_type = DataType{ .list_view = .{ .value_field = dict_field } };
    try std.testing.expect(findDictionaryValueTypeInDataType(list_view_type, 99) != null);

    const map_type = DataType{
        .map = .{
            .key_field = int_field,
            .item_field = dict_field,
            .keys_sorted = false,
            .entries_type = null,
        },
    };
    try std.testing.expect(findDictionaryValueTypeInDataType(map_type, 99) != null);

    const union_fields = [_]Field{
        .{ .name = "a", .data_type = &int32_type, .nullable = true },
        .{ .name = "b", .data_type = &dict_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 0, 1 };
    const sparse_union_type = DataType{
        .sparse_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .sparse,
        },
    };
    try std.testing.expect(findDictionaryValueTypeInDataType(sparse_union_type, 99) != null);

    const ree_type = DataType{
        .run_end_encoded = .{
            .run_end_type = .{ .bit_width = 32, .signed = true },
            .value_type = &dict_type,
        },
    };
    try std.testing.expect(findDictionaryValueTypeInDataType(ree_type, 99) != null);

    const extension_type = DataType{
        .extension = .{
            .name = "ext",
            .storage_type = &dict_type,
            .metadata = null,
        },
    };
    try std.testing.expect(findDictionaryValueTypeInDataType(extension_type, 99) != null);
}

fn expectMetadataEntry(metadata: []const datatype.KeyValue, key: []const u8, value: []const u8) !void {
    for (metadata) |entry| {
        if (std.mem.eql(u8, entry.key, key) and std.mem.eql(u8, entry.value, value)) return;
    }
    return error.MetadataEntryMissing;
}

test "ipc schema rejects fixed-size-binary metadata with non-positive width" {
    const allocator = std.testing.allocator;

    const fsb_t = try allocator.create(fbs.FixedSizeBinaryT);
    fsb_t.* = .{ .byteWidth = 0 };

    var field = fbs.FieldT{
        .name = "bad_fsb",
        .nullable = true,
        .type = .{ .FixedSizeBinary = fsb_t },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer field.deinit(allocator);

    try std.testing.expectError(StreamError.InvalidMetadata, buildDataTypeFromFlatbuf(allocator, field));
}

test "ipc schema rejects dictionary index metadata with invalid bit width" {
    const allocator = std.testing.allocator;

    const int_t = try allocator.create(fbs.IntT);
    int_t.* = .{ .bitWidth = 32, .is_signed = true };

    const dict_index_t = try allocator.create(fbs.IntT);
    dict_index_t.* = .{ .bitWidth = 7, .is_signed = true };
    const dict_t = try allocator.create(fbs.DictionaryEncodingT);
    dict_t.* = .{
        .id = 1,
        .indexType = dict_index_t,
        .isOrdered = false,
        .dictionaryKind = .DenseArray,
    };

    var field = fbs.FieldT{
        .name = "bad_dict",
        .nullable = true,
        .type = .{ .Int = int_t },
        .dictionary = dict_t,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer field.deinit(allocator);

    try std.testing.expectError(StreamError.InvalidMetadata, buildDataTypeFromFlatbuf(allocator, field));
}

test "ipc schema rejects run-end-encoded metadata with unsigned run-end type" {
    const allocator = std.testing.allocator;

    const run_end_int = try allocator.create(fbs.IntT);
    run_end_int.* = .{ .bitWidth = 16, .is_signed = false };
    const run_end_field = fbs.FieldT{
        .name = "run_ends",
        .nullable = false,
        .type = .{ .Int = run_end_int },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const value_int = try allocator.create(fbs.IntT);
    value_int.* = .{ .bitWidth = 32, .is_signed = true };
    const value_field = fbs.FieldT{
        .name = "values",
        .nullable = true,
        .type = .{ .Int = value_int },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const ree_t = try allocator.create(fbs.RunEndEncodedT);
    ree_t.* = .{};

    var children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 2);
    try children.append(allocator, run_end_field);
    try children.append(allocator, value_field);

    var field = fbs.FieldT{
        .name = "bad_ree",
        .nullable = true,
        .type = .{ .RunEndEncoded = ree_t },
        .dictionary = null,
        .children = children,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer field.deinit(allocator);

    try std.testing.expectError(StreamError.InvalidMetadata, buildDataTypeFromFlatbuf(allocator, field));
}

test "ipc schema rejects map metadata with nullable entries field" {
    const allocator = std.testing.allocator;

    const int_t = try allocator.create(fbs.IntT);
    int_t.* = .{ .bitWidth = 32, .is_signed = true };
    const key_field = fbs.FieldT{
        .name = "key",
        .nullable = false,
        .type = .{ .Int = int_t },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const val_t = try allocator.create(fbs.IntT);
    val_t.* = .{ .bitWidth = 32, .is_signed = true };
    const value_field = fbs.FieldT{
        .name = "value",
        .nullable = true,
        .type = .{ .Int = val_t },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const struct_t = try allocator.create(fbs.Struct_T);
    struct_t.* = .{};
    var entries_children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 2);
    try entries_children.append(allocator, key_field);
    try entries_children.append(allocator, value_field);
    const entries_field = fbs.FieldT{
        .name = "entries",
        .nullable = true,
        .type = .{ .Struct_ = struct_t },
        .dictionary = null,
        .children = entries_children,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const map_t = try allocator.create(fbs.MapT);
    map_t.* = .{ .keysSorted = false };
    var map_children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 1);
    try map_children.append(allocator, entries_field);
    var map_field = fbs.FieldT{
        .name = "map_col",
        .nullable = true,
        .type = .{ .Map = map_t },
        .dictionary = null,
        .children = map_children,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer map_field.deinit(allocator);

    try std.testing.expectError(StreamError.InvalidMetadata, buildDataTypeFromFlatbuf(allocator, map_field));
}

test "ipc schema rejects fixed-size-list metadata with negative list size" {
    const allocator = std.testing.allocator;

    const child_int = try allocator.create(fbs.IntT);
    child_int.* = .{ .bitWidth = 32, .is_signed = true };
    const child_field = fbs.FieldT{
        .name = "item",
        .nullable = true,
        .type = .{ .Int = child_int },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const fsl_t = try allocator.create(fbs.FixedSizeListT);
    fsl_t.* = .{ .listSize = -1 };

    var children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 1);
    try children.append(allocator, child_field);
    var field = fbs.FieldT{
        .name = "bad_fsl",
        .nullable = true,
        .type = .{ .FixedSizeList = fsl_t },
        .dictionary = null,
        .children = children,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer field.deinit(allocator);

    try std.testing.expectError(StreamError.InvalidMetadata, buildDataTypeFromFlatbuf(allocator, field));
}

test "ipc schema accepts list-view metadata with one child" {
    const allocator = std.testing.allocator;

    const child_int = try allocator.create(fbs.IntT);
    child_int.* = .{ .bitWidth = 32, .is_signed = true };
    const child_field = fbs.FieldT{
        .name = "item",
        .nullable = true,
        .type = .{ .Int = child_int },
        .dictionary = null,
        .children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };

    const lv_t = try allocator.create(fbs.ListViewT);
    lv_t.* = .{};

    var children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 1);
    try children.append(allocator, child_field);
    var field = fbs.FieldT{
        .name = "lv",
        .nullable = true,
        .type = .{ .ListView = lv_t },
        .dictionary = null,
        .children = children,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer field.deinit(allocator);

    const dtype = try buildDataTypeFromFlatbuf(allocator, field);
    defer switch (dtype) {
        .list_view => |lv| {
            allocator.free(lv.value_field.name);
            allocator.destroy(@constCast(lv.value_field.data_type));
        },
        else => {},
    };
    try std.testing.expect(dtype == .list_view);
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
    defer writer.deinit();
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
    defer writer.deinit();
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

test "ipc stream roundtrip temporal and decimal primitives" {
    const allocator = std.testing.allocator;

    const date_type = DataType{ .date32 = {} };
    const ts_type = DataType{ .timestamp = .{ .unit = .millisecond, .timezone = "UTC" } };
    const dec_type = DataType{ .decimal128 = .{ .precision = 10, .scale = 2 } };
    const fields = [_]Field{
        .{ .name = "d", .data_type = &date_type, .nullable = false },
        .{ .name = "ts", .data_type = &ts_type, .nullable = false },
        .{ .name = "dec", .data_type = &dec_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var date_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .date32 = {} }).init(allocator, 2);
    defer date_builder.deinit();
    try date_builder.append(20000);
    try date_builder.append(20001);
    var date_ref = try date_builder.finish();
    defer date_ref.release();

    var ts_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i64, DataType{ .timestamp = .{ .unit = .millisecond, .timezone = "UTC" } }).init(allocator, 2);
    defer ts_builder.deinit();
    try ts_builder.append(1700000000000);
    try ts_builder.append(1700000001000);
    var ts_ref = try ts_builder.finish();
    defer ts_ref.release();

    var dec_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i128, DataType{ .decimal128 = .{ .precision = 10, .scale = 2 } }).init(allocator, 2);
    defer dec_builder.deinit();
    try dec_builder.append(12345);
    try dec_builder.append(-42);
    var dec_ref = try dec_builder.finish();
    defer dec_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ date_ref, ts_ref, dec_ref });
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expectEqual(@as(usize, 3), out_schema.fields.len);
    try std.testing.expect(out_schema.fields[0].data_type.* == .date32);
    try std.testing.expect(out_schema.fields[1].data_type.* == .timestamp);
    try std.testing.expectEqual(datatype.TimeUnit.millisecond, out_schema.fields[1].data_type.timestamp.unit);
    try std.testing.expectEqualStrings("UTC", out_schema.fields[1].data_type.timestamp.timezone.?);
    try std.testing.expect(out_schema.fields[2].data_type.* == .decimal128);
    try std.testing.expectEqual(@as(u8, 10), out_schema.fields[2].data_type.decimal128.precision);
    try std.testing.expectEqual(@as(i32, 2), out_schema.fields[2].data_type.decimal128.scale);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const d = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = out_batch.columns[0].data() };
    const ts = @import("../array/primitive_array.zig").PrimitiveArray(i64){ .data = out_batch.columns[1].data() };
    const dec = @import("../array/primitive_array.zig").PrimitiveArray(i128){ .data = out_batch.columns[2].data() };
    try std.testing.expectEqual(@as(i32, 20000), d.value(0));
    try std.testing.expectEqual(@as(i64, 1700000001000), ts.value(1));
    try std.testing.expectEqual(@as(i128, -42), dec.value(1));
}

test "ipc stream roundtrip dictionary encoded string column" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .string = {} };
    const dict_type = DataType{
        .dictionary = .{
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
            .ordered = false,
        },
    };
    const fields = [_]Field{
        .{ .name = "color", .data_type = &dict_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var dict_values_builder = try @import("../array/string_array.zig").StringBuilder.init(allocator, 2, 8);
    defer dict_values_builder.deinit();
    try dict_values_builder.append("red");
    try dict_values_builder.append("blue");
    var dict_values = try dict_values_builder.finish();
    defer dict_values.release();

    var dict_builder = try @import("../array/dictionary_array.zig").DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        3,
    );
    defer dict_builder.deinit();
    try dict_builder.appendIndex(1);
    try dict_builder.appendNull();
    try dict_builder.appendIndex(0);
    var dict_col = try dict_builder.finish(dict_values);
    defer dict_col.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{dict_col});
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .dictionary);
    try std.testing.expectEqual(@as(i64, 0), out_schema.fields[0].data_type.dictionary.id.?);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const out_dict = @import("../array/dictionary_array.zig").DictionaryArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 3), out_dict.len());
    try std.testing.expectEqual(@as(i64, 1), out_dict.index(0));
    try std.testing.expect(out_dict.isNull(1));
    try std.testing.expectEqual(@as(i64, 0), out_dict.index(2));

    const dict_view = @import("../array/string_array.zig").StringArray{ .data = out_dict.dictionaryRef().data() };
    try std.testing.expectEqualStrings("red", dict_view.value(0));
    try std.testing.expectEqualStrings("blue", dict_view.value(1));
}

test "ipc reader merges dictionary delta batches" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .string = {} };
    const dict_type = DataType{
        .dictionary = .{
            .id = 0,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
            .ordered = false,
        },
    };
    const fields = [_]Field{
        .{ .name = "color", .data_type = &dict_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);

    var dict1_nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try dict1_nodes.append(allocator, .{ .length = 2, .null_count = 0 });
    var dict1_buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try dict1_buffers.append(allocator, .{ .offset = 0, .length = 0 });
    try dict1_buffers.append(allocator, .{ .offset = 0, .length = 12 });
    try dict1_buffers.append(allocator, .{ .offset = 12, .length = 7 });
    const dict1_rb = try allocator.create(fbs.RecordBatchT);
    dict1_rb.* = .{
        .length = 2,
        .nodes = dict1_nodes,
        .buffers = dict1_buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };
    const dict1_batch = try allocator.create(fbs.DictionaryBatchT);
    dict1_batch.* = .{
        .id = 0,
        .data = dict1_rb,
        .isDelta = false,
    };
    var msg_dict1 = fbs.MessageT{
        .version = .V5,
        .header = .{ .DictionaryBatch = dict1_batch },
        .bodyLength = 19,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg_dict1.deinit(allocator);
    const dict1_body = [_]u8{
        0,   0,   0,   0,
        3,   0,   0,   0,
        7,   0,   0,   0,
        'r', 'e', 'd', 'b',
        'l', 'u', 'e',
    };
    try appendEncodedMessage(allocator, out.writer(), msg_dict1, &dict1_body);

    var dict2_nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try dict2_nodes.append(allocator, .{ .length = 1, .null_count = 0 });
    var dict2_buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try dict2_buffers.append(allocator, .{ .offset = 0, .length = 0 });
    try dict2_buffers.append(allocator, .{ .offset = 0, .length = 8 });
    try dict2_buffers.append(allocator, .{ .offset = 8, .length = 5 });
    const dict2_rb = try allocator.create(fbs.RecordBatchT);
    dict2_rb.* = .{
        .length = 1,
        .nodes = dict2_nodes,
        .buffers = dict2_buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };
    const dict2_batch = try allocator.create(fbs.DictionaryBatchT);
    dict2_batch.* = .{
        .id = 0,
        .data = dict2_rb,
        .isDelta = true,
    };
    var msg_dict2 = fbs.MessageT{
        .version = .V5,
        .header = .{ .DictionaryBatch = dict2_batch },
        .bodyLength = 13,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg_dict2.deinit(allocator);
    const dict2_body = [_]u8{
        0,   0,   0,   0,
        5,   0,   0,   0,
        'g', 'r', 'e', 'e',
        'n',
    };
    try appendEncodedMessage(allocator, out.writer(), msg_dict2, &dict2_body);

    var rb_nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try rb_nodes.append(allocator, .{ .length = 2, .null_count = 0 });
    var rb_buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try rb_buffers.append(allocator, .{ .offset = 0, .length = 0 });
    try rb_buffers.append(allocator, .{ .offset = 0, .length = 8 });
    const rb_ptr = try allocator.create(fbs.RecordBatchT);
    rb_ptr.* = .{
        .length = 2,
        .nodes = rb_nodes,
        .buffers = rb_buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };
    var msg_rb = fbs.MessageT{
        .version = .V5,
        .header = .{ .RecordBatch = rb_ptr },
        .bodyLength = 8,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg_rb.deinit(allocator);
    const rb_body = [_]u8{
        2, 0, 0, 0, // green
        0, 0, 0, 0, // red
    };
    try appendEncodedMessage(allocator, out.writer(), msg_rb, &rb_body);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const out_dict = @import("../array/dictionary_array.zig").DictionaryArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i64, 2), out_dict.index(0));
    try std.testing.expectEqual(@as(i64, 0), out_dict.index(1));

    const dict_view = @import("../array/string_array.zig").StringArray{ .data = out_dict.dictionaryRef().data() };
    try std.testing.expectEqual(@as(usize, 3), dict_view.len());
    try std.testing.expectEqualStrings("red", dict_view.value(0));
    try std.testing.expectEqualStrings("blue", dict_view.value(1));
    try std.testing.expectEqualStrings("green", dict_view.value(2));
}

test "ipc reader dictionary delta merge supports list values" {
    const allocator = std.testing.allocator;
    const arr = @import("../array/array.zig");

    const int_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };
    const list_type = DataType{ .list = .{ .value_field = value_field } };

    var left_values_builder = try arr.Int32Builder.init(allocator, 3);
    defer left_values_builder.deinit();
    try left_values_builder.append(1);
    try left_values_builder.append(2);
    try left_values_builder.append(3);
    var left_values = try left_values_builder.finish();
    defer left_values.release();

    var left_list_builder = try arr.ListBuilder.init(allocator, 2, value_field);
    defer left_list_builder.deinit();
    try left_list_builder.appendLen(2);
    try left_list_builder.appendLen(1);
    var left_list = try left_list_builder.finish(left_values);
    defer left_list.release();

    var right_values_builder = try arr.Int32Builder.init(allocator, 2);
    defer right_values_builder.deinit();
    try right_values_builder.append(4);
    try right_values_builder.append(5);
    var right_values = try right_values_builder.finish();
    defer right_values.release();

    var right_list_builder = try arr.ListBuilder.init(allocator, 2, value_field);
    defer right_list_builder.deinit();
    try right_list_builder.appendLen(1);
    try right_list_builder.appendLen(1);
    var right_list = try right_list_builder.finish(right_values);
    defer right_list.release();

    var merged = try mergeDictionaryValues(allocator, left_list, right_list);
    defer merged.release();

    try std.testing.expect(merged.data().data_type == .list);
    try std.testing.expectEqual(@as(usize, 4), merged.data().length);
    const offsets = merged.data().buffers[1].typedSlice(i32);
    try std.testing.expectEqual(@as(i32, 0), offsets[0]);
    try std.testing.expectEqual(@as(i32, 2), offsets[1]);
    try std.testing.expectEqual(@as(i32, 3), offsets[2]);
    try std.testing.expectEqual(@as(i32, 4), offsets[3]);
    try std.testing.expectEqual(@as(i32, 5), offsets[4]);

    const child = arr.Int32Array{ .data = merged.data().children[0].data() };
    try std.testing.expectEqual(@as(usize, 5), child.len());
    try std.testing.expectEqual(@as(i32, 1), child.value(0));
    try std.testing.expectEqual(@as(i32, 2), child.value(1));
    try std.testing.expectEqual(@as(i32, 3), child.value(2));
    try std.testing.expectEqual(@as(i32, 4), child.value(3));
    try std.testing.expectEqual(@as(i32, 5), child.value(4));
    _ = list_type;
}

test "ipc reader dictionary delta merge supports struct values" {
    const allocator = std.testing.allocator;
    const arr = @import("../array/array.zig");

    const int_type = DataType{ .int32 = {} };
    const str_type = DataType{ .string = {} };
    const struct_fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "name", .data_type = &str_type, .nullable = true },
    };
    const struct_type = DataType{ .struct_ = .{ .fields = struct_fields[0..] } };

    var left_ids_builder = try arr.Int32Builder.init(allocator, 2);
    defer left_ids_builder.deinit();
    try left_ids_builder.append(1);
    try left_ids_builder.append(2);
    var left_ids = try left_ids_builder.finish();
    defer left_ids.release();

    var left_names_builder = try arr.StringBuilder.init(allocator, 2, 2);
    defer left_names_builder.deinit();
    try left_names_builder.append("a");
    try left_names_builder.append("b");
    var left_names = try left_names_builder.finish();
    defer left_names.release();

    var left_struct_builder = arr.StructBuilder.init(allocator, struct_fields[0..]);
    defer left_struct_builder.deinit();
    try left_struct_builder.appendValid();
    try left_struct_builder.appendValid();
    var left_struct = try left_struct_builder.finish(&[_]ArrayRef{ left_ids, left_names });
    defer left_struct.release();

    var right_ids_builder = try arr.Int32Builder.init(allocator, 1);
    defer right_ids_builder.deinit();
    try right_ids_builder.append(3);
    var right_ids = try right_ids_builder.finish();
    defer right_ids.release();

    var right_names_builder = try arr.StringBuilder.init(allocator, 1, 1);
    defer right_names_builder.deinit();
    try right_names_builder.append("c");
    var right_names = try right_names_builder.finish();
    defer right_names.release();

    var right_struct_builder = arr.StructBuilder.init(allocator, struct_fields[0..]);
    defer right_struct_builder.deinit();
    try right_struct_builder.appendValid();
    var right_struct = try right_struct_builder.finish(&[_]ArrayRef{ right_ids, right_names });
    defer right_struct.release();

    var merged = try mergeDictionaryValues(allocator, left_struct, right_struct);
    defer merged.release();

    try std.testing.expect(merged.data().data_type == .struct_);
    try std.testing.expectEqual(@as(usize, 3), merged.data().length);
    const ids = arr.Int32Array{ .data = merged.data().children[0].data() };
    const names = arr.StringArray{ .data = merged.data().children[1].data() };
    try std.testing.expectEqual(@as(i32, 1), ids.value(0));
    try std.testing.expectEqual(@as(i32, 2), ids.value(1));
    try std.testing.expectEqual(@as(i32, 3), ids.value(2));
    try std.testing.expectEqualStrings("a", names.value(0));
    try std.testing.expectEqualStrings("b", names.value(1));
    try std.testing.expectEqualStrings("c", names.value(2));
    _ = struct_type;
}

test "ipc stream roundtrip map int32 to int32" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };
    const map_type = DataType{
        .map = .{
            .key_field = key_field,
            .item_field = item_field,
            .keys_sorted = false,
        },
    };
    const fields = [_]Field{
        .{ .name = "m", .data_type = &map_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var key_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer key_builder.deinit();
    try key_builder.append(1);
    try key_builder.append(2);
    try key_builder.append(3);
    var key_ref = try key_builder.finish();
    defer key_ref.release();

    var item_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer item_builder.deinit();
    try item_builder.append(10);
    try item_builder.append(20);
    try item_builder.append(30);
    var item_ref = try item_builder.finish();
    defer item_ref.release();

    var entries_builder = @import("../array/struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ key_ref, item_ref });
    defer entries_ref.release();

    var map_builder = try @import("../array/advanced_array.zig").MapBuilder.init(allocator, 3, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendNull();
    try map_builder.appendLen(1);
    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{map_ref});
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .map);
    try std.testing.expect(out_schema.fields[0].data_type.map.entries_type != null);
    try std.testing.expectEqualStrings("key", out_schema.fields[0].data_type.map.key_field.name);
    try std.testing.expectEqualStrings("item", out_schema.fields[0].data_type.map.item_field.name);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const map = @import("../array/advanced_array.zig").MapArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 3), map.len());
    try std.testing.expect(map.isNull(1));

    var first = try map.value(0);
    defer first.release();
    try std.testing.expectEqual(@as(usize, 2), first.data().length);
    const first_items = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = first.data().children[1].data() };
    try std.testing.expectEqual(@as(i32, 10), first_items.value(0));
    try std.testing.expectEqual(@as(i32, 20), first_items.value(1));

    var third = try map.value(2);
    defer third.release();
    try std.testing.expectEqual(@as(usize, 1), third.data().length);
    const third_items = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = third.data().children[1].data() };
    try std.testing.expectEqual(@as(i32, 30), third_items.value(0));
}

test "ipc stream roundtrip sparse union int32/bool" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const union_fields = [_]Field{
        .{ .name = "i", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const union_type = datatype.UnionType{
        .type_ids = union_type_ids[0..],
        .fields = union_fields[0..],
        .mode = .sparse,
    };
    const sparse_union_type = DataType{ .sparse_union = union_type };
    const fields = [_]Field{
        .{ .name = "u", .data_type = &sparse_union_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var int_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer int_builder.deinit();
    try int_builder.append(10);
    try int_builder.append(20);
    try int_builder.append(30);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try @import("../array/boolean_array.zig").BooleanBuilder.init(allocator, 3);
    defer bool_builder.deinit();
    try bool_builder.append(false);
    try bool_builder.append(true);
    try bool_builder.append(false);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var union_builder = try @import("../array/advanced_array.zig").SparseUnionBuilder.init(allocator, union_type, 3);
    defer union_builder.deinit();
    try union_builder.appendTypeId(5);
    try union_builder.appendTypeId(7);
    try union_builder.appendTypeId(5);
    var union_ref = try union_builder.finish(&[_]ArrayRef{ int_ref, bool_ref });
    defer union_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{union_ref});
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .sparse_union);
    try std.testing.expectEqual(@as(i8, 5), out_schema.fields[0].data_type.sparse_union.type_ids[0]);
    try std.testing.expectEqual(@as(i8, 7), out_schema.fields[0].data_type.sparse_union.type_ids[1]);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const out_union = @import("../array/advanced_array.zig").SparseUnionArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i8, 5), out_union.typeId(0));
    try std.testing.expectEqual(@as(i8, 7), out_union.typeId(1));
    try std.testing.expectEqual(@as(i8, 5), out_union.typeId(2));

    var v0 = try out_union.value(0);
    defer v0.release();
    var v1 = try out_union.value(1);
    defer v1.release();
    const int_at_0 = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = v0.data() };
    const b1 = @import("../array/boolean_array.zig").BooleanArray{ .data = v1.data() };
    try std.testing.expectEqual(@as(i32, 10), int_at_0.value(0));
    try std.testing.expectEqual(true, b1.value(0));
}

test "ipc stream roundtrip dense union int32/bool" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const union_fields = [_]Field{
        .{ .name = "i", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const union_type = datatype.UnionType{
        .type_ids = union_type_ids[0..],
        .fields = union_fields[0..],
        .mode = .dense,
    };
    const dense_union_type = DataType{ .dense_union = union_type };
    const fields = [_]Field{
        .{ .name = "u", .data_type = &dense_union_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var int_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(10);
    try int_builder.append(20);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try @import("../array/boolean_array.zig").BooleanBuilder.init(allocator, 1);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var union_builder = try @import("../array/advanced_array.zig").DenseUnionBuilder.init(allocator, union_type, 3);
    defer union_builder.deinit();
    try union_builder.append(5, 0);
    try union_builder.append(7, 0);
    try union_builder.append(5, 1);
    var union_ref = try union_builder.finish(&[_]ArrayRef{ int_ref, bool_ref });
    defer union_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{union_ref});
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .dense_union);
    try std.testing.expectEqual(@as(i8, 5), out_schema.fields[0].data_type.dense_union.type_ids[0]);
    try std.testing.expectEqual(@as(i8, 7), out_schema.fields[0].data_type.dense_union.type_ids[1]);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    const out_union = @import("../array/advanced_array.zig").DenseUnionArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i8, 5), out_union.typeId(0));
    try std.testing.expectEqual(@as(i8, 7), out_union.typeId(1));
    try std.testing.expectEqual(@as(i8, 5), out_union.typeId(2));
    try std.testing.expectEqual(@as(i32, 0), out_union.childOffset(0));
    try std.testing.expectEqual(@as(i32, 0), out_union.childOffset(1));
    try std.testing.expectEqual(@as(i32, 1), out_union.childOffset(2));

    var v1 = try out_union.value(1);
    defer v1.release();
    var v2 = try out_union.value(2);
    defer v2.release();
    const b1 = @import("../array/boolean_array.zig").BooleanArray{ .data = v1.data() };
    const int_at_2 = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = v2.data() };
    try std.testing.expectEqual(true, b1.value(0));
    try std.testing.expectEqual(@as(i32, 20), int_at_2.value(0));
}

test "ipc stream roundtrip run-end encoded int32 values" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .int32 = {} };
    const ree_type = DataType{
        .run_end_encoded = .{
            .run_end_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_type,
        },
    };
    const fields = [_]Field{
        .{ .name = "ree", .data_type = &ree_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var values_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(100);
    try values_builder.append(200);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var ree_builder = try @import("../array/advanced_array.zig").RunEndEncodedBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        2,
    );
    defer ree_builder.deinit();
    try ree_builder.appendRunEnd(2);
    try ree_builder.appendRunEnd(5);
    var ree_ref = try ree_builder.finish(values_ref);
    defer ree_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ree_ref});
    defer batch.deinit();

    var out_buf = std.array_list.Managed(u8).init(allocator);
    defer out_buf.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out_buf.writer())).init(allocator, out_buf.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out_buf.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .run_end_encoded);
    try std.testing.expectEqual(@as(u8, 32), out_schema.fields[0].data_type.run_end_encoded.run_end_type.bit_width);

    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();

    try std.testing.expectEqual(@as(usize, 0), out_batch.columns[0].data().buffers.len);
    try std.testing.expectEqual(@as(usize, 2), out_batch.columns[0].data().children.len);

    const ree = @import("../array/advanced_array.zig").RunEndEncodedArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqual(@as(usize, 5), ree.len());

    var v0 = try ree.value(0);
    defer v0.release();
    var v4 = try ree.value(4);
    defer v4.release();
    const a0 = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = v0.data() };
    const a4 = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = v4.data() };
    try std.testing.expectEqual(@as(i32, 100), a0.value(0));
    try std.testing.expectEqual(@as(i32, 200), a4.value(0));
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
    defer writer.deinit();
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
    defer writer_a.deinit();
    var writer_b = @import("stream_writer.zig").StreamWriter(@TypeOf(out_b.writer())).init(allocator, out_b.writer());
    defer writer_b.deinit();
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
    defer real_writer.deinit();
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

test "ipc reader returns EndOfStream for truncated metadata payload" {
    const allocator = std.testing.allocator;

    var bytes = std.array_list.Managed(u8).init(allocator);
    defer bytes.deinit();

    // Declare metadata length as 16 bytes but provide only 4 bytes.
    try format.writeMessageLength(bytes.writer(), 16);
    try bytes.writer().writeAll(&[_]u8{ 0x01, 0x02, 0x03, 0x04 });

    var stream = std.io.fixedBufferStream(bytes.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    try std.testing.expectError(error.EndOfStream, reader.readSchema());
}

test "ipc reader rejects non-schema first message" {
    const allocator = std.testing.allocator;

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    const nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    const buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    const batch_ptr = try allocator.create(fbs.RecordBatchT);
    batch_ptr.* = .{
        .length = 0,
        .nodes = nodes,
        .buffers = buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };

    var msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .RecordBatch = batch_ptr },
        .bodyLength = 0,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg.deinit(allocator);

    try appendEncodedMessage(allocator, out.writer(), msg, &.{});
    try format.writeMessageLength(out.writer(), 0);

    var stream = std.io.fixedBufferStream(out.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    try std.testing.expectError(StreamError.InvalidMessage, reader.readSchema());
}

test "ipc reader rejects record batch buffer offset beyond body" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);

    var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try nodes.append(allocator, .{ .length = 1, .null_count = 0 });

    var buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try buffers.append(allocator, .{ .offset = 0, .length = 0 });
    // int32 values buffer claims bytes far beyond the provided body (len=1).
    try buffers.append(allocator, .{ .offset = 8, .length = 4 });

    const batch_ptr = try allocator.create(fbs.RecordBatchT);
    batch_ptr.* = .{
        .length = 1,
        .nodes = nodes,
        .buffers = buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };

    var malformed_msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .RecordBatch = batch_ptr },
        .bodyLength = 1,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer malformed_msg.deinit(allocator);

    try appendEncodedMessage(allocator, out.writer(), malformed_msg, &[_]u8{0x00});
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    try std.testing.expectError(StreamError.InvalidBody, reader.nextRecordBatch());
}

test "ipc reader rejects record batch with trailing metadata entries" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);

    var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try nodes.append(allocator, .{ .length = 1, .null_count = 0 });
    // Unused trailing node should be rejected.
    try nodes.append(allocator, .{ .length = 999, .null_count = 0 });

    var buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try buffers.append(allocator, .{ .offset = 0, .length = 0 });
    try buffers.append(allocator, .{ .offset = 0, .length = 4 });
    // Unused trailing buffer should be rejected.
    try buffers.append(allocator, .{ .offset = 0, .length = 0 });

    const batch_ptr = try allocator.create(fbs.RecordBatchT);
    batch_ptr.* = .{
        .length = 1,
        .nodes = nodes,
        .buffers = buffers,
        .variadicBufferCounts = try std.ArrayList(i64).initCapacity(allocator, 0),
    };

    var malformed_msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .RecordBatch = batch_ptr },
        .bodyLength = 4,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer malformed_msg.deinit(allocator);

    // One int32 value.
    try appendEncodedMessage(allocator, out.writer(), malformed_msg, &[_]u8{ 1, 0, 0, 0 });
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    try std.testing.expectError(StreamError.InvalidMetadata, reader.nextRecordBatch());
}

test "ipc reader rejects unexpected variadic buffer counts" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = @import("stream_writer.zig").StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);

    var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    try nodes.append(allocator, .{ .length = 1, .null_count = 0 });

    var buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    try buffers.append(allocator, .{ .offset = 0, .length = 0 });
    try buffers.append(allocator, .{ .offset = 0, .length = 4 });

    var variadic_counts = try std.ArrayList(i64).initCapacity(allocator, 1);
    try variadic_counts.append(allocator, 1);

    const batch_ptr = try allocator.create(fbs.RecordBatchT);
    batch_ptr.* = .{
        .length = 1,
        .nodes = nodes,
        .buffers = buffers,
        .variadicBufferCounts = variadic_counts,
    };

    var malformed_msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .RecordBatch = batch_ptr },
        .bodyLength = 4,
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer malformed_msg.deinit(allocator);

    try appendEncodedMessage(allocator, out.writer(), malformed_msg, &[_]u8{ 1, 0, 0, 0 });
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    try std.testing.expectError(StreamError.InvalidMetadata, reader.nextRecordBatch());
}

fn appendEncodedMessage(allocator: std.mem.Allocator, writer: anytype, msg: fbs.MessageT, body: []const u8) !void {
    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();

    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    const msg_off = try fbs.MessageT.Pack(msg, &builder, opts);
    try fbs.Message.FinishBuffer(&builder, msg_off);
    const metadata = try builder.finishedBytes();

    try format.writeMessageLength(writer, @intCast(metadata.len));
    try writer.writeAll(metadata);
    try format.writePadding(writer, format.padLen(metadata.len));

    if (body.len > 0) try writer.writeAll(body);
    try format.writePadding(writer, format.padLen(body.len));
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
