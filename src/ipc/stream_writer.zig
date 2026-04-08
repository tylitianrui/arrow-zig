const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const array_ref = @import("../array/array_ref.zig");
const array_data = @import("../array/array_data.zig");
const buffer = @import("../buffer.zig");
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

const WriterError = StreamError || fb.common.PackError || error{OutOfMemory};

const fbs = struct {
    const Message = arrow_fbs.org_apache_arrow_flatbuf_Message.Message;
    const MessageT = arrow_fbs.org_apache_arrow_flatbuf_Message.MessageT;
    const MessageHeaderT = arrow_fbs.org_apache_arrow_flatbuf_MessageHeader.MessageHeaderT;
    const MetadataVersion = arrow_fbs.org_apache_arrow_flatbuf_MetadataVersion.MetadataVersion;
    const SchemaT = arrow_fbs.org_apache_arrow_flatbuf_Schema.SchemaT;
    const FieldT = arrow_fbs.org_apache_arrow_flatbuf_Field.FieldT;
    const TypeT = arrow_fbs.org_apache_arrow_flatbuf_Type.TypeT;
    const Endianness = arrow_fbs.org_apache_arrow_flatbuf_Endianness.Endianness;
    const KeyValueT = arrow_fbs.org_apache_arrow_flatbuf_KeyValue.KeyValueT;
    const RecordBatchT = arrow_fbs.org_apache_arrow_flatbuf_RecordBatch.RecordBatchT;
    const DictionaryBatchT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryBatch.DictionaryBatchT;
    const FieldNodeT = arrow_fbs.org_apache_arrow_flatbuf_FieldNode.FieldNodeT;
    const BufferT = arrow_fbs.org_apache_arrow_flatbuf_Buffer.BufferT;
    const DictionaryEncodingT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryEncoding.DictionaryEncodingT;
    const IntT = arrow_fbs.org_apache_arrow_flatbuf_Int.IntT;
    const FloatingPointT = arrow_fbs.org_apache_arrow_flatbuf_FloatingPoint.FloatingPointT;
    const Precision = arrow_fbs.org_apache_arrow_flatbuf_Precision.Precision;
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
    const FixedSizeBinaryT = arrow_fbs.org_apache_arrow_flatbuf_FixedSizeBinary.FixedSizeBinaryT;
    const FixedSizeListT = arrow_fbs.org_apache_arrow_flatbuf_FixedSizeList.FixedSizeListT;
    const MapT = arrow_fbs.org_apache_arrow_flatbuf_Map.MapT;
    const BoolT = arrow_fbs.org_apache_arrow_flatbuf_Bool.BoolT;
    const BinaryT = arrow_fbs.org_apache_arrow_flatbuf_Binary.BinaryT;
    const Utf8T = arrow_fbs.org_apache_arrow_flatbuf_Utf8.Utf8T;
    const LargeBinaryT = arrow_fbs.org_apache_arrow_flatbuf_LargeBinary.LargeBinaryT;
    const LargeUtf8T = arrow_fbs.org_apache_arrow_flatbuf_LargeUtf8.LargeUtf8T;
    const Utf8ViewT = arrow_fbs.org_apache_arrow_flatbuf_Utf8View.Utf8ViewT;
    const BinaryViewT = arrow_fbs.org_apache_arrow_flatbuf_BinaryView.BinaryViewT;
    const ListT = arrow_fbs.org_apache_arrow_flatbuf_List.ListT;
    const LargeListT = arrow_fbs.org_apache_arrow_flatbuf_LargeList.LargeListT;
    const ListViewT = arrow_fbs.org_apache_arrow_flatbuf_ListView.ListViewT;
    const LargeListViewT = arrow_fbs.org_apache_arrow_flatbuf_LargeListView.LargeListViewT;
    const Struct_T = arrow_fbs.org_apache_arrow_flatbuf_Struct_.Struct_T;
    const NullT = arrow_fbs.org_apache_arrow_flatbuf_Null.NullT;
};

pub fn StreamWriter(comptime WriterType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        writer: WriterType,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, writer: WriterType) Self {
            return .{ .allocator = allocator, .writer = writer };
        }

        pub fn writeSchema(self: *Self, schema: Schema) (WriterError || @TypeOf(self.writer).Error)!void {
            const schema_ptr = try self.allocator.create(fbs.SchemaT);
            errdefer self.allocator.destroy(schema_ptr);
            var next_dictionary_id: i64 = 0;
            schema_ptr.* = try buildSchemaT(self.allocator, schema, &next_dictionary_id);

            var msg = fbs.MessageT{
                .version = .V5,
                .header = .{ .Schema = schema_ptr },
                .bodyLength = 0,
                .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(self.allocator, 0),
            };
            defer msg.deinit(self.allocator);

            try writeMessage(self.allocator, self.writer, msg, &.{});
        }

        pub fn writeRecordBatch(self: *Self, batch: RecordBatch) (WriterError || @TypeOf(self.writer).Error)!void {
            var dictionary_ids = try std.ArrayList(i64).initCapacity(self.allocator, 0);
            defer dictionary_ids.deinit(self.allocator);
            var next_dictionary_id: i64 = 0;
            for (batch.schema.fields) |field| {
                try collectDictionaryIdsFromField(self.allocator, field, &next_dictionary_id, &dictionary_ids);
            }

            var dictionary_arrays = try std.ArrayList(ArrayRef).initCapacity(self.allocator, 0);
            defer {
                for (dictionary_arrays.items) |dict_ref| {
                    var owned = dict_ref;
                    owned.release();
                }
                dictionary_arrays.deinit(self.allocator);
            }
            for (batch.columns) |col| {
                try collectDictionaryArraysFromData(self.allocator, col.data(), &dictionary_arrays);
            }
            if (dictionary_ids.items.len != dictionary_arrays.items.len) return StreamError.InvalidMetadata;

            for (dictionary_ids.items, dictionary_arrays.items) |dictionary_id, dict_ref| {
                try writeDictionaryBatch(self.allocator, self.writer, dictionary_id, dict_ref.data());
            }

            var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(self.allocator, 0);
            var buffers = try std.ArrayList(fbs.BufferT).initCapacity(self.allocator, 0);
            var variadic_buffer_counts = try std.ArrayList(i64).initCapacity(self.allocator, 0);
            var body_buffers = try std.ArrayList(array_data.SharedBuffer).initCapacity(self.allocator, 0);
            defer body_buffers.deinit(self.allocator);

            var body_offset: u64 = 0;
            for (batch.columns) |col| {
                try appendArrayMeta(self.allocator, col.data(), &nodes, &buffers, &variadic_buffer_counts, &body_buffers, &body_offset);
            }

            const record_batch_ptr = try self.allocator.create(fbs.RecordBatchT);
            errdefer self.allocator.destroy(record_batch_ptr);
            record_batch_ptr.* = .{
                .length = @intCast(batch.numRows()),
                .nodes = nodes,
                .buffers = buffers,
                .variadicBufferCounts = variadic_buffer_counts,
            };

            var msg = fbs.MessageT{
                .version = .V5,
                .header = .{ .RecordBatch = record_batch_ptr },
                .bodyLength = @intCast(body_offset),
                .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(self.allocator, 0),
            };
            defer msg.deinit(self.allocator);

            try writeMessage(self.allocator, self.writer, msg, body_buffers.items);
        }

        pub fn writeEnd(self: *Self) !void {
            // Emit EOS as continuation marker + 0-length metadata (8 bytes).
            try format.writeMessageLength(self.writer, 0);
        }
    };
}

fn writeMessage(allocator: std.mem.Allocator, writer: anytype, msg: fbs.MessageT, body_buffers: []const array_data.SharedBuffer) (WriterError || @TypeOf(writer).Error)!void {
    var builder = fb.Builder.init(allocator);
    defer builder.deinitAll();
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    const msg_off = try fbs.MessageT.Pack(msg, &builder, opts);
    try fbs.Message.FinishBuffer(&builder, msg_off);
    const metadata = try builder.finishedBytes();

    try format.writeMessageLength(writer, @intCast(metadata.len));
    try writer.writeAll(metadata);
    try format.writePadding(writer, format.padLen(metadata.len));

    var pad_bytes: [buffer.ALIGNMENT]u8 = [_]u8{0} ** buffer.ALIGNMENT;
    for (body_buffers) |buf| {
        if (buf.len() > 0) try writer.writeAll(buf.data);
        const padded = buffer.alignedSize(buf.len());
        const pad_len = padded - buf.len();
        if (pad_len > 0) try writer.writeAll(pad_bytes[0..pad_len]);
    }
    try format.writePadding(writer, format.padLen(@as(usize, @intCast(msg.bodyLength))));
}

fn buildSchemaT(allocator: std.mem.Allocator, schema: Schema, next_dictionary_id: *i64) WriterError!fbs.SchemaT {
    var fields = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0);
    for (schema.fields) |field| {
        try fields.append(allocator, try buildFieldT(allocator, field, next_dictionary_id));
    }

    return .{
        .endianness = .Little,
        .fields = fields,
        .custom_metadata = try buildCustomMetadataT(allocator, schema.metadata),
        .features = try std.ArrayList(i64).initCapacity(allocator, 0),
    };
}

fn buildFieldT(allocator: std.mem.Allocator, field: Field, next_dictionary_id: *i64) WriterError!fbs.FieldT {
    var children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0);
    const logical_type = switch (field.data_type.*) {
        .dictionary => |dict| dict.value_type.*,
        else => field.data_type.*,
    };
    const type_t = try buildTypeT(allocator, logical_type);

    switch (logical_type) {
        .list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field, next_dictionary_id));
        },
        .large_list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field, next_dictionary_id));
        },
        .fixed_size_list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field, next_dictionary_id));
        },
        .list_view => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field, next_dictionary_id));
        },
        .large_list_view => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field, next_dictionary_id));
        },
        .struct_ => |st| {
            for (st.fields) |child| try children.append(allocator, try buildFieldT(allocator, child, next_dictionary_id));
        },
        .map => |map_t| {
            var entry_children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0);
            try entry_children.append(allocator, try buildFieldT(allocator, map_t.key_field, next_dictionary_id));
            try entry_children.append(allocator, try buildFieldT(allocator, map_t.item_field, next_dictionary_id));
            try children.append(allocator, .{
                .name = "entries",
                .nullable = false,
                .type = .{ .Struct_ = try allocT(allocator, fbs.Struct_T, .{}) },
                .dictionary = null,
                .children = entry_children,
                .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
            });
        },
        .sparse_union, .dense_union => |uni| {
            for (uni.fields) |child| try children.append(allocator, try buildFieldT(allocator, child, next_dictionary_id));
        },
        .run_end_encoded => |ree| {
            const run_end_dt = dataTypeFromIntType(ree.run_end_type);
            const run_end_field = Field{
                .name = "run_ends",
                .data_type = &run_end_dt,
                .nullable = false,
            };
            const value_dt = ree.value_type.*;
            const value_field = Field{
                .name = "values",
                .data_type = &value_dt,
                .nullable = true,
            };
            try children.append(allocator, try buildFieldT(allocator, run_end_field, next_dictionary_id));
            try children.append(allocator, try buildFieldT(allocator, value_field, next_dictionary_id));
        },
        else => {},
    }

    const dictionary_t = switch (field.data_type.*) {
        .dictionary => |dict| blk: {
            const dictionary_id = dict.id orelse id_blk: {
                const assigned = next_dictionary_id.*;
                next_dictionary_id.* += 1;
                break :id_blk assigned;
            };
            const index_type_ptr = try allocT(allocator, fbs.IntT, .{
                .bitWidth = dict.index_type.bit_width,
                .is_signed = dict.index_type.signed,
            });
            break :blk try allocT(allocator, fbs.DictionaryEncodingT, .{
                .id = dictionary_id,
                .indexType = index_type_ptr,
                .isOrdered = dict.ordered,
                .dictionaryKind = .DenseArray,
            });
        },
        else => null,
    };

    return .{
        .name = field.name,
        .nullable = field.nullable,
        .type = type_t,
        .dictionary = dictionary_t,
        .children = children,
        .custom_metadata = try buildCustomMetadataT(allocator, field.metadata),
    };
}

fn buildCustomMetadataT(allocator: std.mem.Allocator, metadata: ?[]const datatype.KeyValue) WriterError!std.ArrayList(fbs.KeyValueT) {
    const kvs = metadata orelse return std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0);
    if (kvs.len == 0) return std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0);

    const sorted = try allocator.dupe(datatype.KeyValue, kvs);
    defer allocator.free(sorted);
    std.sort.pdq(datatype.KeyValue, sorted, {}, lessThanKeyValue);

    var out = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, sorted.len);
    for (sorted) |kv| {
        try out.append(allocator, .{
            .key = kv.key,
            .value = kv.value,
        });
    }
    return out;
}

fn lessThanKeyValue(_: void, a: datatype.KeyValue, b: datatype.KeyValue) bool {
    const key_order = std.mem.order(u8, a.key, b.key);
    if (key_order != .eq) return key_order == .lt;
    return std.mem.order(u8, a.value, b.value) == .lt;
}

fn buildTypeT(allocator: std.mem.Allocator, dt: DataType) WriterError!fbs.TypeT {
    return switch (dt) {
        .null => .{ .Null = try allocT(allocator, fbs.NullT, .{}) },
        .bool => .{ .Bool = try allocT(allocator, fbs.BoolT, .{}) },
        .uint8 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 8, .is_signed = false }) },
        .int8 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 8, .is_signed = true }) },
        .uint16 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 16, .is_signed = false }) },
        .int16 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 16, .is_signed = true }) },
        .uint32 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 32, .is_signed = false }) },
        .int32 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 32, .is_signed = true }) },
        .uint64 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 64, .is_signed = false }) },
        .int64 => .{ .Int = try allocT(allocator, fbs.IntT, .{ .bitWidth = 64, .is_signed = true }) },
        .half_float => .{ .FloatingPoint = try allocT(allocator, fbs.FloatingPointT, .{ .precision = .HALF }) },
        .float => .{ .FloatingPoint = try allocT(allocator, fbs.FloatingPointT, .{ .precision = .SINGLE }) },
        .double => .{ .FloatingPoint = try allocT(allocator, fbs.FloatingPointT, .{ .precision = .DOUBLE }) },
        .string => .{ .Utf8 = try allocT(allocator, fbs.Utf8T, .{}) },
        .binary => .{ .Binary = try allocT(allocator, fbs.BinaryT, .{}) },
        .large_string => .{ .LargeUtf8 = try allocT(allocator, fbs.LargeUtf8T, .{}) },
        .large_binary => .{ .LargeBinary = try allocT(allocator, fbs.LargeBinaryT, .{}) },
        .string_view => .{ .Utf8View = try allocT(allocator, fbs.Utf8ViewT, .{}) },
        .binary_view => .{ .BinaryView = try allocT(allocator, fbs.BinaryViewT, .{}) },
        .list => .{ .List = try allocT(allocator, fbs.ListT, .{}) },
        .large_list => .{ .LargeList = try allocT(allocator, fbs.LargeListT, .{}) },
        .list_view => .{ .ListView = try allocT(allocator, fbs.ListViewT, .{}) },
        .large_list_view => .{ .LargeListView = try allocT(allocator, fbs.LargeListViewT, .{}) },
        .struct_ => .{ .Struct_ = try allocT(allocator, fbs.Struct_T, .{}) },
        .fixed_size_binary => |fsb| .{ .FixedSizeBinary = try allocT(allocator, fbs.FixedSizeBinaryT, .{ .byteWidth = fsb.byte_width }) },
        .fixed_size_list => |fsl| .{ .FixedSizeList = try allocT(allocator, fbs.FixedSizeListT, .{ .listSize = fsl.list_size }) },
        .date32 => .{ .Date = try allocT(allocator, fbs.DateT, .{ .unit = .DAY }) },
        .date64 => .{ .Date = try allocT(allocator, fbs.DateT, .{ .unit = .MILLISECOND }) },
        .time32 => |t| .{ .Time = try allocT(allocator, fbs.TimeT, .{ .unit = toFbsTimeUnit(t.unit), .bitWidth = 32 }) },
        .time64 => |t| .{ .Time = try allocT(allocator, fbs.TimeT, .{ .unit = toFbsTimeUnit(t.unit), .bitWidth = 64 }) },
        .timestamp => |ts| .{ .Timestamp = try allocT(allocator, fbs.TimestampT, .{ .unit = toFbsTimeUnit(ts.unit), .timezone = ts.timezone orelse "" }) },
        .duration => |d| .{ .Duration = try allocT(allocator, fbs.DurationT, .{ .unit = toFbsTimeUnit(d.unit) }) },
        .interval_months => .{ .Interval = try allocT(allocator, fbs.IntervalT, .{ .unit = .YEAR_MONTH }) },
        .interval_day_time => .{ .Interval = try allocT(allocator, fbs.IntervalT, .{ .unit = .DAY_TIME }) },
        .interval_month_day_nano => .{ .Interval = try allocT(allocator, fbs.IntervalT, .{ .unit = .MONTH_DAY_NANO }) },
        .decimal32 => |d| .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 32 }) },
        .decimal64 => |d| .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 64 }) },
        .decimal128 => |d| .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 128 }) },
        .decimal256 => |d| .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 256 }) },
        .map => |m| .{ .Map = try allocT(allocator, fbs.MapT, .{ .keysSorted = m.keys_sorted }) },
        .sparse_union => |u| blk: {
            var type_ids = try std.ArrayList(i32).initCapacity(allocator, u.type_ids.len);
            for (u.type_ids) |id| try type_ids.append(allocator, id);
            break :blk .{
                .Union = try allocT(allocator, fbs.UnionT, .{
                    .mode = .Sparse,
                    .typeIds = type_ids,
                }),
            };
        },
        .dense_union => |u| blk: {
            var type_ids = try std.ArrayList(i32).initCapacity(allocator, u.type_ids.len);
            for (u.type_ids) |id| try type_ids.append(allocator, id);
            break :blk .{
                .Union = try allocT(allocator, fbs.UnionT, .{
                    .mode = .Dense,
                    .typeIds = type_ids,
                }),
            };
        },
        .run_end_encoded => .{ .RunEndEncoded = try allocT(allocator, fbs.RunEndEncodedT, .{}) },
        .dictionary, .extension => StreamError.UnsupportedType,
        else => StreamError.UnsupportedType,
    };
}

fn toFbsTimeUnit(unit: datatype.TimeUnit) fbs.TimeUnit {
    return switch (unit) {
        .second => .SECOND,
        .millisecond => .MILLISECOND,
        .microsecond => .MICROSECOND,
        .nanosecond => .NANOSECOND,
    };
}

fn allocT(allocator: std.mem.Allocator, comptime T: type, value: T) error{OutOfMemory}!*T {
    const ptr = try allocator.create(T);
    ptr.* = value;
    return ptr;
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

fn collectDictionaryIdsFromField(
    allocator: std.mem.Allocator,
    field: Field,
    next_dictionary_id: *i64,
    ids: *std.ArrayList(i64),
) error{OutOfMemory}!void {
    switch (field.data_type.*) {
        .dictionary => |dict| {
            const dictionary_id = dict.id orelse id_blk: {
                const assigned = next_dictionary_id.*;
                next_dictionary_id.* += 1;
                break :id_blk assigned;
            };
            try ids.append(allocator, dictionary_id);
        },
        .list => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .large_list => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .fixed_size_list => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .struct_ => |st| for (st.fields) |child| try collectDictionaryIdsFromField(allocator, child, next_dictionary_id, ids),
        else => {},
    }
}

fn collectDictionaryArraysFromData(
    allocator: std.mem.Allocator,
    data: *const ArrayData,
    out: *std.ArrayList(ArrayRef),
) error{OutOfMemory}!void {
    switch (data.data_type) {
        .dictionary => {
            if (data.dictionary == null) return;
            try out.append(allocator, data.dictionary.?.retain());
        },
        .list, .large_list, .fixed_size_list, .list_view, .large_list_view => {
            if (data.children.len == 1) try collectDictionaryArraysFromData(allocator, data.children[0].data(), out);
        },
        .struct_ => {
            for (data.children) |child| try collectDictionaryArraysFromData(allocator, child.data(), out);
        },
        else => {},
    }
}

fn writeDictionaryBatch(
    allocator: std.mem.Allocator,
    writer: anytype,
    dictionary_id: i64,
    dictionary_data: *const ArrayData,
) (WriterError || @TypeOf(writer).Error)!void {
    var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(allocator, 0);
    var buffers = try std.ArrayList(fbs.BufferT).initCapacity(allocator, 0);
    var variadic_buffer_counts = try std.ArrayList(i64).initCapacity(allocator, 0);
    var body_buffers = try std.ArrayList(array_data.SharedBuffer).initCapacity(allocator, 0);
    defer body_buffers.deinit(allocator);

    var body_offset: u64 = 0;
    try appendArrayMeta(allocator, dictionary_data, &nodes, &buffers, &variadic_buffer_counts, &body_buffers, &body_offset);

    const record_batch_ptr = try allocator.create(fbs.RecordBatchT);
    errdefer allocator.destroy(record_batch_ptr);
    record_batch_ptr.* = .{
        .length = @intCast(dictionary_data.length),
        .nodes = nodes,
        .buffers = buffers,
        .variadicBufferCounts = variadic_buffer_counts,
    };

    const dictionary_batch_ptr = try allocator.create(fbs.DictionaryBatchT);
    errdefer allocator.destroy(dictionary_batch_ptr);
    dictionary_batch_ptr.* = .{
        .id = dictionary_id,
        .data = record_batch_ptr,
        .isDelta = false,
    };

    var msg = fbs.MessageT{
        .version = .V5,
        .header = .{ .DictionaryBatch = dictionary_batch_ptr },
        .bodyLength = @intCast(body_offset),
        .custom_metadata = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0),
    };
    defer msg.deinit(allocator);

    try writeMessage(allocator, writer, msg, body_buffers.items);
}

fn appendArrayMeta(
    allocator: std.mem.Allocator,
    data: *const ArrayData,
    nodes: *std.ArrayList(fbs.FieldNodeT),
    buffers: *std.ArrayList(fbs.BufferT),
    variadic_buffer_counts: *std.ArrayList(i64),
    body_buffers: *std.ArrayList(array_data.SharedBuffer),
    body_offset: *u64,
) WriterError!void {
    const null_count = if (data.null_count) |count| count else computeNullCount(data);
    try nodes.append(allocator, .{ .length = @intCast(data.length), .null_count = @intCast(null_count) });

    for (data.buffers) |buf| {
        try buffers.append(allocator, .{ .offset = @intCast(body_offset.*), .length = @intCast(buf.len()) });
        try body_buffers.append(allocator, buf);
        body_offset.* += @intCast(buffer.alignedSize(buf.len()));
    }
    if (data.data_type == .string_view or data.data_type == .binary_view) {
        if (data.buffers.len < 2) return StreamError.InvalidMetadata;
        const variadic_count = std.math.sub(usize, data.buffers.len, 2) catch return StreamError.InvalidMetadata;
        try variadic_buffer_counts.append(allocator, @intCast(variadic_count));
    }

    switch (data.data_type) {
        .list, .large_list, .fixed_size_list, .map, .list_view, .large_list_view => {
            if (data.children.len != 1) return StreamError.InvalidMetadata;
            try appendArrayMeta(allocator, data.children[0].data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
        },
        .struct_ => {
            for (data.children) |child| {
                try appendArrayMeta(allocator, child.data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
            }
        },
        .sparse_union, .dense_union => {
            for (data.children) |child| {
                try appendArrayMeta(allocator, child.data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
            }
        },
        .run_end_encoded => |ree| {
            _ = ree;
            if (data.children.len != 1) return StreamError.InvalidMetadata;
            try appendArrayMeta(allocator, data.children[0].data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
        },
        .dictionary => {},
        else => {},
    }
}

fn computeNullCount(data: *const ArrayData) usize {
    const validity = data.validity() orelse return 0;
    return validity.countNulls();
}

fn parseGoldenHexCsv(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);

    var it = std.mem.tokenizeAny(u8, text, ", \n\r\t");
    while (it.next()) |token| {
        const digits = if (std.mem.startsWith(u8, token, "0x") or std.mem.startsWith(u8, token, "0X")) token[2..] else token;
        if (digits.len == 0) continue;
        if (digits.len != 2) return error.InvalidFixtureFormat;
        try out.append(allocator, try std.fmt.parseInt(u8, digits, 16));
    }

    return out.toOwnedSlice(allocator);
}

test "ipc writer stream output matches golden fixture" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var col_ref = try builder.finish();
    defer col_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{col_ref});
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    const golden_text = @embedFile("testdata/writer_simple_stream.hex");
    const expected = try parseGoldenHexCsv(allocator, golden_text);
    defer allocator.free(expected);

    try std.testing.expectEqualSlices(u8, expected, out.items);
}

test "ipc writer roundtrip supports view types and variadicBufferCounts" {
    const allocator = std.testing.allocator;
    const one = [_]u8{0};

    const string_view_type = DataType{ .string_view = {} };
    const binary_view_type = DataType{ .binary_view = {} };
    const list_item_type = DataType{ .int32 = {} };
    const list_item_field = Field{ .name = "item", .data_type = &list_item_type, .nullable = true };
    const list_view_type = DataType{ .list_view = .{ .value_field = list_item_field } };
    const large_list_view_type = DataType{ .large_list_view = .{ .value_field = list_item_field } };
    const fields = [_]Field{
        .{ .name = "sv", .data_type = &string_view_type, .nullable = true },
        .{ .name = "bv", .data_type = &binary_view_type, .nullable = true },
        .{ .name = "lv", .data_type = &list_view_type, .nullable = true },
        .{ .name = "llv", .data_type = &large_list_view_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    const empty_children = try allocator.alloc(ArrayRef, 0);

    const sv_buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    sv_buffers[0] = array_data.SharedBuffer.empty;
    sv_buffers[1] = array_data.SharedBuffer.empty;
    var sv_ref = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = string_view_type,
        .length = 0,
        .null_count = 0,
        .buffers = sv_buffers,
        .children = empty_children,
        .dictionary = null,
    });
    defer sv_ref.release();

    const bv_buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    bv_buffers[0] = array_data.SharedBuffer.empty;
    bv_buffers[1] = array_data.SharedBuffer.empty;
    bv_buffers[2] = array_data.SharedBuffer.init(one[0..]); // one variadic buffer
    const empty_children_bv = try allocator.alloc(ArrayRef, 0);
    var bv_ref = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = binary_view_type,
        .length = 0,
        .null_count = 0,
        .buffers = bv_buffers,
        .children = empty_children_bv,
        .dictionary = null,
    });
    defer bv_ref.release();

    const child_buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    child_buffers[0] = array_data.SharedBuffer.empty;
    child_buffers[1] = array_data.SharedBuffer.empty;
    const child_children = try allocator.alloc(ArrayRef, 0);
    var child_ref = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = list_item_type,
        .length = 0,
        .null_count = 0,
        .buffers = child_buffers,
        .children = child_children,
        .dictionary = null,
    });
    defer child_ref.release();

    const lv_buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    lv_buffers[0] = array_data.SharedBuffer.empty;
    lv_buffers[1] = array_data.SharedBuffer.empty;
    lv_buffers[2] = array_data.SharedBuffer.empty;
    const lv_children = try allocator.alloc(ArrayRef, 1);
    lv_children[0] = child_ref.retain();
    var lv_ref = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = list_view_type,
        .length = 0,
        .null_count = 0,
        .buffers = lv_buffers,
        .children = lv_children,
        .dictionary = null,
    });
    defer lv_ref.release();

    const llv_buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    llv_buffers[0] = array_data.SharedBuffer.empty;
    llv_buffers[1] = array_data.SharedBuffer.empty;
    llv_buffers[2] = array_data.SharedBuffer.empty;
    const llv_children = try allocator.alloc(ArrayRef, 1);
    llv_children[0] = child_ref.retain();
    var llv_ref = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = large_list_view_type,
        .length = 0,
        .null_count = 0,
        .buffers = llv_buffers,
        .children = llv_children,
        .dictionary = null,
    });
    defer llv_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ sv_ref, bv_ref, lv_ref, llv_ref });
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();
    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = @import("stream_reader.zig").StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();
    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .string_view);
    try std.testing.expect(out_schema.fields[1].data_type.* == .binary_view);
    try std.testing.expect(out_schema.fields[2].data_type.* == .list_view);
    try std.testing.expect(out_schema.fields[3].data_type.* == .large_list_view);
    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    try std.testing.expectEqual(@as(usize, 0), out_batch.numRows());
}
