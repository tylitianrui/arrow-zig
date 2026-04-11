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
const extension_name_key = "ARROW:extension:name";
const extension_metadata_key = "ARROW:extension:metadata";

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
        dictionary_values: std.AutoHashMap(i64, ArrayRef),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, writer: WriterType) Self {
            return .{
                .allocator = allocator,
                .writer = writer,
                .dictionary_values = std.AutoHashMap(i64, ArrayRef).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.clearDictionaryValues();
            self.dictionary_values.deinit();
        }

        fn clearDictionaryValues(self: *Self) void {
            var it = self.dictionary_values.iterator();
            while (it.next()) |entry| {
                var dict = entry.value_ptr.*;
                dict.release();
            }
            self.dictionary_values.clearRetainingCapacity();
        }

        pub fn writeSchema(self: *Self, schema: Schema) (WriterError || @TypeOf(self.writer).Error)!void {
            self.clearDictionaryValues();
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
            for (batch.schema().fields) |field| {
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
                const emission = try planDictionaryEmission(self.dictionary_values.get(dictionary_id), dict_ref);
                if (emission.mode != .skip) {
                    try writeDictionaryBatch(self.allocator, self.writer, dictionary_id, emission.array.data(), emission.mode == .delta);
                }
                if (emission.owned_slice) |slice| {
                    var owned = slice;
                    owned.release();
                }

                const previous = try self.dictionary_values.fetchPut(dictionary_id, dict_ref.retain());
                if (previous) |entry| {
                    var old = entry.value;
                    old.release();
                }
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

    // IPC stream framing stores the padded metadata length in the prefix.
    const metadata_len = format.paddedLen(metadata.len);
    try format.writeMessageLength(writer, @intCast(metadata_len));
    try writer.writeAll(metadata);
    try format.writePadding(writer, format.padLen(metadata.len));

    var pad_bytes: [format.Alignment]u8 = [_]u8{0} ** format.Alignment;
    for (body_buffers) |buf| {
        if (buf.len() > 0) try writer.writeAll(buf.data);
        const pad_len = format.padLen(buf.len());
        if (pad_len > 0) try writer.writeAll(pad_bytes[0..pad_len]);
    }
}

fn buildSchemaT(allocator: std.mem.Allocator, schema: Schema, next_dictionary_id: *i64) WriterError!fbs.SchemaT {
    if (schema.endianness != .little) return StreamError.UnsupportedType;
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
    const storage_type = storageDataType(logical_type);
    const extension_meta = switch (logical_type) {
        .extension => |ext| ext,
        else => null,
    };
    const type_t = try buildTypeT(allocator, storage_type);

    switch (storage_type) {
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
            if (!dict.index_type.signed) return StreamError.InvalidMetadata;
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
        .custom_metadata = try buildFieldCustomMetadataT(allocator, field.metadata, extension_meta),
    };
}

fn buildCustomMetadataT(allocator: std.mem.Allocator, metadata: ?[]const datatype.KeyValue) WriterError!std.ArrayList(fbs.KeyValueT) {
    const kvs = metadata orelse return std.ArrayList(fbs.KeyValueT).initCapacity(allocator, 0);
    var out = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, kvs.len);
    for (kvs) |kv| {
        try out.append(allocator, .{ .key = kv.key, .value = kv.value });
    }
    return out;
}

fn buildFieldCustomMetadataT(
    allocator: std.mem.Allocator,
    metadata: ?[]const datatype.KeyValue,
    extension_meta: ?datatype.ExtensionType,
) WriterError!std.ArrayList(fbs.KeyValueT) {
    const base_count = if (metadata) |m| m.len else 0;
    const ext_count: usize = if (extension_meta) |ext| if (ext.metadata != null) 2 else 1 else 0;

    var out = try std.ArrayList(fbs.KeyValueT).initCapacity(allocator, base_count + ext_count);
    if (metadata) |kvs| {
        for (kvs) |kv| {
            try out.append(allocator, .{ .key = kv.key, .value = kv.value });
        }
    }
    if (extension_meta) |ext| {
        try out.append(allocator, .{ .key = extension_name_key, .value = ext.name });
        if (ext.metadata) |meta| {
            try out.append(allocator, .{ .key = extension_metadata_key, .value = meta });
        }
    }
    return out;
}

fn validateDecimalPrecision(precision: u8, max_precision: u8) WriterError!void {
    if (precision == 0 or precision > max_precision) return StreamError.InvalidMetadata;
}

fn storageDataType(dt: DataType) DataType {
    return switch (dt) {
        .extension => |ext| storageDataType(ext.storage_type.*),
        else => dt,
    };
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
        .decimal32 => |d| blk: {
            try validateDecimalPrecision(d.precision, 9);
            break :blk .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 32 }) };
        },
        .decimal64 => |d| blk: {
            try validateDecimalPrecision(d.precision, 18);
            break :blk .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 64 }) };
        },
        .decimal128 => |d| blk: {
            try validateDecimalPrecision(d.precision, 38);
            break :blk .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 128 }) };
        },
        .decimal256 => |d| blk: {
            try validateDecimalPrecision(d.precision, 76);
            break :blk .{ .Decimal = try allocT(allocator, fbs.DecimalT, .{ .precision = @intCast(d.precision), .scale = d.scale, .bitWidth = 256 }) };
        },
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
        .extension => |ext| try buildTypeT(allocator, ext.storage_type.*),
        .dictionary => StreamError.UnsupportedType,
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
        .list_view => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .large_list_view => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .fixed_size_list => |lst| try collectDictionaryIdsFromField(allocator, lst.value_field, next_dictionary_id, ids),
        .map => |map_t| {
            try collectDictionaryIdsFromField(allocator, map_t.key_field, next_dictionary_id, ids);
            try collectDictionaryIdsFromField(allocator, map_t.item_field, next_dictionary_id, ids);
        },
        .struct_ => |st| for (st.fields) |child| try collectDictionaryIdsFromField(allocator, child, next_dictionary_id, ids),
        .sparse_union, .dense_union => |uni| for (uni.fields) |child| try collectDictionaryIdsFromField(allocator, child, next_dictionary_id, ids),
        .run_end_encoded => |ree| {
            const value_field = Field{
                .name = "values",
                .data_type = ree.value_type,
                .nullable = true,
            };
            try collectDictionaryIdsFromField(allocator, value_field, next_dictionary_id, ids);
        },
        .extension => |ext| {
            const storage_field = Field{
                .name = field.name,
                .data_type = ext.storage_type,
                .nullable = field.nullable,
                .metadata = field.metadata,
            };
            try collectDictionaryIdsFromField(allocator, storage_field, next_dictionary_id, ids);
        },
        else => {},
    }
}

fn collectDictionaryArraysFromData(
    allocator: std.mem.Allocator,
    data: *const ArrayData,
    out: *std.ArrayList(ArrayRef),
) error{OutOfMemory}!void {
    switch (storageDataType(data.data_type)) {
        .dictionary => {
            if (data.dictionary == null) return;
            try out.append(allocator, data.dictionary.?.retain());
        },
        .list, .large_list, .fixed_size_list, .list_view, .large_list_view => {
            if (data.children.len == 1) try collectDictionaryArraysFromData(allocator, data.children[0].data(), out);
        },
        .map => {
            if (data.children.len == 1) try collectDictionaryArraysFromData(allocator, data.children[0].data(), out);
        },
        .run_end_encoded => {
            if (data.children.len == 2) try collectDictionaryArraysFromData(allocator, data.children[1].data(), out);
        },
        .struct_ => {
            for (data.children) |child| try collectDictionaryArraysFromData(allocator, child.data(), out);
        },
        .sparse_union, .dense_union => {
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
    is_delta: bool,
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
        .isDelta = is_delta,
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

const DictionaryEmissionMode = enum {
    skip,
    full,
    delta,
};

const DictionaryEmission = struct {
    mode: DictionaryEmissionMode,
    array: ArrayRef,
    owned_slice: ?ArrayRef = null,
};

fn planDictionaryEmission(
    previous_opt: ?ArrayRef,
    current: ArrayRef,
) (StreamError || error{OutOfMemory})!DictionaryEmission {
    if (previous_opt == null) {
        return .{ .mode = .full, .array = current };
    }
    const previous = previous_opt.?;
    if (!datatype.dataTypeEql(previous.data().data_type, current.data().data_type)) return StreamError.InvalidMetadata;

    if (previous.data().length == current.data().length) {
        if (try arraysEqualForDelta(previous, current)) {
            return .{ .mode = .skip, .array = current };
        }
        return .{ .mode = .full, .array = current };
    }
    if (current.data().length < previous.data().length) {
        return .{ .mode = .full, .array = current };
    }
    if (!supportsDictionaryDeltaType(current.data().data_type)) {
        return .{ .mode = .full, .array = current };
    }

    var prefix = try current.slice(0, previous.data().length);
    defer prefix.release();
    if (!try arraysEqualForDelta(previous, prefix)) {
        return .{ .mode = .full, .array = current };
    }

    const delta_len = current.data().length - previous.data().length;
    if (delta_len == 0) {
        return .{ .mode = .skip, .array = current };
    }
    const tail = try current.slice(previous.data().length, delta_len);
    return .{ .mode = .delta, .array = tail, .owned_slice = tail };
}

fn supportsDictionaryDeltaType(dt: DataType) bool {
    return switch (storageDataType(dt)) {
        .null,
        .bool,
        .uint8,
        .int8,
        .uint16,
        .int16,
        .uint32,
        .int32,
        .uint64,
        .int64,
        .half_float,
        .float,
        .double,
        .date32,
        .date64,
        .time32,
        .time64,
        .timestamp,
        .duration,
        .interval_months,
        .interval_day_time,
        .interval_month_day_nano,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .fixed_size_binary,
        .string,
        .binary,
        .large_string,
        .large_binary,
        => true,
        else => false,
    };
}

fn arraysEqualForDelta(left: ArrayRef, right: ArrayRef) (StreamError || error{OutOfMemory})!bool {
    const a = left.data();
    const b = right.data();
    if (!datatype.dataTypeEql(a.data_type, b.data_type)) return false;
    if (a.length != b.length) return false;
    if (a.length == 0) return true;

    const dt = storageDataType(a.data_type);
    switch (dt) {
        .null => return true,
        .bool => {
            var i: usize = 0;
            while (i < a.length) : (i += 1) {
                const a_null = a.isNull(i);
                const b_null = b.isNull(i);
                if (a_null != b_null) return false;
                if (a_null) continue;
                if (bitAt(a.buffers[1], a.offset + i) != bitAt(b.buffers[1], b.offset + i)) return false;
            }
            return true;
        },
        .uint8, .int8 => return try fixedWidthEqual(a, b, 1),
        .uint16, .int16, .half_float => return try fixedWidthEqual(a, b, 2),
        .uint32, .int32, .float, .date32, .time32, .interval_months, .decimal32 => return try fixedWidthEqual(a, b, 4),
        .uint64, .int64, .double, .date64, .time64, .timestamp, .duration, .interval_day_time, .decimal64 => return try fixedWidthEqual(a, b, 8),
        .decimal128, .interval_month_day_nano => return try fixedWidthEqual(a, b, 16),
        .decimal256 => return try fixedWidthEqual(a, b, 32),
        .fixed_size_binary => |fsb| {
            const byte_width = std.math.cast(usize, fsb.byte_width) orelse return StreamError.InvalidMetadata;
            return try fixedWidthEqual(a, b, byte_width);
        },
        .string, .binary => return try variableBinaryEqualI32(a, b),
        .large_string, .large_binary => return try variableBinaryEqualI64(a, b),
        else => return false,
    }
}

fn fixedWidthEqual(a: *const ArrayData, b: *const ArrayData, byte_width: usize) StreamError!bool {
    if (a.buffers.len < 2 or b.buffers.len < 2) return StreamError.InvalidMetadata;
    var i: usize = 0;
    while (i < a.length) : (i += 1) {
        const a_null = a.isNull(i);
        const b_null = b.isNull(i);
        if (a_null != b_null) return false;
        if (a_null) continue;
        const a_idx = std.math.add(usize, a.offset, i) catch return StreamError.InvalidMetadata;
        const b_idx = std.math.add(usize, b.offset, i) catch return StreamError.InvalidMetadata;
        const a_start = std.math.mul(usize, a_idx, byte_width) catch return StreamError.InvalidMetadata;
        const b_start = std.math.mul(usize, b_idx, byte_width) catch return StreamError.InvalidMetadata;
        const a_end = std.math.add(usize, a_start, byte_width) catch return StreamError.InvalidMetadata;
        const b_end = std.math.add(usize, b_start, byte_width) catch return StreamError.InvalidMetadata;
        if (a_end > a.buffers[1].len() or b_end > b.buffers[1].len()) return StreamError.InvalidMetadata;
        if (!std.mem.eql(u8, a.buffers[1].data[a_start..a_end], b.buffers[1].data[b_start..b_end])) return false;
    }
    return true;
}

fn variableBinaryEqualI32(a: *const ArrayData, b: *const ArrayData) StreamError!bool {
    if (a.buffers.len < 3 or b.buffers.len < 3) return StreamError.InvalidMetadata;
    const a_offsets = a.buffers[1].typedSlice(i32);
    const b_offsets = b.buffers[1].typedSlice(i32);
    var i: usize = 0;
    while (i < a.length) : (i += 1) {
        const a_null = a.isNull(i);
        const b_null = b.isNull(i);
        if (a_null != b_null) return false;
        if (a_null) continue;
        const ai0 = std.math.cast(usize, a_offsets[a.offset + i]) orelse return StreamError.InvalidMetadata;
        const ai1 = std.math.cast(usize, a_offsets[a.offset + i + 1]) orelse return StreamError.InvalidMetadata;
        const bi0 = std.math.cast(usize, b_offsets[b.offset + i]) orelse return StreamError.InvalidMetadata;
        const bi1 = std.math.cast(usize, b_offsets[b.offset + i + 1]) orelse return StreamError.InvalidMetadata;
        if (ai1 < ai0 or bi1 < bi0) return StreamError.InvalidMetadata;
        if (ai1 > a.buffers[2].len() or bi1 > b.buffers[2].len()) return StreamError.InvalidMetadata;
        if (!std.mem.eql(u8, a.buffers[2].data[ai0..ai1], b.buffers[2].data[bi0..bi1])) return false;
    }
    return true;
}

fn variableBinaryEqualI64(a: *const ArrayData, b: *const ArrayData) StreamError!bool {
    if (a.buffers.len < 3 or b.buffers.len < 3) return StreamError.InvalidMetadata;
    const a_offsets = a.buffers[1].typedSlice(i64);
    const b_offsets = b.buffers[1].typedSlice(i64);
    var i: usize = 0;
    while (i < a.length) : (i += 1) {
        const a_null = a.isNull(i);
        const b_null = b.isNull(i);
        if (a_null != b_null) return false;
        if (a_null) continue;
        const ai0 = std.math.cast(usize, a_offsets[a.offset + i]) orelse return StreamError.InvalidMetadata;
        const ai1 = std.math.cast(usize, a_offsets[a.offset + i + 1]) orelse return StreamError.InvalidMetadata;
        const bi0 = std.math.cast(usize, b_offsets[b.offset + i]) orelse return StreamError.InvalidMetadata;
        const bi1 = std.math.cast(usize, b_offsets[b.offset + i + 1]) orelse return StreamError.InvalidMetadata;
        if (ai1 < ai0 or bi1 < bi0) return StreamError.InvalidMetadata;
        if (ai1 > a.buffers[2].len() or bi1 > b.buffers[2].len()) return StreamError.InvalidMetadata;
        if (!std.mem.eql(u8, a.buffers[2].data[ai0..ai1], b.buffers[2].data[bi0..bi1])) return false;
    }
    return true;
}

fn bitAt(buf: array_data.SharedBuffer, bit_index: usize) bool {
    return @import("../bitmap.zig").bitIsSet(buf.data, bit_index);
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
    const layout_dt = storageDataType(data.data_type);

    for (data.buffers) |buf| {
        try buffers.append(allocator, .{ .offset = @intCast(body_offset.*), .length = @intCast(buf.len()) });
        try body_buffers.append(allocator, buf);
        body_offset.* += @intCast(format.paddedLen(buf.len()));
    }
    if (layout_dt == .string_view or layout_dt == .binary_view) {
        if (data.buffers.len < 2) return StreamError.InvalidMetadata;
        const variadic_count = std.math.sub(usize, data.buffers.len, 2) catch return StreamError.InvalidMetadata;
        try variadic_buffer_counts.append(allocator, @intCast(variadic_count));
    }

    switch (layout_dt) {
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
            if (data.buffers.len != 0) return StreamError.InvalidMetadata;
            if (data.children.len != 2) return StreamError.InvalidMetadata;
            try appendArrayMeta(allocator, data.children[0].data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
            try appendArrayMeta(allocator, data.children[1].data(), nodes, buffers, variadic_buffer_counts, body_buffers, body_offset);
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

fn readNextMessageForTest(allocator: std.mem.Allocator, reader: anytype) anyerror!?fbs.MessageT {
    const meta_len_opt = try format.readMessageLength(reader);
    if (meta_len_opt == null) return null;
    const meta_len = meta_len_opt.?;

    const metadata = try allocator.alloc(u8, meta_len);
    defer allocator.free(metadata);
    if (meta_len > 0) try reader.readNoEof(metadata);
    try format.skipPadding(reader, format.padLen(meta_len));

    const msg = fbs.Message.GetRootAs(@constCast(metadata), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    const msg_t = try fbs.MessageT.Unpack(msg, opts);

    const body_len = std.math.cast(usize, msg_t.bodyLength) orelse return StreamError.InvalidBody;
    if (body_len > 0) {
        const body = try allocator.alloc(u8, body_len);
        defer allocator.free(body);
        try reader.readNoEof(body);
    }
    try format.skipPadding(reader, format.padLen(body_len));

    return msg_t;
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

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{col_ref});
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
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

    const string_view_type = DataType{ .string_view = {} };
    const binary_view_type = DataType{ .binary_view = {} };
    const fields = [_]Field{
        .{ .name = "sv", .data_type = &string_view_type, .nullable = true },
        .{ .name = "bv", .data_type = &binary_view_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var sv_builder = try @import("../array/view_array.zig").StringViewBuilder.init(allocator, 4, 32);
    defer sv_builder.deinit();
    try sv_builder.append("short");
    try sv_builder.appendNull();
    try sv_builder.append("tiny");
    try sv_builder.append("this string is longer than twelve");
    var sv_ref = try sv_builder.finish();
    defer sv_ref.release();

    var bv_builder = try @import("../array/view_array.zig").BinaryViewBuilder.init(allocator, 4, 32);
    defer bv_builder.deinit();
    try bv_builder.append("ab");
    try bv_builder.append("this-binary-view-is-long");
    try bv_builder.appendNull();
    try bv_builder.append("xy");
    var bv_ref = try bv_builder.finish();
    defer bv_ref.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{ sv_ref, bv_ref });
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();
    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = @import("stream_reader.zig").StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();
    const out_schema = try reader.readSchema();
    try std.testing.expect(out_schema.fields[0].data_type.* == .string_view);
    try std.testing.expect(out_schema.fields[1].data_type.* == .binary_view);
    const out_batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    try std.testing.expectEqual(@as(usize, 4), out_batch.numRows());

    const sv = @import("../array/view_array.zig").StringViewArray{ .data = out_batch.columns[0].data() };
    try std.testing.expectEqualStrings("short", sv.value(0));
    try std.testing.expect(sv.isNull(1));
    try std.testing.expectEqualStrings("tiny", sv.value(2));
    try std.testing.expectEqualStrings("this string is longer than twelve", sv.value(3));

    const bv = @import("../array/view_array.zig").BinaryViewArray{ .data = out_batch.columns[1].data() };
    try std.testing.expectEqualStrings("ab", bv.value(0));
    try std.testing.expectEqualStrings("this-binary-view-is-long", bv.value(1));
    try std.testing.expect(bv.isNull(2));
    try std.testing.expectEqualStrings("xy", bv.value(3));
}

test "ipc writer and reader roundtrip extension field metadata and values" {
    const allocator = std.testing.allocator;

    const storage_type = DataType{ .int32 = {} };
    const extension_type = DataType{
        .extension = .{
            .name = "com.example.int32_ext",
            .storage_type = &storage_type,
            .metadata = "v1",
        },
    };
    const field_metadata = [_]datatype.KeyValue{
        .{ .key = "owner", .value = "core" },
    };
    const fields = [_]Field{
        .{ .name = "id_ext", .data_type = &extension_type, .nullable = true, .metadata = field_metadata[0..] },
    };
    const schema = Schema{ .fields = fields[0..] };

    var storage_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer storage_builder.deinit();
    try storage_builder.append(7);
    try storage_builder.appendNull();
    try storage_builder.append(11);
    var storage_ref = try storage_builder.finish();
    defer storage_ref.release();

    var ext_builder = try @import("../array/extension_array.zig").ExtensionBuilder.init(allocator, extension_type.extension);
    defer ext_builder.deinit();
    var ext_ref = try ext_builder.finish(storage_ref);
    defer ext_ref.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{ext_ref});
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();
    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    var reader = @import("stream_reader.zig").StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const read_schema = try reader.readSchema();
    try std.testing.expect(read_schema.fields[0].data_type.* == .extension);
    try std.testing.expectEqualStrings("com.example.int32_ext", read_schema.fields[0].data_type.extension.name);
    try std.testing.expectEqualStrings("v1", read_schema.fields[0].data_type.extension.metadata.?);
    try std.testing.expect(read_schema.fields[0].data_type.extension.storage_type.* == .int32);
    try std.testing.expect(read_schema.fields[0].metadata != null);
    try std.testing.expectEqual(@as(usize, 1), read_schema.fields[0].metadata.?.len);
    try std.testing.expectEqualStrings("owner", read_schema.fields[0].metadata.?[0].key);
    try std.testing.expectEqualStrings("core", read_schema.fields[0].metadata.?[0].value);

    const batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(batch_opt != null);
    var read_batch = batch_opt.?;
    defer read_batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), read_batch.numRows());
    try std.testing.expect(read_batch.columns[0].data().data_type == .extension);
    const values = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = read_batch.columns[0].data() };
    try std.testing.expectEqual(@as(i32, 7), values.value(0));
    try std.testing.expect(values.isNull(1));
    try std.testing.expectEqual(@as(i32, 11), values.value(2));
}

test "ipc writer emits dictionary delta on append-only dictionary growth" {
    const allocator = std.testing.allocator;

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

    var dict_values_builder_1 = try @import("../array/string_array.zig").StringBuilder.init(allocator, 2, 7);
    defer dict_values_builder_1.deinit();
    try dict_values_builder_1.append("red");
    try dict_values_builder_1.append("blue");
    var dict_values_1 = try dict_values_builder_1.finish();
    defer dict_values_1.release();

    var dict_builder_1 = try @import("../array/dictionary_array.zig").DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        2,
    );
    defer dict_builder_1.deinit();
    try dict_builder_1.appendIndex(0);
    try dict_builder_1.appendIndex(1);
    var dict_col_1 = try dict_builder_1.finish(dict_values_1);
    defer dict_col_1.release();
    var batch_1 = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{dict_col_1});
    defer batch_1.deinit();

    var dict_values_builder_2 = try @import("../array/string_array.zig").StringBuilder.init(allocator, 3, 12);
    defer dict_values_builder_2.deinit();
    try dict_values_builder_2.append("red");
    try dict_values_builder_2.append("blue");
    try dict_values_builder_2.append("green");
    var dict_values_2 = try dict_values_builder_2.finish();
    defer dict_values_2.release();

    var dict_builder_2 = try @import("../array/dictionary_array.zig").DictionaryBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_type,
        1,
    );
    defer dict_builder_2.deinit();
    try dict_builder_2.appendIndex(2);
    var dict_col_2 = try dict_builder_2.finish(dict_values_2);
    defer dict_col_2.release();
    var batch_2 = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{dict_col_2});
    defer batch_2.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch_1);
    try writer.writeRecordBatch(batch_2);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(out.items);
    const reader = stream.reader();

    var dict_flags = std.ArrayList(bool){};
    defer dict_flags.deinit(allocator);

    while (true) {
        const next_opt = try readNextMessageForTest(allocator, reader);
        if (next_opt == null) break;
        var msg = next_opt.?;
        defer msg.deinit(allocator);
        if (msg.header == .DictionaryBatch) {
            try dict_flags.append(allocator, msg.header.DictionaryBatch.?.isDelta);
        }
    }

    try std.testing.expectEqual(@as(usize, 2), dict_flags.items.len);
    try std.testing.expectEqual(false, dict_flags.items[0]);
    try std.testing.expectEqual(true, dict_flags.items[1]);
}

test "ipc writer rejects dictionary with unsigned index type" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .string = {} };
    const dict_type = DataType{
        .dictionary = .{
            .id = null,
            .index_type = .{ .bit_width = 32, .signed = false },
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
    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();

    try std.testing.expectError(StreamError.InvalidMetadata, writer.writeSchema(schema));
}

test "ipc writer rejects decimal precision outside allowed range" {
    const allocator = std.testing.allocator;

    const cases = [_]struct {
        kind: enum { d32, d64, d128, d256 },
        precision: u8,
    }{
        .{ .kind = .d32, .precision = 10 },
        .{ .kind = .d64, .precision = 19 },
        .{ .kind = .d128, .precision = 39 },
        .{ .kind = .d256, .precision = 77 },
    };

    for (cases, 0..) |c, i| {
        const dt = switch (c.kind) {
            .d32 => DataType{ .decimal32 = .{ .precision = c.precision, .scale = 0 } },
            .d64 => DataType{ .decimal64 = .{ .precision = c.precision, .scale = 0 } },
            .d128 => DataType{ .decimal128 = .{ .precision = c.precision, .scale = 0 } },
            .d256 => DataType{ .decimal256 = .{ .precision = c.precision, .scale = 0 } },
        };
        const field_name = switch (i) {
            0 => "d32",
            1 => "d64",
            2 => "d128",
            else => "d256",
        };
        const fields = [_]Field{
            .{ .name = field_name, .data_type = &dt, .nullable = false },
        };
        const schema = Schema{ .fields = fields[0..] };

        var out = std.array_list.Managed(u8).init(allocator);
        defer out.deinit();
        var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
        defer writer.deinit();

        try std.testing.expectError(StreamError.InvalidMetadata, writer.writeSchema(schema));
    }
}

test "ipc writer rejects big-endian schema" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..], .endianness = .big };

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();
    var writer = StreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();

    try std.testing.expectError(StreamError.UnsupportedType, writer.writeSchema(schema));
}
