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
    const FieldNodeT = arrow_fbs.org_apache_arrow_flatbuf_FieldNode.FieldNodeT;
    const BufferT = arrow_fbs.org_apache_arrow_flatbuf_Buffer.BufferT;
    const DictionaryEncodingT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryEncoding.DictionaryEncodingT;
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
            schema_ptr.* = try buildSchemaT(self.allocator, schema);

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
            var nodes = try std.ArrayList(fbs.FieldNodeT).initCapacity(self.allocator, 0);
            var buffers = try std.ArrayList(fbs.BufferT).initCapacity(self.allocator, 0);
            var body_buffers = try std.ArrayList(array_data.SharedBuffer).initCapacity(self.allocator, 0);
            defer body_buffers.deinit(self.allocator);

            var body_offset: u64 = 0;
            for (batch.columns) |col| {
                try appendArrayMeta(self.allocator, col.data(), &nodes, &buffers, &body_buffers, &body_offset);
            }

            const record_batch_ptr = try self.allocator.create(fbs.RecordBatchT);
            errdefer self.allocator.destroy(record_batch_ptr);
            record_batch_ptr.* = .{
                .length = @intCast(batch.numRows()),
                .nodes = nodes,
                .buffers = buffers,
                .variadicBufferCounts = try std.ArrayList(i64).initCapacity(self.allocator, 0),
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

fn buildSchemaT(allocator: std.mem.Allocator, schema: Schema) WriterError!fbs.SchemaT {
    var fields = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0);
    for (schema.fields) |field| {
        try fields.append(allocator, try buildFieldT(allocator, field));
    }

    return .{
        .endianness = .Little,
        .fields = fields,
        .custom_metadata = try buildCustomMetadataT(allocator, schema.metadata),
        .features = try std.ArrayList(i64).initCapacity(allocator, 0),
    };
}

fn buildFieldT(allocator: std.mem.Allocator, field: Field) WriterError!fbs.FieldT {
    var children = try std.ArrayList(fbs.FieldT).initCapacity(allocator, 0);
    const type_t = try buildTypeT(allocator, field.data_type.*);

    switch (field.data_type.*) {
        .list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field));
        },
        .large_list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field));
        },
        .fixed_size_list => |lst| {
            try children.append(allocator, try buildFieldT(allocator, lst.value_field));
        },
        .struct_ => |st| {
            for (st.fields) |child| try children.append(allocator, try buildFieldT(allocator, child));
        },
        else => {},
    }

    return .{
        .name = field.name,
        .nullable = field.nullable,
        .type = type_t,
        .dictionary = null,
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
        .list => .{ .List = try allocT(allocator, fbs.ListT, .{}) },
        .large_list => .{ .LargeList = try allocT(allocator, fbs.LargeListT, .{}) },
        .struct_ => .{ .Struct_ = try allocT(allocator, fbs.Struct_T, .{}) },
        .fixed_size_binary => |fsb| .{ .FixedSizeBinary = try allocT(allocator, fbs.FixedSizeBinaryT, .{ .byteWidth = fsb.byte_width }) },
        .fixed_size_list => |fsl| .{ .FixedSizeList = try allocT(allocator, fbs.FixedSizeListT, .{ .listSize = fsl.list_size }) },
        .dictionary, .map, .list_view, .large_list_view, .binary_view, .string_view, .extension, .decimal32, .decimal64, .decimal128, .decimal256, .date32, .date64, .time32, .time64, .timestamp, .duration, .interval_months, .interval_day_time, .interval_month_day_nano, .sparse_union, .dense_union, .run_end_encoded => StreamError.UnsupportedType,
        else => StreamError.UnsupportedType,
    };
}

fn allocT(allocator: std.mem.Allocator, comptime T: type, value: T) error{OutOfMemory}!*T {
    const ptr = try allocator.create(T);
    ptr.* = value;
    return ptr;
}

fn appendArrayMeta(
    allocator: std.mem.Allocator,
    data: *const ArrayData,
    nodes: *std.ArrayList(fbs.FieldNodeT),
    buffers: *std.ArrayList(fbs.BufferT),
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

    switch (data.data_type) {
        .list, .large_list, .fixed_size_list => {
            if (data.children.len != 1) return StreamError.InvalidMetadata;
            try appendArrayMeta(allocator, data.children[0].data(), nodes, buffers, body_buffers, body_offset);
        },
        .struct_ => {
            for (data.children) |child| {
                try appendArrayMeta(allocator, child.data(), nodes, buffers, body_buffers, body_offset);
            }
        },
        .dictionary => return StreamError.UnsupportedType,
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
