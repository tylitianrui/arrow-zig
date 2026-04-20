const std = @import("std");

// Arrow logical/physical type definitions and shared schema metadata structs.

pub const TypeId = enum(u8) {
    null = 0,
    bool = 1,
    uint8 = 2,
    int8 = 3,
    uint16 = 4,
    int16 = 5,
    uint32 = 6,
    int32 = 7,
    uint64 = 8,
    int64 = 9,
    half_float = 10,
    float = 11,
    double = 12,
    string = 13,
    binary = 14,
    fixed_size_binary = 15,
    date32 = 16,
    date64 = 17,
    timestamp = 18,
    time32 = 19,
    time64 = 20,
    interval_months = 21,
    interval_day_time = 22,
    decimal128 = 23,
    decimal256 = 24,
    list = 25,
    struct_ = 26,
    sparse_union = 27,
    dense_union = 28,
    dictionary = 29,
    map = 30,
    extension = 31,
    fixed_size_list = 32,
    duration = 33,
    large_string = 34,
    large_binary = 35,
    large_list = 36,
    interval_month_day_nano = 37,
    run_end_encoded = 38,
    string_view = 39,
    binary_view = 40,
    list_view = 41,
    large_list_view = 42,
    decimal32 = 43,
    decimal64 = 44,
};

// Sentinel used for range checks and iteration bounds.
// This is intentionally not a real DataType variant.
pub const max_type_id: u8 = 45;

pub const TimeUnit = enum(u8) {
    second = 0,
    millisecond = 1,
    microsecond = 2,
    nanosecond = 3,
};

pub const IntervalUnit = enum(u8) {
    months = 0,
    day_time = 1,
    month_day_nano = 2,
};

pub const UnionMode = enum(u8) {
    sparse = 0,
    dense = 1,
};

pub const FloatPrecision = enum(u8) {
    half = 0,
    single = 1,
    double = 2,
};

pub const Endianness = enum(u8) {
    little = 0,
    big = 1,
};

pub const KeyValue = struct {
    key: []const u8,
    value: []const u8,
};

pub const IntType = struct {
    bit_width: u8,
    signed: bool,

    pub fn fromTypeId(id: TypeId) ?IntType {
        return switch (id) {
            .int8 => .{ .bit_width = 8, .signed = true },
            .uint8 => .{ .bit_width = 8, .signed = false },
            .int16 => .{ .bit_width = 16, .signed = true },
            .uint16 => .{ .bit_width = 16, .signed = false },
            .int32 => .{ .bit_width = 32, .signed = true },
            .uint32 => .{ .bit_width = 32, .signed = false },
            .int64 => .{ .bit_width = 64, .signed = true },
            .uint64 => .{ .bit_width = 64, .signed = false },
            else => null,
        };
    }

    pub fn toTypeId(self: IntType) ?TypeId {
        return switch (self.bit_width) {
            8 => if (self.signed) .int8 else .uint8,
            16 => if (self.signed) .int16 else .uint16,
            32 => if (self.signed) .int32 else .uint32,
            64 => if (self.signed) .int64 else .uint64,
            else => null,
        };
    }
};

pub const DecimalParams = struct {
    precision: u8,
    scale: i32,
};

pub const FixedSizeBinaryType = struct {
    byte_width: i32,
};

pub const TimestampType = struct {
    unit: TimeUnit,
    timezone: ?[]const u8,
};

pub const TimeType = struct {
    unit: TimeUnit,
};

pub const DurationType = struct {
    unit: TimeUnit,
};

pub const IntervalType = struct {
    unit: IntervalUnit,
};

pub const Field = struct {
    name: []const u8,
    data_type: *const DataType,
    nullable: bool = true,
    metadata: ?[]const KeyValue = null,

    /// Initialize and return a new instance.
    pub fn init(name: []const u8, data_type: *const DataType) Field {
        return .{ .name = name, .data_type = data_type };
    }

    pub fn eql(self: Field, other: Field) bool {
        return fieldEql(self, other);
    }
};

pub const StructType = struct {
    fields: []const Field,
};

pub const ListType = struct {
    value_field: Field,
};

pub const ListViewType = struct {
    value_field: Field,
};

pub const FixedSizeListType = struct {
    value_field: Field,
    list_size: i32,
};

pub const MapType = struct {
    key_field: Field,
    item_field: Field,
    keys_sorted: bool = false,
    entries_type: ?*const DataType = null,
};

pub const UnionType = struct {
    type_ids: []const i8,
    fields: []const Field,
    mode: UnionMode,
};

pub const DictionaryType = struct {
    id: ?i64 = null,
    index_type: IntType,
    value_type: *const DataType,
    ordered: bool = false,
};

pub const RunEndEncodedType = struct {
    run_end_type: IntType,
    value_type: *const DataType,
};

pub const ExtensionType = struct {
    name: []const u8,
    storage_type: *const DataType,
    metadata: ?[]const u8 = null,
};

pub const DataType = union(TypeId) {
    null: void,
    bool: void,
    uint8: void,
    int8: void,
    uint16: void,
    int16: void,
    uint32: void,
    int32: void,
    uint64: void,
    int64: void,
    half_float: void,
    float: void,
    double: void,
    string: void,
    binary: void,
    fixed_size_binary: FixedSizeBinaryType,
    date32: void,
    date64: void,
    timestamp: TimestampType,
    time32: TimeType,
    time64: TimeType,
    interval_months: IntervalType,
    interval_day_time: IntervalType,
    decimal128: DecimalParams,
    decimal256: DecimalParams,
    list: ListType,
    struct_: StructType,
    sparse_union: UnionType,
    dense_union: UnionType,
    dictionary: DictionaryType,
    map: MapType,
    extension: ExtensionType,
    fixed_size_list: FixedSizeListType,
    duration: DurationType,
    large_string: void,
    large_binary: void,
    large_list: ListType,
    interval_month_day_nano: IntervalType,
    run_end_encoded: RunEndEncodedType,
    string_view: void,
    binary_view: void,
    list_view: ListViewType,
    large_list_view: ListViewType,
    decimal32: DecimalParams,
    decimal64: DecimalParams,

    pub fn id(self: DataType) TypeId {
        return std.meta.activeTag(self);
    }

    pub fn name(self: DataType) []const u8 {
        return switch (self) {
            .null => "null",
            .bool => "bool",
            .uint8 => "uint8",
            .int8 => "int8",
            .uint16 => "uint16",
            .int16 => "int16",
            .uint32 => "uint32",
            .int32 => "int32",
            .uint64 => "uint64",
            .int64 => "int64",
            .half_float => "halffloat",
            .float => "float",
            .double => "double",
            .string => "utf8",
            .binary => "binary",
            .fixed_size_binary => "fixed_size_binary",
            .date32 => "date32",
            .date64 => "date64",
            .timestamp => "timestamp",
            .time32 => "time32",
            .time64 => "time64",
            .interval_months => "month_interval",
            .interval_day_time => "day_time_interval",
            .interval_month_day_nano => "month_day_nano_interval",
            .decimal32 => "decimal32",
            .decimal64 => "decimal64",
            .decimal128 => "decimal128",
            .decimal256 => "decimal256",
            .list => "list",
            .large_list => "large_list",
            .list_view => "list_view",
            .large_list_view => "large_list_view",
            .fixed_size_list => "fixed_size_list",
            .struct_ => "struct",
            .map => "map",
            .sparse_union => "sparse_union",
            .dense_union => "dense_union",
            .dictionary => "dictionary",
            .run_end_encoded => "run_end_encoded",
            .extension => "extension",
            .duration => "duration",
            .large_string => "large_utf8",
            .large_binary => "large_binary",
            .string_view => "utf8_view",
            .binary_view => "binary_view",
        };
    }

    pub fn eql(self: DataType, other: DataType) bool {
        return dataTypeEql(self, other);
    }

    pub inline fn isInteger(self: DataType) bool {
        return switch (self.physicalType()) {
            .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64 => true,
            else => false,
        };
    }

    pub inline fn isFloating(self: DataType) bool {
        return switch (self.physicalType()) {
            .half_float, .float, .double => true,
            else => false,
        };
    }

    pub inline fn isNumeric(self: DataType) bool {
        return self.isInteger() or self.isFloating() or self.isDecimal();
    }

    pub inline fn isDecimal(self: DataType) bool {
        return switch (self.physicalType()) {
            .decimal32, .decimal64, .decimal128, .decimal256 => true,
            else => false,
        };
    }

    pub inline fn isStringLike(self: DataType) bool {
        return switch (self.physicalType()) {
            .string, .binary, .large_string, .large_binary, .string_view, .binary_view, .fixed_size_binary => true,
            else => false,
        };
    }

    pub inline fn isTemporal(self: DataType) bool {
        return switch (self.physicalType()) {
            .date32, .date64, .time32, .time64, .timestamp, .duration, .interval_months, .interval_day_time, .interval_month_day_nano => true,
            else => false,
        };
    }

    pub inline fn isNested(self: DataType) bool {
        return switch (self) {
            .list,
            .large_list,
            .list_view,
            .large_list_view,
            .fixed_size_list,
            .struct_,
            .map,
            .sparse_union,
            .dense_union,
            .dictionary,
            .run_end_encoded,
            .extension,
            => true,
            else => false,
        };
    }

    pub inline fn bitWidth(self: DataType) ?usize {
        return switch (self.physicalType()) {
            .bool => 1,
            .int8, .uint8 => 8,
            .int16, .uint16, .half_float => 16,
            .int32, .uint32, .float, .date32, .time32, .interval_months, .decimal32 => 32,
            .int64, .uint64, .double, .date64, .time64, .timestamp, .duration, .interval_day_time, .decimal64 => 64,
            .interval_month_day_nano, .decimal128 => 128,
            .decimal256 => 256,
            .fixed_size_binary => |fixed| blk: {
                if (fixed.byte_width < 0) break :blk null;
                const bytes: usize = @intCast(fixed.byte_width);
                break :blk std.math.mul(usize, bytes, 8) catch null;
            },
            else => null,
        };
    }

    pub fn physicalType(self: DataType) DataType {
        return switch (self) {
            .dictionary => |dict| dict.value_type.*.physicalType(),
            .extension => |ext| ext.storage_type.*.physicalType(),
            else => self,
        };
    }
};

fn dataTypePtrEql(lhs: ?*const DataType, rhs: ?*const DataType) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return dataTypeEql(lhs.?.*, rhs.?.*);
}

fn metadataEql(lhs: ?[]const KeyValue, rhs: ?[]const KeyValue) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    const l = lhs.?;
    const r = rhs.?;
    if (l.len != r.len) return false;
    for (l, r) |lv, rv| {
        if (!std.mem.eql(u8, lv.key, rv.key)) return false;
        if (!std.mem.eql(u8, lv.value, rv.value)) return false;
    }
    return true;
}

fn fieldSliceEql(lhs: []const Field, rhs: []const Field) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |lf, rf| {
        if (!fieldEql(lf, rf)) return false;
    }
    return true;
}

pub fn fieldEql(lhs: Field, rhs: Field) bool {
    if (!std.mem.eql(u8, lhs.name, rhs.name)) return false;
    if (lhs.nullable != rhs.nullable) return false;
    if (!metadataEql(lhs.metadata, rhs.metadata)) return false;
    return dataTypeEql(lhs.data_type.*, rhs.data_type.*);
}

pub fn dataTypeEql(lhs: DataType, rhs: DataType) bool {
    if (lhs.id() != rhs.id()) return false;

    return switch (lhs) {
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
        .string,
        .binary,
        .date32,
        .date64,
        .large_string,
        .large_binary,
        .string_view,
        .binary_view,
        => true,
        .fixed_size_binary => |l| l.byte_width == rhs.fixed_size_binary.byte_width,
        .timestamp => |l| blk: {
            const r = rhs.timestamp;
            if (l.unit != r.unit) break :blk false;
            if (l.timezone == null or r.timezone == null) break :blk l.timezone == null and r.timezone == null;
            break :blk std.mem.eql(u8, l.timezone.?, r.timezone.?);
        },
        .time32 => |l| l.unit == rhs.time32.unit,
        .time64 => |l| l.unit == rhs.time64.unit,
        .interval_months => |l| l.unit == rhs.interval_months.unit,
        .interval_day_time => |l| l.unit == rhs.interval_day_time.unit,
        .interval_month_day_nano => |l| l.unit == rhs.interval_month_day_nano.unit,
        .decimal32 => |l| l.precision == rhs.decimal32.precision and l.scale == rhs.decimal32.scale,
        .decimal64 => |l| l.precision == rhs.decimal64.precision and l.scale == rhs.decimal64.scale,
        .decimal128 => |l| l.precision == rhs.decimal128.precision and l.scale == rhs.decimal128.scale,
        .decimal256 => |l| l.precision == rhs.decimal256.precision and l.scale == rhs.decimal256.scale,
        .list => |l| fieldEql(l.value_field, rhs.list.value_field),
        .large_list => |l| fieldEql(l.value_field, rhs.large_list.value_field),
        .list_view => |l| fieldEql(l.value_field, rhs.list_view.value_field),
        .large_list_view => |l| fieldEql(l.value_field, rhs.large_list_view.value_field),
        .fixed_size_list => |l| blk: {
            const r = rhs.fixed_size_list;
            break :blk l.list_size == r.list_size and fieldEql(l.value_field, r.value_field);
        },
        .struct_ => |l| fieldSliceEql(l.fields, rhs.struct_.fields),
        .map => |l| blk: {
            const r = rhs.map;
            break :blk fieldEql(l.key_field, r.key_field) and
                fieldEql(l.item_field, r.item_field) and
                l.keys_sorted == r.keys_sorted and
                dataTypePtrEql(l.entries_type, r.entries_type);
        },
        .sparse_union => |l| unionTypeEql(l, rhs.sparse_union),
        .dense_union => |l| unionTypeEql(l, rhs.dense_union),
        .dictionary => |l| blk: {
            const r = rhs.dictionary;
            break :blk l.id == r.id and
                l.index_type.bit_width == r.index_type.bit_width and
                l.index_type.signed == r.index_type.signed and
                l.ordered == r.ordered and
                dataTypeEql(l.value_type.*, r.value_type.*);
        },
        .run_end_encoded => |l| blk: {
            const r = rhs.run_end_encoded;
            break :blk l.run_end_type.bit_width == r.run_end_type.bit_width and
                l.run_end_type.signed == r.run_end_type.signed and
                dataTypeEql(l.value_type.*, r.value_type.*);
        },
        .extension => |l| blk: {
            const r = rhs.extension;
            if (!std.mem.eql(u8, l.name, r.name)) break :blk false;
            if (!dataTypeEql(l.storage_type.*, r.storage_type.*)) break :blk false;
            if (l.metadata == null or r.metadata == null) break :blk l.metadata == null and r.metadata == null;
            break :blk std.mem.eql(u8, l.metadata.?, r.metadata.?);
        },
        .duration => |l| l.unit == rhs.duration.unit,
    };
}

fn unionTypeEql(lhs: UnionType, rhs: UnionType) bool {
    if (lhs.mode != rhs.mode) return false;
    if (!std.mem.eql(i8, lhs.type_ids, rhs.type_ids)) return false;
    return fieldSliceEql(lhs.fields, rhs.fields);
}

comptime {
    std.debug.assert(@intFromEnum(TypeId.null) == 0);
    std.debug.assert(@intFromEnum(TypeId.bool) == 1);
    std.debug.assert(@intFromEnum(TypeId.decimal64) == 44);
}

test "data type eql compares nested pointer content" {
    const dict_value_1 = DataType{ .string = {} };
    const dict_value_2 = DataType{ .string = {} };
    const lhs = DataType{
        .dictionary = .{
            .id = 7,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &dict_value_1,
            .ordered = false,
        },
    };
    const rhs = DataType{
        .dictionary = .{
            .id = 7,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &dict_value_2,
            .ordered = false,
        },
    };
    try std.testing.expect(lhs.eql(rhs));
}

test "field eql compares metadata and nested type" {
    const value_type_1 = DataType{ .int32 = {} };
    const value_type_2 = DataType{ .int32 = {} };
    const meta_1 = [_]KeyValue{
        .{ .key = "k", .value = "v" },
    };
    const meta_2 = [_]KeyValue{
        .{ .key = "k", .value = "v" },
    };
    const lhs = Field{
        .name = "id",
        .data_type = &value_type_1,
        .nullable = false,
        .metadata = meta_1[0..],
    };
    const rhs = Field{
        .name = "id",
        .data_type = &value_type_2,
        .nullable = false,
        .metadata = meta_2[0..],
    };
    try std.testing.expect(lhs.eql(rhs));
}

test "data type helpers classify primitive categories" {
    const i32_dt = DataType{ .int32 = {} };
    const f64_dt = DataType{ .double = {} };
    const dec_dt = DataType{ .decimal128 = .{ .precision = 10, .scale = 2 } };
    const str_dt = DataType{ .string = {} };
    const ts_dt = DataType{ .timestamp = .{ .unit = .microsecond, .timezone = null } };
    const list_value = DataType{ .int32 = {} };
    const list_field = Field{ .name = "item", .data_type = &list_value };
    const list_dt = DataType{ .list = .{ .value_field = list_field } };

    try std.testing.expect(i32_dt.isInteger());
    try std.testing.expect(i32_dt.isNumeric());
    try std.testing.expect(!i32_dt.isFloating());

    try std.testing.expect(f64_dt.isFloating());
    try std.testing.expect(f64_dt.isNumeric());

    try std.testing.expect(dec_dt.isDecimal());
    try std.testing.expect(dec_dt.isNumeric());
    try std.testing.expectEqual(@as(?usize, 128), dec_dt.bitWidth());

    try std.testing.expect(str_dt.isStringLike());
    try std.testing.expectEqual(@as(?usize, null), str_dt.bitWidth());

    try std.testing.expect(ts_dt.isTemporal());
    try std.testing.expectEqual(@as(?usize, 64), ts_dt.bitWidth());

    try std.testing.expect(list_dt.isNested());
    try std.testing.expectEqual(@as(?usize, null), list_dt.bitWidth());
}

test "data type physical type unwraps dictionary and extension" {
    const storage = DataType{ .int64 = {} };
    const extension = DataType{
        .extension = .{
            .name = "my.ext",
            .storage_type = &storage,
        },
    };
    const dict = DataType{
        .dictionary = .{
            .id = 1,
            .index_type = .{ .bit_width = 32, .signed = true },
            .value_type = &extension,
            .ordered = false,
        },
    };

    const physical = dict.physicalType();
    try std.testing.expect(physical == .int64);
    try std.testing.expect(dict.isInteger());
    try std.testing.expectEqual(@as(?usize, 64), dict.bitWidth());
}
