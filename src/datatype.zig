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

    // Sentinel used for range checks and iteration bounds.
    // Mirrors the C++ Arrow MAX_ID/Type::MAX and similar Rust enum bounds.
    max_id = 45,
};

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
    native = 2,
};

pub const KeyValue = struct {
    key: []const u8,
    value: []const u8,
};

pub const IntType = struct {
    bit_width: u8,
    signed: bool,

    /// Execute fromTypeId logic for this type.
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

    /// Execute toTypeId logic for this type.
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
};

pub const UnionType = struct {
    type_ids: []const i8,
    fields: []const Field,
    mode: UnionMode,
};

pub const DictionaryType = struct {
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
    max_id: void,

    /// Execute id logic for this type.
    pub fn id(self: DataType) TypeId {
        return std.meta.activeTag(self);
    }

    /// Execute name logic for this type.
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
            .max_id => "max_id",
        };
    }
};

comptime {
    std.debug.assert(@intFromEnum(TypeId.null) == 0);
    std.debug.assert(@intFromEnum(TypeId.bool) == 1);
    std.debug.assert(@intFromEnum(TypeId.decimal64) == 44);
}
