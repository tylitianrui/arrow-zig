const std = @import("std");
const datatype = @import("../datatype.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const chunked_array_mod = @import("../chunked_array.zig");
const helpers = @import("helpers.zig");
const options_mod = @import("options.zig");
const registry_mod = @import("registry.zig");

pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref_mod.ArrayRef;
pub const ChunkedArray = chunked_array_mod.ChunkedArray;

pub const FunctionKind = enum {
    scalar,
    vector,
    aggregate,
};

pub const ScalarValue = union(enum) {
    null,
    bool: bool,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    f16: f16,
    f32: f32,
    f64: f64,
    date32: i32,
    date64: i64,
    time32: i32,
    time64: i64,
    timestamp: i64,
    duration: i64,
    interval_months: i32,
    interval_day_time: i64,
    interval_month_day_nano: i128,
    decimal32: i32,
    decimal64: i64,
    decimal128: i128,
    decimal256: i256,
    /// Borrowed UTF-8 bytes. Caller (or ExecContext arena allocator) owns memory.
    string: []const u8,
    /// Borrowed raw bytes. Caller (or ExecContext arena allocator) owns memory.
    binary: []const u8,
    /// Non-null nested list value carried by Scalar.payload (len == 1).
    list,
    /// Non-null nested large_list value carried by Scalar.payload (len == 1).
    large_list,
    /// Non-null nested fixed_size_list value carried by Scalar.payload (len == 1).
    fixed_size_list,
    /// Non-null nested struct value carried by Scalar.payload (len == 1).
    struct_,
};

pub const Scalar = struct {
    data_type: DataType,
    value: ScalarValue,
    /// Optional owned array payload used for nested scalar values.
    ///
    /// For nested scalars this is expected to be length=1 and type-equal to
    /// `data_type`; reference counting is managed by retain/release.
    payload: ?ArrayRef = null,

    fn nestedValueTag(data_type: DataType) ?ScalarValue {
        return switch (data_type) {
            .list => .list,
            .large_list => .large_list,
            .fixed_size_list => .fixed_size_list,
            .struct_ => .struct_,
            else => null,
        };
    }

    pub fn init(data_type: DataType, value: ScalarValue) Scalar {
        return .{
            .data_type = data_type,
            .value = value,
            .payload = null,
        };
    }

    /// Build a nested scalar from a 1-element payload array.
    ///
    /// The payload is retained and owned by the resulting Scalar.
    pub fn initNested(data_type: DataType, payload: ArrayRef) KernelError!Scalar {
        const tag = nestedValueTag(data_type) orelse return error.UnsupportedType;
        const payload_data = payload.data();
        if (payload_data.length != 1) return error.InvalidInput;
        if (!payload_data.data_type.eql(data_type)) return error.InvalidInput;

        return .{
            .data_type = data_type,
            .value = if (payload_data.isNull(0)) ScalarValue.null else tag,
            .payload = payload.retain(),
        };
    }

    pub fn retain(self: Scalar) Scalar {
        return .{
            .data_type = self.data_type,
            .value = self.value,
            .payload = if (self.payload) |payload| payload.retain() else null,
        };
    }

    pub fn release(self: *Scalar) void {
        if (self.payload) |*payload| payload.release();
        self.* = undefined;
    }

    pub fn isNull(self: Scalar) bool {
        return switch (self.value) {
            .null => true,
            else => false,
        };
    }

    pub fn payloadArray(self: Scalar) KernelError!ArrayRef {
        return (self.payload orelse return error.InvalidInput).retain();
    }
};

pub const Datum = union(enum) {
    array: ArrayRef,
    chunked: ChunkedArray,
    scalar: Scalar,

    pub fn fromArray(arr: ArrayRef) Datum {
        return .{ .array = arr };
    }

    pub fn fromChunked(chunks: ChunkedArray) Datum {
        return .{ .chunked = chunks };
    }

    pub fn fromScalar(s: Scalar) Datum {
        return .{ .scalar = s };
    }

    pub inline fn isArray(self: Datum) bool {
        return self == .array;
    }

    pub inline fn isChunked(self: Datum) bool {
        return self == .chunked;
    }

    pub inline fn isScalar(self: Datum) bool {
        return self == .scalar;
    }

    pub fn asArray(self: Datum) ?ArrayRef {
        return switch (self) {
            .array => |arr| arr,
            else => null,
        };
    }

    pub fn asChunked(self: Datum) ?ChunkedArray {
        return switch (self) {
            .chunked => |chunks| chunks,
            else => null,
        };
    }

    pub fn asScalar(self: Datum) ?Scalar {
        return switch (self) {
            .scalar => |s| s,
            else => null,
        };
    }

    pub fn retain(self: Datum) Datum {
        return switch (self) {
            .array => |arr| .{ .array = arr.retain() },
            .chunked => |chunks| .{ .chunked = chunks.retain() },
            .scalar => |s| .{ .scalar = s.retain() },
        };
    }

    pub fn release(self: *Datum) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .chunked => |*chunks| chunks.release(),
            .scalar => |*s| s.release(),
        }
        self.* = undefined;
    }

    pub fn dataType(self: Datum) DataType {
        return switch (self) {
            .array => |arr| arr.data().data_type,
            .chunked => |chunks| chunks.dataType(),
            .scalar => |s| s.data_type,
        };
    }
};

pub const KernelError = error{
    OutOfMemory,
    FunctionNotFound,
    InvalidArity,
    InvalidOptions,
    InvalidInput,
    Overflow,
    DivideByZero,
    InvalidCast,
    UnsupportedType,
    NotImplemented,
    MissingLifecycle,
    AggregateStateMismatch,
    NoMatchingKernel,
};

/// Type-check callback used by kernel signatures.
pub const TypeCheckFn = registry_mod.TypeCheckFn;

pub const OptionsTag = options_mod.OptionsTag;
pub const CastOptions = options_mod.CastOptions;
pub const ArithmeticOptions = options_mod.ArithmeticOptions;
pub const FilterOptions = options_mod.FilterOptions;
pub const SortOrder = options_mod.SortOrder;
pub const SortNullPlacement = options_mod.SortNullPlacement;
pub const SortNaNPlacement = options_mod.SortNaNPlacement;
pub const SortOptions = options_mod.SortOptions;
pub const CustomOptions = options_mod.CustomOptions;
pub const Options = options_mod.Options;

pub const OptionsCheckFn = registry_mod.OptionsCheckFn;
pub const ResultTypeFn = registry_mod.ResultTypeFn;
pub const KernelExecFn = registry_mod.KernelExecFn;
pub const AggregateInitFn = registry_mod.AggregateInitFn;
pub const AggregateUpdateFn = registry_mod.AggregateUpdateFn;
pub const AggregateMergeFn = registry_mod.AggregateMergeFn;
pub const AggregateFinalizeFn = registry_mod.AggregateFinalizeFn;
pub const AggregateDeinitFn = registry_mod.AggregateDeinitFn;
pub const AggregateLifecycle = registry_mod.AggregateLifecycle;
pub const KernelSignature = registry_mod.KernelSignature;
pub const Kernel = registry_mod.Kernel;
pub const AggregateSession = registry_mod.AggregateSession;
pub const Function = registry_mod.Function;
pub const FunctionRegistry = registry_mod.FunctionRegistry;
pub const OverflowMode = registry_mod.OverflowMode;
pub const ExecConfig = registry_mod.ExecConfig;
pub const ExecContext = registry_mod.ExecContext;

pub const ExecChunkValue = helpers.ExecChunkValue;
pub const UnaryExecChunk = helpers.UnaryExecChunk;
pub const BinaryExecChunk = helpers.BinaryExecChunk;
pub const NaryExecChunk = helpers.NaryExecChunk;
pub const ChunkLocalIndex = helpers.ChunkLocalIndex;
pub const UnaryExecChunkIterator = helpers.UnaryExecChunkIterator;
pub const BinaryExecChunkIterator = helpers.BinaryExecChunkIterator;
pub const NaryExecChunkIterator = helpers.NaryExecChunkIterator;

pub const unaryNullPropagates = helpers.unaryNullPropagates;
pub const binaryNullPropagates = helpers.binaryNullPropagates;
pub const naryNullPropagates = helpers.naryNullPropagates;
pub const inferBinaryExecLen = helpers.inferBinaryExecLen;
pub const inferNaryExecLen = helpers.inferNaryExecLen;
pub const chunkedResolveLogicalIndices = helpers.chunkedResolveLogicalIndices;
pub const datumListValueAt = helpers.datumListValueAt;
pub const datumLargeListValueAt = helpers.datumLargeListValueAt;
pub const datumFixedSizeListValueAt = helpers.datumFixedSizeListValueAt;
pub const datumStructField = helpers.datumStructField;
pub const datumFilterSelectionIndices = helpers.datumFilterSelectionIndices;
pub const datumBuildNullLike = helpers.datumBuildNullLike;
pub const datumBuildNullLikeWithAllocator = helpers.datumBuildNullLikeWithAllocator;
pub const datumBuildEmptyLike = helpers.datumBuildEmptyLike;
pub const datumBuildEmptyLikeWithAllocator = helpers.datumBuildEmptyLikeWithAllocator;
pub const datumSliceEmpty = helpers.datumSliceEmpty;
pub const datumTake = helpers.datumTake;
pub const datumTakeNullable = helpers.datumTakeNullable;
pub const datumSelect = helpers.datumSelect;
pub const datumSelectNullable = helpers.datumSelectNullable;
pub const datumFilterChunkAware = helpers.datumFilterChunkAware;
pub const datumFilter = helpers.datumFilter;
pub const intCastOrInvalidCast = helpers.intCastOrInvalidCast;
pub const arithmeticDivI64 = helpers.arithmeticDivI64;
pub const hasArity = helpers.hasArity;
pub const unaryArray = helpers.unaryArray;
pub const unaryChunked = helpers.unaryChunked;
pub const unaryScalar = helpers.unaryScalar;
pub const sameDataTypes = helpers.sameDataTypes;
pub const allNumeric = helpers.allNumeric;
pub const unaryNumeric = helpers.unaryNumeric;
pub const binarySameNumeric = helpers.binarySameNumeric;

test {
    _ = @import("test.zig");
}
