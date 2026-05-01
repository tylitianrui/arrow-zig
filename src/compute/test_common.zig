pub const std = @import("std");
pub const datatype = @import("../datatype.zig");
pub const array_mod = @import("../array/array.zig");
pub const core = @import("core.zig");

pub const DataType = core.DataType;
pub const ArrayRef = core.ArrayRef;
pub const ChunkedArray = core.ChunkedArray;
pub const Scalar = core.Scalar;
pub const Datum = core.Datum;
pub const FunctionRegistry = core.FunctionRegistry;
pub const ExecContext = core.ExecContext;
pub const KernelSignature = core.KernelSignature;
pub const KernelError = core.KernelError;
pub const Options = core.Options;
pub const OptionsTag = core.OptionsTag;
pub const SortOptions = core.SortOptions;
pub const SortOrder = core.SortOrder;
pub const SortNullPlacement = core.SortNullPlacement;
pub const FilterOptions = core.FilterOptions;
pub const UnaryExecChunkIterator = core.UnaryExecChunkIterator;
pub const BinaryExecChunkIterator = core.BinaryExecChunkIterator;
pub const NaryExecChunkIterator = core.NaryExecChunkIterator;

pub const intCastOrInvalidCast = core.intCastOrInvalidCast;
pub const arithmeticDivI64 = core.arithmeticDivI64;
pub const hasArity = core.hasArity;
pub const unaryArray = core.unaryArray;
pub const unaryChunked = core.unaryChunked;
pub const unaryScalar = core.unaryScalar;
pub const sameDataTypes = core.sameDataTypes;
pub const unaryNumeric = core.unaryNumeric;
pub const binarySameNumeric = core.binarySameNumeric;
pub const allNumeric = core.allNumeric;
pub const unaryNullPropagates = core.unaryNullPropagates;
pub const binaryNullPropagates = core.binaryNullPropagates;
pub const naryNullPropagates = core.naryNullPropagates;
pub const inferBinaryExecLen = core.inferBinaryExecLen;
pub const inferNaryExecLen = core.inferNaryExecLen;
pub const datumListValueAt = core.datumListValueAt;
pub const datumStructField = core.datumStructField;
pub const datumBuildEmptyLike = core.datumBuildEmptyLike;
pub const datumBuildEmptyLikeWithAllocator = core.datumBuildEmptyLikeWithAllocator;
pub const datumBuildNullLike = core.datumBuildNullLike;
pub const datumBuildNullLikeWithAllocator = core.datumBuildNullLikeWithAllocator;
pub const datumSliceEmpty = core.datumSliceEmpty;
pub const datumSelect = core.datumSelect;
pub const datumSelectNullable = core.datumSelectNullable;
pub const datumFilter = core.datumFilter;
pub const chunkedResolveLogicalIndices = core.chunkedResolveLogicalIndices;
pub const datumTake = core.datumTake;
pub const datumFilterSelectionIndices = core.datumFilterSelectionIndices;
pub const datumFilterChunkAware = core.datumFilterChunkAware;

pub fn isInt32Datum(args: []const Datum) bool {
    return args.len == 1 and args[0].dataType() == .int32;
}

pub fn isTwoInt32(args: []const Datum) bool {
    return args.len == 2 and args[0].dataType() == .int32 and args[1].dataType() == .int32;
}

pub fn isInt64Scalar(args: []const Datum) bool {
    return args.len == 1 and args[0].isScalar() and args[0].scalar.data_type == .int64;
}

pub fn isTwoInt64Scalars(args: []const Datum) bool {
    return args.len == 2 and args[0].isScalar() and args[1].isScalar() and args[0].scalar.data_type == .int64 and args[1].scalar.data_type == .int64;
}

pub fn allInt32Datums(args: []const Datum) bool {
    for (args) |arg| {
        if (!arg.dataType().eql(.{ .int32 = {} })) return false;
    }
    return true;
}

pub fn passthroughInt32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

pub fn exactArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 303 },
    });
}

pub fn rangeArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 202 },
    });
}

pub fn atLeastArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 101 },
    });
}

pub fn countLenAggregateKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    if (args.len != 1) return error.InvalidArity;

    const count: usize = switch (args[0]) {
        .array => |arr| arr.data().length,
        .chunked => |chunks| chunks.len(),
        .scalar => return error.InvalidInput,
    };

    return Datum.fromScalar(Scalar.init(.{ .int64 = {} }, .{ .i64 = @intCast(count) }));
}

pub fn onlyCastOptions(options: Options) bool {
    return switch (options) {
        .cast => true,
        else => false,
    };
}

pub fn onlyArithmeticOptions(options: Options) bool {
    return switch (options) {
        .arithmetic => true,
        else => false,
    };
}

pub fn onlySortOptions(options: Options) bool {
    return switch (options) {
        .sort => true,
        else => false,
    };
}

pub fn firstArgResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = options;
    if (args.len == 0) return error.InvalidInput;
    return args[0].dataType();
}

pub fn castResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 1) return error.InvalidArity;
    return switch (options) {
        .cast => |cast_opts| cast_opts.to_type orelse args[0].dataType(),
        else => error.InvalidOptions,
    };
}

pub fn castI64ToI32ResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 1) return error.InvalidArity;
    if (!args[0].dataType().eql(.{ .int64 = {} })) return error.InvalidCast;
    return switch (options) {
        .cast => |cast_opts| blk: {
            if (cast_opts.to_type) |to_type| {
                if (!to_type.eql(.{ .int32 = {} })) return error.InvalidCast;
            }
            break :blk DataType{ .int32 = {} };
        },
        else => error.InvalidOptions,
    };
}

pub fn castI64ToI32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    if (args.len != 1) return error.InvalidArity;
    if (!isInt64Scalar(args)) return error.InvalidInput;

    const cast_opts = switch (options) {
        .cast => |o| o,
        else => return error.InvalidOptions,
    };
    if (cast_opts.to_type) |to_type| {
        if (!to_type.eql(.{ .int32 = {} })) return error.InvalidCast;
    }

    const out = try intCastOrInvalidCast(i32, args[0].scalar.value.i64);
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = out },
    });
}

pub fn divI64ScalarResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 2) return error.InvalidArity;
    _ = switch (options) {
        .arithmetic => {},
        else => return error.InvalidOptions,
    };
    if (!isTwoInt64Scalars(args)) return error.InvalidInput;
    return .{ .int64 = {} };
}

pub fn divI64ScalarKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    if (args.len != 2) return error.InvalidArity;
    if (!isTwoInt64Scalars(args)) return error.InvalidInput;
    const arithmetic_opts = switch (options) {
        .arithmetic => |o| o,
        else => return error.InvalidOptions,
    };
    const lhs = args[0].scalar.value.i64;
    const rhs = args[1].scalar.value.i64;
    const out = try arithmeticDivI64(lhs, rhs, arithmetic_opts);
    return Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = out },
    });
}

pub fn countAggregateResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = args;
    _ = options;
    return .{ .int64 = {} };
}

pub const CountAggState = struct {
    count: usize,
};

pub fn countLifecycleInit(ctx: *ExecContext, options: Options) KernelError!*anyopaque {
    _ = options;
    const state = ctx.allocator.create(CountAggState) catch return error.OutOfMemory;
    state.* = .{ .count = 0 };
    return state;
}

pub fn countLifecycleUpdate(ctx: *ExecContext, state_ptr: *anyopaque, args: []const Datum, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    if (!unaryArray(args)) return error.InvalidInput;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const count = args[0].array.data().length;
    state.count = std.math.add(usize, state.count, count) catch return error.Overflow;
}

pub fn countLifecycleMerge(ctx: *ExecContext, state_ptr: *anyopaque, other_ptr: *anyopaque, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const other: *CountAggState = @ptrCast(@alignCast(other_ptr));
    state.count = std.math.add(usize, state.count, other.count) catch return error.Overflow;
}

pub fn countLifecycleFinalize(ctx: *ExecContext, state_ptr: *anyopaque, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    return Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = @intCast(state.count) },
    });
}

pub fn countLifecycleDeinit(ctx: *ExecContext, state_ptr: *anyopaque) void {
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    ctx.allocator.destroy(state);
}

pub fn makeInt32Array(allocator: std.mem.Allocator, values: []const ?i32) !ArrayRef {
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var builder = try int32_builder.init(allocator, values.len);
    defer builder.deinit();

    for (values) |v| {
        if (v) |value| {
            try builder.append(value);
        } else {
            try builder.appendNull();
        }
    }
    return builder.finish();
}

pub fn makeBoolArray(allocator: std.mem.Allocator, values: []const ?bool) !ArrayRef {
    var builder = try array_mod.BooleanBuilder.init(allocator, values.len);
    defer builder.deinit();

    for (values) |v| {
        if (v) |value| {
            try builder.append(value);
        } else {
            try builder.appendNull();
        }
    }
    return builder.finish();
}

pub fn collectInt32ValuesFromDatum(allocator: std.mem.Allocator, datum: Datum) ![]?i32 {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .int32) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, arr.data().length);
            const view = array_mod.Int32Array{ .data = arr.data() };
            var i: usize = 0;
            while (i < out.len) : (i += 1) {
                out[i] = if (view.isNull(i)) null else try view.value(i);
            }
            break :blk out;
        },
        .chunked => |chunks| blk: {
            if (!chunks.dataType().eql(.{ .int32 = {} })) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, chunks.len());
            var out_index: usize = 0;
            var chunk_index: usize = 0;
            while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
                const chunk = chunks.chunk(chunk_index);
                const view = array_mod.Int32Array{ .data = chunk.data() };
                var i: usize = 0;
                while (i < view.len()) : (i += 1) {
                    out[out_index] = if (view.isNull(i)) null else try view.value(i);
                    out_index += 1;
                }
            }
            break :blk out;
        },
        .scalar => |s| blk: {
            if (s.data_type != .int32) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, 1);
            out[0] = if (s.isNull()) null else s.value.i32;
            break :blk out;
        },
    };
}
