const std = @import("std");
const array_mod = @import("../array/array.zig");
const core = @import("core.zig");
const builders = @import("datum_builders.zig");
const common = @import("datum_common.zig");

const ArrayRef = core.ArrayRef;
const ChunkedArray = core.ChunkedArray;
const Scalar = core.Scalar;
const Datum = core.Datum;
const KernelError = core.KernelError;

const mapArrayReadError = common.mapArrayReadError;

const ChunkLookup = struct {
    chunk: *const ArrayRef,
    local_index: usize,
};

/// Chunk-local coordinate for a logical row in a chunked datum.
pub const ChunkLocalIndex = struct {
    chunk_index: usize,
    index_in_chunk: usize,
};

const ChunkIndexResolver = struct {
    offsets: []usize,

    fn init(allocator: std.mem.Allocator, chunks: ChunkedArray) KernelError!ChunkIndexResolver {
        var offsets = allocator.alloc(usize, chunks.numChunks() + 1) catch return error.OutOfMemory;
        errdefer allocator.free(offsets);

        offsets[0] = 0;
        var total: usize = 0;
        var i: usize = 0;
        while (i < chunks.numChunks()) : (i += 1) {
            total = std.math.add(usize, total, chunks.chunk(i).data().length) catch return error.Overflow;
            offsets[i + 1] = total;
        }
        return .{ .offsets = offsets };
    }

    fn deinit(self: *ChunkIndexResolver, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        self.* = undefined;
    }

    fn locate(self: *const ChunkIndexResolver, chunks: ChunkedArray, logical_index: usize) KernelError!ChunkLocalIndex {
        if (logical_index >= chunks.len()) return error.InvalidInput;
        if (chunks.numChunks() == 0) return error.InvalidInput;

        var lo: usize = 0;
        var hi: usize = chunks.numChunks();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.offsets[mid + 1] <= logical_index) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if (lo >= chunks.numChunks()) return error.InvalidInput;
        return .{
            .chunk_index = lo,
            .index_in_chunk = logical_index - self.offsets[lo],
        };
    }
};

/// Resolve logical row indices into chunk-local coordinates.
///
/// This is a building block for permutation-producing kernels (e.g. sort_indices)
/// that need to bridge global logical indices back to chunk-local positions.
pub fn chunkedResolveLogicalIndices(
    allocator: std.mem.Allocator,
    chunks: ChunkedArray,
    logical_indices: []const usize,
) KernelError![]ChunkLocalIndex {
    var out = allocator.alloc(ChunkLocalIndex, logical_indices.len) catch return error.OutOfMemory;
    errdefer allocator.free(out);

    var resolver = try ChunkIndexResolver.init(allocator, chunks);
    defer resolver.deinit(allocator);

    for (logical_indices, 0..) |logical_index, i| {
        out[i] = try resolver.locate(chunks, logical_index);
    }
    return out;
}

fn lookupChunkAt(chunks: ChunkedArray, logical_index: usize) ?ChunkLookup {
    if (logical_index >= chunks.len()) return null;

    var remaining = logical_index;
    var chunk_index: usize = 0;
    while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
        const chunk_ref = chunks.chunk(chunk_index);
        const chunk_len = chunk_ref.data().length;
        if (remaining < chunk_len) {
            return .{
                .chunk = chunk_ref,
                .local_index = remaining,
            };
        }
        remaining -= chunk_len;
    }
    return null;
}

fn scalarFromSingleArrayRef(value: ArrayRef) KernelError!Scalar {
    const data = value.data();
    if (data.length != 1) return error.InvalidInput;
    if (data.isNull(0)) return Scalar.init(data.data_type, .null);

    const pValue = struct {
        fn get(comptime T: type, arr: anytype) KernelError!T {
            return arr.value(0) catch |err| return mapArrayReadError(err);
        }
    }.get;

    return switch (data.data_type) {
        .bool => Scalar.init(data.data_type, .{ .bool = (array_mod.BooleanArray{ .data = data }).value(0) }),
        .int8 => Scalar.init(data.data_type, .{ .i8 = try pValue(i8, array_mod.Int8Array{ .data = data }) }),
        .int16 => Scalar.init(data.data_type, .{ .i16 = try pValue(i16, array_mod.Int16Array{ .data = data }) }),
        .int32 => Scalar.init(data.data_type, .{ .i32 = try pValue(i32, array_mod.Int32Array{ .data = data }) }),
        .int64 => Scalar.init(data.data_type, .{ .i64 = try pValue(i64, array_mod.Int64Array{ .data = data }) }),
        .uint8 => Scalar.init(data.data_type, .{ .u8 = try pValue(u8, array_mod.UInt8Array{ .data = data }) }),
        .uint16 => Scalar.init(data.data_type, .{ .u16 = try pValue(u16, array_mod.UInt16Array{ .data = data }) }),
        .uint32 => Scalar.init(data.data_type, .{ .u32 = try pValue(u32, array_mod.UInt32Array{ .data = data }) }),
        .uint64 => Scalar.init(data.data_type, .{ .u64 = try pValue(u64, array_mod.UInt64Array{ .data = data }) }),
        .half_float => Scalar.init(data.data_type, .{ .f16 = try pValue(f16, array_mod.HalfFloatArray{ .data = data }) }),
        .float => Scalar.init(data.data_type, .{ .f32 = try pValue(f32, array_mod.Float32Array{ .data = data }) }),
        .double => Scalar.init(data.data_type, .{ .f64 = try pValue(f64, array_mod.Float64Array{ .data = data }) }),
        .date32 => Scalar.init(data.data_type, .{ .date32 = try pValue(i32, array_mod.Date32Array{ .data = data }) }),
        .date64 => Scalar.init(data.data_type, .{ .date64 = try pValue(i64, array_mod.Date64Array{ .data = data }) }),
        .time32 => Scalar.init(data.data_type, .{ .time32 = try pValue(i32, array_mod.Time32Array{ .data = data }) }),
        .time64 => Scalar.init(data.data_type, .{ .time64 = try pValue(i64, array_mod.Time64Array{ .data = data }) }),
        .timestamp => Scalar.init(data.data_type, .{ .timestamp = try pValue(i64, array_mod.TimestampArray{ .data = data }) }),
        .duration => Scalar.init(data.data_type, .{ .duration = try pValue(i64, array_mod.DurationArray{ .data = data }) }),
        .interval_months => Scalar.init(data.data_type, .{ .interval_months = try pValue(i32, array_mod.IntervalMonthsArray{ .data = data }) }),
        .interval_day_time => Scalar.init(data.data_type, .{ .interval_day_time = try pValue(i64, array_mod.IntervalDayTimeArray{ .data = data }) }),
        .interval_month_day_nano => Scalar.init(data.data_type, .{ .interval_month_day_nano = try pValue(i128, array_mod.IntervalMonthDayNanoArray{ .data = data }) }),
        .decimal32 => Scalar.init(data.data_type, .{ .decimal32 = try pValue(i32, array_mod.Decimal32Array{ .data = data }) }),
        .decimal64 => Scalar.init(data.data_type, .{ .decimal64 = try pValue(i64, array_mod.Decimal64Array{ .data = data }) }),
        .decimal128 => Scalar.init(data.data_type, .{ .decimal128 = try pValue(i128, array_mod.Decimal128Array{ .data = data }) }),
        .decimal256 => Scalar.init(data.data_type, .{ .decimal256 = try pValue(i256, array_mod.Decimal256Array{ .data = data }) }),
        .string => .{
            .data_type = data.data_type,
            .value = .{ .string = (array_mod.StringArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .large_string => .{
            .data_type = data.data_type,
            .value = .{ .string = (array_mod.LargeStringArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.BinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .large_binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.LargeBinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .fixed_size_binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.FixedSizeBinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .list, .large_list, .fixed_size_list, .struct_ => Scalar.initNested(data.data_type, value),
        else => error.UnsupportedType,
    };
}

/// Extract one logical list value from list-like datums (array/chunked/scalar).
pub fn datumListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one logical large_list value from list-like datums (array/chunked/scalar).
pub fn datumLargeListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .large_list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .large_list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .large_list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .large_list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one logical fixed_size_list value from list-like datums (array/chunked/scalar).
pub fn datumFixedSizeListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .fixed_size_list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .fixed_size_list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .fixed_size_list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .fixed_size_list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one struct field while preserving array-like/scalar-like semantics.
///
/// - `array(struct)` -> `array(field)`
/// - `chunked(struct)` -> `chunked(field)`
/// - `scalar(struct)` -> `scalar(field)`
pub fn datumStructField(datum: Datum, field_index: usize) KernelError!Datum {
    return switch (datum) {
        .array => |arr| blk: {
            const dt = arr.data().data_type;
            if (dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= dt.struct_.fields.len) break :blk error.InvalidInput;
            const view = array_mod.StructArray{ .data = arr.data() };
            const field = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
            break :blk Datum.fromArray(field);
        },
        .chunked => |chunks| blk: {
            const struct_dt = chunks.dataType();
            if (struct_dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= struct_dt.struct_.fields.len) break :blk error.InvalidInput;
            const field_dt = struct_dt.struct_.fields[field_index].data_type.*;
            const allocator = chunks.node.allocator;

            var fields = allocator.alloc(ArrayRef, chunks.numChunks()) catch break :blk error.OutOfMemory;
            var field_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < field_count) : (i += 1) fields[i].release();
                allocator.free(fields);
            }

            var chunk_index: usize = 0;
            while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
                const chunk = chunks.chunk(chunk_index);
                const chunk_dt = chunk.data().data_type;
                if (chunk_dt != .struct_ or field_index >= chunk_dt.struct_.fields.len) break :blk error.InvalidInput;
                const view = array_mod.StructArray{ .data = chunk.data() };
                fields[field_count] = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
                field_count += 1;
            }

            const out = core.ChunkedArray.init(allocator, field_dt, fields[0..field_count]) catch |err| break :blk common.mapChunkedError(err);
            var i: usize = 0;
            while (i < field_count) : (i += 1) fields[i].release();
            allocator.free(fields);

            break :blk Datum.fromChunked(out);
        },
        .scalar => |s| blk: {
            const dt = s.data_type;
            if (dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= dt.struct_.fields.len) break :blk error.InvalidInput;
            const field_dt = dt.struct_.fields[field_index].data_type.*;

            if (s.isNull()) {
                break :blk Datum.fromScalar(Scalar.init(field_dt, .null));
            }

            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .struct_ or payload.data().length != 1) break :blk error.InvalidInput;

            const view = array_mod.StructArray{ .data = payload.data() };
            var field_array = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
            defer field_array.release();

            const field_scalar = try scalarFromSingleArrayRef(field_array);
            break :blk Datum.fromScalar(field_scalar);
        },
    };
}

pub fn datumElementArrayAt(allocator: std.mem.Allocator, datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            break :blk arr.slice(logical_index, 1) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            break :blk located.chunk.slice(located.local_index, 1) catch |err| mapArrayReadError(err);
        },
        .scalar => |scalar| builders.scalarToSingleArrayRef(allocator, scalar),
    };
}

/// Create an empty datum preserving the input datum's logical type.
pub fn datumSliceEmpty(datum: Datum) KernelError!Datum {
    return switch (datum) {
        .array => |arr| Datum.fromArray(arr.slice(0, 0) catch |err| return mapArrayReadError(err)),
        .chunked => |chunks| Datum.fromChunked(chunks.slice(chunks.node.allocator, 0, 0) catch |err| return common.mapChunkedError(err)),
        .scalar => |s| builders.datumBuildEmptyLikeWithAllocator(common.inferDatumAllocator(datum) orelse std.heap.page_allocator, s.data_type),
    };
}
