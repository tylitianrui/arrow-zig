const std = @import("std");
const datatype = @import("../datatype.zig");
const array_mod = @import("../array/array.zig");
const common = @import("test_common.zig");

const DataType = common.DataType;
const ArrayRef = common.ArrayRef;
const ChunkedArray = common.ChunkedArray;
const Scalar = common.Scalar;
const Datum = common.Datum;
const FunctionRegistry = common.FunctionRegistry;
const ExecContext = common.ExecContext;
const KernelSignature = common.KernelSignature;
const KernelError = common.KernelError;
const Options = common.Options;
const OptionsTag = common.OptionsTag;
const SortOptions = common.SortOptions;
const SortOrder = common.SortOrder;
const SortNullPlacement = common.SortNullPlacement;
const FilterOptions = common.FilterOptions;
const UnaryExecChunkIterator = common.UnaryExecChunkIterator;
const BinaryExecChunkIterator = common.BinaryExecChunkIterator;
const NaryExecChunkIterator = common.NaryExecChunkIterator;

const intCastOrInvalidCast = common.intCastOrInvalidCast;
const arithmeticDivI64 = common.arithmeticDivI64;
const hasArity = common.hasArity;
const unaryArray = common.unaryArray;
const unaryChunked = common.unaryChunked;
const unaryScalar = common.unaryScalar;
const sameDataTypes = common.sameDataTypes;
const unaryNumeric = common.unaryNumeric;
const binarySameNumeric = common.binarySameNumeric;
const allNumeric = common.allNumeric;
const unaryNullPropagates = common.unaryNullPropagates;
const binaryNullPropagates = common.binaryNullPropagates;
const naryNullPropagates = common.naryNullPropagates;
const inferBinaryExecLen = common.inferBinaryExecLen;
const inferNaryExecLen = common.inferNaryExecLen;
const datumListValueAt = common.datumListValueAt;
const datumStructField = common.datumStructField;
const datumBuildEmptyLike = common.datumBuildEmptyLike;
const datumBuildEmptyLikeWithAllocator = common.datumBuildEmptyLikeWithAllocator;
const datumBuildNullLike = common.datumBuildNullLike;
const datumBuildNullLikeWithAllocator = common.datumBuildNullLikeWithAllocator;
const datumSliceEmpty = common.datumSliceEmpty;
const datumSelect = common.datumSelect;
const datumSelectNullable = common.datumSelectNullable;
const datumFilter = common.datumFilter;
const chunkedResolveLogicalIndices = common.chunkedResolveLogicalIndices;
const datumTake = common.datumTake;
const datumFilterSelectionIndices = common.datumFilterSelectionIndices;
const datumFilterChunkAware = common.datumFilterChunkAware;

const isInt32Datum = common.isInt32Datum;
const isTwoInt32 = common.isTwoInt32;
const isInt64Scalar = common.isInt64Scalar;
const isTwoInt64Scalars = common.isTwoInt64Scalars;
const allInt32Datums = common.allInt32Datums;
const passthroughInt32Kernel = common.passthroughInt32Kernel;
const exactArityMarkerKernel = common.exactArityMarkerKernel;
const rangeArityMarkerKernel = common.rangeArityMarkerKernel;
const atLeastArityMarkerKernel = common.atLeastArityMarkerKernel;
const countLenAggregateKernel = common.countLenAggregateKernel;
const onlyCastOptions = common.onlyCastOptions;
const onlyArithmeticOptions = common.onlyArithmeticOptions;
const onlySortOptions = common.onlySortOptions;
const firstArgResultType = common.firstArgResultType;
const castResultType = common.castResultType;
const castI64ToI32ResultType = common.castI64ToI32ResultType;
const castI64ToI32Kernel = common.castI64ToI32Kernel;
const divI64ScalarResultType = common.divI64ScalarResultType;
const divI64ScalarKernel = common.divI64ScalarKernel;
const countAggregateResultType = common.countAggregateResultType;
const countLifecycleInit = common.countLifecycleInit;
const countLifecycleUpdate = common.countLifecycleUpdate;
const countLifecycleMerge = common.countLifecycleMerge;
const countLifecycleFinalize = common.countLifecycleFinalize;
const countLifecycleDeinit = common.countLifecycleDeinit;
const makeInt32Array = common.makeInt32Array;
const makeBoolArray = common.makeBoolArray;
const collectInt32ValuesFromDatum = common.collectInt32ValuesFromDatum;

test "compute execution helpers align chunked chunks for binary kernels" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    var l0 = try makeInt32Array(allocator, &[_]?i32{ 10, 11 });
    defer l0.release();
    var l1 = try makeInt32Array(allocator, &[_]?i32{ 20, 21, 22 });
    defer l1.release();
    var r0 = try makeInt32Array(allocator, &[_]?i32{100});
    defer r0.release();
    var r1 = try makeInt32Array(allocator, &[_]?i32{ 101, 102, 103, 104 });
    defer r1.release();

    var left_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ l0, l1 });
    defer left_chunks.release();
    var right_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ r0, r1 });
    defer right_chunks.release();

    var lhs = Datum.fromChunked(left_chunks.retain());
    defer lhs.release();
    var rhs = Datum.fromChunked(right_chunks.retain());
    defer rhs.release();

    var iter = try BinaryExecChunkIterator.init(lhs, rhs);
    const expected_chunk_lengths = [_]usize{ 1, 1, 3 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected_chunk_lengths.len);
        try std.testing.expectEqual(expected_chunk_lengths[idx], chunk.len);
        try std.testing.expect(chunk.lhs == .array);
        try std.testing.expect(chunk.rhs == .array);
    }
    try std.testing.expectEqual(expected_chunk_lengths.len, idx);

    var iter_values = try BinaryExecChunkIterator.init(lhs, rhs);

    var c0 = (try iter_values.next()).?;
    defer c0.deinit();
    const l0_arr = int32_array{ .data = c0.lhs.array.data() };
    const r0_arr = int32_array{ .data = c0.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 10), l0_arr.value(0));
    try std.testing.expectEqual(@as(i32, 100), r0_arr.value(0));

    var c1 = (try iter_values.next()).?;
    defer c1.deinit();
    const l1_arr = int32_array{ .data = c1.lhs.array.data() };
    const r1_arr = int32_array{ .data = c1.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 11), l1_arr.value(0));
    try std.testing.expectEqual(@as(i32, 101), r1_arr.value(0));

    var c2 = (try iter_values.next()).?;
    defer c2.deinit();
    const l2_arr = int32_array{ .data = c2.lhs.array.data() };
    const r2_arr = int32_array{ .data = c2.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 20), l2_arr.value(0));
    try std.testing.expectEqual(@as(i32, 22), l2_arr.value(2));
    try std.testing.expectEqual(@as(i32, 102), r2_arr.value(0));
    try std.testing.expectEqual(@as(i32, 104), r2_arr.value(2));

    try std.testing.expect((try iter_values.next()) == null);
}

test "compute execution helpers support scalar broadcast and null propagation" {
    const allocator = std.testing.allocator;

    var r0 = try makeInt32Array(allocator, &[_]?i32{ 1, null });
    defer r0.release();
    var r1 = try makeInt32Array(allocator, &[_]?i32{3});
    defer r1.release();
    var right_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ r0, r1 });
    defer right_chunks.release();

    const lhs = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .null,
    });
    var rhs = Datum.fromChunked(right_chunks.retain());
    defer rhs.release();

    try std.testing.expectEqual(@as(usize, 3), try inferBinaryExecLen(lhs, rhs));
    var iter = try BinaryExecChunkIterator.init(lhs, rhs);

    var seen: usize = 0;
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            try std.testing.expect(binaryNullPropagates(chunk.lhs, chunk.rhs, i));
            try std.testing.expect(chunk.binaryNullAt(i));
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "compute unary execution helper propagates nulls over chunked input" {
    const allocator = std.testing.allocator;

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 7, null });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ null, 9 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    var input = Datum.fromChunked(chunks.retain());
    defer input.release();

    var iter = UnaryExecChunkIterator.init(input);
    var first = (try iter.next()).?;
    defer first.deinit();
    try std.testing.expect(!first.unaryNullAt(0));
    try std.testing.expect(first.unaryNullAt(1));

    var second = (try iter.next()).?;
    defer second.deinit();
    try std.testing.expect(unaryNullPropagates(second.values, 0));
    try std.testing.expect(!unaryNullPropagates(second.values, 1));

    try std.testing.expect((try iter.next()) == null);
}

test "compute nary execution helper supports array scalar chunked mixed broadcast" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    var base = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer base.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 11 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{12});
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const array_input = Datum.fromArray(base.retain());
    defer {
        var d = array_input;
        d.release();
    }
    const scalar_input = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 9 },
    });
    const chunked_input = Datum.fromChunked(chunks.retain());
    defer {
        var d = chunked_input;
        d.release();
    }

    const args = [_]Datum{ array_input, scalar_input, chunked_input };
    try std.testing.expectEqual(@as(usize, 3), try inferNaryExecLen(args[0..]));

    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    const expected_chunk_lengths = [_]usize{ 2, 1 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected_chunk_lengths.len);
        try std.testing.expectEqual(expected_chunk_lengths[idx], chunk.len);
        try std.testing.expectEqual(@as(usize, 3), chunk.values.len);
        try std.testing.expect(chunk.values[0] == .array);
        try std.testing.expect(chunk.values[1] == .scalar);
        try std.testing.expect(chunk.values[2] == .array);

        const left = int32_array{ .data = chunk.values[0].array.data() };
        const right = int32_array{ .data = chunk.values[2].array.data() };
        if (idx == 0) {
            try std.testing.expectEqual(@as(i32, 1), left.value(0));
            try std.testing.expectEqual(@as(i32, 2), left.value(1));
            try std.testing.expectEqual(@as(i32, 10), right.value(0));
            try std.testing.expectEqual(@as(i32, 11), right.value(1));
        } else {
            try std.testing.expectEqual(@as(i32, 3), left.value(0));
            try std.testing.expectEqual(@as(i32, 12), right.value(0));
        }
        try std.testing.expectEqual(@as(i32, 9), chunk.values[1].scalar.value.i32);
    }
    try std.testing.expectEqual(expected_chunk_lengths.len, idx);
}

test "compute nary execution helper aligns misaligned chunk boundaries" {
    const allocator = std.testing.allocator;

    var a0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer a0.release();
    var a1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4, 5 });
    defer a1.release();
    var b0 = try makeInt32Array(allocator, &[_]?i32{11});
    defer b0.release();
    var b1 = try makeInt32Array(allocator, &[_]?i32{ 12, 13, 14, 15 });
    defer b1.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{ 21, 22, 23 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 24, 25 });
    defer c1.release();

    var chunks_a = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ a0, a1 });
    defer chunks_a.release();
    var chunks_b = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ b0, b1 });
    defer chunks_b.release();
    var chunks_c = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks_c.release();

    const d_a = Datum.fromChunked(chunks_a.retain());
    defer {
        var d = d_a;
        d.release();
    }
    const d_b = Datum.fromChunked(chunks_b.retain());
    defer {
        var d = d_b;
        d.release();
    }
    const d_c = Datum.fromChunked(chunks_c.retain());
    defer {
        var d = d_c;
        d.release();
    }
    const args = [_]Datum{ d_a, d_b, d_c };

    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    const expected = [_]usize{ 1, 1, 1, 2 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected.len);
        try std.testing.expectEqual(expected[idx], chunk.len);
        for (chunk.values) |value| {
            try std.testing.expect(value == .array);
        }
    }
    try std.testing.expectEqual(expected.len, idx);
}

test "compute nary execution helper propagates nulls across all inputs" {
    const allocator = std.testing.allocator;

    var arr = try makeInt32Array(allocator, &[_]?i32{ 1, null, 3 });
    defer arr.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{4});
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ null, 6 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const array_input = Datum.fromArray(arr.retain());
    defer {
        var d = array_input;
        d.release();
    }
    const scalar_null = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .null,
    });
    const chunked_input = Datum.fromChunked(chunks.retain());
    defer {
        var d = chunked_input;
        d.release();
    }

    const args = [_]Datum{ array_input, scalar_null, chunked_input };
    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    var seen: usize = 0;
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            try std.testing.expect(naryNullPropagates(chunk.values, i));
            try std.testing.expect(chunk.naryNullAt(i));
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "compute inferNaryExecLen rejects non-broadcast length mismatch" {
    const allocator = std.testing.allocator;

    var a = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer a.release();
    var b = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 12 });
    defer b.release();

    const d_a = Datum.fromArray(a.retain());
    defer {
        var d = d_a;
        d.release();
    }
    const d_b = Datum.fromArray(b.retain());
    defer {
        var d = d_b;
        d.release();
    }
    const d_scalar = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 7 },
    });
    const args = [_]Datum{ d_a, d_b, d_scalar };
    try std.testing.expectError(error.InvalidInput, inferNaryExecLen(args[0..]));
}

test "compute inferBinaryExecLen rejects non-broadcast length mismatch" {
    const allocator = std.testing.allocator;

    var l = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer l.release();
    var r = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer r.release();

    const lhs = Datum.fromArray(l.retain());
    defer {
        var d = lhs;
        d.release();
    }
    const rhs = Datum.fromArray(r.retain());
    defer {
        var d = rhs;
        d.release();
    }

    try std.testing.expectError(error.InvalidInput, inferBinaryExecLen(lhs, rhs));
}
