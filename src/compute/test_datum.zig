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

test "compute datum chunked variant retain and release" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;

    var builder = try int32_builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var arr = try builder.finish();
    defer arr.release();

    var chunked = try ChunkedArray.fromSingle(allocator, arr);
    defer chunked.release();

    var datum = Datum{ .chunked = chunked.retain() };
    defer datum.release();

    try std.testing.expect(datum.dataType().eql(.{ .int32 = {} }));
    try std.testing.expectEqual(@as(usize, 2), datum.chunked.len());
}

test "compute datum helpers and shared type predicates" {
    const scalar_i32 = Scalar.init(.{ .int32 = {} }, .{ .i32 = 42 });
    const scalar_i64 = Scalar.init(.{ .int64 = {} }, .{ .i64 = 42 });

    const args_numeric = [_]Datum{
        Datum.fromScalar(scalar_i32),
        Datum.fromScalar(scalar_i64),
    };

    try std.testing.expect(hasArity(args_numeric[0..], 2));
    try std.testing.expect(!sameDataTypes(args_numeric[0..]));
    try std.testing.expect(allNumeric(args_numeric[0..]));
    try std.testing.expect(!binarySameNumeric(args_numeric[0..]));

    const args_same_numeric = [_]Datum{
        Datum.fromScalar(scalar_i32),
        Datum.fromScalar(scalar_i32),
    };
    try std.testing.expect(sameDataTypes(args_same_numeric[0..]));
    try std.testing.expect(binarySameNumeric(args_same_numeric[0..]));
    try std.testing.expect(unaryScalar(args_same_numeric[0..1]));
}

test "compute datum accessors and signature helpers" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var builder = try int32_builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var arr = try builder.finish();
    defer arr.release();

    var chunked = try ChunkedArray.fromSingle(allocator, arr);
    defer chunked.release();

    var datum_arr = Datum.fromArray(arr.retain());
    defer datum_arr.release();
    try std.testing.expect(datum_arr.isArray());
    try std.testing.expect(!datum_arr.isChunked());
    try std.testing.expect(!datum_arr.isScalar());
    try std.testing.expect(datum_arr.asArray() != null);
    try std.testing.expect(datum_arr.asChunked() == null);
    try std.testing.expect(datum_arr.asScalar() == null);

    var datum_chunked = Datum.fromChunked(chunked.retain());
    defer datum_chunked.release();
    try std.testing.expect(datum_chunked.isChunked());
    try std.testing.expect(datum_chunked.asChunked() != null);
    try std.testing.expectEqual(@as(usize, 2), datum_chunked.asChunked().?.len());

    const scalar = Scalar.init(.{ .int32 = {} }, .{ .i32 = 7 });
    var datum_scalar = Datum.fromScalar(scalar);
    defer datum_scalar.release();
    try std.testing.expect(datum_scalar.isScalar());
    try std.testing.expectEqual(@as(i32, 7), datum_scalar.asScalar().?.value.i32);

    const args_bad = [_]Datum{ Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 1 })), Datum.fromScalar(Scalar.init(.{ .int64 = {} }, .{ .i64 = 2 })) };
    const args_ok = [_]Datum{ Datum.fromScalar(scalar), Datum.fromScalar(scalar) };
    const sig_any = KernelSignature.any(2);
    const sig_binary_int32 = KernelSignature.binary(isTwoInt32);
    try std.testing.expect(sig_any.matches(args_bad[0..]));
    try std.testing.expect(!sig_binary_int32.matches(args_bad[0..]));
    try std.testing.expect(sig_binary_int32.matches(args_ok[0..]));
    const inferred_binary = try sig_binary_int32.inferResultType(args_ok[0..], Options.noneValue());
    try std.testing.expect(inferred_binary.eql(.{ .int32 = {} }));
}

test "compute kernel signature supports exact at_least and range arity" {
    const scalar_i32 = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 1 },
    });
    const one = [_]Datum{scalar_i32};
    const two = [_]Datum{ scalar_i32, scalar_i32 };
    const three = [_]Datum{ scalar_i32, scalar_i32, scalar_i32 };
    const four = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    const five = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32, scalar_i32 };

    const sig_exact = KernelSignature.any(2);
    const sig_at_least = KernelSignature.atLeast(2);
    const sig_range = KernelSignature.range(2, 4);

    try std.testing.expect(!sig_exact.matches(one[0..]));
    try std.testing.expect(sig_exact.matches(two[0..]));
    try std.testing.expect(!sig_exact.matches(three[0..]));

    try std.testing.expect(!sig_at_least.matches(one[0..]));
    try std.testing.expect(sig_at_least.matches(two[0..]));
    try std.testing.expect(sig_at_least.matches(five[0..]));

    try std.testing.expect(!sig_range.matches(one[0..]));
    try std.testing.expect(sig_range.matches(two[0..]));
    try std.testing.expect(sig_range.matches(three[0..]));
    try std.testing.expect(sig_range.matches(four[0..]));
    try std.testing.expect(!sig_range.matches(five[0..]));
}

test "compute scalar value temporal and decimal coverage" {
    const scalars = [_]Scalar{
        .{ .data_type = .{ .date32 = {} }, .value = .{ .date32 = 18_630 } },
        .{ .data_type = .{ .date64 = {} }, .value = .{ .date64 = 1_609_545_600_000 } },
        .{ .data_type = .{ .time32 = .{ .unit = .millisecond } }, .value = .{ .time32 = 1234 } },
        .{ .data_type = .{ .time64 = .{ .unit = .nanosecond } }, .value = .{ .time64 = 99_000 } },
        .{ .data_type = .{ .timestamp = .{ .unit = .microsecond, .timezone = "UTC" } }, .value = .{ .timestamp = 1_700_000_000_123_456 } },
        .{ .data_type = .{ .duration = .{ .unit = .nanosecond } }, .value = .{ .duration = 42 } },
        .{ .data_type = .{ .interval_months = .{ .unit = .months } }, .value = .{ .interval_months = 7 } },
        .{ .data_type = .{ .interval_day_time = .{ .unit = .day_time } }, .value = .{ .interval_day_time = -43_200_000 } },
        .{ .data_type = .{ .interval_month_day_nano = .{ .unit = .month_day_nano } }, .value = .{ .interval_month_day_nano = -123_456_789_012_345_678 } },
        .{ .data_type = .{ .decimal32 = .{ .precision = 9, .scale = 2 } }, .value = .{ .decimal32 = 123_45 } },
        .{ .data_type = .{ .decimal64 = .{ .precision = 18, .scale = 4 } }, .value = .{ .decimal64 = -9_876_543_210 } },
        .{ .data_type = .{ .decimal128 = .{ .precision = 38, .scale = 10 } }, .value = .{ .decimal128 = -987_654_321_098_765_432 } },
        .{ .data_type = .{ .decimal256 = .{ .precision = 76, .scale = 20 } }, .value = .{ .decimal256 = -987_654_321_098_765_432_109_876_543_210 } },
        .{ .data_type = .{ .string = {} }, .value = .{ .string = "borrowed-string" } },
        .{ .data_type = .{ .binary = {} }, .value = .{ .binary = "borrowed-binary" } },
    };

    try std.testing.expectEqual(@as(usize, 15), scalars.len);
    try std.testing.expect(scalars[0].data_type == .date32);
    try std.testing.expect(scalars[12].data_type == .decimal256);
    try std.testing.expectEqual(@as(i32, 18_630), scalars[0].value.date32);
    try std.testing.expectEqual(@as(i256, -987_654_321_098_765_432_109_876_543_210), scalars[12].value.decimal256);
    try std.testing.expectEqualStrings("borrowed-string", scalars[13].value.string);
    try std.testing.expectEqualStrings("borrowed-binary", scalars[14].value.binary);
}

test "compute exec context config and scalar payload duplication" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = ExecContext.initWithConfig(allocator, &registry, .{
        .safe_cast = false,
        .overflow_mode = .wrapping,
        .threads = 0,
        .arena_allocator = arena.allocator(),
    });
    try std.testing.expect(!ctx.safeCastEnabled());
    try std.testing.expect(ctx.overflowMode() == .wrapping);
    try std.testing.expectEqual(@as(usize, 1), ctx.threads());

    const s = try ctx.dupScalarString("hello");
    const b = try ctx.dupScalarBinary("world");
    try std.testing.expectEqualStrings("hello", s);
    try std.testing.expectEqualStrings("world", b);
}

test "compute nested scalar payload is retained across datum and exec chunk lifecycle" {
    const allocator = std.testing.allocator;

    const item_type = DataType{ .int32 = {} };
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &item_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer values.release();

    var list_builder = try array_mod.ListBuilder.init(allocator, 1, item_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2);
    var list_array = try list_builder.finish(values);
    defer list_array.release();

    var scalar = try Scalar.initNested(list_array.data().data_type, list_array);
    defer scalar.release();
    try std.testing.expect(scalar.value == .list);
    try std.testing.expect(scalar.payload != null);

    var datum = Datum.fromScalar(scalar.retain());
    defer datum.release();

    var iter_input = datum.retain();
    defer iter_input.release();
    var iter = UnaryExecChunkIterator.init(iter_input);
    var chunk = (try iter.next()).?;
    defer chunk.deinit();
    try std.testing.expectEqual(@as(usize, 1), chunk.len);
    try std.testing.expect(chunk.values == .scalar);
    try std.testing.expect(chunk.values.scalar.value == .list);
    try std.testing.expect(chunk.values.scalar.payload != null);
}

test "compute datum list extraction supports array chunked and scalar inputs" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    const item_type = DataType{ .int32 = {} };
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &item_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer values.release();

    var list_builder = try array_mod.ListBuilder.init(allocator, 3, item_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2); // [1, 2]
    try list_builder.appendNull(); // null
    try list_builder.appendLen(1); // [3]
    var list_array = try list_builder.finish(values);
    defer list_array.release();

    var list_slice = try list_array.slice(1, 2); // [null, [3]]
    defer list_slice.release();
    var array_datum = Datum.fromArray(list_slice.retain());
    defer array_datum.release();
    var out_array = try datumListValueAt(array_datum, 1);
    defer out_array.release();
    const out_array_view = int32_array{ .data = out_array.data() };
    try std.testing.expectEqual(@as(usize, 1), out_array_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_array_view.value(0));

    var list_chunk0 = try list_array.slice(0, 1);
    defer list_chunk0.release();
    var list_chunk1 = try list_array.slice(1, 2);
    defer list_chunk1.release();
    var chunked = try ChunkedArray.init(allocator, list_array.data().data_type, &[_]ArrayRef{ list_chunk0, list_chunk1 });
    defer chunked.release();
    var chunked_datum = Datum.fromChunked(chunked.retain());
    defer chunked_datum.release();
    var out_chunked = try datumListValueAt(chunked_datum, 2);
    defer out_chunked.release();
    const out_chunked_view = int32_array{ .data = out_chunked.data() };
    try std.testing.expectEqual(@as(usize, 1), out_chunked_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_chunked_view.value(0));

    var scalar_payload = try list_array.slice(2, 1);
    defer scalar_payload.release();
    var scalar_datum = Datum.fromScalar(try Scalar.initNested(list_array.data().data_type, scalar_payload));
    defer scalar_datum.release();
    var out_scalar = try datumListValueAt(scalar_datum, 1234);
    defer out_scalar.release();
    const out_scalar_view = int32_array{ .data = out_scalar.data() };
    try std.testing.expectEqual(@as(usize, 1), out_scalar_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_scalar_view.value(0));

    var null_payload = try list_array.slice(1, 1);
    defer null_payload.release();
    var null_scalar_datum = Datum.fromScalar(try Scalar.initNested(list_array.data().data_type, null_payload));
    defer null_scalar_datum.release();
    try std.testing.expectError(error.InvalidInput, datumListValueAt(null_scalar_datum, 0));
}

test "compute datumStructField supports scalar struct bool fields" {
    const allocator = std.testing.allocator;

    const bool_type = DataType{ .bool = {} };
    const fields = [_]datatype.Field{
        .{ .name = "a", .data_type = &bool_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };

    var child_a = try makeBoolArray(allocator, &[_]?bool{true});
    defer child_a.release();
    var child_b = try makeBoolArray(allocator, &[_]?bool{false});
    defer child_b.release();

    var struct_builder = array_mod.StructBuilder.init(allocator, fields[0..]);
    defer struct_builder.deinit();
    try struct_builder.appendValid();
    var struct_array = try struct_builder.finish(&[_]ArrayRef{ child_a, child_b });
    defer struct_array.release();

    var struct_scalar_datum = Datum.fromScalar(try Scalar.initNested(struct_array.data().data_type, struct_array));
    defer struct_scalar_datum.release();

    var field0 = try datumStructField(struct_scalar_datum, 0);
    defer field0.release();
    try std.testing.expect(field0.isScalar());
    try std.testing.expect(field0.scalar.data_type == .bool);
    try std.testing.expectEqual(true, field0.scalar.value.bool);

    var field1 = try datumStructField(struct_scalar_datum, 1);
    defer field1.release();
    try std.testing.expect(field1.isScalar());
    try std.testing.expectEqual(false, field1.scalar.value.bool);

    var struct_array_datum = Datum.fromArray(struct_array.retain());
    defer struct_array_datum.release();
    var field0_array = try datumStructField(struct_array_datum, 0);
    defer field0_array.release();
    try std.testing.expect(field0_array.isArray());
    const bool_view = array_mod.BooleanArray{ .data = field0_array.array.data() };
    try std.testing.expectEqual(@as(usize, 1), bool_view.len());
    try std.testing.expectEqual(true, bool_view.value(0));

    var null_child_a = try makeBoolArray(allocator, &[_]?bool{true});
    defer null_child_a.release();
    var null_child_b = try makeBoolArray(allocator, &[_]?bool{false});
    defer null_child_b.release();
    var null_struct_builder = array_mod.StructBuilder.init(allocator, fields[0..]);
    defer null_struct_builder.deinit();
    try null_struct_builder.appendNull();
    var null_struct_array = try null_struct_builder.finish(&[_]ArrayRef{ null_child_a, null_child_b });
    defer null_struct_array.release();

    var null_struct_scalar = Datum.fromScalar(try Scalar.initNested(null_struct_array.data().data_type, null_struct_array));
    defer null_struct_scalar.release();
    var null_field = try datumStructField(null_struct_scalar, 0);
    defer null_field.release();
    try std.testing.expect(null_field.isScalar());
    try std.testing.expect(null_field.scalar.isNull());
}

test "compute datumBuildEmptyLike and datumSliceEmpty preserve nested layout" {
    const allocator = std.testing.allocator;

    const int32_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const list_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const struct_fields = [_]datatype.Field{
        .{ .name = "x", .data_type = &int32_type, .nullable = true },
        .{ .name = "flag", .data_type = &bool_type, .nullable = true },
    };

    const list_type = DataType{ .list = .{ .value_field = list_field } };
    const fixed_size_list_type = DataType{ .fixed_size_list = .{ .list_size = 2, .value_field = list_field } };
    const struct_type = DataType{ .struct_ = .{ .fields = struct_fields[0..] } };

    var empty_list = try datumBuildEmptyLike(list_type);
    defer empty_list.release();
    try std.testing.expect(empty_list.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_list.array.data().length);
    try std.testing.expect(empty_list.array.data().data_type.eql(list_type));
    try empty_list.array.data().validateLayout();

    var empty_fsl = try datumBuildEmptyLike(fixed_size_list_type);
    defer empty_fsl.release();
    try std.testing.expect(empty_fsl.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_fsl.array.data().length);
    try std.testing.expect(empty_fsl.array.data().data_type.eql(fixed_size_list_type));
    try empty_fsl.array.data().validateLayout();

    var empty_struct = try datumBuildEmptyLike(struct_type);
    defer empty_struct.release();
    try std.testing.expect(empty_struct.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_struct.array.data().length);
    try std.testing.expect(empty_struct.array.data().data_type.eql(struct_type));
    try empty_struct.array.data().validateLayout();

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer values.release();
    var list_builder = try array_mod.ListBuilder.init(allocator, 1, list_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2);
    var list_one = try list_builder.finish(values);
    defer list_one.release();

    var scalar_list = Datum.fromScalar(try Scalar.initNested(list_type, list_one));
    defer scalar_list.release();
    var sliced_empty = try datumSliceEmpty(scalar_list);
    defer sliced_empty.release();
    try std.testing.expect(sliced_empty.isArray());
    try std.testing.expectEqual(@as(usize, 0), sliced_empty.array.data().length);
    try std.testing.expect(sliced_empty.array.data().data_type.eql(list_type));
    try sliced_empty.array.data().validateLayout();
}

test "compute chunkedResolveLogicalIndices maps chunk-local coordinates and validates bounds" {
    const allocator = std.testing.allocator;

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 30, 40, 50 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const mapped = try chunkedResolveLogicalIndices(allocator, chunks, &[_]usize{ 0, 1, 2, 4 });
    defer allocator.free(mapped);

    try std.testing.expectEqual(@as(usize, 4), mapped.len);
    try std.testing.expectEqual(@as(usize, 0), mapped[0].chunk_index);
    try std.testing.expectEqual(@as(usize, 0), mapped[0].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 0), mapped[1].chunk_index);
    try std.testing.expectEqual(@as(usize, 1), mapped[1].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 1), mapped[2].chunk_index);
    try std.testing.expectEqual(@as(usize, 0), mapped[2].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 1), mapped[3].chunk_index);
    try std.testing.expectEqual(@as(usize, 2), mapped[3].index_in_chunk);

    try std.testing.expectError(
        error.InvalidInput,
        chunkedResolveLogicalIndices(allocator, chunks, &[_]usize{5}),
    );
}

test "compute datumTake keeps chunked output and matches array logical result on misaligned boundaries" {
    const allocator = std.testing.allocator;

    var base = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30, 40, 50 });
    defer base.release();

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{30});
    defer c1.release();
    var c2 = try makeInt32Array(allocator, &[_]?i32{ 40, 50 });
    defer c2.release();
    var chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1, c2 });
    defer chunked.release();

    var array_datum = Datum.fromArray(base.retain());
    defer array_datum.release();
    var chunked_datum = Datum.fromChunked(chunked.retain());
    defer chunked_datum.release();

    const indices = [_]usize{ 1, 2, 4, 0 };

    var out_array = try datumTake(array_datum, indices[0..]);
    defer out_array.release();
    var out_chunked = try datumTake(chunked_datum, indices[0..]);
    defer out_chunked.release();

    try std.testing.expect(out_array.isArray());
    try std.testing.expect(out_chunked.isChunked());
    try std.testing.expectEqual(@as(usize, 4), out_chunked.chunked.len());

    const array_values = try collectInt32ValuesFromDatum(allocator, out_array);
    defer allocator.free(array_values);
    const chunked_values = try collectInt32ValuesFromDatum(allocator, out_chunked);
    defer allocator.free(chunked_values);

    try std.testing.expectEqual(@as(usize, array_values.len), chunked_values.len);
    var i: usize = 0;
    while (i < array_values.len) : (i += 1) {
        try std.testing.expectEqual(array_values[i], chunked_values[i]);
    }
    try std.testing.expectEqual(@as(?i32, 20), chunked_values[0]);
    try std.testing.expectEqual(@as(?i32, 30), chunked_values[1]);
    try std.testing.expectEqual(@as(?i32, 50), chunked_values[2]);
    try std.testing.expectEqual(@as(?i32, 10), chunked_values[3]);
}

test "compute datumFilterSelectionIndices and datumFilterChunkAware stay consistent for array and chunked" {
    const allocator = std.testing.allocator;

    var values_array = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5 });
    defer values_array.release();
    var pred_array = try makeBoolArray(allocator, &[_]?bool{ true, false, null, true, true });
    defer pred_array.release();

    var v0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer v0.release();
    var v1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4, 5 });
    defer v1.release();
    var values_chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ v0, v1 });
    defer values_chunked.release();

    var p0 = try makeBoolArray(allocator, &[_]?bool{true});
    defer p0.release();
    var p1 = try makeBoolArray(allocator, &[_]?bool{ false, null, true });
    defer p1.release();
    var p2 = try makeBoolArray(allocator, &[_]?bool{true});
    defer p2.release();
    var pred_chunked = try ChunkedArray.init(allocator, .{ .bool = {} }, &[_]ArrayRef{ p0, p1, p2 });
    defer pred_chunked.release();

    var pred_array_datum = Datum.fromArray(pred_array.retain());
    defer pred_array_datum.release();
    const selections = try datumFilterSelectionIndices(allocator, pred_array_datum, 5, .{ .drop_nulls = false });
    defer allocator.free(selections);
    try std.testing.expectEqual(@as(usize, 4), selections.len);
    try std.testing.expectEqual(@as(?usize, 0), selections[0]);
    try std.testing.expectEqual(@as(?usize, null), selections[1]);
    try std.testing.expectEqual(@as(?usize, 3), selections[2]);
    try std.testing.expectEqual(@as(?usize, 4), selections[3]);

    var array_datum = Datum.fromArray(values_array.retain());
    defer array_datum.release();
    var chunked_datum = Datum.fromChunked(values_chunked.retain());
    defer chunked_datum.release();
    var pred_chunked_datum = Datum.fromChunked(pred_chunked.retain());
    defer pred_chunked_datum.release();

    var filtered_array = try datumFilterChunkAware(array_datum, pred_array_datum, .{ .drop_nulls = false });
    defer filtered_array.release();
    var filtered_chunked = try datumFilterChunkAware(chunked_datum, pred_chunked_datum, .{ .drop_nulls = false });
    defer filtered_chunked.release();

    try std.testing.expect(filtered_array.isArray());
    try std.testing.expect(filtered_chunked.isChunked());

    const expected = try collectInt32ValuesFromDatum(allocator, filtered_array);
    defer allocator.free(expected);
    const actual = try collectInt32ValuesFromDatum(allocator, filtered_chunked);
    defer allocator.free(actual);

    try std.testing.expectEqual(@as(usize, expected.len), actual.len);
    var i: usize = 0;
    while (i < expected.len) : (i += 1) {
        try std.testing.expectEqual(expected[i], actual[i]);
    }
}

test "compute datumFilter keeps compatibility array output shape for chunked inputs" {
    const allocator = std.testing.allocator;

    var v0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer v0.release();
    var v1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4 });
    defer v1.release();
    var values_chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ v0, v1 });
    defer values_chunked.release();

    var p0 = try makeBoolArray(allocator, &[_]?bool{ true, false, true, true });
    defer p0.release();
    var values_datum = Datum.fromChunked(values_chunked.retain());
    defer values_datum.release();
    var pred_datum = Datum.fromArray(p0.retain());
    defer pred_datum.release();

    var filtered = try datumFilter(values_datum, pred_datum, .{ .drop_nulls = true });
    defer filtered.release();
    try std.testing.expect(filtered.isArray());
}

test "compute datumFilter supports scalar array chunked and fixed_size_list null alignment" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;

    const int32_type = DataType{ .int32 = {} };
    const list_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const list_type = DataType{ .list = .{ .value_field = list_field } };

    var scalar_values_child = try makeInt32Array(allocator, &[_]?i32{ 7, 8 });
    defer scalar_values_child.release();
    var scalar_list_builder = try array_mod.ListBuilder.init(allocator, 1, list_field);
    defer scalar_list_builder.deinit();
    try scalar_list_builder.appendLen(2);
    var scalar_list_payload = try scalar_list_builder.finish(scalar_values_child);
    defer scalar_list_payload.release();

    var scalar_datum = Datum.fromScalar(try Scalar.initNested(list_type, scalar_list_payload));
    defer scalar_datum.release();

    var pred_array = try makeBoolArray(allocator, &[_]?bool{ true, false, true });
    defer pred_array.release();
    var pred_datum = Datum.fromArray(pred_array.retain());
    defer pred_datum.release();

    var filtered_scalar = try datumFilter(scalar_datum, pred_datum, .{ .drop_nulls = true });
    defer filtered_scalar.release();
    try std.testing.expect(filtered_scalar.isArray());
    try std.testing.expectEqual(@as(usize, 2), filtered_scalar.array.data().length);
    try filtered_scalar.array.data().validateLayout();
    const filtered_scalar_list = array_mod.ListArray{ .data = filtered_scalar.array.data() };
    var fs0 = try filtered_scalar_list.value(0);
    defer fs0.release();
    const fs0_values = int32_array{ .data = fs0.data() };
    try std.testing.expectEqual(@as(i32, 7), fs0_values.value(0));
    try std.testing.expectEqual(@as(i32, 8), fs0_values.value(1));

    var list_values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer list_values.release();
    var list_builder = try array_mod.ListBuilder.init(allocator, 3, list_field);
    defer list_builder.deinit();
    try list_builder.appendLen(1);
    try list_builder.appendLen(1);
    try list_builder.appendLen(1);
    var list_array = try list_builder.finish(list_values);
    defer list_array.release();

    var pred_chunk0 = try makeBoolArray(allocator, &[_]?bool{ true, null });
    defer pred_chunk0.release();
    var pred_chunk1 = try makeBoolArray(allocator, &[_]?bool{false});
    defer pred_chunk1.release();
    var pred_chunked = try ChunkedArray.init(allocator, .{ .bool = {} }, &[_]ArrayRef{ pred_chunk0, pred_chunk1 });
    defer pred_chunked.release();

    var list_datum = Datum.fromArray(list_array.retain());
    defer list_datum.release();
    var pred_chunked_datum = Datum.fromChunked(pred_chunked.retain());
    defer pred_chunked_datum.release();

    var filtered_drop_nulls = try datumFilter(list_datum, pred_chunked_datum, .{ .drop_nulls = true });
    defer filtered_drop_nulls.release();
    try std.testing.expectEqual(@as(usize, 1), filtered_drop_nulls.array.data().length);

    var filtered_keep_nulls = try datumFilter(list_datum, pred_chunked_datum, .{ .drop_nulls = false });
    defer filtered_keep_nulls.release();
    try std.testing.expectEqual(@as(usize, 2), filtered_keep_nulls.array.data().length);
    try filtered_keep_nulls.array.data().validateLayout();
    const filtered_keep_view = array_mod.ListArray{ .data = filtered_keep_nulls.array.data() };
    try std.testing.expect(!filtered_keep_view.isNull(0));
    try std.testing.expect(filtered_keep_view.isNull(1));

    var fsl_values = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 20, 21, 30, 31 });
    defer fsl_values.release();
    var fsl_builder = try array_mod.FixedSizeListBuilder.init(allocator, list_field, 2);
    defer fsl_builder.deinit();
    try fsl_builder.appendValid();
    try fsl_builder.appendNull();
    try fsl_builder.appendValid();
    var fsl_array = try fsl_builder.finish(fsl_values);
    defer fsl_array.release();

    var fsl_pred = try makeBoolArray(allocator, &[_]?bool{ false, true, true });
    defer fsl_pred.release();
    var fsl_datum = Datum.fromArray(fsl_array.retain());
    defer fsl_datum.release();
    var fsl_pred_datum = Datum.fromArray(fsl_pred.retain());
    defer fsl_pred_datum.release();

    var filtered_fsl = try datumFilter(fsl_datum, fsl_pred_datum, .{ .drop_nulls = true });
    defer filtered_fsl.release();
    try std.testing.expect(filtered_fsl.isArray());
    try std.testing.expectEqual(@as(usize, 2), filtered_fsl.array.data().length);
    try filtered_fsl.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = filtered_fsl.array.data() };
    try std.testing.expect(fsl_view.isNull(0));
    try std.testing.expectEqual(@as(usize, 4), fsl_view.valuesRef().data().length);
}

test "compute datumSelect supports mixed candidate datum forms" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;

    var a = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30 });
    defer a.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{100});
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 200, 300 });
    defer c1.release();
    var chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunked.release();

    const values = [_]Datum{
        Datum.fromArray(a.retain()),
        Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 99 })),
        Datum.fromChunked(chunked.retain()),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelect(&[_]usize{ 0, 1, 2 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(i32, 10), view.value(0));
    try std.testing.expectEqual(@as(i32, 99), view.value(1));
    try std.testing.expectEqual(@as(i32, 300), view.value(2));
}

test "compute datumSelectNullable and datumBuildNullLikeWithAllocator support null output rows" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;
    const int32_type = DataType{ .int32 = {} };

    var base = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30 });
    defer base.release();

    const values = [_]Datum{
        Datum.fromArray(base.retain()),
        Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 88 })),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelectNullable(&[_]?usize{ 0, null, 1 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    try out.array.data().validateLayout();
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(i32, 10), view.value(0));
    try std.testing.expect(view.isNull(1));
    try std.testing.expectEqual(@as(i32, 88), view.value(2));

    var nulls = try datumBuildNullLikeWithAllocator(allocator, .{ .int32 = {} }, 3);
    defer nulls.release();
    try std.testing.expect(nulls.isArray());
    try std.testing.expectEqual(@as(usize, 3), nulls.array.data().length);
    const null_view = int32_array{ .data = nulls.array.data() };
    try std.testing.expect(null_view.isNull(0));
    try std.testing.expect(null_view.isNull(1));
    try std.testing.expect(null_view.isNull(2));

    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const fixed_size_list_type = DataType{
        .fixed_size_list = .{
            .list_size = 2,
            .value_field = item_field,
        },
    };
    var fsl_nulls = try datumBuildNullLikeWithAllocator(allocator, fixed_size_list_type, 3);
    defer fsl_nulls.release();
    try std.testing.expect(fsl_nulls.isArray());
    try std.testing.expectEqual(@as(usize, 3), fsl_nulls.array.data().length);
    try fsl_nulls.array.data().validateLayout();
    const fsl_view = array_mod.FixedSizeListArray{ .data = fsl_nulls.array.data() };
    try std.testing.expect(fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expect(fsl_view.isNull(2));
    try std.testing.expectEqual(@as(usize, 6), fsl_view.valuesRef().data().length);
}

test "compute datumSelectNullable emits fixed_size_list null rows without child mismatch" {
    const allocator = std.testing.allocator;
    const int32_type = DataType{ .int32 = {} };
    const int32_array = array_mod.Int32Array;
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };

    var values0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5, 6 });
    defer values0.release();
    var builder0 = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder0.deinit();
    try builder0.appendValid();
    try builder0.appendValid();
    try builder0.appendValid();
    var fsl0 = try builder0.finish(values0);
    defer fsl0.release();

    var values1 = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 20, 21, 30, 31 });
    defer values1.release();
    var builder1 = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder1.deinit();
    try builder1.appendValid();
    try builder1.appendValid();
    try builder1.appendValid();
    var fsl1 = try builder1.finish(values1);
    defer fsl1.release();

    const values = [_]Datum{
        Datum.fromArray(fsl0.retain()),
        Datum.fromArray(fsl1.retain()),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelectNullable(&[_]?usize{ 0, null, 1 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    try out.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = out.array.data() };
    try std.testing.expect(!fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expect(!fsl_view.isNull(2));
    try std.testing.expectEqual(@as(usize, 6), fsl_view.valuesRef().data().length);

    const child = int32_array{ .data = fsl_view.valuesRef().data() };
    try std.testing.expectEqual(@as(i32, 1), child.value(0));
    try std.testing.expectEqual(@as(i32, 2), child.value(1));
    try std.testing.expect(child.isNull(2));
    try std.testing.expect(child.isNull(3));
    try std.testing.expectEqual(@as(i32, 30), child.value(4));
    try std.testing.expectEqual(@as(i32, 31), child.value(5));
}

test "compute datumFilter emits fixed_size_list null rows when predicate nulls are kept" {
    const allocator = std.testing.allocator;
    const int32_type = DataType{ .int32 = {} };
    const int32_array = array_mod.Int32Array;
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5, 6 });
    defer values.release();
    var builder = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();
    try builder.appendValid();
    var fsl = try builder.finish(values);
    defer fsl.release();

    var pred = try makeBoolArray(allocator, &[_]?bool{ true, null, false });
    defer pred.release();

    var in_datum = Datum.fromArray(fsl.retain());
    defer in_datum.release();
    var pred_datum = Datum.fromArray(pred.retain());
    defer pred_datum.release();

    var out = try datumFilter(in_datum, pred_datum, .{ .drop_nulls = false });
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 2), out.array.data().length);
    try out.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = out.array.data() };
    try std.testing.expect(!fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expectEqual(@as(usize, 4), fsl_view.valuesRef().data().length);

    const child = int32_array{ .data = fsl_view.valuesRef().data() };
    try std.testing.expectEqual(@as(i32, 1), child.value(0));
    try std.testing.expectEqual(@as(i32, 2), child.value(1));
    try std.testing.expect(child.isNull(2));
    try std.testing.expect(child.isNull(3));
}
