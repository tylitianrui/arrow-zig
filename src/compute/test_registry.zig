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

test "compute registry registers and invokes scalar kernel" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
        },
        .exec = passthroughInt32Kernel,
    });

    const int32_builder = @import("../array/array.zig").Int32Builder;
    const int32_array = @import("../array/array.zig").Int32Array;

    var builder = try int32_builder.init(allocator, 3);
    defer builder.deinit();
    try builder.append(7);
    try builder.append(8);
    try builder.append(9);

    var arr = try builder.finish();
    defer arr.release();
    const args = [_]Datum{
        .{ .array = arr.retain() },
    };
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var out = try ctx.invokeScalar("identity", args[0..], Options.noneValue());
    defer out.release();

    try std.testing.expect(out == .array);
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(usize, 3), view.len());
    try std.testing.expectEqual(@as(i32, 7), view.value(0));
    try std.testing.expectEqual(@as(i32, 9), view.value(2));
}

test "compute registry reports function and arity errors" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
        },
        .exec = passthroughInt32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.FunctionNotFound,
        ctx.invoke("missing", .scalar, &[_]Datum{}, Options.noneValue()),
    );
    try std.testing.expectError(
        error.InvalidArity,
        ctx.invoke("identity", .scalar, &[_]Datum{}, Options.noneValue()),
    );
}

test "compute registry keeps separate indices per function kind" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("same_name", .{
        .signature = .{ .arity = 1, .type_check = isInt32Datum },
        .exec = passthroughInt32Kernel,
    });
    try registry.registerVectorKernel("same_name", .{
        .signature = .{ .arity = 1, .type_check = isInt32Datum },
        .exec = passthroughInt32Kernel,
    });

    try std.testing.expect(registry.findFunction("same_name", .scalar) != null);
    try std.testing.expect(registry.findFunction("same_name", .vector) != null);
    try std.testing.expect(registry.findFunction("same_name", .aggregate) == null);
}

test "compute registry helper APIs and function metadata" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try registry.registerVectorKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });

    try std.testing.expect(registry.containsFunction("identity", .scalar));
    try std.testing.expect(registry.containsFunction("identity", .vector));
    try std.testing.expect(!registry.containsFunction("identity", .aggregate));
    try std.testing.expectEqual(@as(usize, 2), registry.functionCount());
    try std.testing.expectEqual(@as(usize, 1), registry.kernelCount("identity", .scalar));
    try std.testing.expectEqual(@as(usize, 1), registry.kernelCount("identity", .vector));
    try std.testing.expectEqual(@as(usize, 0), registry.kernelCount("identity", .aggregate));

    const f0 = registry.functionAt(0).?;
    try std.testing.expect(f0.kernelCount() > 0);
    try std.testing.expect(f0.kernelsSlice().len > 0);
}

test "compute invoke helpers cover vector and aggregate kernels" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    const int32_array = @import("../array/array.zig").Int32Array;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerVectorKernel("vec_identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try registry.registerAggregateKernel("count_len", .{
        .signature = KernelSignature.unary(unaryArray),
        .exec = countLenAggregateKernel,
    });

    var builder = try int32_builder.init(allocator, 4);
    defer builder.deinit();
    try builder.append(10);
    try builder.append(20);
    try builder.append(30);
    try builder.append(40);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var vec_out = try ctx.invokeVector("vec_identity", args[0..], Options.noneValue());
    defer vec_out.release();
    try std.testing.expect(vec_out.isArray());
    const vec_view = int32_array{ .data = vec_out.array.data() };
    try std.testing.expectEqual(@as(usize, 4), vec_view.len());
    try std.testing.expectEqual(@as(i32, 10), vec_view.value(0));
    try std.testing.expectEqual(@as(i32, 40), vec_view.value(3));

    var agg_out = try ctx.invokeAggregate("count_len", args[0..], Options.noneValue());
    defer agg_out.release();
    try std.testing.expect(agg_out.isScalar());
    try std.testing.expectEqual(@as(i64, 4), agg_out.scalar.value.i64);
}

test "compute registry functionAt out of range returns null" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try std.testing.expect(registry.functionAt(0) != null);
    try std.testing.expect(registry.functionAt(1) == null);
    try std.testing.expect(registry.functionAt(99) == null);
}

test "compute resolveKernel prefers exact then range then at_least arity" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .type_check = allInt32Datums,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .max_arity = 4,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });
    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 3,
            .type_check = allInt32Datums,
        },
        .exec = exactArityMarkerKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const scalar_i32 = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 1 },
    });

    const args3 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32 };
    var out3 = try ctx.invokeScalar("pick_by_arity", args3[0..], Options.noneValue());
    defer out3.release();
    try std.testing.expect(out3.isScalar());
    try std.testing.expectEqual(@as(i32, 303), out3.scalar.value.i32);

    const args4 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    var out4 = try ctx.invokeScalar("pick_by_arity", args4[0..], Options.noneValue());
    defer out4.release();
    try std.testing.expect(out4.isScalar());
    try std.testing.expectEqual(@as(i32, 202), out4.scalar.value.i32);

    const args5 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    var out5 = try ctx.invokeScalar("pick_by_arity", args5[0..], Options.noneValue());
    defer out5.release();
    try std.testing.expect(out5.isScalar());
    try std.testing.expectEqual(@as(i32, 101), out5.scalar.value.i32);
}

test "compute explainResolveKernelFailure prefers minimum arity diagnostic in mixed models" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("mixed_arity", .{
        .signature = .{
            .arity = 3,
            .variadic = true,
            .type_check = allInt32Datums,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("mixed_arity", .{
        .signature = .{
            .arity = 5,
            .variadic = true,
            .max_arity = 7,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });

    const two_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("mixed_arity", .scalar, two_i32[0..], Options.noneValue()),
    );
    const reason = registry.explainResolveKernelFailure("mixed_arity", .scalar, two_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, reason, "no kernel matched minimum arity"));
}

test "compute registerKernel rejects invalid arity model combinations" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try std.testing.expectError(
        error.InvalidInput,
        registry.registerScalarKernel("invalid_sig_non_variadic_max", .{
            .signature = .{
                .arity = 2,
                .variadic = false,
                .max_arity = 3,
                .type_check = allInt32Datums,
            },
            .exec = exactArityMarkerKernel,
        }),
    );

    try std.testing.expectError(
        error.InvalidInput,
        registry.registerScalarKernel("invalid_sig_reversed_range", .{
            .signature = .{
                .arity = 4,
                .variadic = true,
                .max_arity = 3,
                .type_check = allInt32Datums,
            },
            .exec = rangeArityMarkerKernel,
        }),
    );
}

test "compute aggregate lifecycle session supports init/update/merge/finalize" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerAggregateKernel("count_stateful", .{
        .signature = .{
            .arity = 1,
            .type_check = unaryArray,
            .result_type_fn = countAggregateResultType,
        },
        .exec = countLenAggregateKernel,
        .aggregate_lifecycle = .{
            .init = countLifecycleInit,
            .update = countLifecycleUpdate,
            .merge = countLifecycleMerge,
            .finalize = countLifecycleFinalize,
            .deinit = countLifecycleDeinit,
        },
    });

    var b1 = try int32_builder.init(allocator, 3);
    defer b1.deinit();
    try b1.append(1);
    try b1.append(2);
    try b1.append(3);
    var a1 = try b1.finish();
    defer a1.release();

    var b2 = try int32_builder.init(allocator, 2);
    defer b2.deinit();
    try b2.append(10);
    try b2.append(20);
    var a2 = try b2.finish();
    defer a2.release();

    const args1 = [_]Datum{Datum.fromArray(a1.retain())};
    defer {
        var d = args1[0];
        d.release();
    }
    const args2 = [_]Datum{Datum.fromArray(a2.retain())};
    defer {
        var d = args2[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var s1 = try ctx.beginAggregate("count_stateful", args1[0..], Options.noneValue());
    defer s1.deinit();
    var s2 = try ctx.beginAggregate("count_stateful", args2[0..], Options.noneValue());
    defer s2.deinit();

    try s1.update(args1[0..]);
    try s2.update(args2[0..]);
    try s1.merge(&s2);

    var out = try s1.finalize();
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i64, 5), out.scalar.value.i64);
}

test "compute beginAggregate returns MissingLifecycle when not provided" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerAggregateKernel("count_stateless_only", .{
        .signature = KernelSignature.unary(unaryArray),
        .exec = countLenAggregateKernel,
    });

    var b = try int32_builder.init(allocator, 1);
    defer b.deinit();
    try b.append(1);
    var arr = try b.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.MissingLifecycle,
        ctx.beginAggregate("count_stateless_only", args[0..], Options.noneValue()),
    );
}
