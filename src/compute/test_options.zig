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

test "compute kernel resolution keeps InvalidArity InvalidOptions NoMatchingKernel precedence" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("priority", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .type_check = allInt32Datums,
            .options_check = onlyCastOptions,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("priority_range", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .max_arity = 3,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });

    const one_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("priority", .scalar, one_i32[0..], Options.noneValue()),
    );
    const min_reason = registry.explainResolveKernelFailure("priority", .scalar, one_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, min_reason, "no kernel matched minimum arity"));

    const two_i64 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 2 } }),
    };
    try std.testing.expectError(
        error.NoMatchingKernel,
        registry.resolveKernel("priority", .scalar, two_i64[0..], Options.noneValue()),
    );

    const two_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
    };
    try std.testing.expectError(
        error.InvalidOptions,
        registry.resolveKernel("priority", .scalar, two_i32[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const four_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 3 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 4 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("priority_range", .scalar, four_i32[0..], Options.noneValue()),
    );
    const range_reason = registry.explainResolveKernelFailure("priority_range", .scalar, four_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, range_reason, "no kernel matched arity range"));
}

test "compute kernel signature result type inference and typed options" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlyCastOptions,
            .result_type_fn = castResultType,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(5);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    const cast_to_i64 = Options{
        .cast = .{
            .safe = true,
            .to_type = .{ .int64 = {} },
        },
    };
    const inferred_i64 = try registry.resolveResultType("cast_identity", .scalar, args[0..], cast_to_i64);
    try std.testing.expect(inferred_i64.eql(.{ .int64 = {} }));

    var ctx = ExecContext.init(allocator, &registry);
    var out = try ctx.invokeScalar("cast_identity", args[0..], cast_to_i64);
    defer out.release();
    try std.testing.expect(out.isArray());

    try std.testing.expectError(
        error.InvalidOptions,
        ctx.invokeScalar("cast_identity", args[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const sig = KernelSignature.unaryWithResult(isInt32Datum, firstArgResultType);
    const inferred_i32 = try sig.inferResultType(args[0..], Options.noneValue());
    try std.testing.expect(inferred_i32.eql(.{ .int32 = {} }));
}

test "compute explain helpers provide readable diagnostics" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlyCastOptions,
            .result_type_fn = castResultType,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(123);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    const function = registry.findFunction("cast_identity", .scalar).?;
    const signature = function.kernelsSlice()[0].signature;

    const bad_options = Options{ .filter = .{ .drop_nulls = true } };
    const mismatch = signature.explainMismatch(args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, mismatch, "options mismatch: options did not satisfy kernel options_check"));

    const infer_reason = signature.explainInferResultTypeFailure(args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, infer_reason, "cannot infer result type: options_check failed"));

    const resolve_reason = registry.explainResolveKernelFailure("cast_identity", .scalar, args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, resolve_reason, "kernel matched args but options were invalid"));

    const result_type_reason = registry.explainResolveResultTypeFailure("cast_identity", .scalar, args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, result_type_reason, "kernel matched args but options were invalid"));
}

test "compute sort options defaults and tag dispatch" {
    const defaults = SortOptions{};
    try std.testing.expectEqual(SortOrder.ascending, defaults.order);
    try std.testing.expectEqual(SortNullPlacement.at_end, defaults.null_placement);
    try std.testing.expect(defaults.nan_placement == null);
    try std.testing.expect(!defaults.stable);

    const sort_options = Options{ .sort = .{} };
    try std.testing.expectEqual(OptionsTag.sort, sort_options.tag());
    try std.testing.expect(switch (sort_options) {
        .sort => |opts| opts.order == .ascending and opts.null_placement == .at_end and opts.nan_placement == null and !opts.stable,
        else => false,
    });
}

test "compute kernel signature options_check validates sort options" {
    const sig = KernelSignature{
        .arity = 1,
        .type_check = isInt32Datum,
        .options_check = onlySortOptions,
    };

    try std.testing.expect(sig.matchesOptions(.{ .sort = .{} }));
    try std.testing.expect(!sig.matchesOptions(.{ .cast = .{} }));
}

test "compute sort options mismatch path reports InvalidOptions" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;

    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("sort_options_gate", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlySortOptions,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(42);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.InvalidOptions,
        ctx.invokeScalar("sort_options_gate", args[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const reason = registry.explainResolveKernelFailure("sort_options_gate", .scalar, args[0..], .{ .filter = .{ .drop_nulls = true } });
    try std.testing.expect(std.mem.eql(u8, reason, "kernel matched args but options were invalid"));
}

test "compute KernelError includes strategy-aware variants" {
    const overflow: KernelError = error.Overflow;
    const divide_by_zero: KernelError = error.DivideByZero;
    const invalid_cast: KernelError = error.InvalidCast;
    const not_implemented: KernelError = error.NotImplemented;

    try std.testing.expect(overflow == error.Overflow);
    try std.testing.expect(divide_by_zero == error.DivideByZero);
    try std.testing.expect(invalid_cast == error.InvalidCast);
    try std.testing.expect(not_implemented == error.NotImplemented);
}

test "compute arithmetic kernel maps divide-by-zero to DivideByZero" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("div_i64", .{
        .signature = .{
            .arity = 2,
            .type_check = isTwoInt64Scalars,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = divI64ScalarResultType,
        },
        .exec = divI64ScalarKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const args = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 42 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 0 } }),
    };

    try std.testing.expectError(
        error.DivideByZero,
        ctx.invokeScalar("div_i64", args[0..], .{ .arithmetic = .{} }),
    );
}

test "compute arithmetic kernel non-error divide-by-zero mode stays non-failing" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("div_i64_relaxed", .{
        .signature = .{
            .arity = 2,
            .type_check = isTwoInt64Scalars,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = divI64ScalarResultType,
        },
        .exec = divI64ScalarKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const args = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 42 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 0 } }),
    };

    var out = try ctx.invokeScalar(
        "div_i64_relaxed",
        args[0..],
        .{ .arithmetic = .{ .check_overflow = true, .divide_by_zero_is_error = false } },
    );
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i64, 0), out.scalar.value.i64);
}

test "compute cast kernel maps invalid conversion to InvalidCast" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_i64_to_i32", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt64Scalar,
            .options_check = onlyCastOptions,
            .result_type_fn = castI64ToI32ResultType,
        },
        .exec = castI64ToI32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const too_large = Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = std.math.maxInt(i64) },
    });
    const args = [_]Datum{too_large};

    try std.testing.expectError(
        error.InvalidCast,
        ctx.invokeScalar(
            "cast_i64_to_i32",
            args[0..],
            .{ .cast = .{ .safe = true, .to_type = .{ .int32 = {} } } },
        ),
    );
}

test "compute cast kernel succeeds with valid conversion" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_i64_to_i32_ok", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt64Scalar,
            .options_check = onlyCastOptions,
            .result_type_fn = castI64ToI32ResultType,
        },
        .exec = castI64ToI32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const value = Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = 123 },
    });
    const args = [_]Datum{value};

    var out = try ctx.invokeScalar(
        "cast_i64_to_i32_ok",
        args[0..],
        .{ .cast = .{ .safe = true, .to_type = .{ .int32 = {} } } },
    );
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i32, 123), out.scalar.value.i32);
}
