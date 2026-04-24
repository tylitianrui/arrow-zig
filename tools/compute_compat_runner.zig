const std = @import("std");
const zcore = @import("zarrow-core");

const compute = zcore.compute;

const DatumKind = enum {
    array,
    chunked,
    scalar,
};

const Operation = enum {
    add_i64,
    divide_i64,
    cast_i64_to_i32,
};

const DatumInput = struct {
    kind: DatumKind,
    values: ?[]const ?i64 = null,
    chunks: ?[]const usize = null,
    scalar: ?i64 = null,
    scalar_is_null: bool = false,
};

const ArithmeticOptionsInput = struct {
    check_overflow: bool = true,
    divide_by_zero_is_error: bool = true,
};

const CastOptionsInput = struct {
    safe: bool = true,
};

const CaseInput = struct {
    name: []const u8,
    operation: Operation,
    lhs: DatumInput,
    rhs: ?DatumInput = null,
    arithmetic_options: ?ArithmeticOptionsInput = null,
    cast_options: ?CastOptionsInput = null,
};

const Input = struct {
    cases: []const CaseInput,
};

const CaseStatus = enum {
    ok,
    @"error",
};

const CaseOutput = struct {
    name: []const u8,
    status: CaseStatus,
    values: ?[]?i64 = null,
    @"error": ?[]const u8 = null,
};

const Output = struct {
    results: []const CaseOutput,
};

fn onlyArithmeticOptions(options: compute.Options) bool {
    return switch (options) {
        .arithmetic => true,
        else => false,
    };
}

fn onlyCastOptions(options: compute.Options) bool {
    return switch (options) {
        .cast => true,
        else => false,
    };
}

fn datumLen(datum: compute.Datum) usize {
    return switch (datum) {
        .array => |arr| arr.data().length,
        .chunked => |chunks| chunks.len(),
        .scalar => 1,
    };
}

fn datumIsInt64(datum: compute.Datum) bool {
    return datum.dataType().eql(.{ .int64 = {} });
}

fn unaryInt64(args: []const compute.Datum) bool {
    return args.len == 1 and datumIsInt64(args[0]);
}

fn binaryInt64(args: []const compute.Datum) bool {
    return args.len == 2 and datumIsInt64(args[0]) and datumIsInt64(args[1]);
}

fn resultI64(args: []const compute.Datum, options: compute.Options) compute.KernelError!compute.DataType {
    _ = args;
    _ = options;
    return .{ .int64 = {} };
}

fn resultI32(args: []const compute.Datum, options: compute.Options) compute.KernelError!compute.DataType {
    _ = args;
    return switch (options) {
        .cast => .{ .int32 = {} },
        else => error.InvalidOptions,
    };
}

fn readI64(value: compute.ExecChunkValue, logical_index: usize) compute.KernelError!i64 {
    return switch (value) {
        .scalar => |s| switch (s.value) {
            .i64 => |v| v,
            else => error.InvalidInput,
        },
        .array => |arr| blk: {
            const dt = arr.data().data_type;
            if (dt.eql(.{ .int64 = {} })) {
                const view = zcore.Int64Array{ .data = arr.data() };
                break :blk view.value(logical_index) catch return error.InvalidInput;
            }
            if (dt.eql(.{ .int32 = {} })) {
                const view = zcore.Int32Array{ .data = arr.data() };
                const v = view.value(logical_index) catch return error.InvalidInput;
                break :blk @as(i64, v);
            }
            break :blk error.UnsupportedType;
        },
    };
}

fn addI64Kernel(ctx: *compute.ExecContext, args: []const compute.Datum, options: compute.Options) compute.KernelError!compute.Datum {
    if (args.len != 2) return error.InvalidArity;
    const arithmetic_opts = switch (options) {
        .arithmetic => |o| o,
        else => return error.InvalidOptions,
    };

    const out_len = try compute.inferBinaryExecLen(args[0], args[1]);
    var builder = try zcore.Int64Builder.init(ctx.tempAllocator(), out_len);
    defer builder.deinit();

    var iter = try compute.BinaryExecChunkIterator.init(args[0], args[1]);
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            if (chunk.binaryNullAt(i)) {
                builder.appendNull() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidInput,
                };
                continue;
            }
            const lhs = try readI64(chunk.lhs, i);
            const rhs = try readI64(chunk.rhs, i);
            const sum = if (arithmetic_opts.check_overflow)
                std.math.add(i64, lhs, rhs) catch return error.Overflow
            else
                lhs +% rhs;
            builder.append(sum) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidInput,
            };
        }
    }

    const out = builder.finish() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidInput,
    };
    return compute.Datum.fromArray(out);
}

fn divideI64Kernel(ctx: *compute.ExecContext, args: []const compute.Datum, options: compute.Options) compute.KernelError!compute.Datum {
    if (args.len != 2) return error.InvalidArity;
    const arithmetic_opts = switch (options) {
        .arithmetic => |o| o,
        else => return error.InvalidOptions,
    };

    const out_len = try compute.inferBinaryExecLen(args[0], args[1]);
    var builder = try zcore.Int64Builder.init(ctx.tempAllocator(), out_len);
    defer builder.deinit();

    var iter = try compute.BinaryExecChunkIterator.init(args[0], args[1]);
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            if (chunk.binaryNullAt(i)) {
                builder.appendNull() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidInput,
                };
                continue;
            }
            const lhs = try readI64(chunk.lhs, i);
            const rhs = try readI64(chunk.rhs, i);
            const out = try compute.arithmeticDivI64(lhs, rhs, arithmetic_opts);
            builder.append(out) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidInput,
            };
        }
    }

    const out = builder.finish() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidInput,
    };
    return compute.Datum.fromArray(out);
}

fn castI64ToI32Kernel(ctx: *compute.ExecContext, args: []const compute.Datum, options: compute.Options) compute.KernelError!compute.Datum {
    if (args.len != 1) return error.InvalidArity;
    const cast_opts = switch (options) {
        .cast => |o| o,
        else => return error.InvalidOptions,
    };
    if (cast_opts.to_type) |to_type| {
        if (!to_type.eql(.{ .int32 = {} })) return error.InvalidCast;
    }

    const out_len = datumLen(args[0]);
    var builder = try zcore.Int32Builder.init(ctx.tempAllocator(), out_len);
    defer builder.deinit();

    var iter = compute.UnaryExecChunkIterator.init(args[0]);
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            if (chunk.unaryNullAt(i)) {
                builder.appendNull() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidInput,
                };
                continue;
            }
            const value_i64 = try readI64(chunk.values, i);
            const casted: i32 = if (cast_opts.safe)
                try compute.intCastOrInvalidCast(i32, value_i64)
            else
                @truncate(value_i64);
            builder.append(casted) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidInput,
            };
        }
    }

    const out = builder.finish() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidInput,
    };
    return compute.Datum.fromArray(out);
}

fn makeInt64Array(allocator: std.mem.Allocator, values: []const ?i64) !zcore.ArrayRef {
    var builder = try zcore.Int64Builder.init(allocator, values.len);
    defer builder.deinit();
    for (values) |v| {
        if (v) |x| {
            try builder.append(x);
        } else {
            try builder.appendNull();
        }
    }
    return builder.finish();
}

fn buildDatum(allocator: std.mem.Allocator, input: DatumInput) compute.KernelError!compute.Datum {
    return switch (input.kind) {
        .array => blk: {
            const values = input.values orelse return error.InvalidInput;
            const arr = makeInt64Array(allocator, values) catch return error.OutOfMemory;
            break :blk compute.Datum.fromArray(arr);
        },
        .chunked => blk: {
            const values = input.values orelse return error.InvalidInput;
            const chunk_sizes = input.chunks orelse return error.InvalidInput;
            var refs: std.ArrayListUnmanaged(zcore.ArrayRef) = .{};
            defer refs.deinit(allocator);
            errdefer {
                for (refs.items) |*chunk| {
                    chunk.release();
                }
            }

            var consumed: usize = 0;
            for (chunk_sizes) |chunk_size| {
                const end = std.math.add(usize, consumed, chunk_size) catch return error.InvalidInput;
                if (end > values.len) return error.InvalidInput;
                const chunk = makeInt64Array(allocator, values[consumed..end]) catch return error.OutOfMemory;
                refs.append(allocator, chunk) catch return error.OutOfMemory;
                consumed = end;
            }
            if (consumed != values.len) return error.InvalidInput;

            const chunked = compute.ChunkedArray.init(allocator, .{ .int64 = {} }, refs.items) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidInput,
            };
            for (refs.items) |*chunk| {
                chunk.release();
            }
            break :blk compute.Datum.fromChunked(chunked);
        },
        .scalar => blk: {
            if (input.scalar_is_null) {
                break :blk compute.Datum.fromScalar(.{
                    .data_type = .{ .int64 = {} },
                    .value = .null,
                });
            }
            const scalar_value = input.scalar orelse return error.InvalidInput;
            break :blk compute.Datum.fromScalar(.{
                .data_type = .{ .int64 = {} },
                .value = .{ .i64 = scalar_value },
            });
        },
    };
}

fn datumToMaybeI64List(allocator: std.mem.Allocator, datum: compute.Datum) compute.KernelError![]?i64 {
    return switch (datum) {
        .scalar => |s| blk: {
            var out = allocator.alloc(?i64, 1) catch return error.OutOfMemory;
            out[0] = switch (s.value) {
                .null => null,
                .i64 => |v| v,
                .i32 => |v| @as(i64, v),
                else => return error.UnsupportedType,
            };
            break :blk out;
        },
        .array => |arr| blk: {
            var out = allocator.alloc(?i64, arr.data().length) catch return error.OutOfMemory;
            const dt = arr.data().data_type;
            if (dt.eql(.{ .int64 = {} })) {
                const view = zcore.Int64Array{ .data = arr.data() };
                var i: usize = 0;
                while (i < view.len()) : (i += 1) {
                    out[i] = if (view.isNull(i))
                        null
                    else
                        (view.value(i) catch return error.InvalidInput);
                }
                break :blk out;
            }
            if (dt.eql(.{ .int32 = {} })) {
                const view = zcore.Int32Array{ .data = arr.data() };
                var i: usize = 0;
                while (i < view.len()) : (i += 1) {
                    out[i] = if (view.isNull(i))
                        null
                    else
                        @as(i64, view.value(i) catch return error.InvalidInput);
                }
                break :blk out;
            }
            return error.UnsupportedType;
        },
        .chunked => |chunks| blk: {
            var out = allocator.alloc(?i64, chunks.len()) catch return error.OutOfMemory;
            var cursor: usize = 0;
            var chunk_index: usize = 0;
            while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
                const chunk = chunks.chunk(chunk_index).*;
                const dt = chunk.data().data_type;
                if (dt.eql(.{ .int64 = {} })) {
                    const view = zcore.Int64Array{ .data = chunk.data() };
                    var i: usize = 0;
                    while (i < view.len()) : (i += 1) {
                        out[cursor] = if (view.isNull(i))
                            null
                        else
                            (view.value(i) catch return error.InvalidInput);
                        cursor += 1;
                    }
                    continue;
                }
                if (dt.eql(.{ .int32 = {} })) {
                    const view = zcore.Int32Array{ .data = chunk.data() };
                    var i: usize = 0;
                    while (i < view.len()) : (i += 1) {
                        out[cursor] = if (view.isNull(i))
                            null
                        else
                            @as(i64, view.value(i) catch return error.InvalidInput);
                        cursor += 1;
                    }
                    continue;
                }
                return error.UnsupportedType;
            }
            break :blk out;
        },
    };
}

fn registerCompatKernels(registry: *compute.FunctionRegistry) compute.KernelError!void {
    try registry.registerVectorKernel("add_i64", .{
        .signature = .{
            .arity = 2,
            .type_check = binaryInt64,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = resultI64,
        },
        .exec = addI64Kernel,
    });
    try registry.registerVectorKernel("divide_i64", .{
        .signature = .{
            .arity = 2,
            .type_check = binaryInt64,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = resultI64,
        },
        .exec = divideI64Kernel,
    });
    try registry.registerVectorKernel("cast_i64_to_i32", .{
        .signature = .{
            .arity = 1,
            .type_check = unaryInt64,
            .options_check = onlyCastOptions,
            .result_type_fn = resultI32,
        },
        .exec = castI64ToI32Kernel,
    });
}

fn runCase(
    allocator: std.mem.Allocator,
    ctx: *compute.ExecContext,
    case: CaseInput,
) compute.KernelError![]?i64 {
    var lhs = try buildDatum(allocator, case.lhs);
    defer lhs.release();

    switch (case.operation) {
        .cast_i64_to_i32 => {
            const args = [_]compute.Datum{lhs};
            const cast_opts = case.cast_options orelse CastOptionsInput{};
            var out = try ctx.invokeVector("cast_i64_to_i32", args[0..], .{
                .cast = .{
                    .safe = cast_opts.safe,
                    .to_type = .{ .int32 = {} },
                },
            });
            defer out.release();
            return datumToMaybeI64List(allocator, out);
        },
        .add_i64, .divide_i64 => {
            const rhs_input = case.rhs orelse return error.InvalidInput;
            var rhs = try buildDatum(allocator, rhs_input);
            defer rhs.release();
            const args = [_]compute.Datum{ lhs, rhs };
            const arithmetic_opts = case.arithmetic_options orelse ArithmeticOptionsInput{};
            const kernel_name: []const u8 = switch (case.operation) {
                .add_i64 => "add_i64",
                .divide_i64 => "divide_i64",
                else => unreachable,
            };
            var out = try ctx.invokeVector(kernel_name, args[0..], .{
                .arithmetic = .{
                    .check_overflow = arithmetic_opts.check_overflow,
                    .divide_by_zero_is_error = arithmetic_opts.divide_by_zero_is_error,
                },
            });
            defer out.release();
            return datumToMaybeI64List(allocator, out);
        },
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = std.process.args();
    _ = args.next();
    const input_path = args.next() orelse {
        std.debug.print("usage: compute-compat-runner <input.json>\n", .{});
        return error.InvalidInput;
    };
    if (args.next() != null) {
        std.debug.print("usage: compute-compat-runner <input.json>\n", .{});
        return error.InvalidInput;
    }

    const bytes = try std.fs.cwd().readFileAlloc(allocator, input_path, 1024 * 1024);
    defer allocator.free(bytes);

    const parsed = try std.json.parseFromSlice(Input, allocator, bytes, .{});
    defer parsed.deinit();

    var registry = compute.FunctionRegistry.init(allocator);
    defer registry.deinit();
    try registerCompatKernels(&registry);
    var ctx = compute.ExecContext.init(allocator, &registry);

    var results: std.ArrayListUnmanaged(CaseOutput) = .{};
    defer {
        for (results.items) |item| {
            if (item.values) |vals| allocator.free(vals);
        }
        results.deinit(allocator);
    }

    for (parsed.value.cases) |case| {
        const values_or_err = runCase(allocator, &ctx, case);
        if (values_or_err) |values| {
            results.append(allocator, .{
                .name = case.name,
                .status = .ok,
                .values = values,
                .@"error" = null,
            }) catch return error.OutOfMemory;
        } else |err| {
            results.append(allocator, .{
                .name = case.name,
                .status = .@"error",
                .values = null,
                .@"error" = @errorName(err),
            }) catch return error.OutOfMemory;
        }
    }

    const stdout_file = std.io.getStdOut();
    const out_writer = stdout_file.writer();
    try std.json.stringify(Output{ .results = results.items }, .{}, out_writer);
    try stdout_file.writeAll("\n");
}
