const std = @import("std");
const datatype = @import("../datatype.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const chunked_array_mod = @import("../chunked_array.zig");

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
    decimal128: i128,
    string: []const u8,
    binary: []const u8,
};

pub const Scalar = struct {
    data_type: DataType,
    value: ScalarValue,

    pub fn init(data_type: DataType, value: ScalarValue) Scalar {
        return .{
            .data_type = data_type,
            .value = value,
        };
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

    pub fn isArray(self: Datum) bool {
        return self == .array;
    }

    pub fn isChunked(self: Datum) bool {
        return self == .chunked;
    }

    pub fn isScalar(self: Datum) bool {
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
            .scalar => |s| .{ .scalar = s },
        };
    }

    pub fn release(self: *Datum) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .chunked => |*chunks| chunks.release(),
            .scalar => {},
        }
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
    UnsupportedType,
    NoMatchingKernel,
};

pub const TypeCheckFn = *const fn (args: []const Datum) bool;
pub const KernelExecFn = *const fn (ctx: *ExecContext, args: []const Datum, options: ?*const anyopaque) KernelError!Datum;

pub const KernelSignature = struct {
    arity: usize,
    type_check: ?TypeCheckFn = null,

    pub fn any(arity: usize) KernelSignature {
        return .{
            .arity = arity,
            .type_check = null,
        };
    }

    pub fn unary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 1,
            .type_check = type_check,
        };
    }

    pub fn binary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 2,
            .type_check = type_check,
        };
    }

    pub fn matches(self: KernelSignature, args: []const Datum) bool {
        if (args.len != self.arity) return false;
        if (self.type_check) |check| return check(args);
        return true;
    }
};

pub const Kernel = struct {
    signature: KernelSignature,
    exec: KernelExecFn,
};

const function_kind_count = @typeInfo(FunctionKind).@"enum".fields.len;
const FunctionIndexMap = std.StringHashMap(usize);

pub const Function = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    kind: FunctionKind,
    kernels: std.ArrayList(Kernel),

    pub fn kernelCount(self: *const Function) usize {
        return self.kernels.items.len;
    }

    pub fn kernelsSlice(self: *const Function) []const Kernel {
        return self.kernels.items;
    }

    fn deinit(self: *Function) void {
        self.kernels.deinit(self.allocator);
        self.allocator.free(self.name);
    }
};

pub const FunctionRegistry = struct {
    allocator: std.mem.Allocator,
    functions: std.ArrayList(Function),
    function_index_by_kind: [function_kind_count]FunctionIndexMap,

    pub fn init(allocator: std.mem.Allocator) FunctionRegistry {
        return .{
            .allocator = allocator,
            .functions = .{},
            .function_index_by_kind = initFunctionIndexMaps(allocator),
        };
    }

    pub fn deinit(self: *FunctionRegistry) void {
        for (&self.function_index_by_kind) |*index_map| {
            index_map.deinit();
        }
        for (self.functions.items) |*entry| {
            entry.deinit();
        }
        self.functions.deinit(self.allocator);
    }

    pub fn registerKernel(
        self: *FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        kernel: Kernel,
    ) KernelError!void {
        if (self.findFunctionIndex(name, kind)) |idx| {
            try self.functions.items[idx].kernels.append(self.allocator, kernel);
            return;
        }

        var entry = Function{
            .allocator = self.allocator,
            .name = try self.allocator.dupe(u8, name),
            .kind = kind,
            .kernels = .{},
        };
        errdefer self.allocator.free(entry.name);
        try entry.kernels.append(self.allocator, kernel);
        try self.functions.append(self.allocator, entry);
        errdefer {
            var popped = self.functions.pop().?;
            popped.deinit();
        }
        const new_idx = self.functions.items.len - 1;
        try self.getIndexMap(kind).put(self.functions.items[new_idx].name, new_idx);
    }

    pub fn registerScalarKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .scalar, kernel);
    }

    pub fn registerVectorKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .vector, kernel);
    }

    pub fn registerAggregateKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .aggregate, kernel);
    }

    pub fn containsFunction(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) bool {
        return self.findFunction(name, kind) != null;
    }

    pub fn functionCount(self: *const FunctionRegistry) usize {
        return self.functions.items.len;
    }

    pub fn functionAt(self: *const FunctionRegistry, index: usize) ?*const Function {
        if (index >= self.functions.items.len) return null;
        return &self.functions.items[index];
    }

    pub fn kernelCount(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) usize {
        const function = self.findFunction(name, kind) orelse return 0;
        return function.kernels.items.len;
    }

    pub fn findFunction(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?*const Function {
        const idx = self.findFunctionIndex(name, kind) orelse return null;
        return &self.functions.items[idx];
    }

    pub fn resolveKernel(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind, args: []const Datum) KernelError!*const Kernel {
        const function = self.findFunction(name, kind) orelse return error.FunctionNotFound;
        if (function.kernels.items.len == 0) return error.NoMatchingKernel;

        var saw_matching_arity = false;
        for (function.kernels.items) |*kernel| {
            if (kernel.signature.arity != args.len) continue;
            saw_matching_arity = true;
            if (kernel.signature.matches(args)) return kernel;
        }
        if (!saw_matching_arity) return error.InvalidArity;
        return error.NoMatchingKernel;
    }

    pub fn invoke(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        const kernel = try self.resolveKernel(name, kind, args);
        return kernel.exec(ctx, args, options);
    }

    pub fn invokeScalar(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .scalar, args, options);
    }

    pub fn invokeVector(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .vector, args, options);
    }

    pub fn invokeAggregate(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .aggregate, args, options);
    }

    fn findFunctionIndex(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?usize {
        const idx = self.getIndexMapConst(kind).get(name) orelse return null;
        if (idx >= self.functions.items.len) return null;
        return idx;
    }

    fn getIndexMap(self: *FunctionRegistry, kind: FunctionKind) *FunctionIndexMap {
        return &self.function_index_by_kind[@intFromEnum(kind)];
    }

    fn getIndexMapConst(self: *const FunctionRegistry, kind: FunctionKind) *const FunctionIndexMap {
        return &self.function_index_by_kind[@intFromEnum(kind)];
    }
};

fn initFunctionIndexMaps(allocator: std.mem.Allocator) [function_kind_count]FunctionIndexMap {
    var maps: [function_kind_count]FunctionIndexMap = undefined;
    inline for (0..function_kind_count) |i| {
        maps[i] = FunctionIndexMap.init(allocator);
    }
    return maps;
}

pub const ExecContext = struct {
    allocator: std.mem.Allocator,
    registry: *const FunctionRegistry,

    pub fn init(allocator: std.mem.Allocator, registry: *const FunctionRegistry) ExecContext {
        return .{
            .allocator = allocator,
            .registry = registry,
        };
    }

    pub fn invoke(
        self: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.registry.invoke(self, name, kind, args, options);
    }

    pub fn invokeScalar(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.registry.invokeScalar(self, name, args, options);
    }

    pub fn invokeVector(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.registry.invokeVector(self, name, args, options);
    }

    pub fn invokeAggregate(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: ?*const anyopaque,
    ) KernelError!Datum {
        return self.registry.invokeAggregate(self, name, args, options);
    }
};

pub fn hasArity(args: []const Datum, expected_arity: usize) bool {
    return args.len == expected_arity;
}

pub fn unaryArray(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isArray();
}

pub fn unaryChunked(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isChunked();
}

pub fn unaryScalar(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isScalar();
}

pub fn sameDataTypes(args: []const Datum) bool {
    if (args.len <= 1) return true;
    const first = args[0].dataType();
    for (args[1..]) |arg| {
        if (!first.eql(arg.dataType())) return false;
    }
    return true;
}

pub fn allNumeric(args: []const Datum) bool {
    for (args) |arg| {
        if (!arg.dataType().isNumeric()) return false;
    }
    return true;
}

pub fn unaryNumeric(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].dataType().isNumeric();
}

pub fn binarySameNumeric(args: []const Datum) bool {
    return hasArity(args, 2) and sameDataTypes(args) and allNumeric(args);
}

fn isInt32Datum(args: []const Datum) bool {
    return args.len == 1 and args[0].dataType() == .int32;
}

fn isTwoInt32(args: []const Datum) bool {
    return args.len == 2 and args[0].dataType() == .int32 and args[1].dataType() == .int32;
}

fn passthroughInt32Kernel(ctx: *ExecContext, args: []const Datum, options: ?*const anyopaque) KernelError!Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

fn countLenAggregateKernel(ctx: *ExecContext, args: []const Datum, options: ?*const anyopaque) KernelError!Datum {
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
    var out = try ctx.invokeScalar("identity", args[0..], null);
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
        ctx.invoke("missing", .scalar, &[_]Datum{}, null),
    );
    try std.testing.expectError(
        error.InvalidArity,
        ctx.invoke("identity", .scalar, &[_]Datum{}, null),
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
    var vec_out = try ctx.invokeVector("vec_identity", args[0..], null);
    defer vec_out.release();
    try std.testing.expect(vec_out.isArray());
    const vec_view = int32_array{ .data = vec_out.array.data() };
    try std.testing.expectEqual(@as(usize, 4), vec_view.len());
    try std.testing.expectEqual(@as(i32, 10), vec_view.value(0));
    try std.testing.expectEqual(@as(i32, 40), vec_view.value(3));

    var agg_out = try ctx.invokeAggregate("count_len", args[0..], null);
    defer agg_out.release();
    try std.testing.expect(agg_out.isScalar());
    try std.testing.expectEqual(@as(i64, 4), agg_out.scalar.value.i64);
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
