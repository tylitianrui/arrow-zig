const std = @import("std");
const datatype = @import("../datatype.zig");
const array_ref_mod = @import("../array/array_ref.zig");

pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref_mod.ArrayRef;

pub const FunctionKind = enum {
    scalar,
    vector,
    aggregate,
};

pub const ScalarValue = union(enum) {
    null,
    bool: bool,
    i64: i64,
    u64: u64,
    f64: f64,
};

pub const Scalar = struct {
    data_type: DataType,
    value: ScalarValue,
};

pub const Datum = union(enum) {
    array: ArrayRef,
    scalar: Scalar,

    pub fn retain(self: Datum) Datum {
        return switch (self) {
            .array => |arr| .{ .array = arr.retain() },
            .scalar => |s| .{ .scalar = s },
        };
    }

    pub fn release(self: *Datum) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .scalar => {},
        }
    }

    pub fn dataType(self: Datum) DataType {
        return switch (self) {
            .array => |arr| arr.data().data_type,
            .scalar => |s| s.data_type,
        };
    }
};

pub const KernelError = error{
    OutOfMemory,
    FunctionNotFound,
    InvalidArity,
    NoMatchingKernel,
};

pub const TypeCheckFn = *const fn (args: []const Datum) bool;
pub const KernelExecFn = *const fn (ctx: *ExecContext, args: []const Datum, options: ?*const anyopaque) KernelError!Datum;

pub const KernelSignature = struct {
    arity: usize,
    type_check: ?TypeCheckFn = null,

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

const RegisteredFunction = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    kind: FunctionKind,
    kernels: std.ArrayList(Kernel),

    fn deinit(self: *RegisteredFunction) void {
        self.kernels.deinit(self.allocator);
        self.allocator.free(self.name);
    }
};

pub const FunctionRegistry = struct {
    allocator: std.mem.Allocator,
    functions: std.ArrayList(RegisteredFunction),

    pub fn init(allocator: std.mem.Allocator) FunctionRegistry {
        return .{
            .allocator = allocator,
            .functions = .{},
        };
    }

    pub fn deinit(self: *FunctionRegistry) void {
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

        var entry = RegisteredFunction{
            .allocator = self.allocator,
            .name = try self.allocator.dupe(u8, name),
            .kind = kind,
            .kernels = .{},
        };
        errdefer self.allocator.free(entry.name);
        try entry.kernels.append(self.allocator, kernel);
        try self.functions.append(self.allocator, entry);
    }

    pub fn findFunction(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?*const RegisteredFunction {
        const idx = self.findFunctionIndex(name, kind) orelse return null;
        return &self.functions.items[idx];
    }

    pub fn resolveKernel(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind, args: []const Datum) KernelError!*const Kernel {
        const function = self.findFunction(name, kind) orelse return error.FunctionNotFound;
        if (function.kernels.items.len == 0) return error.NoMatchingKernel;

        for (function.kernels.items) |*kernel| {
            if (kernel.signature.matches(args)) return kernel;
        }
        if (function.kernels.items.len > 0 and args.len != function.kernels.items[0].signature.arity) return error.InvalidArity;
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

    fn findFunctionIndex(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?usize {
        for (self.functions.items, 0..) |entry, i| {
            if (entry.kind == kind and std.mem.eql(u8, entry.name, name)) return i;
        }
        return null;
    }
};

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
};

fn isInt32Datum(args: []const Datum) bool {
    return args.len == 1 and args[0].dataType() == .int32;
}

fn passthroughInt32Kernel(ctx: *ExecContext, args: []const Datum, options: ?*const anyopaque) KernelError!Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

test "compute registry registers and invokes scalar kernel" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerKernel("identity", .scalar, .{
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
    var out = try ctx.invoke("identity", .scalar, args[0..], null);
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

    try registry.registerKernel("identity", .scalar, .{
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

