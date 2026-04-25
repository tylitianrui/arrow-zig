const std = @import("std");
const datatype = @import("../datatype.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const array_mod = @import("../array/array.zig");
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

pub const TypeCheckFn = *const fn (args: []const Datum) bool;
/// Well-known options payload tags used by compute kernels.
pub const OptionsTag = enum {
    /// Kernel does not require options.
    none,
    /// Type conversion / cast behavior.
    cast,
    /// Arithmetic behavior (overflow, divide-by-zero, etc.).
    arithmetic,
    /// Filter behavior.
    filter,
    /// Escape hatch for downstream custom kernels.
    custom,
};

/// Common options for cast-like kernels.
pub const CastOptions = struct {
    /// If true, conversion must fail on lossy/overflowing cast.
    safe: bool = true,
    /// Optional target type override used by cast kernels.
    to_type: ?DataType = null,
};

/// Common options for arithmetic kernels.
pub const ArithmeticOptions = struct {
    /// If true, overflow should produce an error instead of wrapping.
    check_overflow: bool = true,
    /// If true, division by zero should return an error.
    divide_by_zero_is_error: bool = true,
};

/// Common options for filter-like kernels.
pub const FilterOptions = struct {
    /// If true, nulls in predicate are treated as false and dropped.
    drop_nulls: bool = true,
};

/// Custom untyped options hook for downstream extension kernels.
pub const CustomOptions = struct {
    /// Caller-defined discriminator.
    tag: []const u8,
    /// Optional opaque payload owned by caller.
    payload: ?*const anyopaque = null,
};

/// Type-safe options payload passed to all kernel signatures and executors.
pub const Options = union(OptionsTag) {
    none: void,
    cast: CastOptions,
    arithmetic: ArithmeticOptions,
    filter: FilterOptions,
    custom: CustomOptions,

    pub fn noneValue() Options {
        return .{ .none = {} };
    }

    pub fn tag(self: Options) OptionsTag {
        return std.meta.activeTag(self);
    }
};

pub const OptionsCheckFn = *const fn (options: Options) bool;
pub const ResultTypeFn = *const fn (args: []const Datum, options: Options) KernelError!DataType;
pub const KernelExecFn = *const fn (ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum;
pub const AggregateInitFn = *const fn (ctx: *ExecContext, options: Options) KernelError!*anyopaque;
pub const AggregateUpdateFn = *const fn (ctx: *ExecContext, state: *anyopaque, args: []const Datum, options: Options) KernelError!void;
pub const AggregateMergeFn = *const fn (ctx: *ExecContext, state: *anyopaque, other_state: *anyopaque, options: Options) KernelError!void;
pub const AggregateFinalizeFn = *const fn (ctx: *ExecContext, state: *anyopaque, options: Options) KernelError!Datum;
pub const AggregateDeinitFn = *const fn (ctx: *ExecContext, state: *anyopaque) void;

/// Stateful aggregate lifecycle callbacks used for incremental/grouped aggregation.
pub const AggregateLifecycle = struct {
    init: AggregateInitFn,
    update: AggregateUpdateFn,
    merge: AggregateMergeFn,
    finalize: AggregateFinalizeFn,
    deinit: AggregateDeinitFn,
};

/// Kernel signature metadata used for dispatch and type inference.
pub const KernelSignature = struct {
    /// Required argument count (exact arity or minimum arity for variadic kernels).
    arity: usize,
    /// Whether the signature accepts variadic arguments.
    variadic: bool = false,
    /// Optional maximum arity for bounded variadic signatures.
    max_arity: ?usize = null,
    /// Optional argument validator for logical type matching.
    type_check: ?TypeCheckFn = null,
    /// Optional options validator for type-safe options enforcement.
    options_check: ?OptionsCheckFn = null,
    /// Optional result type inference callback.
    result_type_fn: ?ResultTypeFn = null,

    pub fn any(arity: usize) KernelSignature {
        return .{
            .arity = arity,
            .variadic = false,
            .max_arity = null,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn atLeast(min_arity: usize) KernelSignature {
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = null,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn range(min_arity: usize, max_arity: usize) KernelSignature {
        std.debug.assert(max_arity >= min_arity);
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = max_arity,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn unary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 1,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn binary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 2,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn unaryWithResult(type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = 1,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    pub fn binaryWithResult(type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = 2,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    pub fn variadicWithResult(min_arity: usize, type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    const ArityModel = enum {
        exact,
        at_least,
        range,
    };

    fn arityModel(self: KernelSignature) ArityModel {
        if (!self.variadic) return .exact;
        if (self.max_arity != null) return .range;
        return .at_least;
    }

    fn aritySpecificityRank(self: KernelSignature) u8 {
        return switch (self.arityModel()) {
            .exact => 3,
            .range => 2,
            .at_least => 1,
        };
    }

    fn hasValidArityModel(self: KernelSignature) bool {
        if (!self.variadic) return self.max_arity == null;
        if (self.max_arity) |max| return max >= self.arity;
        return true;
    }

    pub fn isValidArityModel(self: KernelSignature) bool {
        return self.hasValidArityModel();
    }

    pub fn matchesArity(self: KernelSignature, arg_count: usize) bool {
        std.debug.assert(self.hasValidArityModel());
        if (!self.hasValidArityModel()) return false;
        return switch (self.arityModel()) {
            .exact => arg_count == self.arity,
            .at_least => arg_count >= self.arity,
            .range => arg_count >= self.arity and arg_count <= self.max_arity.?,
        };
    }

    pub fn matches(self: KernelSignature, args: []const Datum) bool {
        if (!self.matchesArity(args.len)) return false;
        if (self.type_check) |check| return check(args);
        return true;
    }

    pub fn matchesOptions(self: KernelSignature, options: Options) bool {
        if (self.options_check) |check| return check(options);
        return true;
    }

    pub fn accepts(self: KernelSignature, args: []const Datum, options: Options) bool {
        return self.matches(args) and self.matchesOptions(options);
    }

    /// Human-readable mismatch reason for diagnostics and debugging.
    pub fn explainMismatch(self: KernelSignature, args: []const Datum, options: Options) []const u8 {
        if (!self.matchesArity(args.len)) {
            return switch (self.arityModel()) {
                .exact => "arity mismatch: argument count does not match exact kernel arity",
                .at_least => "arity mismatch: argument count is below minimum kernel arity",
                .range => "arity mismatch: argument count is outside kernel arity range",
            };
        }
        if (self.type_check) |check| {
            if (!check(args)) return "type mismatch: arguments did not satisfy kernel type_check";
        }
        if (self.options_check) |check| {
            if (!check(options)) return "options mismatch: options did not satisfy kernel options_check";
        }
        return "signature accepted";
    }

    pub fn inferResultType(self: KernelSignature, args: []const Datum, options: Options) KernelError!DataType {
        if (!self.matchesArity(args.len)) return error.InvalidArity;
        if (self.type_check) |check| {
            if (!check(args)) return error.NoMatchingKernel;
        }
        if (self.options_check) |check| {
            if (!check(options)) return error.InvalidOptions;
        }
        if (self.result_type_fn) |infer| {
            return infer(args, options);
        }
        if (args.len == 0) return error.InvalidInput;
        return args[0].dataType();
    }

    /// Human-readable reason for result-type inference failure.
    pub fn explainInferResultTypeFailure(self: KernelSignature, args: []const Datum, options: Options) []const u8 {
        if (!self.matchesArity(args.len)) return "cannot infer result type: invalid arity";
        if (self.type_check) |check| {
            if (!check(args)) return "cannot infer result type: argument type_check failed";
        }
        if (self.options_check) |check| {
            if (!check(options)) return "cannot infer result type: options_check failed";
        }
        if (self.result_type_fn == null and args.len == 0) return "cannot infer result type: no arguments and no result_type_fn";
        return "result type inference should succeed";
    }
};

pub const Kernel = struct {
    signature: KernelSignature,
    exec: KernelExecFn,
    aggregate_lifecycle: ?AggregateLifecycle = null,

    pub fn supportsAggregateLifecycle(self: Kernel) bool {
        return self.aggregate_lifecycle != null;
    }
};

/// Live aggregate state handle created from an aggregate kernel lifecycle.
pub const AggregateSession = struct {
    ctx: *ExecContext,
    kernel: *const Kernel,
    lifecycle: AggregateLifecycle,
    options: Options,
    state: *anyopaque,

    pub fn update(self: *AggregateSession, args: []const Datum) KernelError!void {
        if (!self.kernel.signature.matches(args)) return error.NoMatchingKernel;
        if (!self.kernel.signature.matchesOptions(self.options)) return error.InvalidOptions;
        return self.lifecycle.update(self.ctx, self.state, args, self.options);
    }

    pub fn merge(self: *AggregateSession, other: *AggregateSession) KernelError!void {
        if (self.kernel != other.kernel) return error.AggregateStateMismatch;
        return self.lifecycle.merge(self.ctx, self.state, other.state, self.options);
    }

    pub fn finalize(self: *AggregateSession) KernelError!Datum {
        return self.lifecycle.finalize(self.ctx, self.state, self.options);
    }

    pub fn deinit(self: *AggregateSession) void {
        self.lifecycle.deinit(self.ctx, self.state);
        self.* = undefined;
    }
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
        if (!kernel.signature.isValidArityModel()) return error.InvalidInput;
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

    pub fn resolveKernel(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!*const Kernel {
        const function = self.findFunction(name, kind) orelse return error.FunctionNotFound;
        if (function.kernels.items.len == 0) return error.NoMatchingKernel;

        var saw_matching_arity = false;
        var saw_matching_type = false;
        var best_kernel: ?*const Kernel = null;
        var best_specificity: u8 = 0;
        for (function.kernels.items) |*kernel| {
            if (!kernel.signature.matchesArity(args.len)) continue;
            saw_matching_arity = true;
            if (!kernel.signature.matches(args)) continue;
            saw_matching_type = true;
            if (!kernel.signature.matchesOptions(options)) continue;

            const specificity = kernel.signature.aritySpecificityRank();
            if (best_kernel == null or specificity > best_specificity) {
                best_kernel = kernel;
                best_specificity = specificity;
            }
        }
        if (best_kernel) |kernel| return kernel;
        if (!saw_matching_arity) return error.InvalidArity;
        if (saw_matching_type) return error.InvalidOptions;
        return error.NoMatchingKernel;
    }

    /// Explain why kernel resolution would fail for the given call site.
    pub fn explainResolveKernelFailure(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) []const u8 {
        const function = self.findFunction(name, kind) orelse return "function not found";
        if (function.kernels.items.len == 0) return "function has no registered kernels";

        var saw_matching_arity = false;
        var saw_matching_type = false;
        var best_specificity: u8 = 0;
        var saw_matching_kernel = false;
        var has_exact = false;
        var has_range = false;
        var has_at_least = false;
        var any_min_gt_arg = false;
        var any_range_excludes_arg = false;
        for (function.kernels.items) |*kernel| {
            switch (kernel.signature.arityModel()) {
                .exact => has_exact = true,
                .range => has_range = true,
                .at_least => has_at_least = true,
            }
            if (args.len < kernel.signature.arity) any_min_gt_arg = true;
            if (kernel.signature.arityModel() == .range and !kernel.signature.matchesArity(args.len)) {
                any_range_excludes_arg = true;
            }

            if (!kernel.signature.matchesArity(args.len)) continue;
            saw_matching_arity = true;
            if (!kernel.signature.matches(args)) continue;
            saw_matching_type = true;
            if (!kernel.signature.matchesOptions(options)) continue;
            const specificity = kernel.signature.aritySpecificityRank();
            if (!saw_matching_kernel or specificity > best_specificity) {
                best_specificity = specificity;
                saw_matching_kernel = true;
            }
        }
        if (saw_matching_kernel) return "kernel resolution should succeed";
        if (!saw_matching_arity) {
            if (has_at_least and any_min_gt_arg) return "no kernel matched minimum arity";
            if (has_range and any_range_excludes_arg) return "no kernel matched arity range";
            if (has_exact) return "no kernel matched exact arity";
            return "no kernel matched arity";
        }
        if (saw_matching_type) return "kernel matched args but options were invalid";
        return "no kernel matched argument types";
    }

    pub fn resolveResultType(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!DataType {
        const kernel = try self.resolveKernel(name, kind, args, options);
        return kernel.signature.inferResultType(args, options);
    }

    /// Explain why result-type inference would fail for the given call site.
    pub fn explainResolveResultTypeFailure(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) []const u8 {
        const function = self.findFunction(name, kind) orelse return "cannot infer result type: function not found";
        if (function.kernels.items.len == 0) return "cannot infer result type: function has no kernels";
        for (function.kernels.items) |*kernel| {
            if (!kernel.signature.accepts(args, options)) continue;
            return kernel.signature.explainInferResultTypeFailure(args, options);
        }
        return self.explainResolveKernelFailure(name, kind, args, options);
    }

    pub fn invoke(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        const kernel = try self.resolveKernel(name, kind, args, options);
        _ = try kernel.signature.inferResultType(args, options);
        return kernel.exec(ctx, args, options);
    }

    pub fn invokeScalar(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .scalar, args, options);
    }

    pub fn invokeVector(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .vector, args, options);
    }

    pub fn invokeAggregate(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .aggregate, args, options);
    }

    /// Create a stateful aggregate session from an aggregate kernel lifecycle.
    pub fn beginAggregate(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        prototype_args: []const Datum,
        options: Options,
    ) KernelError!AggregateSession {
        const kernel = try self.resolveKernel(name, .aggregate, prototype_args, options);
        const lifecycle = kernel.aggregate_lifecycle orelse return error.MissingLifecycle;
        const state = try lifecycle.init(ctx, options);
        return .{
            .ctx = ctx,
            .kernel = kernel,
            .lifecycle = lifecycle,
            .options = options,
            .state = state,
        };
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

/// Overflow policy for arithmetic kernels.
pub const OverflowMode = enum {
    checked,
    wrapping,
    saturating,
};

/// Execution configuration shared by all kernel invocations in a context.
pub const ExecConfig = struct {
    /// Safe cast mode for cast kernels (fail on lossy casts when true).
    safe_cast: bool = true,
    /// Overflow policy for arithmetic kernels.
    overflow_mode: OverflowMode = .checked,
    /// Preferred thread count for vector/aggregate execution.
    threads: usize = 1,
    /// Optional arena-like allocator used for temporary/borrowed scalar payloads.
    arena_allocator: ?std.mem.Allocator = null,
};

pub const ExecContext = struct {
    allocator: std.mem.Allocator,
    registry: *const FunctionRegistry,
    config: ExecConfig,

    pub fn init(allocator: std.mem.Allocator, registry: *const FunctionRegistry) ExecContext {
        return initWithConfig(allocator, registry, .{});
    }

    pub fn initWithConfig(allocator: std.mem.Allocator, registry: *const FunctionRegistry, config: ExecConfig) ExecContext {
        var normalized = config;
        if (normalized.threads == 0) normalized.threads = 1;
        return .{
            .allocator = allocator,
            .registry = registry,
            .config = normalized,
        };
    }

    pub fn tempAllocator(self: *const ExecContext) std.mem.Allocator {
        return self.config.arena_allocator orelse self.allocator;
    }

    /// Duplicate UTF-8 bytes into the context temp allocator for scalar string payloads.
    pub fn dupScalarString(self: *const ExecContext, value: []const u8) KernelError![]const u8 {
        return self.tempAllocator().dupe(u8, value) catch error.OutOfMemory;
    }

    /// Duplicate raw bytes into the context temp allocator for scalar binary payloads.
    pub fn dupScalarBinary(self: *const ExecContext, value: []const u8) KernelError![]const u8 {
        return self.tempAllocator().dupe(u8, value) catch error.OutOfMemory;
    }

    pub fn safeCastEnabled(self: *const ExecContext) bool {
        return self.config.safe_cast;
    }

    pub fn overflowMode(self: *const ExecContext) OverflowMode {
        return self.config.overflow_mode;
    }

    pub fn threads(self: *const ExecContext) usize {
        return self.config.threads;
    }

    pub fn invoke(
        self: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invoke(self, name, kind, args, options);
    }

    pub fn invokeScalar(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeScalar(self, name, args, options);
    }

    pub fn invokeVector(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeVector(self, name, args, options);
    }

    pub fn invokeAggregate(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeAggregate(self, name, args, options);
    }

    pub fn beginAggregate(
        self: *ExecContext,
        name: []const u8,
        prototype_args: []const Datum,
        options: Options,
    ) KernelError!AggregateSession {
        return self.registry.beginAggregate(self, name, prototype_args, options);
    }
};

pub const ExecChunkValue = union(enum) {
    array: ArrayRef,
    scalar: Scalar,

    pub fn dataType(self: ExecChunkValue) DataType {
        return switch (self) {
            .array => |arr| arr.data().data_type,
            .scalar => |s| s.data_type,
        };
    }

    pub fn isNullAt(self: ExecChunkValue, logical_index: usize) bool {
        return switch (self) {
            .array => |arr| blk: {
                std.debug.assert(logical_index < arr.data().length);
                break :blk arr.data().isNull(logical_index);
            },
            .scalar => |s| s.isNull(),
        };
    }

    pub fn release(self: *ExecChunkValue) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .scalar => |*s| s.release(),
        }
        self.* = undefined;
    }
};

/// Common null propagation helper for unary kernels.
pub fn unaryNullPropagates(input: ExecChunkValue, logical_index: usize) bool {
    return input.isNullAt(logical_index);
}

/// Common null propagation helper for binary kernels.
pub fn binaryNullPropagates(lhs: ExecChunkValue, rhs: ExecChunkValue, logical_index: usize) bool {
    return lhs.isNullAt(logical_index) or rhs.isNullAt(logical_index);
}

/// Common null propagation helper for n-ary kernels.
pub fn naryNullPropagates(values: []const ExecChunkValue, logical_index: usize) bool {
    for (values) |value| {
        if (value.isNullAt(logical_index)) return true;
    }
    return false;
}

pub const UnaryExecChunk = struct {
    values: ExecChunkValue,
    len: usize,

    pub fn unaryNullAt(self: UnaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return unaryNullPropagates(self.values, logical_index);
    }

    pub fn deinit(self: *UnaryExecChunk) void {
        self.values.release();
        self.* = undefined;
    }
};

pub const BinaryExecChunk = struct {
    lhs: ExecChunkValue,
    rhs: ExecChunkValue,
    len: usize,

    pub fn binaryNullAt(self: BinaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return binaryNullPropagates(self.lhs, self.rhs, logical_index);
    }

    pub fn deinit(self: *BinaryExecChunk) void {
        self.lhs.release();
        self.rhs.release();
        self.* = undefined;
    }
};

pub const NaryExecChunk = struct {
    allocator: std.mem.Allocator,
    values: []ExecChunkValue,
    len: usize,

    pub fn naryNullAt(self: NaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return naryNullPropagates(self.values, logical_index);
    }

    pub fn deinit(self: *NaryExecChunk) void {
        for (self.values) |*value| {
            value.release();
        }
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

fn datumArrayLikeLen(datum: Datum) ?usize {
    return switch (datum) {
        .array => |arr| arr.data().length,
        .chunked => |chunks| chunks.len(),
        .scalar => null,
    };
}

/// Infer binary execution length with scalar-broadcast semantics.
pub fn inferBinaryExecLen(lhs: Datum, rhs: Datum) KernelError!usize {
    const lhs_len = datumArrayLikeLen(lhs);
    const rhs_len = datumArrayLikeLen(rhs);

    if (lhs_len == null and rhs_len == null) return 1;
    if (lhs_len == null) return rhs_len.?;
    if (rhs_len == null) return lhs_len.?;
    if (lhs_len.? != rhs_len.?) return error.InvalidInput;
    return lhs_len.?;
}

/// Infer n-ary execution length with scalar-broadcast semantics.
pub fn inferNaryExecLen(args: []const Datum) KernelError!usize {
    if (args.len == 0) return error.InvalidArity;

    var array_like_len: ?usize = null;
    for (args) |arg| {
        const current_len = datumArrayLikeLen(arg);
        if (current_len == null) continue;
        if (array_like_len == null) {
            array_like_len = current_len.?;
            continue;
        }
        if (array_like_len.? != current_len.?) return error.InvalidInput;
    }
    return array_like_len orelse 1;
}

fn mapArrayReadError(err: anyerror) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidInput,
    };
}

fn mapChunkedError(err: chunked_array_mod.Error) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.LengthOverflow => error.Overflow,
        else => error.InvalidInput,
    };
}

const ChunkLookup = struct {
    chunk: *const ArrayRef,
    local_index: usize,
};

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

            const out = ChunkedArray.init(allocator, field_dt, fields[0..field_count]) catch |err| break :blk mapChunkedError(err);
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

const ExecDatumCursor = union(enum) {
    array: struct {
        array: ArrayRef,
        offset: usize = 0,
    },
    chunked: struct {
        chunked: ChunkedArray,
        chunk_index: usize = 0,
        offset: usize = 0,
    },
    scalar: struct {
        scalar: Scalar,
    },

    fn init(datum: Datum) ExecDatumCursor {
        return switch (datum) {
            .array => |arr| .{ .array = .{ .array = arr } },
            .chunked => |chunks| .{ .chunked = .{ .chunked = chunks } },
            .scalar => |s| .{ .scalar = .{ .scalar = s } },
        };
    }

    fn normalize(self: *ExecDatumCursor) void {
        switch (self.*) {
            .array => {},
            .chunked => |*s| {
                while (s.chunk_index < s.chunked.numChunks()) {
                    const chunk_len = s.chunked.chunk(s.chunk_index).data().length;
                    if (chunk_len == 0 or s.offset == chunk_len) {
                        s.chunk_index += 1;
                        s.offset = 0;
                        continue;
                    }
                    break;
                }
            },
            .scalar => {},
        }
    }

    fn remainingCurrent(self: *ExecDatumCursor) usize {
        self.normalize();
        return switch (self.*) {
            .array => |*s| s.array.data().length - s.offset,
            .chunked => |*s| blk: {
                if (s.chunk_index >= s.chunked.numChunks()) break :blk 0;
                const chunk_len = s.chunked.chunk(s.chunk_index).data().length;
                break :blk chunk_len - s.offset;
            },
            .scalar => std.math.maxInt(usize),
        };
    }

    fn take(self: *ExecDatumCursor, len: usize) KernelError!ExecChunkValue {
        self.normalize();
        return switch (self.*) {
            .array => |*s| blk: {
                const arr_len = s.array.data().length;
                if (len > arr_len - s.offset) return error.InvalidInput;
                const out = if (s.offset == 0 and len == arr_len)
                    s.array.retain()
                else
                    s.array.slice(s.offset, len) catch return error.OutOfMemory;
                s.offset += len;
                break :blk .{ .array = out };
            },
            .chunked => |*s| blk: {
                if (s.chunk_index >= s.chunked.numChunks()) return error.InvalidInput;
                const chunk_ref = s.chunked.chunk(s.chunk_index).*;
                const chunk_len = chunk_ref.data().length;
                if (len > chunk_len - s.offset) return error.InvalidInput;

                const out = if (s.offset == 0 and len == chunk_len)
                    chunk_ref.retain()
                else
                    chunk_ref.slice(s.offset, len) catch return error.OutOfMemory;

                s.offset += len;
                if (s.offset == chunk_len) {
                    s.chunk_index += 1;
                    s.offset = 0;
                }
                break :blk .{ .array = out };
            },
            .scalar => |s| .{ .scalar = s.scalar.retain() },
        };
    }
};

pub const UnaryExecChunkIterator = struct {
    cursor: ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(datum: Datum) UnaryExecChunkIterator {
        return .{
            .cursor = ExecDatumCursor.init(datum),
            .total_len = switch (datum) {
                .array => |arr| arr.data().length,
                .chunked => |chunks| chunks.len(),
                .scalar => 1,
            },
        };
    }

    pub fn next(self: *UnaryExecChunkIterator) KernelError!?UnaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;
        const current_remaining = self.cursor.remainingCurrent();
        if (current_remaining == 0) return error.InvalidInput;
        const run_len = @min(remaining_total, current_remaining);
        var values = try self.cursor.take(run_len);
        errdefer values.release();
        self.consumed += run_len;
        return .{
            .values = values,
            .len = run_len,
        };
    }
};

pub const BinaryExecChunkIterator = struct {
    lhs_cursor: ExecDatumCursor,
    rhs_cursor: ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(lhs: Datum, rhs: Datum) KernelError!BinaryExecChunkIterator {
        return .{
            .lhs_cursor = ExecDatumCursor.init(lhs),
            .rhs_cursor = ExecDatumCursor.init(rhs),
            .total_len = try inferBinaryExecLen(lhs, rhs),
        };
    }

    pub fn next(self: *BinaryExecChunkIterator) KernelError!?BinaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;

        const lhs_remaining = self.lhs_cursor.remainingCurrent();
        const rhs_remaining = self.rhs_cursor.remainingCurrent();
        if (lhs_remaining == 0 or rhs_remaining == 0) return error.InvalidInput;

        const run_len = @min(remaining_total, @min(lhs_remaining, rhs_remaining));
        if (run_len == 0) return error.InvalidInput;

        var lhs = try self.lhs_cursor.take(run_len);
        errdefer lhs.release();
        var rhs = try self.rhs_cursor.take(run_len);
        errdefer rhs.release();

        self.consumed += run_len;
        return .{
            .lhs = lhs,
            .rhs = rhs,
            .len = run_len,
        };
    }
};

pub const NaryExecChunkIterator = struct {
    allocator: std.mem.Allocator,
    cursors: []ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(allocator: std.mem.Allocator, args: []const Datum) KernelError!NaryExecChunkIterator {
        if (args.len == 0) return error.InvalidArity;

        var cursors = allocator.alloc(ExecDatumCursor, args.len) catch return error.OutOfMemory;
        errdefer allocator.free(cursors);
        for (args, 0..) |arg, idx| {
            cursors[idx] = ExecDatumCursor.init(arg);
        }
        return .{
            .allocator = allocator,
            .cursors = cursors,
            .total_len = try inferNaryExecLen(args),
        };
    }

    pub fn deinit(self: *NaryExecChunkIterator) void {
        self.allocator.free(self.cursors);
        self.* = undefined;
    }

    pub fn next(self: *NaryExecChunkIterator) KernelError!?NaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;

        var run_len = remaining_total;
        for (self.cursors) |*cursor| {
            const remaining = cursor.remainingCurrent();
            if (remaining == 0) return error.InvalidInput;
            run_len = @min(run_len, remaining);
        }
        if (run_len == 0) return error.InvalidInput;

        var values = self.allocator.alloc(ExecChunkValue, self.cursors.len) catch return error.OutOfMemory;
        var taken: usize = 0;
        errdefer {
            while (taken > 0) {
                taken -= 1;
                values[taken].release();
            }
            self.allocator.free(values);
        }
        for (self.cursors, 0..) |*cursor, idx| {
            values[idx] = try cursor.take(run_len);
            taken += 1;
        }

        self.consumed += run_len;
        return .{
            .allocator = self.allocator,
            .values = values,
            .len = run_len,
        };
    }
};

/// Convert integer-like values using a standardized InvalidCast error path.
pub fn intCastOrInvalidCast(comptime T: type, value: anytype) KernelError!T {
    return std.math.cast(T, value) orelse error.InvalidCast;
}

/// i64 division helper with standardized DivideByZero / Overflow behavior.
pub fn arithmeticDivI64(lhs: i64, rhs: i64, options: ArithmeticOptions) KernelError!i64 {
    if (rhs == 0) {
        if (options.divide_by_zero_is_error) return error.DivideByZero;
        return 0;
    }
    if (options.check_overflow and lhs == std.math.minInt(i64) and rhs == -1) {
        return error.Overflow;
    }
    return @divTrunc(lhs, rhs);
}

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

fn isInt64Scalar(args: []const Datum) bool {
    return args.len == 1 and args[0].isScalar() and args[0].scalar.data_type == .int64;
}

fn isTwoInt64Scalars(args: []const Datum) bool {
    return args.len == 2 and args[0].isScalar() and args[1].isScalar() and args[0].scalar.data_type == .int64 and args[1].scalar.data_type == .int64;
}

fn allInt32Datums(args: []const Datum) bool {
    for (args) |arg| {
        if (!arg.dataType().eql(.{ .int32 = {} })) return false;
    }
    return true;
}

fn passthroughInt32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

fn exactArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 303 },
    });
}

fn rangeArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 202 },
    });
}

fn atLeastArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 101 },
    });
}

fn countLenAggregateKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
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

fn onlyCastOptions(options: Options) bool {
    return switch (options) {
        .cast => true,
        else => false,
    };
}

fn onlyArithmeticOptions(options: Options) bool {
    return switch (options) {
        .arithmetic => true,
        else => false,
    };
}

fn firstArgResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = options;
    if (args.len == 0) return error.InvalidInput;
    return args[0].dataType();
}

fn castResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 1) return error.InvalidArity;
    return switch (options) {
        .cast => |cast_opts| cast_opts.to_type orelse args[0].dataType(),
        else => error.InvalidOptions,
    };
}

fn castI64ToI32ResultType(args: []const Datum, options: Options) KernelError!DataType {
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

fn castI64ToI32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
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

fn divI64ScalarResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 2) return error.InvalidArity;
    _ = switch (options) {
        .arithmetic => {},
        else => return error.InvalidOptions,
    };
    if (!isTwoInt64Scalars(args)) return error.InvalidInput;
    return .{ .int64 = {} };
}

fn divI64ScalarKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
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

fn countAggregateResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = args;
    _ = options;
    return .{ .int64 = {} };
}

const CountAggState = struct {
    count: usize,
};

fn countLifecycleInit(ctx: *ExecContext, options: Options) KernelError!*anyopaque {
    _ = options;
    const state = ctx.allocator.create(CountAggState) catch return error.OutOfMemory;
    state.* = .{ .count = 0 };
    return state;
}

fn countLifecycleUpdate(ctx: *ExecContext, state_ptr: *anyopaque, args: []const Datum, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    if (!unaryArray(args)) return error.InvalidInput;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const count = args[0].array.data().length;
    state.count = std.math.add(usize, state.count, count) catch return error.Overflow;
}

fn countLifecycleMerge(ctx: *ExecContext, state_ptr: *anyopaque, other_ptr: *anyopaque, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const other: *CountAggState = @ptrCast(@alignCast(other_ptr));
    state.count = std.math.add(usize, state.count, other.count) catch return error.Overflow;
}

fn countLifecycleFinalize(ctx: *ExecContext, state_ptr: *anyopaque, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    return Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = @intCast(state.count) },
    });
}

fn countLifecycleDeinit(ctx: *ExecContext, state_ptr: *anyopaque) void {
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    ctx.allocator.destroy(state);
}

fn makeInt32Array(allocator: std.mem.Allocator, values: []const ?i32) !ArrayRef {
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

fn makeBoolArray(allocator: std.mem.Allocator, values: []const ?bool) !ArrayRef {
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
