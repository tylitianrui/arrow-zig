const std = @import("std");
const core = @import("core.zig");

const DataType = core.DataType;
const ArrayRef = core.ArrayRef;
const ChunkedArray = core.ChunkedArray;
const Scalar = core.Scalar;
const Datum = core.Datum;
const KernelError = core.KernelError;
const ArithmeticOptions = core.ArithmeticOptions;

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
