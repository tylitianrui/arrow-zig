const std = @import("std");
const array_mod = @import("../array/array.zig");
const core = @import("core.zig");
const common = @import("datum_common.zig");

const DataType = core.DataType;
const ArrayRef = core.ArrayRef;
const Scalar = core.Scalar;
const Datum = core.Datum;
const KernelError = core.KernelError;

const mapArrayReadError = common.mapArrayReadError;

fn makeNullArray(allocator: std.mem.Allocator, len: usize) KernelError!ArrayRef {
    var builder = try array_mod.NullBuilder.init(allocator, len);
    defer builder.deinit();
    builder.appendNulls(len) catch |err| return mapArrayReadError(err);
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBoolArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?bool) KernelError!ArrayRef {
    var builder = try array_mod.BooleanBuilder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeInt32ArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?i32) KernelError!ArrayRef {
    var builder = try array_mod.Int32Builder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeInt64ArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?i64) KernelError!ArrayRef {
    var builder = try array_mod.Int64Builder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeStringArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.StringBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeLargeStringArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.LargeStringBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBinaryArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.BinaryBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeLargeBinaryArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.LargeBinaryBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeStringViewArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.StringViewBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBinaryViewArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.BinaryViewBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

pub fn buildNullLikeArray(allocator: std.mem.Allocator, data_type: DataType, len: usize) KernelError!ArrayRef {
    return switch (data_type) {
        .null => makeNullArray(allocator, len),
        .bool => makeBoolArrayFilled(allocator, len, null),
        .int32 => makeInt32ArrayFilled(allocator, len, null),
        .int64 => makeInt64ArrayFilled(allocator, len, null),
        .string => makeStringArrayFilled(allocator, len, null),
        .large_string => makeLargeStringArrayFilled(allocator, len, null),
        .binary => makeBinaryArrayFilled(allocator, len, null),
        .large_binary => makeLargeBinaryArrayFilled(allocator, len, null),
        .string_view => makeStringViewArrayFilled(allocator, len, null),
        .binary_view => makeBinaryViewArrayFilled(allocator, len, null),
        .list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.ListBuilder.init(allocator, len, list_ty.value_field);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values.array) catch |err| mapArrayReadError(err);
        },
        .large_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.LargeListBuilder.init(allocator, len, list_ty.value_field);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values.array) catch |err| mapArrayReadError(err);
        },
        .fixed_size_list => |list_ty| blk: {
            const list_size = std.math.cast(usize, list_ty.list_size) orelse return error.InvalidInput;
            const values_len = std.math.mul(usize, len, list_size) catch return error.Overflow;
            var values = try buildNullLikeArray(allocator, list_ty.value_field.data_type.*, values_len);
            defer values.release();

            var builder = array_mod.FixedSizeListBuilder.init(allocator, list_ty.value_field, list_size) catch |err| return mapArrayReadError(err);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values) catch |err| mapArrayReadError(err);
        },
        .struct_ => |struct_ty| blk: {
            var children = allocator.alloc(ArrayRef, struct_ty.fields.len) catch return error.OutOfMemory;
            var child_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < child_count) : (i += 1) children[i].release();
                allocator.free(children);
            }

            for (struct_ty.fields, 0..) |field, i| {
                children[i] = try buildNullLikeArray(allocator, field.data_type.*, len);
                child_count += 1;
            }

            var builder = array_mod.StructBuilder.init(allocator, struct_ty.fields);
            defer builder.deinit();
            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }

            const out = builder.finish(children[0..child_count]) catch |err| return mapArrayReadError(err);
            var i_release: usize = 0;
            while (i_release < child_count) : (i_release += 1) children[i_release].release();
            allocator.free(children);
            break :blk out;
        },
        else => error.UnsupportedType,
    };
}

pub fn scalarToSingleArrayRef(allocator: std.mem.Allocator, scalar: Scalar) KernelError!ArrayRef {
    if (scalar.payload) |payload| {
        if (payload.data().length != 1) return error.InvalidInput;
        return payload.retain();
    }

    return switch (scalar.data_type) {
        .null => makeNullArray(allocator, 1),
        .bool => makeBoolArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.bool),
        .int32 => makeInt32ArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.i32),
        .int64 => makeInt64ArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.i64),
        .string => makeStringArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.string),
        .large_string => makeLargeStringArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.string),
        .binary => makeBinaryArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.binary),
        .large_binary => makeLargeBinaryArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.binary),
        .list, .large_list, .fixed_size_list, .struct_ => {
            if (!scalar.isNull()) return error.InvalidInput;
            return buildNullLikeArray(allocator, scalar.data_type, 1);
        },
        else => error.UnsupportedType,
    };
}

/// Build an all-null datum for the requested logical type and length.
pub fn datumBuildNullLike(data_type: DataType, len: usize) KernelError!Datum {
    return datumBuildNullLikeWithAllocator(std.heap.page_allocator, data_type, len);
}

/// Build an all-null datum for the requested logical type and length.
pub fn datumBuildNullLikeWithAllocator(allocator: std.mem.Allocator, data_type: DataType, len: usize) KernelError!Datum {
    return Datum.fromArray(try buildNullLikeArray(allocator, data_type, len));
}

/// Build an empty datum preserving the requested logical type and nested layout.
pub fn datumBuildEmptyLike(data_type: DataType) KernelError!Datum {
    return datumBuildEmptyLikeWithAllocator(std.heap.page_allocator, data_type);
}

/// Build an empty datum preserving the requested logical type and nested layout.
pub fn datumBuildEmptyLikeWithAllocator(allocator: std.mem.Allocator, data_type: DataType) KernelError!Datum {
    const out = switch (data_type) {
        .null => try makeNullArray(allocator, 0),
        .bool => try makeBoolArrayFilled(allocator, 0, null),
        .int32 => try makeInt32ArrayFilled(allocator, 0, null),
        .int64 => try makeInt64ArrayFilled(allocator, 0, null),
        .string => try makeStringArrayFilled(allocator, 0, null),
        .large_string => try makeLargeStringArrayFilled(allocator, 0, null),
        .binary => try makeBinaryArrayFilled(allocator, 0, null),
        .large_binary => try makeLargeBinaryArrayFilled(allocator, 0, null),
        .string_view => try makeStringViewArrayFilled(allocator, 0, null),
        .binary_view => try makeBinaryViewArrayFilled(allocator, 0, null),
        .list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.ListBuilder.init(allocator, 0, list_ty.value_field);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .large_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.LargeListBuilder.init(allocator, 0, list_ty.value_field);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .fixed_size_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            const list_size = std.math.cast(usize, list_ty.list_size) orelse return error.InvalidInput;
            var builder = array_mod.FixedSizeListBuilder.init(allocator, list_ty.value_field, list_size) catch |err| return mapArrayReadError(err);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .struct_ => |struct_ty| blk: {
            var children = allocator.alloc(ArrayRef, struct_ty.fields.len) catch return error.OutOfMemory;
            var child_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < child_count) : (i += 1) children[i].release();
                allocator.free(children);
            }

            for (struct_ty.fields, 0..) |field, i| {
                const child_empty = try datumBuildEmptyLikeWithAllocator(allocator, field.data_type.*);
                if (child_empty != .array) return error.InvalidInput;
                children[i] = child_empty.array;
                child_count += 1;
            }

            var builder = array_mod.StructBuilder.init(allocator, struct_ty.fields);
            defer builder.deinit();
            const out = builder.finish(children[0..child_count]) catch |err| return mapArrayReadError(err);

            var i_release: usize = 0;
            while (i_release < child_count) : (i_release += 1) children[i_release].release();
            allocator.free(children);
            break :blk out;
        },
        else => return error.UnsupportedType,
    };

    return Datum.fromArray(out);
}
