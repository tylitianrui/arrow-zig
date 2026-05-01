const std = @import("std");
const chunked_array_mod = @import("../chunked_array.zig");
const core = @import("core.zig");

const Datum = core.Datum;
const KernelError = core.KernelError;

pub fn mapArrayReadError(err: anyerror) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidInput,
    };
}

pub fn mapChunkedError(err: chunked_array_mod.Error) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.LengthOverflow => error.Overflow,
        else => error.InvalidInput,
    };
}

pub fn mapConcatError(err: anyerror) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.UnsupportedType => error.UnsupportedType,
        else => error.InvalidInput,
    };
}

pub fn inferDatumAllocator(datum: Datum) ?std.mem.Allocator {
    return switch (datum) {
        .array => |arr| arr.node.allocator,
        .chunked => |chunks| chunks.node.allocator,
        .scalar => |s| if (s.payload) |payload| payload.node.allocator else null,
    };
}

pub fn inferDatumsAllocator(datums: []const Datum) std.mem.Allocator {
    for (datums) |datum| {
        if (inferDatumAllocator(datum)) |allocator| return allocator;
    }
    return std.heap.page_allocator;
}
