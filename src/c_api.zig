const std = @import("std");
const ffi = @import("ffi/mod.zig");
const datatype = @import("datatype.zig");

const DataType = datatype.DataType;
const c_allocator = std.heap.c_allocator;

const SchemaHandle = struct {
    owned: ffi.CDataOwnedSchema,
};

const ArrayHandle = struct {
    schema: ffi.CDataOwnedSchema,
    array: ffi.c_data.ArrayRef,
};

const StreamHandle = struct {
    owned: ffi.CStreamOwnedRecordBatchStream,
};

pub const ZARROW_C_STATUS_OK: c_int = 0;
pub const ZARROW_C_STATUS_INVALID_ARGUMENT: c_int = 1;
pub const ZARROW_C_STATUS_OUT_OF_MEMORY: c_int = 2;
pub const ZARROW_C_STATUS_RELEASED: c_int = 3;
pub const ZARROW_C_STATUS_INVALID_DATA: c_int = 4;
pub const ZARROW_C_STATUS_INTERNAL: c_int = 5;

fn mapError(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => ZARROW_C_STATUS_OUT_OF_MEMORY,
        error.Released => ZARROW_C_STATUS_RELEASED,
        error.InvalidFormat,
        error.TopLevelSchemaMustBeStruct,
        error.InvalidChildren,
        error.InvalidBufferCount,
        error.InvalidLength,
        error.InvalidOffset,
        error.InvalidNullCount,
        error.MissingDictionary,
        error.UnsupportedType,
        error.InvalidStream,
        error.StreamCallbackFailed,
        error.SchemaMismatch,
        error.FieldCountMismatch,
        error.RowCountMismatch,
        error.SchemaEndiannessMismatch,
        error.SchemaFieldMismatch,
        error.InvalidArgument,
        error.LayoutMismatch,
        => ZARROW_C_STATUS_INVALID_DATA,
        else => ZARROW_C_STATUS_INTERNAL,
    };
}

pub export fn zarrow_c_abi_version() callconv(.c) u32 {
    return 1;
}

pub export fn zarrow_c_status_string(status: c_int) callconv(.c) [*c]const u8 {
    return switch (status) {
        ZARROW_C_STATUS_OK => "ok",
        ZARROW_C_STATUS_INVALID_ARGUMENT => "invalid argument",
        ZARROW_C_STATUS_OUT_OF_MEMORY => "out of memory",
        ZARROW_C_STATUS_RELEASED => "released input",
        ZARROW_C_STATUS_INVALID_DATA => "invalid data",
        ZARROW_C_STATUS_INTERNAL => "internal error",
        else => "unknown status",
    };
}

pub export fn zarrow_c_import_schema(
    c_schema: ?*ffi.ArrowSchema,
    out_handle: ?*?*SchemaHandle,
) callconv(.c) c_int {
    if (c_schema == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    var owned = ffi.importSchemaOwned(c_allocator, c_schema.?) catch |err| return mapError(err);
    errdefer owned.deinit();

    const handle = c_allocator.create(SchemaHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{ .owned = owned };
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_schema(
    handle: ?*const SchemaHandle,
    out_schema: ?*ffi.ArrowSchema,
) callconv(.c) c_int {
    if (handle == null or out_schema == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_schema.?.* = ffi.exportSchema(c_allocator, handle.?.owned.schema) catch |err| return mapError(err);
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_schema(handle: ?*SchemaHandle) callconv(.c) void {
    if (handle == null) return;
    handle.?.owned.deinit();
    c_allocator.destroy(handle.?);
}

pub export fn zarrow_c_import_array(
    schema_handle: ?*const SchemaHandle,
    c_array: ?*ffi.ArrowArray,
    out_handle: ?*?*ArrayHandle,
) callconv(.c) c_int {
    if (schema_handle == null or c_array == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    // Keep a dedicated schema copy in the array handle so field/type pointers remain valid
    // even if the original schema handle is released.
    var exported_schema = ffi.exportSchema(c_allocator, schema_handle.?.owned.schema) catch |err| return mapError(err);
    defer if (exported_schema.release) |release_fn| release_fn(&exported_schema);
    var owned_schema = ffi.importSchemaOwned(c_allocator, &exported_schema) catch |err| return mapError(err);
    errdefer owned_schema.deinit();

    const top_level_type = DataType{ .struct_ = .{ .fields = owned_schema.schema.fields } };
    var arr = ffi.importArray(c_allocator, &top_level_type, c_array.?) catch |err| return mapError(err);
    errdefer arr.release();

    const handle = c_allocator.create(ArrayHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{
        .schema = owned_schema,
        .array = arr,
    };
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_array(
    handle: ?*const ArrayHandle,
    out_array: ?*ffi.ArrowArray,
) callconv(.c) c_int {
    if (handle == null or out_array == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_array.?.* = ffi.exportArray(c_allocator, handle.?.array) catch |err| return mapError(err);
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_array(handle: ?*ArrayHandle) callconv(.c) void {
    if (handle == null) return;
    handle.?.array.release();
    handle.?.schema.deinit();
    c_allocator.destroy(handle.?);
}

pub export fn zarrow_c_import_stream(
    c_stream: ?*ffi.ArrowArrayStream,
    out_handle: ?*?*StreamHandle,
) callconv(.c) c_int {
    if (c_stream == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    var owned = ffi.importRecordBatchStreamOwned(c_allocator, c_stream.?) catch |err| return mapError(err);
    errdefer owned.deinit();

    const handle = c_allocator.create(StreamHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{ .owned = owned };
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_stream(
    handle: ?*const StreamHandle,
    out_stream: ?*ffi.ArrowArrayStream,
) callconv(.c) c_int {
    if (handle == null or out_stream == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_stream.?.* = ffi.exportRecordBatchStream(
        c_allocator,
        handle.?.owned.schema.schema,
        handle.?.owned.batches,
    ) catch |err| return mapError(err);
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_stream(handle: ?*StreamHandle) callconv(.c) void {
    if (handle == null) return;
    handle.?.owned.deinit();
    c_allocator.destroy(handle.?);
}

test "c api status string smoke" {
    try std.testing.expectEqualStrings("ok", std.mem.span(zarrow_c_status_string(ZARROW_C_STATUS_OK)));
    try std.testing.expectEqualStrings("unknown status", std.mem.span(zarrow_c_status_string(999)));
}
