const std = @import("std");
const c_data = @import("c_data.zig");
const schema_mod = @import("../schema.zig");
const record_batch_mod = @import("../record_batch.zig");
const array_data_mod = @import("../array/array_data.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const struct_array_mod = @import("../array/struct_array.zig");
const array_mod = @import("../array/array.zig");

pub const ArrowSchema = c_data.ArrowSchema;
pub const ArrowArray = c_data.ArrowArray;
pub const DataType = c_data.DataType;
pub const Field = c_data.Field;
pub const Schema = c_data.Schema;
pub const ArrayData = c_data.ArrayData;
pub const ArrayRef = c_data.ArrayRef;
pub const SharedBuffer = c_data.SharedBuffer;
pub const RecordBatch = record_batch_mod.RecordBatch;

pub const ArrowArrayStream = extern struct {
    get_schema: ?*const fn (?*ArrowArrayStream, ?*ArrowSchema) callconv(.c) c_int,
    get_next: ?*const fn (?*ArrowArrayStream, ?*ArrowArray) callconv(.c) c_int,
    get_last_error: ?*const fn (?*ArrowArrayStream) callconv(.c) [*c]const u8,
    release: ?*const fn (?*ArrowArrayStream) callconv(.c) void,
    private_data: ?*anyopaque,
};

pub const Error = c_data.Error || record_batch_mod.RecordBatchError || array_data_mod.ValidationError || error{
    InvalidStream,
    StreamCallbackFailed,
    SchemaMismatch,
};

pub const OwnedRecordBatchStream = struct {
    allocator: std.mem.Allocator,
    schema: c_data.OwnedSchema,
    batches: []RecordBatch,

    pub fn deinit(self: *OwnedRecordBatchStream) void {
        for (self.batches) |*batch| {
            batch.deinit();
        }
        self.allocator.free(self.batches);
        self.schema.deinit();
    }
};

const ExportedStreamPrivate = struct {
    allocator: std.mem.Allocator,
    schema_ref: schema_mod.SchemaRef,
    batches: []RecordBatch,
    next_batch_index: usize = 0,
    has_last_error: bool = false,
    last_error_buf: [256]u8 = [_]u8{0} ** 256,
};

const C_OK: c_int = 0;
const C_ENOMEM: c_int = 12;
const C_EINVAL: c_int = 22;

pub fn exportRecordBatchStream(allocator: std.mem.Allocator, schema: Schema, batches: []const RecordBatch) Error!ArrowArrayStream {
    for (batches) |*batch| {
        if (!schemaCompatible(&schema, batch.schema())) return error.SchemaMismatch;
    }

    var schema_ref = try schema_mod.SchemaRef.fromBorrowed(allocator, schema);
    errdefer schema_ref.release();

    const owned_batches = try allocator.alloc(RecordBatch, batches.len);
    var owned_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < owned_count) : (i += 1) {
            owned_batches[i].deinit();
        }
        allocator.free(owned_batches);
    }

    for (batches, 0..) |*batch, i| {
        owned_batches[i] = try batch.slice(0, batch.numRows());
        owned_count += 1;
    }

    const priv = try allocator.create(ExportedStreamPrivate);
    priv.* = .{
        .allocator = allocator,
        .schema_ref = schema_ref,
        .batches = owned_batches,
    };

    return .{
        .get_schema = streamGetSchema,
        .get_next = streamGetNext,
        .get_last_error = streamGetLastError,
        .release = streamRelease,
        .private_data = priv,
    };
}

pub fn importRecordBatchStreamOwned(allocator: std.mem.Allocator, c_stream: *ArrowArrayStream) Error!OwnedRecordBatchStream {
    if (c_stream.release == null) return error.Released;
    if (c_stream.get_schema == null or c_stream.get_next == null or c_stream.get_last_error == null) return error.InvalidStream;

    errdefer if (c_stream.release) |release_fn| release_fn(c_stream);

    var c_schema = releasedArrowSchema();
    const schema_rc = c_stream.get_schema.?(c_stream, &c_schema);
    if (schema_rc != 0) return error.StreamCallbackFailed;

    var schema_owned = try c_data.importSchemaOwned(allocator, &c_schema);
    errdefer schema_owned.deinit();

    const top_level_type = DataType{ .struct_ = .{ .fields = schema_owned.schema.fields } };

    var out_batches = std.ArrayListUnmanaged(RecordBatch){};
    errdefer {
        for (out_batches.items) |*batch| {
            batch.deinit();
        }
        out_batches.deinit(allocator);
    }

    while (true) {
        var c_array = releasedArrowArray();
        const rc = c_stream.get_next.?(c_stream, &c_array);
        if (rc != 0) return error.StreamCallbackFailed;
        if (c_array.release == null) break; // EOS

        var top = try c_data.importArray(allocator, &top_level_type, &c_array);
        defer top.release();
        if (top.data().data_type != .struct_) return error.InvalidStream;

        const struct_view = struct_array_mod.StructArray{ .data = top.data() };
        if (struct_view.fieldCount() != schema_owned.schema.fields.len) return error.InvalidChildren;

        const cols = try allocator.alloc(ArrayRef, schema_owned.schema.fields.len);
        var cols_count: usize = 0;
        defer {
            var i: usize = 0;
            while (i < cols_count) : (i += 1) {
                var owned = cols[i];
                owned.release();
            }
            allocator.free(cols);
        }

        var i: usize = 0;
        while (i < cols.len) : (i += 1) {
            cols[i] = try struct_view.field(i);
            cols_count += 1;
        }

        const batch = try RecordBatch.initBorrowed(allocator, schema_owned.schema, cols);
        try out_batches.append(allocator, batch);
    }

    if (c_stream.release) |release_fn| release_fn(c_stream);

    return .{
        .allocator = allocator,
        .schema = schema_owned,
        .batches = try out_batches.toOwnedSlice(allocator),
    };
}

fn schemaCompatible(expected: *const Schema, actual: *const Schema) bool {
    if (expected.endianness != actual.endianness) return false;
    if (expected.fields.len != actual.fields.len) return false;
    for (expected.fields, actual.fields) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs.name, rhs.name)) return false;
        if (lhs.nullable != rhs.nullable) return false;
        if (!lhs.data_type.eql(rhs.data_type.*)) return false;
    }
    return true;
}

fn makeRecordBatchStructArray(allocator: std.mem.Allocator, schema: *const Schema, batch: *const RecordBatch) Error!ArrayRef {
    if (schema.fields.len != batch.numColumns()) return error.SchemaMismatch;

    const struct_dt = DataType{ .struct_ = .{ .fields = schema.fields } };
    const buffers = try allocator.alloc(SharedBuffer, 1);
    errdefer allocator.free(buffers);
    buffers[0] = SharedBuffer.empty;

    const children = try allocator.alloc(ArrayRef, batch.numColumns());
    var child_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < child_count) : (i += 1) {
            var owned = children[i];
            owned.release();
        }
        allocator.free(children);
    }

    var i: usize = 0;
    while (i < children.len) : (i += 1) {
        children[i] = batch.column(i).*.retain();
        child_count += 1;
    }

    const layout = ArrayData{
        .data_type = struct_dt,
        .length = batch.numRows(),
        .null_count = 0,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try layout.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, layout);
}

fn streamGetSchema(raw_stream: ?*ArrowArrayStream, raw_out_schema: ?*ArrowSchema) callconv(.c) c_int {
    if (raw_stream == null or raw_out_schema == null) return C_EINVAL;
    const stream = raw_stream.?;
    const out_schema = raw_out_schema.?;
    out_schema.* = releasedArrowSchema();
    if (stream.release == null) return C_EINVAL;

    const priv = streamPrivate(stream) orelse return C_EINVAL;
    clearLastError(priv);

    const out = c_data.exportSchema(priv.allocator, priv.schema_ref.schema().*) catch |err| {
        setLastErrorFromError(priv, err);
        return mapErrorCode(err);
    };
    out_schema.* = out;
    return C_OK;
}

fn streamGetNext(raw_stream: ?*ArrowArrayStream, raw_out_array: ?*ArrowArray) callconv(.c) c_int {
    if (raw_stream == null or raw_out_array == null) return C_EINVAL;
    const stream = raw_stream.?;
    const out_array = raw_out_array.?;
    out_array.* = releasedArrowArray();
    if (stream.release == null) return C_EINVAL;

    const priv = streamPrivate(stream) orelse return C_EINVAL;
    clearLastError(priv);

    if (priv.next_batch_index >= priv.batches.len) {
        return C_OK; // EOS
    }

    const batch = &priv.batches[priv.next_batch_index];
    var top = makeRecordBatchStructArray(priv.allocator, priv.schema_ref.schema(), batch) catch |err| {
        setLastErrorFromError(priv, err);
        return mapErrorCode(err);
    };
    defer top.release();

    const exported = c_data.exportArray(priv.allocator, top) catch |err| {
        setLastErrorFromError(priv, err);
        return mapErrorCode(err);
    };
    out_array.* = exported;
    priv.next_batch_index += 1;
    return C_OK;
}

fn streamGetLastError(raw_stream: ?*ArrowArrayStream) callconv(.c) [*c]const u8 {
    if (raw_stream == null) return null;
    const stream = raw_stream.?;
    if (stream.release == null) return null;
    const priv = streamPrivate(stream) orelse return null;
    if (!priv.has_last_error) return null;
    return @ptrCast(priv.last_error_buf[0..].ptr);
}

fn streamRelease(raw_stream: ?*ArrowArrayStream) callconv(.c) void {
    if (raw_stream == null) return;
    const stream = raw_stream.?;
    if (stream.release == null) return;

    const priv = streamPrivate(stream);
    stream.get_schema = null;
    stream.get_next = null;
    stream.get_last_error = null;
    stream.release = null;
    stream.private_data = null;
    if (priv == null) return;

    var schema_ref = priv.?.schema_ref;
    schema_ref.release();
    for (priv.?.batches) |*batch| {
        batch.deinit();
    }
    priv.?.allocator.free(priv.?.batches);
    priv.?.allocator.destroy(priv.?);
}

fn streamPrivate(stream: *ArrowArrayStream) ?*ExportedStreamPrivate {
    const ptr = stream.private_data orelse return null;
    return @ptrCast(@alignCast(ptr));
}

fn clearLastError(priv: *ExportedStreamPrivate) void {
    priv.has_last_error = false;
    priv.last_error_buf[0] = 0;
}

fn setLastError(priv: *ExportedStreamPrivate, msg: []const u8) void {
    const cap = priv.last_error_buf.len - 1;
    const n = @min(msg.len, cap);
    @memcpy(priv.last_error_buf[0..n], msg[0..n]);
    priv.last_error_buf[n] = 0;
    priv.has_last_error = true;
}

fn setLastErrorFromError(priv: *ExportedStreamPrivate, err: anyerror) void {
    setLastError(priv, @errorName(err));
}

fn mapErrorCode(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => C_ENOMEM,
        else => C_EINVAL,
    };
}

fn releasedArrowSchema() ArrowSchema {
    return .{
        .format = null,
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
}

fn releasedArrowArray() ArrowArray {
    return .{
        .length = 0,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 0,
        .n_children = 0,
        .buffers = null,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
}

test "c stream export callbacks yield schema, batch, and eos" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var builder = try array_mod.Int32Builder.init(allocator, 3);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    try builder.append(3);
    var ids = try builder.finish();
    defer ids.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{ids});
    defer batch.deinit();

    var stream = try exportRecordBatchStream(allocator, schema, &[_]RecordBatch{batch});
    defer if (stream.release) |release_fn| release_fn(&stream);

    var c_schema = releasedArrowSchema();
    try std.testing.expectEqual(@as(c_int, 0), stream.get_schema.?(&stream, &c_schema));
    var imported_schema = try c_data.importSchemaOwned(allocator, &c_schema);
    defer imported_schema.deinit();
    try std.testing.expectEqual(@as(usize, 1), imported_schema.schema.fields.len);

    var c_array = releasedArrowArray();
    try std.testing.expectEqual(@as(c_int, 0), stream.get_next.?(&stream, &c_array));
    try std.testing.expect(c_array.release != null);
    const top_type = DataType{ .struct_ = .{ .fields = schema.fields } };
    var imported = try c_data.importArray(allocator, &top_type, &c_array);
    defer imported.release();
    try std.testing.expectEqual(@as(usize, 3), imported.data().length);

    var eos = releasedArrowArray();
    try std.testing.expectEqual(@as(c_int, 0), stream.get_next.?(&stream, &eos));
    try std.testing.expect(eos.release == null);
}

test "c stream import drains stream into owned record batches" {
    const allocator = std.testing.allocator;

    const id_type = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var builder = try array_mod.Int32Builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(10);
    try builder.append(20);
    var ids = try builder.finish();
    defer ids.release();

    var batch = try RecordBatch.initBorrowed(allocator, schema, &[_]ArrayRef{ids});
    defer batch.deinit();

    var stream = try exportRecordBatchStream(allocator, schema, &[_]RecordBatch{batch});
    var owned = try importRecordBatchStreamOwned(allocator, &stream);
    defer owned.deinit();

    try std.testing.expect(stream.release == null);
    try std.testing.expectEqual(@as(usize, 1), owned.batches.len);
    try std.testing.expectEqual(@as(usize, 2), owned.batches[0].numRows());
    const ids_arr = array_mod.Int32Array{ .data = owned.batches[0].column(0).data() };
    try std.testing.expectEqual(@as(i32, 10), ids_arr.value(0));
    try std.testing.expectEqual(@as(i32, 20), ids_arr.value(1));
}
