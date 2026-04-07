const std = @import("std");
const schema_mod = @import("schema.zig");
const array = @import("array/array.zig");

// Tabular wrapper around aligned Arrow columns plus a schema contract.

pub const Schema = schema_mod.Schema;
pub const Field = schema_mod.Field;
pub const ArrayRef = array.ArrayRef;
pub const ArrayData = array.ArrayData;

pub const RecordBatchError = error{
    InvalidColumnCount,
    InvalidColumnLength,
    InvalidColumnType,
    NullInNonNullableField,
    SliceOutOfBounds,
};

pub const RecordBatchBuilderError = error{
    AlreadyFinished,
    NotFinished,
    InvalidColumnIndex,
    UnknownField,
    ColumnAlreadySet,
    MissingColumn,
};

pub const RecordBatch = struct {
    allocator: std.mem.Allocator,
    schema: Schema,
    columns: []ArrayRef,
    num_rows: usize,

    const Self = @This();

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, schema: Schema, columns: []const ArrayRef) !Self {
        if (schema.fields.len != columns.len) return RecordBatchError.InvalidColumnCount;

        const num_rows = if (columns.len == 0) 0 else columns[0].data().length;
        for (columns, 0..) |col_ref, i| {
            const data = col_ref.data();
            try data.validateLayout();
            if (data.length != num_rows) return RecordBatchError.InvalidColumnLength;
            if (!std.meta.eql(data.data_type, schema.fields[i].data_type.*)) return RecordBatchError.InvalidColumnType;
            if (!schema.fields[i].nullable and data.hasNulls()) return RecordBatchError.NullInNonNullableField;
        }

        const owned_columns = try allocator.alloc(ArrayRef, columns.len);
        errdefer allocator.free(owned_columns);
        for (columns, 0..) |col_ref, i| {
            owned_columns[i] = col_ref.retain();
        }

        return .{
            .allocator = allocator,
            .schema = schema,
            .columns = owned_columns,
            .num_rows = num_rows,
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *Self) void {
        for (self.columns) |col_ref| {
            var owned = col_ref;
            owned.release();
        }
        self.allocator.free(self.columns);
    }

    /// Execute numRows logic for this type.
    pub fn numRows(self: Self) usize {
        return self.num_rows;
    }

    /// Execute numColumns logic for this type.
    pub fn numColumns(self: Self) usize {
        return self.columns.len;
    }

    /// Execute column logic for this type.
    pub fn column(self: *const Self, index: usize) *const ArrayRef {
        std.debug.assert(index < self.columns.len);
        return &self.columns[index];
    }

    /// Execute columnByName logic for this type.
    pub fn columnByName(self: *const Self, name: []const u8) ?*const ArrayRef {
        for (self.schema.fields, 0..) |field, i| {
            if (std.mem.eql(u8, field.name, name)) return &self.columns[i];
        }
        return null;
    }

    /// Create a logical slice view over the current value.
    pub fn slice(self: *const Self, offset: usize, length: usize) !Self {
        if (offset > self.num_rows or offset + length > self.num_rows) return RecordBatchError.SliceOutOfBounds;

        const sliced_columns = try self.allocator.alloc(ArrayRef, self.columns.len);
        var count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < count) : (i += 1) {
                sliced_columns[i].release();
            }
            self.allocator.free(sliced_columns);
        }

        for (self.columns, 0..) |column_ref, i| {
            sliced_columns[i] = try column_ref.slice(offset, length);
            count += 1;
        }

        return .{
            .allocator = self.allocator,
            .schema = self.schema,
            .columns = sliced_columns,
            .num_rows = length,
        };
    }
};

pub const RecordBatchBuilder = struct {
    allocator: std.mem.Allocator,
    schema: Schema,
    columns: []?ArrayRef,
    finished: bool = false,

    const Self = @This();

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, schema: Schema) !Self {
        const slots = try allocator.alloc(?ArrayRef, schema.fields.len);
        @memset(slots, null);
        return .{
            .allocator = allocator,
            .schema = schema,
            .columns = slots,
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *Self) void {
        self.releaseColumns();
        self.allocator.free(self.columns);
    }

    /// Execute releaseColumns logic for this type.
    fn releaseColumns(self: *Self) void {
        for (self.columns, 0..) |slot, i| {
            if (slot) |col_ref| {
                var owned = col_ref;
                owned.release();
                self.columns[i] = null;
            }
        }
    }

    /// Execute setColumn logic for this type.
    pub fn setColumn(self: *Self, index: usize, column_ref: ArrayRef) RecordBatchBuilderError!void {
        if (self.finished) return RecordBatchBuilderError.AlreadyFinished;
        if (index >= self.columns.len) return RecordBatchBuilderError.InvalidColumnIndex;
        if (self.columns[index] != null) return RecordBatchBuilderError.ColumnAlreadySet;
        self.columns[index] = column_ref.retain();
    }

    /// Execute setColumnByName logic for this type.
    pub fn setColumnByName(self: *Self, name: []const u8, column_ref: ArrayRef) RecordBatchBuilderError!void {
        if (self.finished) return RecordBatchBuilderError.AlreadyFinished;
        for (self.schema.fields, 0..) |field, i| {
            if (std.mem.eql(u8, field.name, name)) return self.setColumn(i, column_ref);
        }
        return RecordBatchBuilderError.UnknownField;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *Self) !RecordBatch {
        if (self.finished) return RecordBatchBuilderError.AlreadyFinished;

        const refs = try self.allocator.alloc(ArrayRef, self.columns.len);
        defer self.allocator.free(refs);

        for (self.columns, 0..) |slot, i| {
            const col_ref = slot orelse return RecordBatchBuilderError.MissingColumn;
            refs[i] = col_ref;
        }

        const batch = try RecordBatch.init(self.allocator, self.schema, refs);
        // The resulting batch now owns retained column refs, so the builder
        // should not keep additional holds after a successful finish.
        self.releaseColumns();
        self.finished = true;
        return batch;
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *Self) RecordBatchBuilderError!void {
        // Reset keeps capacity and can be called in any state.
        self.releaseColumns();
        self.finished = false;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *Self) RecordBatchBuilderError!void {
        // Clear is a hard cleanup operation and is allowed even before finish.
        self.releaseColumns();
        self.finished = false;
    }
};

test "record batch init and accessors" {
    const allocator = std.testing.allocator;

    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const bool_type = @import("datatype.zig").DataType{ .bool = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "ok", .data_type = &bool_type, .nullable = false },
    };

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(1);
    try int_builder.append(2);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try @import("array/boolean_array.zig").BooleanBuilder.init(allocator, 2);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    try bool_builder.append(false);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var batch = try RecordBatch.init(allocator, .{ .fields = fields[0..] }, &[_]ArrayRef{ int_ref, bool_ref });
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), batch.numRows());
    try std.testing.expectEqual(@as(usize, 2), batch.numColumns());
    try std.testing.expect(batch.columnByName("id") != null);
    try std.testing.expect(batch.columnByName("missing") == null);
}

test "record batch rejects mismatched column count" {
    const allocator = std.testing.allocator;

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer int_builder.deinit();
    try int_builder.append(1);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    try std.testing.expectError(
        RecordBatchError.InvalidColumnCount,
        RecordBatch.init(allocator, .{ .fields = &[_]Field{} }, &[_]ArrayRef{int_ref}),
    );
}

test "record batch rejects nullable violation" {
    const allocator = std.testing.allocator;

    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer int_builder.deinit();
    try int_builder.appendNull();
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    try std.testing.expectError(
        RecordBatchError.NullInNonNullableField,
        RecordBatch.init(allocator, .{ .fields = fields[0..] }, &[_]ArrayRef{int_ref}),
    );
}

test "record batch rejects mismatched column length" {
    const allocator = std.testing.allocator;

    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &int_type, .nullable = true },
    };

    var a_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 2);
    defer a_builder.deinit();
    try a_builder.append(1);
    try a_builder.append(2);
    var a_ref = try a_builder.finish();
    defer a_ref.release();

    var b_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer b_builder.deinit();
    try b_builder.append(9);
    var b_ref = try b_builder.finish();
    defer b_ref.release();

    try std.testing.expectError(
        RecordBatchError.InvalidColumnLength,
        RecordBatch.init(allocator, .{ .fields = fields[0..] }, &[_]ArrayRef{ a_ref, b_ref }),
    );
}

test "record batch slice returns sliced columns" {
    const allocator = std.testing.allocator;

    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 4);
    defer builder.deinit();
    try builder.append(10);
    try builder.append(20);
    try builder.append(30);
    try builder.append(40);
    var col_ref = try builder.finish();
    defer col_ref.release();

    var batch = try RecordBatch.init(allocator, .{ .fields = fields[0..] }, &[_]ArrayRef{col_ref});
    defer batch.deinit();

    var sliced = try batch.slice(1, 2);
    defer sliced.deinit();

    try std.testing.expectEqual(@as(usize, 2), sliced.numRows());
    try std.testing.expectEqual(@as(usize, 1), sliced.numColumns());

    const view = @import("array/primitive_array.zig").PrimitiveArray(i32){ .data = sliced.column(0).data() };
    try std.testing.expectEqual(@as(usize, 2), view.len());
    try std.testing.expectEqual(@as(i32, 20), view.value(0));
    try std.testing.expectEqual(@as(i32, 30), view.value(1));
}

test "record batch slice rejects out of bounds" {
    const allocator = std.testing.allocator;

    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var col_ref = try builder.finish();
    defer col_ref.release();

    var batch = try RecordBatch.init(allocator, .{ .fields = fields[0..] }, &[_]ArrayRef{col_ref});
    defer batch.deinit();

    try std.testing.expectError(RecordBatchError.SliceOutOfBounds, batch.slice(3, 1));
    try std.testing.expectError(RecordBatchError.SliceOutOfBounds, batch.slice(1, 2));
}

test "record batch builder builds batch" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const bool_type = @import("datatype.zig").DataType{ .bool = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "ok", .data_type = &bool_type, .nullable = false },
    };

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(1);
    try int_builder.append(2);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try @import("array/boolean_array.zig").BooleanBuilder.init(allocator, 2);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    try bool_builder.append(false);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var builder = try RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer builder.deinit();

    try builder.setColumn(0, int_ref);
    try builder.setColumnByName("ok", bool_ref);

    var batch = try builder.finish();
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), batch.numRows());
    try std.testing.expectEqual(@as(usize, 2), batch.numColumns());
}

test "record batch builder rejects missing column" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const bool_type = @import("datatype.zig").DataType{ .bool = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "ok", .data_type = &bool_type, .nullable = false },
    };

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer int_builder.deinit();
    try int_builder.append(1);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var builder = try RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer builder.deinit();
    try builder.setColumnByName("id", int_ref);

    try std.testing.expectError(RecordBatchBuilderError.MissingColumn, builder.finish());
}

test "record batch builder reset allows reuse" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var a_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer a_builder.deinit();
    try a_builder.append(7);
    var a_ref = try a_builder.finish();
    defer a_ref.release();

    var b_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer b_builder.deinit();
    try b_builder.append(9);
    var b_ref = try b_builder.finish();
    defer b_ref.release();

    var builder = try RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer builder.deinit();

    try builder.setColumn(0, a_ref);
    var first = try builder.finish();
    defer first.deinit();

    try builder.reset();
    try builder.setColumn(0, b_ref);
    var second = try builder.finish();
    defer second.deinit();

    try std.testing.expectEqual(@as(usize, 1), second.numRows());
}

test "record batch builder clear before finish releases slots" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer int_builder.deinit();
    try int_builder.append(5);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var builder = try RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer builder.deinit();

    try builder.setColumn(0, int_ref);
    try builder.clear();
    try builder.setColumn(0, int_ref);
    var batch = try builder.finish();
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 1), batch.numRows());
}

test "record batch builder reset before finish releases slots" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &int_type, .nullable = false }};

    var int_builder = try @import("array/primitive_array.zig").PrimitiveBuilder(i32, @import("datatype.zig").DataType{ .int32 = {} }).init(allocator, 1);
    defer int_builder.deinit();
    try int_builder.append(8);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var builder = try RecordBatchBuilder.init(allocator, .{ .fields = fields[0..] });
    defer builder.deinit();

    try builder.setColumn(0, int_ref);
    try builder.reset();
    try builder.setColumn(0, int_ref);
    var batch = try builder.finish();
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 1), batch.numRows());
}
