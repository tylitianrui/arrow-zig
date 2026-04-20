const std = @import("std");
const schema_mod = @import("schema.zig");
const chunked_array_mod = @import("chunked_array.zig");

pub const Schema = schema_mod.Schema;
pub const SchemaRef = schema_mod.SchemaRef;
pub const Field = schema_mod.Field;
pub const KeyValue = schema_mod.KeyValue;
pub const ChunkedArray = chunked_array_mod.ChunkedArray;

pub const Error = error{
    OutOfMemory,
    InvalidColumnCount,
    InvalidColumnLength,
    InvalidColumnType,
    UnknownField,
    SliceOutOfBounds,
};

pub const Table = struct {
    allocator: std.mem.Allocator,
    schema_ref: SchemaRef,
    columns: []ChunkedArray,
    num_rows: usize,

    const Self = @This();

    /// Initialize and return a new table instance.
    /// Takes ownership of `schema_ref` on success.
    pub fn init(allocator: std.mem.Allocator, schema_ref: SchemaRef, columns: []const ChunkedArray) Error!Self {
        const sc = schema_ref.schema();
        if (sc.fields.len != columns.len) return error.InvalidColumnCount;

        const num_rows = if (columns.len == 0) 0 else columns[0].len();
        for (columns, 0..) |col, i| {
            if (col.len() != num_rows) return error.InvalidColumnLength;
            if (!col.dataType().eql(sc.fields[i].data_type.*)) return error.InvalidColumnType;
        }

        const owned_columns = try allocator.alloc(ChunkedArray, columns.len);
        var owned_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < owned_count) : (i += 1) {
                owned_columns[i].release();
            }
            allocator.free(owned_columns);
        }

        for (columns, 0..) |col, i| {
            owned_columns[i] = col.retain();
            owned_count += 1;
        }

        return .{
            .allocator = allocator,
            .schema_ref = schema_ref,
            .columns = owned_columns,
            .num_rows = num_rows,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.columns) |*col| {
            col.release();
        }
        self.allocator.free(self.columns);
        self.schema_ref.release();
    }

    pub fn schema(self: *const Self) *const Schema {
        return self.schema_ref.schema();
    }

    pub fn numRows(self: Self) usize {
        return self.num_rows;
    }

    pub fn numColumns(self: Self) usize {
        return self.columns.len;
    }

    pub fn column(self: *const Self, index: usize) *const ChunkedArray {
        std.debug.assert(index < self.columns.len);
        return &self.columns[index];
    }

    pub fn columnByName(self: *const Self, name: []const u8) ?*const ChunkedArray {
        const index = self.schema_ref.fieldIndex(name) orelse return null;
        if (index >= self.columns.len) return null;
        return &self.columns[index];
    }

    pub fn slice(self: *const Self, offset: usize, length: usize) Error!Self {
        if (offset > self.num_rows) return error.SliceOutOfBounds;
        const end = std.math.add(usize, offset, length) catch return error.SliceOutOfBounds;
        if (end > self.num_rows) return error.SliceOutOfBounds;

        const sliced_columns = try self.allocator.alloc(ChunkedArray, self.columns.len);
        var count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < count) : (i += 1) {
                sliced_columns[i].release();
            }
            self.allocator.free(sliced_columns);
        }

        for (self.columns, 0..) |col, i| {
            sliced_columns[i] = col.slice(self.allocator, offset, length) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.SliceOutOfBounds => return error.SliceOutOfBounds,
                error.InvalidChunkType => return error.InvalidColumnType,
                error.LengthOverflow => return error.InvalidColumnLength,
            };
            count += 1;
        }

        return .{
            .allocator = self.allocator,
            .schema_ref = self.schema_ref.retain(),
            .columns = sliced_columns,
            .num_rows = length,
        };
    }

    pub fn select(self: *const Self, field_names: []const []const u8) Error!Self {
        const selected_fields = try self.allocator.alloc(Field, field_names.len);
        defer self.allocator.free(selected_fields);

        const selected_columns = try self.allocator.alloc(ChunkedArray, field_names.len);
        defer self.allocator.free(selected_columns);

        for (field_names, 0..) |name, i| {
            const idx = self.findFieldIndex(name) orelse return error.UnknownField;
            selected_fields[i] = self.schema_ref.schema().fields[idx];
            selected_columns[i] = self.columns[idx];
        }

        var projected_ref = try SchemaRef.fromBorrowed(self.allocator, .{
            .fields = selected_fields,
            .endianness = self.schema_ref.schema().endianness,
            .metadata = self.schema_ref.schema().metadata,
        });
        errdefer projected_ref.release();

        return try Self.init(self.allocator, projected_ref, selected_columns);
    }

    fn findFieldIndex(self: *const Self, name: []const u8) ?usize {
        for (self.schema_ref.schema().fields, 0..) |field, i| {
            if (std.mem.eql(u8, field.name, name)) return i;
        }
        return null;
    }
};

test "table init and accessors" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const schema_fields = [_]Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &int_type, .nullable = true },
    };

    const schema_ref = try SchemaRef.fromBorrowed(allocator, .{ .fields = schema_fields[0..] });

    var b1 = try @import("array/array.zig").Int32Builder.init(allocator, 2);
    defer b1.deinit();
    try b1.append(1);
    try b1.append(2);
    var a1 = try b1.finish();
    defer a1.release();

    var b2 = try @import("array/array.zig").Int32Builder.init(allocator, 2);
    defer b2.deinit();
    try b2.append(3);
    try b2.append(4);
    var a2 = try b2.finish();
    defer a2.release();

    var c1 = try ChunkedArray.fromSingle(allocator, a1);
    defer c1.release();
    var c2 = try ChunkedArray.fromSingle(allocator, a2);
    defer c2.release();

    var table = try Table.init(allocator, schema_ref, &[_]ChunkedArray{ c1, c2 });
    defer table.deinit();

    try std.testing.expectEqual(@as(usize, 2), table.numRows());
    try std.testing.expectEqual(@as(usize, 2), table.numColumns());
    try std.testing.expect(table.columnByName("a") != null);
    try std.testing.expect(table.columnByName("missing") == null);
}

test "table slice keeps schema and rows" {
    const allocator = std.testing.allocator;
    const int_type = @import("datatype.zig").DataType{ .int32 = {} };
    const schema_fields = [_]Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
    };

    const schema_ref = try SchemaRef.fromBorrowed(allocator, .{ .fields = schema_fields[0..] });

    var b1 = try @import("array/array.zig").Int32Builder.init(allocator, 4);
    defer b1.deinit();
    try b1.append(10);
    try b1.append(11);
    try b1.append(12);
    try b1.append(13);
    var a1 = try b1.finish();
    defer a1.release();

    var chunked = try ChunkedArray.fromSingle(allocator, a1);
    defer chunked.release();

    var table = try Table.init(allocator, schema_ref, &[_]ChunkedArray{chunked});
    defer table.deinit();

    var sliced = try table.slice(1, 2);
    defer sliced.deinit();

    try std.testing.expectEqual(@as(usize, 2), sliced.numRows());
    try std.testing.expectEqual(@as(usize, 1), sliced.numColumns());
}
