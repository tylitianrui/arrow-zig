const std = @import("std");
const schema_mod = @import("schema.zig");
const record_batch_mod = @import("record_batch.zig");

pub const Schema = schema_mod.Schema;
pub const SchemaRef = schema_mod.SchemaRef;
pub const RecordBatch = record_batch_mod.RecordBatch;

/// Unified stream reader interface for producing RecordBatch values.
pub const RecordBatchReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        schema: *const fn (ptr: *anyopaque) *const Schema,
        read_next: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!?RecordBatch,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    const Self = @This();

    pub fn schema(self: Self) *const Schema {
        return self.vtable.schema(self.ptr);
    }

    pub fn readNext(self: Self, allocator: std.mem.Allocator) anyerror!?RecordBatch {
        return self.vtable.read_next(self.ptr, allocator);
    }

    pub fn deinit(self: *Self) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }

    /// Drain the stream and return all record batches.
    /// Caller owns each batch in the returned slice and must call `deinit`.
    pub fn readAll(self: Self, allocator: std.mem.Allocator) anyerror![]RecordBatch {
        var out: std.ArrayListUnmanaged(RecordBatch) = .{};
        defer out.deinit(allocator);
        errdefer {
            for (out.items) |*batch| {
                batch.deinit();
            }
        }

        while (try self.readNext(allocator)) |batch| {
            try out.append(allocator, batch);
        }

        return out.toOwnedSlice(allocator);
    }

    /// Wrap an owned reader that exposes:
    /// - `readSchema(self: *ReaderType) !Schema`
    /// - `nextRecordBatch(self: *ReaderType) !?RecordBatch`
    /// - `deinit(self: *ReaderType) void`
    pub fn fromOwnedReader(allocator: std.mem.Allocator, reader: anytype) anyerror!Self {
        const ReaderType = @TypeOf(reader);

        const Adapter = struct {
            const State = struct {
                allocator: std.mem.Allocator,
                reader: ReaderType,
                schema_ref: SchemaRef,
            };

            fn getSchema(ptr: *anyopaque) *const Schema {
                const state: *State = @ptrCast(@alignCast(ptr));
                return state.schema_ref.schema();
            }

            fn readNextImpl(ptr: *anyopaque, allocator_arg: std.mem.Allocator) anyerror!?RecordBatch {
                _ = allocator_arg;
                const state: *State = @ptrCast(@alignCast(ptr));
                return state.reader.nextRecordBatch();
            }

            fn deinitImpl(ptr: *anyopaque) void {
                const state: *State = @ptrCast(@alignCast(ptr));
                state.schema_ref.release();
                state.reader.deinit();
                state.allocator.destroy(state);
            }

            const vtable: VTable = .{
                .schema = getSchema,
                .read_next = readNextImpl,
                .deinit = deinitImpl,
            };
        };

        const state = try allocator.create(Adapter.State);
        state.allocator = allocator;
        state.reader = reader;
        errdefer allocator.destroy(state);
        errdefer state.reader.deinit();

        const read_schema = try state.reader.readSchema();
        state.schema_ref = try SchemaRef.fromBorrowed(allocator, read_schema);

        return .{
            .ptr = state,
            .vtable = &Adapter.vtable,
        };
    }
};

test "record batch reader wraps owned reader and streams batches" {
    const allocator = std.testing.allocator;

    const dt = @import("datatype.zig").DataType{ .int32 = {} };
    const fields = [_]@import("datatype.zig").Field{
        .{ .name = "value", .data_type = &dt, .nullable = false },
    };
    const static_schema = Schema{ .fields = fields[0..] };

    const MockReader = struct {
        allocator: std.mem.Allocator,
        emitted: bool = false,

        pub fn readSchema(self: *@This()) !Schema {
            _ = self;
            return static_schema;
        }

        pub fn nextRecordBatch(self: *@This()) !?RecordBatch {
            if (self.emitted) return null;
            self.emitted = true;

            var builder = try @import("array/array.zig").Int32Builder.init(self.allocator, 2);
            defer builder.deinit();
            try builder.append(7);
            try builder.append(9);
            var arr = try builder.finish();
            defer arr.release();

            return try RecordBatch.initBorrowed(self.allocator, static_schema, &[_]@import("array/array.zig").ArrayRef{arr});
        }

        pub fn deinit(self: *@This()) void {
            _ = self;
        }
    };

    var reader = try RecordBatchReader.fromOwnedReader(allocator, MockReader{ .allocator = allocator });
    defer reader.deinit();

    try std.testing.expectEqualStrings("value", reader.schema().fields[0].name);

    var batches = try reader.readAll(allocator);
    defer {
        for (batches) |*batch| {
            batch.deinit();
        }
        allocator.free(batches);
    }

    try std.testing.expectEqual(@as(usize, 1), batches.len);
    try std.testing.expectEqual(@as(usize, 2), batches[0].numRows());
}
