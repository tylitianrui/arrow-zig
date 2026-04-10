const std = @import("std");
const array_data = @import("array_data.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const buffer = @import("../buffer.zig");

// Array view and builder for Arrow null type.

pub const ArrayData = array_data.ArrayData;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;
pub const SharedBuffer = buffer.SharedBuffer;
pub const DataType = datatype.DataType;

pub const NullArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: NullArray) usize {
        return self.data.length;
    }

    /// Every element in a null array is null by definition.
    pub fn isNull(self: NullArray, i: usize) bool {
        return self.data.isNull(i);
    }
};

pub const NullBuilder = struct {
    allocator: std.mem.Allocator,
    len: usize = 0,
    state: BuilderState = .ready,

    const Self = @This();
    pub const BuilderError = error{ AlreadyFinished, NotFinished };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, _: usize) !Self {
        return .{ .allocator = allocator };
    }

    /// Release resources owned by this instance.
    pub fn deinit(_: *Self) void {}

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.state = .ready;
    }

    /// Append one null logical value.
    pub fn appendNull(self: *Self) BuilderError!void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        self.len += 1;
    }

    /// Append multiple null logical values.
    pub fn appendNulls(self: *Self, count: usize) BuilderError!void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        self.len += count;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *Self) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;

        const buffers = try self.allocator.alloc(SharedBuffer, 0);
        const children = try self.allocator.alloc(ArrayRef, 0);

        const data = ArrayData{
            .data_type = DataType{ .null = {} },
            .length = self.len,
            .null_count = self.len,
            .buffers = buffers,
            .children = children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *Self) !ArrayRef {
        const finished_ref = try self.finish();
        try self.reset();
        return finished_ref;
    }
};

test "null builder appends nulls and supports reuse" {
    var builder = try NullBuilder.init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.appendNull();
    try builder.appendNulls(2);

    var first = try builder.finishReset();
    defer first.release();
    const first_arr = NullArray{ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 3), first_arr.len());
    try std.testing.expect(first_arr.isNull(0));
    try std.testing.expect(first_arr.isNull(2));
    try std.testing.expect(first.data().data_type == .null);
    try std.testing.expectEqual(@as(?usize, 3), first.data().null_count);

    try builder.appendNulls(2);
    var second = try builder.finish();
    defer second.release();
    const second_arr = NullArray{ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 2), second_arr.len());
    try std.testing.expect(second_arr.isNull(0));
    try std.testing.expect(second_arr.isNull(1));
}
