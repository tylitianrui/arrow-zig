const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

// FixedSizeBinary and FixedSizeList array views/builders.

const SharedBuffer = buffer.SharedBuffer;
const OwnedBuffer = buffer.OwnedBuffer;
const ArrayData = array_data.ArrayData;
const DataType = datatype.DataType;
pub const Field = datatype.Field;
const ArrayRef = array_ref.ArrayRef;
const BuilderState = builder_state.BuilderState;

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

pub const FixedSizeBinaryArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: FixedSizeBinaryArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: FixedSizeBinaryArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn byteWidth(self: FixedSizeBinaryArray) usize {
        return @intCast(self.data.data_type.fixed_size_binary.byte_width);
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: FixedSizeBinaryArray, i: usize) []const u8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        const width = self.byteWidth();
        const start = (self.data.offset + i) * width;
        const end = start + width;
        return self.data.buffers[1].data[start..end];
    }
};

pub const FixedSizeBinaryBuilder = struct {
    allocator: std.mem.Allocator,
    byte_width: usize,
    data: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    data_len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidByteWidth, InvalidValueWidth };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, byte_width: usize, capacity: usize) !FixedSizeBinaryBuilder {
        if (byte_width == 0) return BuilderError.InvalidByteWidth;
        _ = std.math.cast(i32, byte_width) orelse return BuilderError.InvalidByteWidth;
        return .{
            .allocator = allocator,
            .byte_width = byte_width,
            .data = try OwnedBuffer.init(allocator, byte_width * capacity),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *FixedSizeBinaryBuilder) void {
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *FixedSizeBinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *FixedSizeBinaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.data.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.data_len = 0;
        self.state = .ready;
    }

    fn ensureDataCapacity(self: *FixedSizeBinaryBuilder, needed_len: usize) !void {
        if (needed_len <= self.data.len()) return;
        try self.data.resize(needed_len);
    }

    fn ensureValidityForNull(self: *FixedSizeBinaryBuilder, new_len: usize) !void {
        if (self.validity == null) {
            var buf = try initValidityAllValid(self.allocator, new_len);
            bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
            self.validity = buf;
            self.null_count += 1;
            return;
        }
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, new_len);
        bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
        self.null_count += 1;
    }

    fn setValidBit(self: *FixedSizeBinaryBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    /// Append one logical value into the builder.
    pub fn append(self: *FixedSizeBinaryBuilder, value: []const u8) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (value.len != self.byte_width) return BuilderError.InvalidValueWidth;

        const next_len = self.len + 1;
        const next_data_len = self.data_len + self.byte_width;
        try self.ensureDataCapacity(next_data_len);
        @memcpy(self.data.data[self.data_len..next_data_len], value);

        try self.setValidBit(self.len);
        self.len = next_len;
        self.data_len = next_data_len;
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *FixedSizeBinaryBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;

        const next_len = self.len + 1;
        const next_data_len = self.data_len + self.byte_width;
        try self.ensureDataCapacity(next_data_len);
        @memset(self.data.data[self.data_len..next_data_len], 0);
        try self.ensureValidityForNull(next_len);

        self.len = next_len;
        self.data_len = next_data_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *FixedSizeBinaryBuilder) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.data.toShared(self.data_len);

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];

        const data = ArrayData{
            .data_type = DataType{ .fixed_size_binary = .{ .byte_width = std.math.cast(i32, self.byte_width) orelse return BuilderError.InvalidByteWidth } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *FixedSizeBinaryBuilder) !ArrayRef {
        const out = try self.finish();
        try self.reset();
        return out;
    }

    /// Finalize output and then clear builder state and buffers.
    pub fn finishClear(self: *FixedSizeBinaryBuilder) !ArrayRef {
        const out = try self.finish();
        try self.clear();
        return out;
    }
};

pub const FixedSizeListArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: FixedSizeListArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: FixedSizeListArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn valuesRef(self: FixedSizeListArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    pub fn listSize(self: FixedSizeListArray) usize {
        return @intCast(self.data.data_type.fixed_size_list.list_size);
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: FixedSizeListArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.children.len == 1);

        const list_size = self.listSize();
        const start = (self.data.offset + i) * list_size;
        return self.data.children[0].slice(start, list_size);
    }
};

pub const FixedSizeListBuilder = struct {
    allocator: std.mem.Allocator,
    value_field: Field,
    list_size: usize,
    validity: ?OwnedBuffer = null,
    buffers: [1]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    values_len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidListSize, InvalidChildLength, Overflow };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, value_field: Field, list_size: usize) !FixedSizeListBuilder {
        _ = std.math.cast(i32, list_size) orelse return BuilderError.InvalidListSize;
        return .{
            .allocator = allocator,
            .value_field = value_field,
            .list_size = list_size,
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *FixedSizeListBuilder) void {
        if (self.validity) |*valid| valid.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *FixedSizeListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *FixedSizeListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    fn ensureValidityForNull(self: *FixedSizeListBuilder, new_len: usize) !void {
        if (self.validity == null) {
            var buf = try initValidityAllValid(self.allocator, new_len);
            bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
            self.validity = buf;
            self.null_count += 1;
            return;
        }
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, new_len);
        bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
        self.null_count += 1;
    }

    fn setValidBit(self: *FixedSizeListBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    fn bumpValuesLen(self: *FixedSizeListBuilder) BuilderError!void {
        const added = std.math.mul(usize, 1, self.list_size) catch return BuilderError.Overflow;
        self.values_len = std.math.add(usize, self.values_len, added) catch return BuilderError.Overflow;
    }

    /// Append a non-null entry into the builder.
    pub fn appendValid(self: *FixedSizeListBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.setValidBit(self.len);
        self.len = next_len;
        try self.bumpValuesLen();
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *FixedSizeListBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
        try self.bumpValuesLen();
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *FixedSizeListBuilder, values: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (values.data().length != self.values_len) return BuilderError.InvalidChildLength;

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;

        const buffers = try self.allocator.alloc(SharedBuffer, 1);
        buffers[0] = self.buffers[0];

        const children = try self.allocator.alloc(ArrayRef, 1);
        children[0] = values.retain();

        const data = ArrayData{
            .data_type = DataType{ .fixed_size_list = .{ .value_field = self.value_field, .list_size = std.math.cast(i32, self.list_size) orelse return BuilderError.InvalidListSize } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .children = children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *FixedSizeListBuilder, values: ArrayRef) !ArrayRef {
        const out = try self.finish(values);
        try self.reset();
        return out;
    }

    /// Finalize output and then clear builder state and buffers.
    pub fn finishClear(self: *FixedSizeListBuilder, values: ArrayRef) !ArrayRef {
        const out = try self.finish(values);
        try self.clear();
        return out;
    }
};

test "fixed size binary array reads fixed-width values" {
    const dtype = DataType{ .fixed_size_binary = .{ .byte_width = 2 } };
    const bytes = [_]u8{ 'a', 'b', 'c', 'd' };
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(bytes[0..]) },
    };

    const array = FixedSizeBinaryArray{ .data = &data };
    try std.testing.expectEqualStrings("ab", array.value(0));
    try std.testing.expectEqualStrings("cd", array.value(1));
}

test "fixed size binary builder append and slice" {
    var builder = try FixedSizeBinaryBuilder.init(std.testing.allocator, 2, 2);
    defer builder.deinit();

    try builder.append("ab");
    try builder.appendNull();
    try builder.append("cd");

    var out = try builder.finishReset();
    defer out.release();

    const array = FixedSizeBinaryArray{ .data = out.data() };
    try std.testing.expectEqual(@as(usize, 3), array.len());
    try std.testing.expectEqualStrings("ab", array.value(0));
    try std.testing.expect(array.isNull(1));
    try std.testing.expectEqualStrings("cd", array.value(2));

    var sliced = try out.slice(1, 2);
    defer sliced.release();
    const sliced_array = FixedSizeBinaryArray{ .data = sliced.data() };
    try std.testing.expect(sliced_array.isNull(0));
    try std.testing.expectEqualStrings("cd", sliced_array.value(1));

    try builder.append("xy");
    var out2 = try builder.finishClear();
    defer out2.release();
    const array2 = FixedSizeBinaryArray{ .data = out2.data() };
    try std.testing.expectEqual(@as(usize, 1), array2.len());
    try std.testing.expectEqualStrings("xy", array2.value(0));
}

test "fixed size binary builder rejects invalid width" {
    var builder = try FixedSizeBinaryBuilder.init(std.testing.allocator, 3, 1);
    defer builder.deinit();
    try std.testing.expectError(error.InvalidValueWidth, builder.append("ab"));
}

test "fixed size list array value slices child" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 4);
    defer child_builder.deinit();
    try child_builder.append(1);
    try child_builder.append(2);
    try child_builder.append(3);
    try child_builder.append(4);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    const buffers = &[_]SharedBuffer{SharedBuffer.empty};
    const children = &[_]ArrayRef{child_ref};
    const data = ArrayData{
        .data_type = DataType{ .fixed_size_list = .{ .value_field = value_field, .list_size = 2 } },
        .length = 2,
        .buffers = buffers,
        .children = children,
    };

    const array = FixedSizeListArray{ .data = &data };
    var first = try array.value(0);
    defer first.release();
    const first_view = @import("primitive_array.zig").PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 2), first_view.len());
    try std.testing.expectEqual(@as(i32, 1), first_view.value(0));
    try std.testing.expectEqual(@as(i32, 2), first_view.value(1));
}

test "fixed size list builder finish validates child length" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var builder = try FixedSizeListBuilder.init(allocator, value_field, 2);
    defer builder.deinit();
    try builder.appendValid();

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    try std.testing.expectError(error.InvalidChildLength, builder.finish(child_ref));
}

test "fixed size list builder supports reset clear and nulls" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var builder = try FixedSizeListBuilder.init(allocator, value_field, 2);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendNull();

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 4);
    defer child_builder.deinit();
    try child_builder.append(10);
    try child_builder.append(11);
    try child_builder.append(12);
    try child_builder.append(13);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    var out = try builder.finishReset(child_ref);
    defer out.release();

    const array = FixedSizeListArray{ .data = out.data() };
    try std.testing.expectEqual(@as(usize, 2), array.len());
    try std.testing.expect(array.isNull(1));

    try builder.appendValid();
    var child_builder2 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer child_builder2.deinit();
    try child_builder2.append(20);
    try child_builder2.append(21);
    var child_ref2 = try child_builder2.finish();
    defer child_ref2.release();

    var out2 = try builder.finishClear(child_ref2);
    defer out2.release();
    const array2 = FixedSizeListArray{ .data = out2.data() };
    try std.testing.expectEqual(@as(usize, 1), array2.len());
}
