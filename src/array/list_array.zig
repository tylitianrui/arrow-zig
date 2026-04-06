const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;

fn initValidityAllValid(allocator: std.mem.Allocator, bit_len: usize) !OwnedBuffer {
    const used_bytes = bitmap.byteLength(bit_len);
    var buf = try OwnedBuffer.init(allocator, used_bytes);
    if (used_bytes > 0) {
        @memset(buf.data[0..used_bytes], 0xFF);
        const remainder = bit_len & 7;
        if (remainder != 0) {
            const keep_mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
            buf.data[used_bytes - 1] &= keep_mask;
        }
    }
    return buf;
}

fn ensureBitmapCapacity(buf: *OwnedBuffer, bit_len: usize) !void {
    const needed = bitmap.byteLength(bit_len);
    if (needed <= buf.len()) return;
    try buf.resize(needed);
}

pub const ListArray = struct {
    data: *const ArrayData,

    pub fn len(self: ListArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: ListArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn valuesRef(self: ListArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    pub fn value(self: ListArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        std.debug.assert(self.data.children.len == 1);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const base = self.data.offset + i;
        const start: usize = @intCast(offsets[base]);
        const end: usize = @intCast(offsets[base + 1]);
        return self.data.children[0].slice(start, end - start);
    }
};

pub const LargeListArray = struct {
    data: *const ArrayData,

    pub fn len(self: LargeListArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: LargeListArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn valuesRef(self: LargeListArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    pub fn value(self: LargeListArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        std.debug.assert(self.data.children.len == 1);

        const offsets = self.data.buffers[1].typedSlice(i64);
        const base = self.data.offset + i;
        const start: usize = @intCast(offsets[base]);
        const end: usize = @intCast(offsets[base + 1]);
        return self.data.children[0].slice(start, end - start);
    }
};

pub const ListBuilder = struct {
    allocator: std.mem.Allocator,
    value_field: Field,
    offsets: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: isize = 0,
    values_len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow, InvalidChildLength };

    pub fn init(allocator: std.mem.Allocator, capacity: usize, value_field: Field) !ListBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i32));
        const offsets_slice = std.mem.bytesAsSlice(i32, offsets.data);
        offsets_slice[0] = 0;
        return .{
            .allocator = allocator,
            .value_field = value_field,
            .offsets = offsets,
        };
    }

    pub fn deinit(self: *ListBuilder) void {
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *ListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (!self.offsets.isEmpty()) {
            const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
            offsets_slice[0] = 0;
        }
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    pub fn clear(self: *ListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    fn ensureOffsetsCapacity(self: *ListBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i32);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i32));
        if (was_empty) {
            const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
            offsets_slice[0] = 0;
        }
    }

    fn ensureValidityForNull(self: *ListBuilder, new_len: usize) !void {
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

    fn setValidBit(self: *ListBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn appendLen(self: *ListBuilder, value_len: usize) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);

        const next_offset = self.values_len + value_len;
        const cast_offset = std.math.cast(i32, next_offset) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = cast_offset;

        try self.setValidBit(self.len);
        self.len = next_len;
        self.values_len = next_offset;
    }

    pub fn appendNull(self: *ListBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const offsets_slice = std.mem.bytesAsSlice(i32, self.offsets.data);
        offsets_slice[next_len] = std.math.cast(i32, self.values_len) orelse return BuilderError.OffsetOverflow;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *ListBuilder, values: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (values.data().length != self.values_len) return BuilderError.InvalidChildLength;

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(i32));

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        var filled: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < filled) : (i += 1) {
                var owned = buffers[i];
                owned.release();
            }
            self.allocator.free(buffers);
        }
        buffers[0] = self.buffers[0];
        filled = 1;
        buffers[1] = self.buffers[1];
        filled = 2;

        const children = try self.allocator.alloc(ArrayRef, 1);
        errdefer self.allocator.free(children);
        children[0] = values.retain();
        errdefer children[0].release();

        const data = ArrayData{
            .data_type = DataType{ .list = .{ .value_field = self.value_field } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .children = children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    pub fn finishReset(self: *ListBuilder, values: ArrayRef) !ArrayRef {
        const ref = try self.finish(values);
        try self.reset();
        return ref;
    }
};

pub const LargeListBuilder = struct {
    allocator: std.mem.Allocator,
    value_field: Field,
    offsets: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: isize = 0,
    values_len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow, InvalidChildLength };

    pub fn init(allocator: std.mem.Allocator, capacity: usize, value_field: Field) !LargeListBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i64));
        const offsets_slice = std.mem.bytesAsSlice(i64, offsets.data);
        offsets_slice[0] = 0;
        return .{
            .allocator = allocator,
            .value_field = value_field,
            .offsets = offsets,
        };
    }

    pub fn deinit(self: *LargeListBuilder) void {
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *LargeListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (!self.offsets.isEmpty()) {
            const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
            offsets_slice[0] = 0;
        }
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    pub fn clear(self: *LargeListBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    fn ensureOffsetsCapacity(self: *LargeListBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i64);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i64));
        if (was_empty) {
            const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
            offsets_slice[0] = 0;
        }
    }

    fn ensureValidityForNull(self: *LargeListBuilder, new_len: usize) !void {
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

    fn setValidBit(self: *LargeListBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn appendLen(self: *LargeListBuilder, value_len: usize) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);

        const next_offset = self.values_len + value_len;
        const cast_offset = std.math.cast(i64, next_offset) orelse return BuilderError.OffsetOverflow;
        const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
        offsets_slice[next_len] = cast_offset;

        try self.setValidBit(self.len);
        self.len = next_len;
        self.values_len = next_offset;
    }

    pub fn appendNull(self: *LargeListBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const offsets_slice = std.mem.bytesAsSlice(i64, self.offsets.data);
        offsets_slice[next_len] = std.math.cast(i64, self.values_len) orelse return BuilderError.OffsetOverflow;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *LargeListBuilder, values: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (values.data().length != self.values_len) return BuilderError.InvalidChildLength;

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(i64));

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        var filled: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < filled) : (i += 1) {
                var owned = buffers[i];
                owned.release();
            }
            self.allocator.free(buffers);
        }
        buffers[0] = self.buffers[0];
        filled = 1;
        buffers[1] = self.buffers[1];
        filled = 2;

        const children = try self.allocator.alloc(ArrayRef, 1);
        errdefer self.allocator.free(children);
        children[0] = values.retain();
        errdefer children[0].release();

        const data = ArrayData{
            .data_type = DataType{ .large_list = .{ .value_field = self.value_field } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .children = children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    pub fn finishReset(self: *LargeListBuilder, values: ArrayRef) !ArrayRef {
        const ref = try self.finish(values);
        try self.reset();
        return ref;
    }
};

test "list array reads values" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 4);
    defer values_builder.deinit();
    try values_builder.append(1);
    try values_builder.append(2);
    try values_builder.append(3);
    try values_builder.append(4);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try ListBuilder.init(allocator, 3, field);
    defer builder.deinit();
    try builder.appendLen(2);
    try builder.appendNull();
    try builder.appendLen(2);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = ListArray{ .data = list_ref.data() };

    try std.testing.expectEqual(@as(usize, 3), list.len());
    try std.testing.expect(!list.isNull(0));
    try std.testing.expect(list.isNull(1));

    var first = try list.value(0);
    defer first.release();
    const first_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 2), first_values.len());
    try std.testing.expectEqual(@as(i32, 1), first_values.value(0));
    try std.testing.expectEqual(@as(i32, 2), first_values.value(1));

    var third = try list.value(2);
    defer third.release();
    const third_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = third.data() };
    try std.testing.expectEqual(@as(usize, 2), third_values.len());
    try std.testing.expectEqual(@as(i32, 3), third_values.value(0));
    try std.testing.expectEqual(@as(i32, 4), third_values.value(1));
}

test "large list array reads values" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(5);
    try values_builder.append(6);
    try values_builder.append(7);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try LargeListBuilder.init(allocator, 2, field);
    defer builder.deinit();
    try builder.appendLen(1);
    try builder.appendLen(2);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = LargeListArray{ .data = list_ref.data() };

    try std.testing.expectEqual(@as(usize, 2), list.len());

    var first = try list.value(0);
    defer first.release();
    const first_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 1), first_values.len());
    try std.testing.expectEqual(@as(i32, 5), first_values.value(0));

    var second = try list.value(1);
    defer second.release();
    const second_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 2), second_values.len());
    try std.testing.expectEqual(@as(i32, 6), second_values.value(0));
    try std.testing.expectEqual(@as(i32, 7), second_values.value(1));
}

test "list builder finishReset allows reuse" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(10);
    try values_builder.append(20);
    try values_builder.append(30);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try ListBuilder.init(allocator, 2, field);
    defer builder.deinit();
    try builder.appendLen(1);
    try builder.appendLen(2);

    var list_ref = try builder.finishReset(values_ref);
    defer list_ref.release();
    const list = ListArray{ .data = list_ref.data() };
    try std.testing.expectEqual(@as(usize, 2), list.len());

    var values_builder2 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder2.deinit();
    try values_builder2.append(99);
    var values_ref2 = try values_builder2.finish();
    defer values_ref2.release();

    try builder.appendLen(1);
    var list_ref2 = try builder.finish(values_ref2);
    defer list_ref2.release();
    const list2 = ListArray{ .data = list_ref2.data() };
    try std.testing.expectEqual(@as(usize, 1), list2.len());
}

test "large list builder clear rebuilds offsets" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder.deinit();
    try values_builder.append(42);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try LargeListBuilder.init(allocator, 1, field);
    defer builder.deinit();
    try builder.appendLen(1);
    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();

    try builder.clear();

    var values_builder2 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder2.deinit();
    try values_builder2.append(7);
    var values_ref2 = try values_builder2.finish();
    defer values_ref2.release();

    try builder.appendLen(1);
    var list_ref2 = try builder.finish(values_ref2);
    defer list_ref2.release();
    const list2 = LargeListArray{ .data = list_ref2.data() };
    try std.testing.expectEqual(@as(usize, 1), list2.len());
}
