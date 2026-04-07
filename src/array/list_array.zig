const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

// List and LargeList array views/builders with offset-based child slicing.

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

pub const ListArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: ListArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: ListArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Execute valuesRef logic for this type.
    pub fn valuesRef(self: ListArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    /// Return the logical value view at the requested index.
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

    /// Return the logical length.
    pub fn len(self: LargeListArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: LargeListArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Execute valuesRef logic for this type.
    pub fn valuesRef(self: LargeListArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    /// Return the logical value view at the requested index.
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

/// Execute GenericListBuilder logic for this type.
pub fn GenericListBuilder(comptime OffsetsT: type) type {
    return struct {
        allocator: std.mem.Allocator,
        value_field: Field,
        offsets: OwnedBuffer,
        validity: ?OwnedBuffer = null,
        buffers: [2]SharedBuffer = undefined,
        len: usize = 0,
        null_count: usize = 0,
        values_len: usize = 0,
        state: BuilderState = .ready,

        const Self = @This();
        const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow, InvalidChildLength };

        /// Execute listDataType logic for this type.
        fn listDataType(value_field: Field) DataType {
            if (OffsetsT == i32) {
                return DataType{ .list = .{ .value_field = value_field } };
            }
            if (OffsetsT == i64) {
                return DataType{ .large_list = .{ .value_field = value_field } };
            }
            @compileError("GenericListBuilder only supports i32 or i64 offsets");
        }

        /// Initialize and return a new instance.
        pub fn init(allocator: std.mem.Allocator, capacity: usize, value_field: Field) !Self {
            const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(OffsetsT));
            const offsets_slice = std.mem.bytesAsSlice(OffsetsT, offsets.data);
            offsets_slice[0] = 0;
            return .{
                .allocator = allocator,
                .value_field = value_field,
                .offsets = offsets,
            };
        }

        /// Release resources owned by this instance.
        pub fn deinit(self: *Self) void {
            self.offsets.deinit();
            if (self.validity) |*valid| valid.deinit();
        }

        /// Reset state while retaining reusable capacity when possible.
        pub fn reset(self: *Self) BuilderError!void {
            if (self.state != .finished) return BuilderError.NotFinished;
            if (!self.offsets.isEmpty()) {
                const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
                offsets_slice[0] = 0;
            }
            self.len = 0;
            self.null_count = 0;
            self.values_len = 0;
            self.state = .ready;
        }

        /// Clear state and release reusable buffers when required.
        pub fn clear(self: *Self) BuilderError!void {
            if (self.state != .finished) return BuilderError.NotFinished;
            self.offsets.deinit();
            if (self.validity) |*valid| valid.deinit();
            self.validity = null;
            self.len = 0;
            self.null_count = 0;
            self.values_len = 0;
            self.state = .ready;
        }

        /// Ensure there is enough capacity for upcoming appends.
        pub fn reserve(self: *Self, additional: usize) !void {
            try self.ensureOffsetsCapacity(self.len + additional + 1);
            if (self.validity) |*valid| {
                try ensureBitmapCapacity(valid, self.len + additional);
            }
        }

        /// Execute ensureOffsetsCapacity logic for this type.
        fn ensureOffsetsCapacity(self: *Self, needed_len: usize) !void {
            const capacity = self.offsets.len() / @sizeOf(OffsetsT);
            if (needed_len <= capacity) return;
            const was_empty = capacity == 0;
            try self.offsets.resize(needed_len * @sizeOf(OffsetsT));
            if (was_empty) {
                const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
                offsets_slice[0] = 0;
            }
        }

        /// Execute ensureValidityForNull logic for this type.
        fn ensureValidityForNull(self: *Self, new_len: usize) !void {
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

        /// Execute setValidBit logic for this type.
        fn setValidBit(self: *Self, index: usize) !void {
            if (self.validity == null) return;
            var buf = &self.validity.?;
            try ensureBitmapCapacity(buf, index + 1);
            bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
        }

        /// Execute appendLen logic for this type.
        pub fn appendLen(self: *Self, value_len: usize) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            const next_len = self.len + 1;
            try self.ensureOffsetsCapacity(next_len + 1);

            const next_offset = self.values_len + value_len;
            const cast_offset = std.math.cast(OffsetsT, next_offset) orelse return BuilderError.OffsetOverflow;
            const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
            offsets_slice[next_len] = cast_offset;

            try self.setValidBit(self.len);
            self.len = next_len;
            self.values_len = next_offset;
        }

        /// Execute appendLens logic for this type.
        pub fn appendLens(self: *Self, lengths: []const usize) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            if (lengths.len == 0) return;

            const next_len = self.len + lengths.len;
            try self.ensureOffsetsCapacity(next_len + 1);

            var validity_bytes: []u8 = &.{};
            if (self.validity) |*valid| {
                try ensureBitmapCapacity(valid, next_len);
                validity_bytes = valid.data[0..bitmap.byteLength(next_len)];
            }

            const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
            var index = self.len;
            for (lengths) |value_len| {
                const next_offset = self.values_len + value_len;
                const cast_offset = std.math.cast(OffsetsT, next_offset) orelse return BuilderError.OffsetOverflow;
                offsets_slice[index + 1] = cast_offset;
                if (validity_bytes.len > 0) {
                    bitmap.setBit(validity_bytes, index);
                }
                index += 1;
                self.values_len = next_offset;
            }

            self.len = next_len;
        }

        /// Append a null entry into the builder.
        pub fn appendNull(self: *Self) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            const next_len = self.len + 1;
            try self.ensureOffsetsCapacity(next_len + 1);
            const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
            offsets_slice[next_len] = std.math.cast(OffsetsT, self.values_len) orelse return BuilderError.OffsetOverflow;
            try self.ensureValidityForNull(next_len);
            self.len = next_len;
        }

        /// Execute appendNulls logic for this type.
        pub fn appendNulls(self: *Self, count: usize) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            if (count == 0) return;

            const next_len = self.len + count;
            try self.ensureOffsetsCapacity(next_len + 1);

            const cast_offset = std.math.cast(OffsetsT, self.values_len) orelse return BuilderError.OffsetOverflow;
            const offsets_slice = std.mem.bytesAsSlice(OffsetsT, self.offsets.data);
            @memset(offsets_slice[self.len + 1 .. next_len + 1], cast_offset);

            if (self.validity == null) {
                const buf = try initValidityAllValid(self.allocator, next_len);
                self.validity = buf;
            } else {
                try ensureBitmapCapacity(&self.validity.?, next_len);
            }

            var i: usize = self.len;
            while (i < next_len) : (i += 1) {
                bitmap.clearBit(self.validity.?.data[0..bitmap.byteLength(next_len)], i);
            }

            self.null_count += count;
            self.len = next_len;
        }

        /// Finalize builder state and return an immutable array reference.
        pub fn finish(self: *Self, values: ArrayRef) !ArrayRef {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            if (values.data().length != self.values_len) return BuilderError.InvalidChildLength;

            try self.ensureOffsetsCapacity(self.len + 1);

            const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
            self.buffers[0] = validity_buf;
            self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(OffsetsT));

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
                .data_type = listDataType(self.value_field),
                .length = self.len,
                .null_count = self.null_count,
                .buffers = buffers,
                .children = children,
            };

            const finished_ref = try ArrayRef.fromOwnedUnsafe(self.allocator, data);
            self.state = .finished;
            return finished_ref;
        }

        /// Finalize output and then reset builder state for reuse.
        pub fn finishReset(self: *Self, values: ArrayRef) !ArrayRef {
            const ref = try self.finish(values);
            try self.reset();
            return ref;
        }
    };
}

pub const ListBuilder = GenericListBuilder(i32);
pub const LargeListBuilder = GenericListBuilder(i64);

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

test "list builder appendLens batches offsets" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(11);
    try values_builder.append(22);
    try values_builder.append(33);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try ListBuilder.init(allocator, 0, field);
    defer builder.deinit();
    try builder.reserve(2);
    try builder.appendLens(&[_]usize{ 1, 2 });

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = ListArray{ .data = list_ref.data() };

    try std.testing.expectEqual(@as(usize, 2), list.len());

    var first = try list.value(0);
    defer first.release();
    const first_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 1), first_values.len());
    try std.testing.expectEqual(@as(i32, 11), first_values.value(0));

    var second = try list.value(1);
    defer second.release();
    const second_values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 2), second_values.len());
    try std.testing.expectEqual(@as(i32, 22), second_values.value(0));
    try std.testing.expectEqual(@as(i32, 33), second_values.value(1));
}

test "list builder appendNulls batches nulls" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 0);
    defer values_builder.deinit();
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try ListBuilder.init(allocator, 0, field);
    defer builder.deinit();
    try builder.appendNulls(2);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();
    const list = ListArray{ .data = list_ref.data() };

    try std.testing.expectEqual(@as(usize, 2), list.len());
    try std.testing.expect(list.isNull(0));
    try std.testing.expect(list.isNull(1));
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
