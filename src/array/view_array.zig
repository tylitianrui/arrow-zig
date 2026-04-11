const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");
const array_utils = @import("array_utils.zig");
const builder_state = @import("builder_state.zig");
const array_data = @import("array_data.zig");
const array_ref = @import("array_ref.zig");
const primitive_array = @import("primitive_array.zig");
const list_array = @import("list_array.zig");

// View-type arrays/builders (string_view/binary_view/list_view/large_list_view).

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const ArrayData = array_data.ArrayData;
pub const ArrayRef = array_ref.ArrayRef;
pub const PrimitiveArray = primitive_array.PrimitiveArray;
pub const BuilderState = builder_state.BuilderState;

const STRING_VIEW_TYPE = DataType{ .string_view = {} };
const BINARY_VIEW_TYPE = DataType{ .binary_view = {} };
const MAX_INLINE_VIEW_LEN: usize = 12;
const VIEW_RECORD_SIZE: usize = 16;
const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

fn readU32Le(bytes: []const u8) u32 {
    std.debug.assert(bytes.len >= 4);
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn writeU32Le(dst: []u8, value: u32) void {
    std.debug.assert(dst.len >= 4);
    dst[0] = @truncate(value);
    dst[1] = @truncate(value >> 8);
    dst[2] = @truncate(value >> 16);
    dst[3] = @truncate(value >> 24);
}

fn viewRecordAt(data: *const ArrayData, i: usize) []const u8 {
    std.debug.assert(data.buffers.len >= 2);
    const base = data.offset + i;
    const start = base * VIEW_RECORD_SIZE;
    const end = start + VIEW_RECORD_SIZE;
    std.debug.assert(end <= data.buffers[1].len());
    return data.buffers[1].data[start..end];
}

fn viewValueAt(data: *const ArrayData, i: usize) []const u8 {
    std.debug.assert(i < data.length);
    const record = viewRecordAt(data, i);
    const len_u32 = readU32Le(record[0..4]);
    const len = @as(usize, @intCast(len_u32));
    if (len <= MAX_INLINE_VIEW_LEN) {
        return record[4 .. 4 + len];
    }

    const buffer_index: usize = @intCast(readU32Le(record[8..12]));
    const byte_offset: usize = @intCast(readU32Le(record[12..16]));
    const variadic_idx = 2 + buffer_index;
    std.debug.assert(variadic_idx < data.buffers.len);

    const variadic = data.buffers[variadic_idx].data;
    const end = byte_offset + len;
    std.debug.assert(end <= variadic.len);
    std.debug.assert(std.mem.eql(u8, record[4..8], variadic[byte_offset .. byte_offset + 4]));
    return variadic[byte_offset..end];
}

pub const StringViewArray = struct {
    data: *const ArrayData,

    pub fn len(self: StringViewArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: StringViewArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn value(self: StringViewArray, i: usize) []const u8 {
        return viewValueAt(self.data, i);
    }
};

pub const BinaryViewArray = struct {
    data: *const ArrayData,

    pub fn len(self: BinaryViewArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: BinaryViewArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn value(self: BinaryViewArray, i: usize) []const u8 {
        return viewValueAt(self.data, i);
    }
};

pub const ListViewArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: ListViewArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: ListViewArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Execute valuesRef logic for this type.
    pub fn valuesRef(self: ListViewArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: ListViewArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);
        std.debug.assert(self.data.children.len == 1);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const sizes = self.data.buffers[2].typedSlice(i32);
        const base = self.data.offset + i;
        const start: usize = @intCast(offsets[base]);
        const size: usize = @intCast(sizes[base]);
        return self.data.children[0].slice(start, size);
    }
};

pub const LargeListViewArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: LargeListViewArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: LargeListViewArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Execute valuesRef logic for this type.
    pub fn valuesRef(self: LargeListViewArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: LargeListViewArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 3);
        std.debug.assert(self.data.children.len == 1);

        const offsets = self.data.buffers[1].typedSlice(i64);
        const sizes = self.data.buffers[2].typedSlice(i64);
        const base = self.data.offset + i;
        const start: usize = @intCast(offsets[base]);
        const size: usize = @intCast(sizes[base]);
        return self.data.children[0].slice(start, size);
    }
};

fn convertListToView(
    comptime OffsetT: type,
    allocator: std.mem.Allocator,
    base_ref: ArrayRef,
    dtype: DataType,
) !ArrayRef {
    const base = base_ref.data();
    std.debug.assert(base.buffers.len >= 2);
    std.debug.assert(base.children.len == 1);

    const total_len = std.math.add(usize, base.offset, base.length) catch return error.InvalidOffsets;
    var sizes = try OwnedBuffer.init(allocator, total_len * @sizeOf(OffsetT));
    errdefer sizes.deinit();

    const sizes_slice = std.mem.bytesAsSlice(OffsetT, sizes.data)[0..total_len];
    @memset(sizes_slice, 0);
    const offsets = base.buffers[1].typedSlice(OffsetT);
    var i: usize = 0;
    while (i < base.length) : (i += 1) {
        const idx = base.offset + i;
        sizes_slice[idx] = offsets[idx + 1] - offsets[idx];
    }

    var sizes_shared = try sizes.toShared(total_len * @sizeOf(OffsetT));
    errdefer sizes_shared.release();

    const buffers = try allocator.alloc(SharedBuffer, 3);
    var filled_buffers: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < filled_buffers) : (j += 1) {
            var owned = buffers[j];
            owned.release();
        }
        allocator.free(buffers);
    }
    buffers[0] = base.buffers[0].retain();
    filled_buffers = 1;
    buffers[1] = base.buffers[1].retain();
    filled_buffers = 2;
    buffers[2] = sizes_shared;
    filled_buffers = 3;
    sizes_shared = SharedBuffer.empty;

    const children = try allocator.alloc(ArrayRef, base.children.len);
    var filled_children: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < filled_children) : (j += 1) {
            var owned = children[j];
            owned.release();
        }
        allocator.free(children);
    }
    for (base.children, 0..) |child, idx| {
        children[idx] = child.retain();
        filled_children += 1;
    }

    var dict_ref: ?ArrayRef = null;
    if (base.dictionary) |dict| dict_ref = dict.retain();
    errdefer if (dict_ref) |*owned| owned.release();

    const out = ArrayData{
        .data_type = dtype,
        .length = base.length,
        .offset = base.offset,
        .null_count = base.null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = dict_ref,
    };
    return ArrayRef.fromOwnedUnsafe(allocator, out);
}

fn ViewBuilder(comptime view_type: DataType) type {
    return struct {
        allocator: std.mem.Allocator,
        views: OwnedBuffer,
        data: OwnedBuffer,
        validity: ?OwnedBuffer = null,
        buffers: [3]SharedBuffer = undefined,
        len: usize = 0,
        null_count: usize = 0,
        data_len: usize = 0,
        state: BuilderState = .ready,

        const Self = @This();
        const BuilderError = error{ AlreadyFinished, NotFinished };

        pub fn init(allocator: std.mem.Allocator, capacity: usize, data_capacity: usize) !Self {
            return .{
                .allocator = allocator,
                .views = try OwnedBuffer.init(allocator, capacity * VIEW_RECORD_SIZE),
                .data = try OwnedBuffer.init(allocator, data_capacity),
            };
        }

        pub fn deinit(self: *Self) void {
            self.views.deinit();
            self.data.deinit();
            if (self.validity) |*valid| valid.deinit();
        }

        pub fn reset(self: *Self) BuilderError!void {
            if (self.state != .finished) return BuilderError.NotFinished;
            self.len = 0;
            self.null_count = 0;
            self.data_len = 0;
            self.state = .ready;
        }

        pub fn clear(self: *Self) BuilderError!void {
            if (self.state != .finished) return BuilderError.NotFinished;
            self.views.deinit();
            self.data.deinit();
            if (self.validity) |*valid| valid.deinit();
            self.validity = null;
            self.len = 0;
            self.null_count = 0;
            self.data_len = 0;
            self.state = .ready;
        }

        fn ensureViewsCapacity(self: *Self, needed_len: usize) !void {
            const needed_bytes = needed_len * VIEW_RECORD_SIZE;
            if (needed_bytes <= self.views.len()) return;
            try self.views.resize(needed_bytes);
        }

        fn ensureDataCapacity(self: *Self, needed_len: usize) !void {
            if (needed_len <= self.data.len()) return;
            try self.data.resize(needed_len);
        }

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

        fn setValidBit(self: *Self, index: usize) !void {
            if (self.validity == null) return;
            var buf = &self.validity.?;
            try ensureBitmapCapacity(buf, index + 1);
            bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
        }

        fn writeInline(record: []u8, value: []const u8) void {
            writeU32Le(record[0..4], @intCast(value.len));
            @memset(record[4..VIEW_RECORD_SIZE], 0);
            @memcpy(record[4 .. 4 + value.len], value);
        }

        fn writeOutlined(record: []u8, value: []const u8, data_offset: usize) void {
            std.debug.assert(value.len <= std.math.maxInt(u32));
            std.debug.assert(data_offset <= std.math.maxInt(u32));
            writeU32Le(record[0..4], @intCast(value.len));
            @memcpy(record[4..8], value[0..4]);
            writeU32Le(record[8..12], 0); // buffer_index in variadic section
            writeU32Le(record[12..16], @intCast(data_offset));
        }

        pub fn append(self: *Self, value: []const u8) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            const next_len = self.len + 1;
            try self.ensureViewsCapacity(next_len);

            const record = self.views.data[self.len * VIEW_RECORD_SIZE .. next_len * VIEW_RECORD_SIZE];
            if (value.len <= MAX_INLINE_VIEW_LEN) {
                writeInline(record, value);
            } else {
                try self.ensureDataCapacity(self.data_len + value.len);
                @memcpy(self.data.data[self.data_len .. self.data_len + value.len], value);
                writeOutlined(record, value, self.data_len);
                self.data_len += value.len;
            }

            try self.setValidBit(self.len);
            self.len = next_len;
        }

        pub fn appendNull(self: *Self) !void {
            if (self.state == .finished) return BuilderError.AlreadyFinished;
            const next_len = self.len + 1;
            try self.ensureViewsCapacity(next_len);
            @memset(self.views.data[self.len * VIEW_RECORD_SIZE .. next_len * VIEW_RECORD_SIZE], 0);
            try self.ensureValidityForNull(next_len);
            self.len = next_len;
        }

        pub fn finish(self: *Self) !ArrayRef {
            if (self.state == .finished) return BuilderError.AlreadyFinished;

            self.buffers[0] = if (self.validity) |*valid| try valid.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
            self.buffers[1] = try self.views.toShared(self.len * VIEW_RECORD_SIZE);
            const has_variadic = self.data_len > 0;
            if (has_variadic) self.buffers[2] = try self.data.toShared(self.data_len);

            const out_len: usize = if (has_variadic) 3 else 2;
            const buffers = try self.allocator.alloc(SharedBuffer, out_len);
            buffers[0] = self.buffers[0];
            buffers[1] = self.buffers[1];
            if (has_variadic) buffers[2] = self.buffers[2];

            const data = ArrayData{
                .data_type = view_type,
                .length = self.len,
                .null_count = self.null_count,
                .buffers = buffers,
            };

            self.state = .finished;
            return ArrayRef.fromOwnedUnsafe(self.allocator, data);
        }

        pub fn finishReset(self: *Self) !ArrayRef {
            const out = try self.finish();
            try self.reset();
            return out;
        }

        pub fn finishClear(self: *Self) !ArrayRef {
            const out = try self.finish();
            try self.clear();
            return out;
        }
    };
}

pub const StringViewBuilder = ViewBuilder(STRING_VIEW_TYPE);
pub const BinaryViewBuilder = ViewBuilder(BINARY_VIEW_TYPE);

pub const ListViewBuilder = struct {
    allocator: std.mem.Allocator,
    inner: list_array.ListBuilder,

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, value_field: Field) !ListViewBuilder {
        return .{
            .allocator = allocator,
            .inner = try list_array.ListBuilder.init(allocator, capacity, value_field),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *ListViewBuilder) void {
        self.inner.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *ListViewBuilder) !void {
        try self.inner.reset();
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *ListViewBuilder) !void {
        try self.inner.clear();
    }

    /// Ensure there is enough capacity for upcoming appends.
    pub fn reserve(self: *ListViewBuilder, additional: usize) !void {
        try self.inner.reserve(additional);
    }

    /// Append one logical list length.
    pub fn appendLen(self: *ListViewBuilder, value_len: usize) !void {
        try self.inner.appendLen(value_len);
    }

    /// Append multiple logical list lengths.
    pub fn appendLens(self: *ListViewBuilder, lengths: []const usize) !void {
        try self.inner.appendLens(lengths);
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *ListViewBuilder) !void {
        try self.inner.appendNull();
    }

    /// Append multiple null entries into the builder.
    pub fn appendNulls(self: *ListViewBuilder, count: usize) !void {
        try self.inner.appendNulls(count);
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *ListViewBuilder, values: ArrayRef) !ArrayRef {
        var base = try self.inner.finish(values);
        defer base.release();
        return convertListToView(
            i32,
            self.allocator,
            base,
            DataType{ .list_view = .{ .value_field = self.inner.value_field } },
        );
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *ListViewBuilder, values: ArrayRef) !ArrayRef {
        const ref = try self.finish(values);
        try self.reset();
        return ref;
    }
};

pub const LargeListViewBuilder = struct {
    allocator: std.mem.Allocator,
    inner: list_array.LargeListBuilder,

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, value_field: Field) !LargeListViewBuilder {
        return .{
            .allocator = allocator,
            .inner = try list_array.LargeListBuilder.init(allocator, capacity, value_field),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *LargeListViewBuilder) void {
        self.inner.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *LargeListViewBuilder) !void {
        try self.inner.reset();
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *LargeListViewBuilder) !void {
        try self.inner.clear();
    }

    /// Ensure there is enough capacity for upcoming appends.
    pub fn reserve(self: *LargeListViewBuilder, additional: usize) !void {
        try self.inner.reserve(additional);
    }

    /// Append one logical list length.
    pub fn appendLen(self: *LargeListViewBuilder, value_len: usize) !void {
        try self.inner.appendLen(value_len);
    }

    /// Append multiple logical list lengths.
    pub fn appendLens(self: *LargeListViewBuilder, lengths: []const usize) !void {
        try self.inner.appendLens(lengths);
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *LargeListViewBuilder) !void {
        try self.inner.appendNull();
    }

    /// Append multiple null entries into the builder.
    pub fn appendNulls(self: *LargeListViewBuilder, count: usize) !void {
        try self.inner.appendNulls(count);
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *LargeListViewBuilder, values: ArrayRef) !ArrayRef {
        var base = try self.inner.finish(values);
        defer base.release();
        return convertListToView(
            i64,
            self.allocator,
            base,
            DataType{ .large_list_view = .{ .value_field = self.inner.value_field } },
        );
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *LargeListViewBuilder, values: ArrayRef) !ArrayRef {
        const ref = try self.finish(values);
        try self.reset();
        return ref;
    }
};

test "string view builder builds string_view array" {
    const allocator = std.testing.allocator;

    var builder = try StringViewBuilder.init(allocator, 4, 32);
    defer builder.deinit();
    try builder.append("a");
    try builder.appendNull();
    try builder.append("bc");
    try builder.append("this string is definitely longer than twelve");

    var arr_ref = try builder.finish();
    defer arr_ref.release();
    const arr = StringViewArray{ .data = arr_ref.data() };
    try std.testing.expectEqual(@as(usize, 4), arr.len());
    try std.testing.expect(arr.isNull(1));
    try std.testing.expectEqualStrings("a", arr.value(0));
    try std.testing.expectEqualStrings("bc", arr.value(2));
    try std.testing.expectEqualStrings("this string is definitely longer than twelve", arr.value(3));
    try std.testing.expect(arr_ref.data().data_type == .string_view);
    try std.testing.expectEqual(@as(usize, 3), arr_ref.data().buffers.len);
}

test "binary view builder builds binary_view array" {
    const allocator = std.testing.allocator;

    var builder = try BinaryViewBuilder.init(allocator, 4, 32);
    defer builder.deinit();
    try builder.append("ab");
    try builder.appendNull();
    try builder.append("c");
    try builder.append("this-binary-view-is-long");

    var arr_ref = try builder.finish();
    defer arr_ref.release();
    const arr = BinaryViewArray{ .data = arr_ref.data() };
    try std.testing.expectEqual(@as(usize, 4), arr.len());
    try std.testing.expect(arr.isNull(1));
    try std.testing.expectEqualStrings("ab", arr.value(0));
    try std.testing.expectEqualStrings("c", arr.value(2));
    try std.testing.expectEqualStrings("this-binary-view-is-long", arr.value(3));
    try std.testing.expect(arr_ref.data().data_type == .binary_view);
    try std.testing.expectEqual(@as(usize, 3), arr_ref.data().buffers.len);
}

test "list view builder builds list_view array" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try primitive_array.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 4);
    defer values_builder.deinit();
    try values_builder.append(1);
    try values_builder.append(2);
    try values_builder.append(3);
    try values_builder.append(4);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try ListViewBuilder.init(allocator, 3, value_field);
    defer builder.deinit();
    try builder.appendLen(2);
    try builder.appendNull();
    try builder.appendLen(2);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();

    const list = ListViewArray{ .data = list_ref.data() };
    try std.testing.expectEqual(@as(usize, 3), list.len());
    try std.testing.expect(list.isNull(1));
    try std.testing.expect(list_ref.data().data_type == .list_view);
    try std.testing.expectEqual(@as(usize, 3), list_ref.data().buffers.len);

    var first = try list.value(0);
    defer first.release();
    const first_values = PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 2), first_values.len());
    try std.testing.expectEqual(@as(i32, 1), first_values.value(0));
    try std.testing.expectEqual(@as(i32, 2), first_values.value(1));

    var third = try list.value(2);
    defer third.release();
    const third_values = PrimitiveArray(i32){ .data = third.data() };
    try std.testing.expectEqual(@as(usize, 2), third_values.len());
    try std.testing.expectEqual(@as(i32, 3), third_values.value(0));
    try std.testing.expectEqual(@as(i32, 4), third_values.value(1));
}

test "large list view builder builds large_list_view array" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .int32 = {} };
    const value_field = Field{ .name = "item", .data_type = &value_type, .nullable = true };

    var values_builder = try primitive_array.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(5);
    try values_builder.append(6);
    try values_builder.append(7);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try LargeListViewBuilder.init(allocator, 2, value_field);
    defer builder.deinit();
    try builder.appendLen(1);
    try builder.appendLen(2);

    var list_ref = try builder.finish(values_ref);
    defer list_ref.release();

    const list = LargeListViewArray{ .data = list_ref.data() };
    try std.testing.expectEqual(@as(usize, 2), list.len());
    try std.testing.expect(list_ref.data().data_type == .large_list_view);
    try std.testing.expectEqual(@as(usize, 3), list_ref.data().buffers.len);

    var first = try list.value(0);
    defer first.release();
    const first_values = PrimitiveArray(i32){ .data = first.data() };
    try std.testing.expectEqual(@as(usize, 1), first_values.len());
    try std.testing.expectEqual(@as(i32, 5), first_values.value(0));

    var second = try list.value(1);
    defer second.release();
    const second_values = PrimitiveArray(i32){ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 2), second_values.len());
    try std.testing.expectEqual(@as(i32, 6), second_values.value(0));
    try std.testing.expectEqual(@as(i32, 7), second_values.value(1));
}
