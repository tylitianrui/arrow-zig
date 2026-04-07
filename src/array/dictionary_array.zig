const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;
pub const IntType = datatype.IntType;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

pub const DictionaryArray = struct {
    data: *const ArrayData,

    pub fn len(self: DictionaryArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: DictionaryArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn dictionaryRef(self: DictionaryArray) *const ArrayRef {
        std.debug.assert(self.data.dictionary != null);
        return &self.data.dictionary.?;
    }

    pub fn index(self: DictionaryArray, i: usize) i64 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);

        const idx_ty = self.data.data_type.dictionary.index_type;
        const pos = self.data.offset + i;
        return switch (idx_ty.bit_width) {
            8 => if (idx_ty.signed)
                @as(i64, self.data.buffers[1].typedSlice(i8)[pos])
            else
                @as(i64, @intCast(self.data.buffers[1].typedSlice(u8)[pos])),
            16 => if (idx_ty.signed)
                @as(i64, self.data.buffers[1].typedSlice(i16)[pos])
            else
                @as(i64, @intCast(self.data.buffers[1].typedSlice(u16)[pos])),
            32 => if (idx_ty.signed)
                @as(i64, self.data.buffers[1].typedSlice(i32)[pos])
            else
                @as(i64, @intCast(self.data.buffers[1].typedSlice(u32)[pos])),
            64 => if (idx_ty.signed)
                self.data.buffers[1].typedSlice(i64)[pos]
            else
                @as(i64, @intCast(self.data.buffers[1].typedSlice(u64)[pos])),
            else => unreachable,
        };
    }
};

pub const DictionaryBuilder = struct {
    allocator: std.mem.Allocator,
    index_type: IntType,
    value_type: *const DataType,
    ordered: bool = false,
    indices: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{
        AlreadyFinished,
        NotFinished,
        InvalidIndexType,
        IndexOutOfRange,
        InvalidDictionaryType,
    };

    pub fn init(allocator: std.mem.Allocator, index_type: IntType, value_type: *const DataType, capacity: usize) !DictionaryBuilder {
        if (index_type.bit_width != 8 and index_type.bit_width != 16 and index_type.bit_width != 32 and index_type.bit_width != 64) {
            return BuilderError.InvalidIndexType;
        }
        const byte_width = @as(usize, index_type.bit_width) / 8;
        return .{
            .allocator = allocator,
            .index_type = index_type,
            .value_type = value_type,
            .indices = try OwnedBuffer.init(allocator, capacity * byte_width),
        };
    }

    pub fn deinit(self: *DictionaryBuilder) void {
        self.indices.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *DictionaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    pub fn clear(self: *DictionaryBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.indices.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    fn ensureIndicesCapacity(self: *DictionaryBuilder, needed_len: usize) !void {
        const byte_width = @as(usize, self.index_type.bit_width) / 8;
        const needed_bytes = needed_len * byte_width;
        if (needed_bytes <= self.indices.len()) return;
        try self.indices.resize(needed_bytes);
    }

    fn ensureValidityForNull(self: *DictionaryBuilder, new_len: usize) !void {
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

    fn setValidBit(self: *DictionaryBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    fn setIndex(self: *DictionaryBuilder, pos: usize, index: i64) BuilderError!void {
        switch (self.index_type.bit_width) {
            8 => if (self.index_type.signed) {
                const v = std.math.cast(i8, index) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(i8, self.indices.data)[pos] = v;
            } else {
                if (index < 0) return BuilderError.IndexOutOfRange;
                const v = std.math.cast(u8, @as(u64, @intCast(index))) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(u8, self.indices.data)[pos] = v;
            },
            16 => if (self.index_type.signed) {
                const v = std.math.cast(i16, index) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(i16, self.indices.data)[pos] = v;
            } else {
                if (index < 0) return BuilderError.IndexOutOfRange;
                const v = std.math.cast(u16, @as(u64, @intCast(index))) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(u16, self.indices.data)[pos] = v;
            },
            32 => if (self.index_type.signed) {
                const v = std.math.cast(i32, index) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(i32, self.indices.data)[pos] = v;
            } else {
                if (index < 0) return BuilderError.IndexOutOfRange;
                const v = std.math.cast(u32, @as(u64, @intCast(index))) orelse return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(u32, self.indices.data)[pos] = v;
            },
            64 => if (self.index_type.signed) {
                std.mem.bytesAsSlice(i64, self.indices.data)[pos] = index;
            } else {
                if (index < 0) return BuilderError.IndexOutOfRange;
                std.mem.bytesAsSlice(u64, self.indices.data)[pos] = @intCast(index);
            },
            else => return BuilderError.InvalidIndexType,
        }
    }

    pub fn appendIndex(self: *DictionaryBuilder, index: i64) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureIndicesCapacity(next_len);
        try self.setIndex(self.len, index);
        try self.setValidBit(self.len);
        self.len = next_len;
    }

    pub fn appendNull(self: *DictionaryBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureIndicesCapacity(next_len);
        try self.setIndex(self.len, 0);
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *DictionaryBuilder, dictionary: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (!std.meta.eql(dictionary.data().data_type, self.value_type.*)) return BuilderError.InvalidDictionaryType;

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.indices.toShared(self.len * (@as(usize, self.index_type.bit_width) / 8));

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];

        const data = ArrayData{
            .data_type = DataType{ .dictionary = .{ .index_type = self.index_type, .value_type = self.value_type, .ordered = self.ordered } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .dictionary = dictionary.retain(),
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }

    pub fn finishReset(self: *DictionaryBuilder, dictionary: ArrayRef) !ArrayRef {
        const out = try self.finish(dictionary);
        try self.reset();
        return out;
    }

    pub fn finishClear(self: *DictionaryBuilder, dictionary: ArrayRef) !ArrayRef {
        const out = try self.finish(dictionary);
        try self.clear();
        return out;
    }
};

test "dictionary builder builds and slices" {
    const allocator = std.testing.allocator;

    var dict_builder = try @import("string_array.zig").StringBuilder.init(allocator, 2, 8);
    defer dict_builder.deinit();
    try dict_builder.append("red");
    try dict_builder.append("blue");
    var dict_ref = try dict_builder.finish();
    defer dict_ref.release();

    const value_type = DataType{ .string = {} };
    var builder = try DictionaryBuilder.init(allocator, .{ .bit_width = 32, .signed = true }, &value_type, 3);
    defer builder.deinit();

    try builder.appendIndex(1);
    try builder.appendNull();
    try builder.appendIndex(0);

    var out = try builder.finishReset(dict_ref);
    defer out.release();

    const arr = DictionaryArray{ .data = out.data() };
    try std.testing.expectEqual(@as(usize, 3), arr.len());
    try std.testing.expectEqual(@as(i64, 1), arr.index(0));
    try std.testing.expect(arr.isNull(1));
    try std.testing.expectEqual(@as(i64, 0), arr.index(2));

    const dict_view = @import("string_array.zig").StringArray{ .data = arr.dictionaryRef().data() };
    try std.testing.expectEqualStrings("red", dict_view.value(0));
    try std.testing.expectEqualStrings("blue", dict_view.value(1));

    var sliced = try out.slice(1, 2);
    defer sliced.release();
    const sliced_arr = DictionaryArray{ .data = sliced.data() };
    try std.testing.expectEqual(@as(usize, 2), sliced_arr.len());
    try std.testing.expect(sliced_arr.isNull(0));
    try std.testing.expectEqual(@as(i64, 0), sliced_arr.index(1));
}

test "dictionary builder rejects invalid dictionary type" {
    const allocator = std.testing.allocator;

    var dict_builder = try @import("string_array.zig").StringBuilder.init(allocator, 1, 4);
    defer dict_builder.deinit();
    try dict_builder.append("x");
    var dict_ref = try dict_builder.finish();
    defer dict_ref.release();

    const value_type = DataType{ .int32 = {} };
    var builder = try DictionaryBuilder.init(allocator, .{ .bit_width = 8, .signed = false }, &value_type, 1);
    defer builder.deinit();
    try builder.appendIndex(0);

    try std.testing.expectError(error.InvalidDictionaryType, builder.finish(dict_ref));
}

test "array data dictionary layout requires dictionary" {
    const value_type = DataType{ .string = {} };
    const dict_type = DataType{ .dictionary = .{ .index_type = .{ .bit_width = 16, .signed = true }, .value_type = &value_type, .ordered = false } };

    var indices: [2]i16 = .{ 0, 1 };
    const data = ArrayData{
        .data_type = dict_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(std.mem.sliceAsBytes(indices[0..])) },
    };

    try std.testing.expectError(error.MissingDictionary, data.validateLayout());
}
