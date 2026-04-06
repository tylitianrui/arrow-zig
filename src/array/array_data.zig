const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("array_ref.zig");
const datatype = @import("../datatype.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref.ArrayRef;

pub const ValidationError = error{
    InvalidBufferCount,
    BufferTooSmall,
    InvalidOffsetBuffer,
    InvalidOffsets,
    InvalidNullCount,
    MissingDictionary,
    InvalidChildren,
};

/// Core Arrow array metadata and buffers.
///
/// Field semantics:
/// - data_type: logical Arrow type describing the array.
/// - length: number of logical elements.
/// - offset: logical slice offset into buffers.
/// - null_count: number of nulls (null means unknown; 0 means no nulls).
/// - buffers: Arrow buffers in type-specific order.
/// - children: child arrays for nested types (list/struct/union/map).
/// - dictionary: dictionary values for dictionary-encoded arrays.
///
/// Common buffer layouts:
/// - Primitive (int/float): [validity], [values].
/// - Boolean: [validity], [bit-packed values].
/// - String/Binary: [validity], [i32 offsets], [data bytes].
/// - List: [validity], [i32 offsets], children[0] = values.
/// - ListView: [validity], [i32 offsets], [i32 sizes], children[0] = values.
/// - FixedSizeList: [validity], children[0] = values.
/// - Struct: [validity], children = fields in order.
/// - Dictionary: [validity], [indices], dictionary = values array.
/// - Union (sparse): [type_ids], children = fields; no offsets buffer.
/// - Union (dense): [type_ids], [offsets], children = fields.
/// - Map: [validity], [i32 offsets], children[0] = struct of (key, item).
/// - RunEndEncoded: [run_ends], children[0] = values (length == run count).
pub const ArrayData = struct {
    data_type: DataType,
    length: usize,
    offset: usize = 0,
    null_count: ?usize = null, // null means unknown; 0 means no nulls
    buffers: []const SharedBuffer,
    children: []const ArrayRef = &.{},
    dictionary: ?ArrayRef = null,

    const Self = @This();

    pub fn validity(self: Self) ?ValidityBitmap {
        if (self.buffers.len == 0) return null;
        if (self.buffers[0].isEmpty()) return null;
        return ValidityBitmap.fromBuffer(self.buffers[0], self.length + self.offset);
    }

    pub fn isNull(self: Self, i: usize) bool {
        std.debug.assert(i < self.length);
        if (self.null_count) |count| {
            if (count == 0) return false;
        }
        const validity_bitmap = self.validity() orelse return false;
        return !validity_bitmap.isValid(self.offset + i);
    }

    pub fn isValid(self: Self, i: usize) bool {
        return !self.isNull(i);
    }

    pub fn hasNulls(self: Self) bool {
        if (self.null_count) |count| return count != 0;
        const validity_bitmap = self.validity() orelse return false;
        return validity_bitmap.countNulls() > 0;
    }

    pub fn setNullCountUnknown(self: *Self) void {
        self.null_count = null;
    }

    pub fn setNullCountKnown(self: *Self, count: usize) void {
        self.null_count = count;
    }

    pub fn slice(self: Self, offset: usize, length: usize) Self {
        std.debug.assert(offset <= self.length);
        std.debug.assert(offset + length <= self.length);

        return .{
            .data_type = self.data_type,
            .length = length,
            .offset = self.offset + offset,
            .null_count = if (self.null_count == 0) 0 else null,
            .buffers = self.buffers,
            .children = self.children,
            .dictionary = self.dictionary,
        };
    }

    pub fn nullCount(self: *Self) usize {
        if (self.null_count) |count| return count;
        const validity_bitmap = self.validity() orelse {
            self.null_count = 0;
            return 0;
        };
        var count: usize = 0;
        var i: usize = 0;
        while (i < self.length) : (i += 1) {
            if (!validity_bitmap.isValid(self.offset + i)) count += 1;
        }
        self.null_count = count;
        return count;
    }

    fn fixedWidthByteSize(dt: DataType) ?usize {
        return switch (dt) {
            .bool => 1,
            .uint8, .int8 => 1,
            .uint16, .int16 => 2,
            .uint32, .int32 => 4,
            .uint64, .int64 => 8,
            .half_float => 2,
            .float => 4,
            .double => 8,
            .date32 => 4,
            .date64 => 8,
            .time32 => 4,
            .time64 => 8,
            .timestamp => 8,
            .duration => 8,
            .interval_months => 4,
            .interval_day_time => 8,
            .interval_month_day_nano => 16,
            .decimal32 => 4,
            .decimal64 => 8,
            .decimal128 => 16,
            .decimal256 => 32,
            .fixed_size_binary => |fsb| @intCast(fsb.byte_width),
            else => null,
        };
    }

    fn offsetByteWidth(dt: DataType) ?usize {
        return switch (dt) {
            .string, .binary, .list, .list_view, .map => 4,
            .large_string, .large_binary, .large_list, .large_list_view => 8,
            else => null,
        };
    }

    fn validateOffsetsI32(offsets: []const i32, total_len: usize, data_len: usize) ValidationError!void {
        if (offsets.len < total_len + 1) return error.BufferTooSmall;
        var prev: i32 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < total_len + 1) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        if (@as(usize, @intCast(prev)) > data_len) return error.InvalidOffsets;
    }

    // ListView offsets/sizes are per-element views into the child array.
    fn validateOffsetsSizesI32(offsets: []const i32, sizes: []const i32, total_len: usize, child_len: usize) ValidationError!void {
        if (offsets.len < total_len) return error.BufferTooSmall;
        if (sizes.len < total_len) return error.BufferTooSmall;
        var i: usize = 0;
        while (i < total_len) : (i += 1) {
            const off = offsets[i];
            const size = sizes[i];
            if (off < 0 or size < 0) return error.InvalidOffsets;
            const end = @as(usize, @intCast(off)) + @as(usize, @intCast(size));
            if (end > child_len) return error.InvalidOffsets;
        }
    }

    fn validateOffsetsI64(offsets: []const i64, total_len: usize, data_len: usize) ValidationError!void {
        if (offsets.len < total_len + 1) return error.BufferTooSmall;
        var prev: i64 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < total_len + 1) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        if (@as(usize, @intCast(prev)) > data_len) return error.InvalidOffsets;
    }

    // Run ends must be strictly increasing and cover the logical length.
    fn validateRunEndsSigned(comptime T: type, run_ends: []const T, total_len: usize) ValidationError!void {
        if (total_len == 0) {
            if (run_ends.len != 0) return error.InvalidOffsets;
            return;
        }
        if (run_ends.len == 0) return error.BufferTooSmall;
        var prev: i64 = 0;
        for (run_ends) |end| {
            const val: i64 = @intCast(end);
            if (val <= prev) return error.InvalidOffsets;
            prev = val;
        }
        if (prev < @as(i64, @intCast(total_len))) return error.InvalidOffsets;
    }

    fn validateRunEndsUnsigned(comptime T: type, run_ends: []const T, total_len: usize) ValidationError!void {
        if (total_len == 0) {
            if (run_ends.len != 0) return error.InvalidOffsets;
            return;
        }
        if (run_ends.len == 0) return error.BufferTooSmall;
        var prev: u64 = 0;
        for (run_ends) |end| {
            const val: u64 = @intCast(end);
            if (val <= prev) return error.InvalidOffsets;
            prev = val;
        }
        if (prev < @as(u64, @intCast(total_len))) return error.InvalidOffsets;
    }

    // LargeListView offsets/sizes use 64-bit indices.
    fn validateOffsetsSizesI64(offsets: []const i64, sizes: []const i64, total_len: usize, child_len: usize) ValidationError!void {
        if (offsets.len < total_len) return error.BufferTooSmall;
        if (sizes.len < total_len) return error.BufferTooSmall;
        var i: usize = 0;
        while (i < total_len) : (i += 1) {
            const off = offsets[i];
            const size = sizes[i];
            if (off < 0 or size < 0) return error.InvalidOffsets;
            const end = @as(usize, @intCast(off)) + @as(usize, @intCast(size));
            if (end > child_len) return error.InvalidOffsets;
        }
    }

    pub fn validateLayout(self: Self) ValidationError!void {
        if (self.null_count) |count| {
            if (count != 0 and (self.buffers.len == 0 or self.buffers[0].isEmpty())) return error.InvalidNullCount;
        }

        const total_len = self.offset + self.length;
        if (self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
            const needed = bitmap.byteLength(total_len);
            if (self.buffers[0].len() < needed) return error.BufferTooSmall;
        }

        switch (self.data_type) {
            .null => {},
            .bool => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const needed = bitmap.byteLength(total_len);
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
            },
            .list_view, .large_list_view => {
                if (self.buffers.len < 3) return error.InvalidBufferCount;
                if (self.children.len != 1) return error.InvalidChildren;

                const offset_width = offsetByteWidth(self.data_type).?;
                if (self.buffers[1].len() % offset_width != 0) return error.InvalidOffsetBuffer;
                if (self.buffers[2].len() % offset_width != 0) return error.InvalidOffsetBuffer;

                // Views are validated against the child logical length.
                const child_data = self.children[0].data();
                const child_len = child_data.length + child_data.offset;
                if (offset_width == 4) {
                    const offsets = self.buffers[1].typedSlice(i32);
                    const sizes = self.buffers[2].typedSlice(i32);
                    try validateOffsetsSizesI32(offsets, sizes, total_len, child_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64);
                    const sizes = self.buffers[2].typedSlice(i64);
                    try validateOffsetsSizesI64(offsets, sizes, total_len, child_len);
                }
            },
            .string, .binary, .list, .map, .large_string, .large_binary, .large_list => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const offset_width = offsetByteWidth(self.data_type).?;
                if (self.buffers[1].len() % offset_width != 0) return error.InvalidOffsetBuffer;
                if (self.buffers.len < 3 and (self.data_type == .string or self.data_type == .binary or self.data_type == .large_string or self.data_type == .large_binary)) {
                    return error.InvalidBufferCount;
                }

                const data_len = if (self.buffers.len >= 3) self.buffers[2].len() else 0;
                if (offset_width == 4) {
                    const offsets = self.buffers[1].typedSlice(i32);
                    try validateOffsetsI32(offsets, total_len, data_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64);
                    try validateOffsetsI64(offsets, total_len, data_len);
                }

                if (self.data_type == .list or self.data_type == .large_list or self.data_type == .map) {
                    if (self.children.len != 1) return error.InvalidChildren;
                }
            },
            .fixed_size_list, .struct_ => {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                const expected = switch (self.data_type) {
                    .fixed_size_list => 1,
                    .struct_ => |st| st.fields.len,
                    else => 0,
                };
                if (self.children.len != expected) return error.InvalidChildren;
            },
            .dictionary => |dict| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const byte_width = dict.index_type.bit_width / 8;
                if (byte_width == 0) return error.InvalidOffsetBuffer;
                if (self.buffers[1].len() < total_len * byte_width) return error.BufferTooSmall;
                if (self.dictionary == null) return error.MissingDictionary;
            },
            .sparse_union => |uni| {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
            },
            .dense_union => |uni| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
            },
            .run_end_encoded => {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                if (self.children.len != 1) return error.InvalidChildren;

                const byte_width = switch (self.data_type) {
                    .run_end_encoded => |ree| ree.run_end_type.bit_width / 8,
                    else => 0,
                };
                if (byte_width == 0) return error.InvalidOffsetBuffer;
                if (self.buffers[0].len() % byte_width != 0) return error.InvalidOffsetBuffer;

                // One run end per value in the values child.
                const run_count = self.buffers[0].len() / byte_width;
                if (self.children[0].data().length != run_count) return error.InvalidChildren;

                switch (byte_width) {
                    1 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i8, self.buffers[0].typedSlice(i8), total_len)
                    else
                        try validateRunEndsUnsigned(u8, self.buffers[0].typedSlice(u8), total_len),
                    2 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i16, self.buffers[0].typedSlice(i16), total_len)
                    else
                        try validateRunEndsUnsigned(u16, self.buffers[0].typedSlice(u16), total_len),
                    4 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i32, self.buffers[0].typedSlice(i32), total_len)
                    else
                        try validateRunEndsUnsigned(u32, self.buffers[0].typedSlice(u32), total_len),
                    8 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i64, self.buffers[0].typedSlice(i64), total_len)
                    else
                        try validateRunEndsUnsigned(u64, self.buffers[0].typedSlice(u64), total_len),
                    else => return error.InvalidOffsetBuffer,
                }
            },
            else => {
                if (fixedWidthByteSize(self.data_type)) |byte_width| {
                    if (self.buffers.len < 2) return error.InvalidBufferCount;
                    if (byte_width == 0) return error.InvalidOffsetBuffer;
                    if (self.buffers[1].len() < total_len * byte_width) return error.BufferTooSmall;
                }
            },
        }
    }

    pub fn validateFull(self: Self) ValidationError!void {
        try self.validateLayout();

        if (self.null_count) |expected_count| {
            if (self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
                const validity_bitmap = ValidityBitmap.fromBuffer(self.buffers[0], self.offset + self.length);
                var actual_count: usize = 0;
                var i: usize = 0;
                while (i < self.length) : (i += 1) {
                    if (!validity_bitmap.isValid(self.offset + i)) actual_count += 1;
                }
                if (actual_count != expected_count) return error.InvalidNullCount;
            }
        }

        for (self.children) |child| try child.data().validateFull();
        if (self.dictionary) |dict| try dict.data().validateFull();
    }
};

test "array data slice updates offset and length" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 10,
        .offset = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{SharedBuffer.empty},
    };

    const sliced = data.slice(3, 4);
    try std.testing.expectEqual(@as(usize, 4), sliced.length);
    try std.testing.expectEqual(@as(usize, 5), sliced.offset);
    try std.testing.expectEqual(@as(?usize, 0), sliced.null_count);
}

test "array data nullCount caches when unknown" {
    const dtype = DataType{ .int32 = {} };
    var validity: [1]u8 = .{0b0000_1101};
    var data = ArrayData{
        .data_type = dtype,
        .length = 4,
        .null_count = null,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(validity[0..])},
    };

    try std.testing.expectEqual(@as(usize, 1), data.nullCount());
    try std.testing.expectEqual(@as(?usize, 1), data.null_count);
    try std.testing.expect(data.hasNulls());
}

test "array data hasNulls handles no validity" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 3,
        .null_count = null,
        .buffers = &[_]SharedBuffer{SharedBuffer.empty},
    };

    try std.testing.expect(!data.hasNulls());
}

test "array data slice on unknown null count" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 6,
        .offset = 1,
        .null_count = null,
        .buffers = &[_]SharedBuffer{SharedBuffer.empty},
    };

    const sliced = data.slice(2, 3);
    try std.testing.expectEqual(@as(usize, 3), sliced.length);
    try std.testing.expectEqual(@as(usize, 3), sliced.offset);
    try std.testing.expectEqual(@as(?usize, null), sliced.null_count);
}

test "array data validateLayout accepts primitive" {
    const dtype = DataType{ .int32 = {} };
    var values_bytes: [3 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3 }));
    const data = ArrayData{
        .data_type = dtype,
        .length = 3,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };

    try data.validateLayout();
    try data.validateFull();
}

test "array data validateLayout catches invalid offsets" {
    const dtype = DataType{ .binary = {} };
    const offsets = [_]i32{ 0, 4, 2 };
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(offset_bytes[0..]),
            SharedBuffer.fromSlice("zzzz"),
        },
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data validateLayout accepts list_view" {
    const value_type = DataType{ .int32 = {} };
    const field = datatype.Field{ .name = "item", .data_type = &value_type, .nullable = true };
    const list_view_type = DataType{ .list_view = .{ .value_field = field } };

    var child_values: [5 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4, 5 }));
    const child = ArrayData{
        .data_type = value_type,
        .length = 5,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const offsets = [_]i32{ 0, 2 };
    const sizes = [_]i32{ 2, 3 };
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    var size_bytes: [sizes.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    @memcpy(size_bytes[0..], std.mem.sliceAsBytes(sizes[0..]));

    const data = ArrayData{
        .data_type = list_view_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(offset_bytes[0..]), SharedBuffer.fromSlice(size_bytes[0..]) },
        .children = children,
    };

    try data.validateLayout();
}

test "array data validateLayout rejects large_list_view sizes beyond child" {
    const value_type = DataType{ .int32 = {} };
    const field = datatype.Field{ .name = "item", .data_type = &value_type, .nullable = true };
    const list_view_type = DataType{ .large_list_view = .{ .value_field = field } };

    var child_values: [5 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4, 5 }));
    const child = ArrayData{
        .data_type = value_type,
        .length = 5,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const offsets = [_]i64{ 2, 3 };
    const sizes = [_]i64{ 4, 3 };
    var offset_bytes: [offsets.len * @sizeOf(i64)]u8 align(buffer.ALIGNMENT) = undefined;
    var size_bytes: [sizes.len * @sizeOf(i64)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    @memcpy(size_bytes[0..], std.mem.sliceAsBytes(sizes[0..]));

    const data = ArrayData{
        .data_type = list_view_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(offset_bytes[0..]), SharedBuffer.fromSlice(size_bytes[0..]) },
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data validateLayout accepts run_end_encoded" {
    const value_type = DataType{ .int32 = {} };
    const run_end_type = datatype.IntType{ .bit_width = 32, .signed = true };
    const ree_type = DataType{ .run_end_encoded = .{ .run_end_type = run_end_type, .value_type = &value_type } };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 20 }));
    const child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const run_ends = [_]i32{ 2, 5 };
    var run_end_bytes: [run_ends.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(run_end_bytes[0..])},
        .children = children,
    };

    try data.validateLayout();
}

test "array data validateLayout rejects run_end_encoded nonmonotonic" {
    const value_type = DataType{ .int32 = {} };
    const run_end_type = datatype.IntType{ .bit_width = 32, .signed = true };
    const ree_type = DataType{ .run_end_encoded = .{ .run_end_type = run_end_type, .value_type = &value_type } };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 20 }));
    const child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const run_ends = [_]i32{ 3, 2 };
    var run_end_bytes: [run_ends.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(run_end_bytes[0..])},
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}
