const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("array_ref.zig");
const datatype = @import("../datatype.zig");

// Raw Arrow layout container with validation rules for buffers and children.

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
/// - RunEndEncoded: no top-level buffers; children[0] = run_ends, children[1] = values.
pub const ArrayData = struct {
    data_type: DataType,
    length: usize,
    offset: usize = 0,
    null_count: ?usize = null, // null means unknown; 0 means no nulls
    buffers: []const SharedBuffer,
    children: []const ArrayRef = &.{},
    dictionary: ?ArrayRef = null,

    const Self = @This();

    /// Execute validity logic for this type.
    pub fn validity(self: Self) ?ValidityBitmap {
        if (self.buffers.len == 0) return null;
        if (self.buffers[0].isEmpty()) return null;
        const total_len = std.math.add(usize, self.length, self.offset) catch return null;
        return ValidityBitmap.fromBuffer(self.buffers[0], total_len);
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: Self, i: usize) bool {
        std.debug.assert(i < self.length);
        if (self.null_count) |count| {
            if (count == 0) return false;
            if (count == self.length) return true;
        }
        const validity_bitmap = self.validity() orelse return false;
        return !validity_bitmap.isValid(self.offset + i);
    }

    /// Check whether the element at index is valid.
    pub fn isValid(self: Self, i: usize) bool {
        return !self.isNull(i);
    }

    /// Execute hasNulls logic for this type.
    pub fn hasNulls(self: Self) bool {
        if (self.null_count) |count| return count != 0;
        const validity_bitmap = self.validity() orelse return false;
        return validity_bitmap.countNulls() > 0;
    }

    /// Execute setNullCountUnknown logic for this type.
    pub fn setNullCountUnknown(self: *Self) void {
        self.null_count = null;
    }

    /// Execute setNullCountKnown logic for this type.
    pub fn setNullCountKnown(self: *Self, count: usize) void {
        self.null_count = count;
    }

    /// Create a logical slice view over the current value.
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

    /// Execute nullCount logic for this type.
    pub fn nullCount(self: *Self) usize {
        if (self.null_count) |count| return count;
        if (self.data_type == .null) {
            self.null_count = self.length;
            return self.length;
        }
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

    /// Execute fixedWidthByteSize logic for this type.
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
            .fixed_size_binary => |fsb| std.math.cast(usize, fsb.byte_width) orelse 0,
            else => null,
        };
    }

    /// Execute offsetByteWidth logic for this type.
    fn offsetByteWidth(dt: DataType) ?usize {
        return switch (dt) {
            .string, .binary, .list, .list_view, .map => 4,
            .large_string, .large_binary, .large_list, .large_list_view => 8,
            else => null,
        };
    }

    /// Execute validateOffsetsI32 logic for this type.
    fn validateOffsetsI32(offsets: []const i32, total_len: usize, data_len: usize) ValidationError!void {
        const needed_len = std.math.add(usize, total_len, 1) catch return error.InvalidOffsets;
        if (offsets.len < needed_len) return error.BufferTooSmall;
        var prev: i32 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < needed_len) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        const prev_usize = std.math.cast(usize, prev) orelse return error.InvalidOffsets;
        if (prev_usize > data_len) return error.InvalidOffsets;
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
            const off_usize = std.math.cast(usize, off) orelse return error.InvalidOffsets;
            const size_usize = std.math.cast(usize, size) orelse return error.InvalidOffsets;
            const end = std.math.add(usize, off_usize, size_usize) catch return error.InvalidOffsets;
            if (end > child_len) return error.InvalidOffsets;
        }
    }

    /// Execute validateOffsetsI64 logic for this type.
    fn validateOffsetsI64(offsets: []const i64, total_len: usize, data_len: usize) ValidationError!void {
        const needed_len = std.math.add(usize, total_len, 1) catch return error.InvalidOffsets;
        if (offsets.len < needed_len) return error.BufferTooSmall;
        var prev: i64 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < needed_len) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        const prev_usize = std.math.cast(usize, prev) orelse return error.InvalidOffsets;
        if (prev_usize > data_len) return error.InvalidOffsets;
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
        const total_len_i64 = std.math.cast(i64, total_len) orelse return error.InvalidOffsets;
        if (prev < total_len_i64) return error.InvalidOffsets;
    }

    /// Execute validateRunEndsUnsigned logic for this type.
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
        const total_len_u64 = std.math.cast(u64, total_len) orelse return error.InvalidOffsets;
        if (prev < total_len_u64) return error.InvalidOffsets;
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
            const off_usize = std.math.cast(usize, off) orelse return error.InvalidOffsets;
            const size_usize = std.math.cast(usize, size) orelse return error.InvalidOffsets;
            const end = std.math.add(usize, off_usize, size_usize) catch return error.InvalidOffsets;
            if (end > child_len) return error.InvalidOffsets;
        }
    }

    /// Execute validateLayout logic for this type.
    pub fn validateLayout(self: Self) ValidationError!void {
        if (self.null_count) |count| {
            // Null arrays have no validity bitmap; all elements are implicitly null.
            if (self.data_type == .null) {
                if (count != self.length) return error.InvalidNullCount;
            } else {
                if (count != 0 and (self.buffers.len == 0 or self.buffers[0].isEmpty())) return error.InvalidNullCount;
            }
        }

        const total_len = std.math.add(usize, self.offset, self.length) catch return error.InvalidOffsets;
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
                const child_len = std.math.add(usize, child_data.length, child_data.offset) catch return error.InvalidChildren;
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

                const is_list = self.data_type == .list or self.data_type == .large_list or self.data_type == .map;
                if (is_list) {
                    if (self.children.len != 1) return error.InvalidChildren;
                } else if (self.buffers.len < 3) {
                    return error.InvalidBufferCount;
                }

                const data_len = if (is_list) blk: {
                    const child_data = self.children[0].data();
                    break :blk std.math.add(usize, child_data.length, child_data.offset) catch return error.InvalidChildren;
                } else if (self.buffers.len >= 3) self.buffers[2].len() else 0;

                if (offset_width == 4) {
                    const offsets = self.buffers[1].typedSlice(i32);
                    try validateOffsetsI32(offsets, total_len, data_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64);
                    try validateOffsetsI64(offsets, total_len, data_len);
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
                if (self.data_type == .fixed_size_list) {
                    const list_size = std.math.cast(usize, self.data_type.fixed_size_list.list_size) orelse return error.InvalidChildren;
                    const required = std.math.mul(usize, total_len, list_size) catch return error.InvalidChildren;
                    const child_data = self.children[0].data();
                    const child_total = std.math.add(usize, child_data.length, child_data.offset) catch return error.InvalidChildren;
                    if (child_total < required) return error.InvalidChildren;
                }
            },
            .dictionary => |dict| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const byte_width = dict.index_type.bit_width / 8;
                if (byte_width == 0) return error.InvalidOffsetBuffer;
                const needed = std.math.mul(usize, total_len, byte_width) catch return error.BufferTooSmall;
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
                if (self.dictionary == null) return error.MissingDictionary;
            },
            .sparse_union => |uni| {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
                if (self.buffers[0].len() < total_len) return error.BufferTooSmall;
            },
            .dense_union => |uni| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
                if (self.buffers[0].len() < total_len) return error.BufferTooSmall;
                const needed = std.math.mul(usize, total_len, @sizeOf(i32)) catch return error.BufferTooSmall;
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
            },
            .run_end_encoded => {
                if (self.buffers.len != 0) return error.InvalidBufferCount;
                if (self.children.len != 2) return error.InvalidChildren;

                const run_ends = self.children[0].data();
                const values = self.children[1].data();
                const run_end_dt = switch (self.data_type.run_end_encoded.run_end_type.bit_width) {
                    8 => if (self.data_type.run_end_encoded.run_end_type.signed) DataType{ .int8 = {} } else DataType{ .uint8 = {} },
                    16 => if (self.data_type.run_end_encoded.run_end_type.signed) DataType{ .int16 = {} } else DataType{ .uint16 = {} },
                    32 => if (self.data_type.run_end_encoded.run_end_type.signed) DataType{ .int32 = {} } else DataType{ .uint32 = {} },
                    64 => if (self.data_type.run_end_encoded.run_end_type.signed) DataType{ .int64 = {} } else DataType{ .uint64 = {} },
                    else => return error.InvalidOffsetBuffer,
                };
                if (!std.meta.eql(run_ends.data_type, run_end_dt)) return error.InvalidChildren;
                if (run_ends.buffers.len < 2) return error.InvalidChildren;

                const byte_width = switch (self.data_type) {
                    .run_end_encoded => |ree| ree.run_end_type.bit_width / 8,
                    else => 0,
                };
                if (byte_width == 0) return error.InvalidOffsetBuffer;
                if (run_ends.buffers[1].len() % byte_width != 0) return error.InvalidOffsetBuffer;

                // One run end per value in the values child.
                const run_count = run_ends.length;
                if (values.length != run_count) return error.InvalidChildren;

                const run_ends_slice_start = run_ends.offset;
                const run_ends_slice_end = std.math.add(usize, run_ends.offset, run_ends.length) catch return error.InvalidOffsets;

                switch (byte_width) {
                    1 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i8, run_ends.buffers[1].typedSlice(i8)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u8, run_ends.buffers[1].typedSlice(u8)[run_ends_slice_start..run_ends_slice_end], total_len),
                    2 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i16, run_ends.buffers[1].typedSlice(i16)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u16, run_ends.buffers[1].typedSlice(u16)[run_ends_slice_start..run_ends_slice_end], total_len),
                    4 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i32, run_ends.buffers[1].typedSlice(i32)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u32, run_ends.buffers[1].typedSlice(u32)[run_ends_slice_start..run_ends_slice_end], total_len),
                    8 => if (self.data_type.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i64, run_ends.buffers[1].typedSlice(i64)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u64, run_ends.buffers[1].typedSlice(u64)[run_ends_slice_start..run_ends_slice_end], total_len),
                    else => return error.InvalidOffsetBuffer,
                }
            },
            else => {
                if (fixedWidthByteSize(self.data_type)) |byte_width| {
                    if (self.buffers.len < 2) return error.InvalidBufferCount;
                    if (byte_width == 0) return error.InvalidOffsetBuffer;
                    const needed = std.math.mul(usize, total_len, byte_width) catch return error.BufferTooSmall;
                    if (self.buffers[1].len() < needed) return error.BufferTooSmall;
                }
            },
        }
    }

    /// Execute validateFull logic for this type.
    pub fn validateFull(self: Self) ValidationError!void {
        try self.validateLayout();

        if (self.null_count) |expected_count| {
            if (self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
                const total_len = std.math.add(usize, self.offset, self.length) catch return error.InvalidOffsets;
                const validity_bitmap = ValidityBitmap.fromBuffer(self.buffers[0], total_len);
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

test "array data nullCount for null type without validity uses length" {
    var data = ArrayData{
        .data_type = DataType{ .null = {} },
        .length = 4,
        .null_count = null,
        .buffers = &[_]SharedBuffer{},
    };

    try std.testing.expectEqual(@as(usize, 4), data.nullCount());
    try std.testing.expectEqual(@as(?usize, 4), data.null_count);
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

test "array data validateLayout rejects null count without validity" {
    const dtype = DataType{ .int32 = {} };
    var values_bytes: [@sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memset(values_bytes[0..], 0);
    const data = ArrayData{
        .data_type = dtype,
        .length = 1,
        .null_count = 1,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };

    try std.testing.expectError(error.InvalidNullCount, data.validateLayout());
}

test "array data validateLayout accepts null type without validity bitmap" {
    const data = ArrayData{
        .data_type = DataType{ .null = {} },
        .length = 3,
        .null_count = 3,
        .buffers = &[_]SharedBuffer{},
    };

    try data.validateLayout();
}

test "array data validateLayout rejects null type invalid null count" {
    const data = ArrayData{
        .data_type = DataType{ .null = {} },
        .length = 3,
        .null_count = 2,
        .buffers = &[_]SharedBuffer{},
    };

    try std.testing.expectError(error.InvalidNullCount, data.validateLayout());
}

test "array data validateLayout rejects struct child count mismatch" {
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]datatype.Field{.{ .name = "x", .data_type = &value_type, .nullable = true }};
    const data = ArrayData{
        .data_type = DataType{ .struct_ = .{ .fields = fields } },
        .length = 1,
        .buffers = &[_]SharedBuffer{SharedBuffer.empty},
        .children = &[_]ArrayRef{},
    };

    try std.testing.expectError(error.InvalidChildren, data.validateLayout());
}

test "array data validateFull checks null count matches bitmap" {
    const dtype = DataType{ .int32 = {} };
    var validity: [1]u8 = .{0xFF};
    var values_bytes: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memset(values_bytes[0..], 0);
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .null_count = 1,
        .buffers = &[_]SharedBuffer{ SharedBuffer.fromSlice(validity[0..]), SharedBuffer.fromSlice(values_bytes[0..]) },
    };

    try std.testing.expectError(error.InvalidNullCount, data.validateFull());
}

test "array data validateLayout rejects dictionary index buffer too small" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .string = {} };
    const dict_type = DataType{ .dictionary = .{ .index_type = .{ .bit_width = 32, .signed = true }, .value_type = &value_type, .ordered = false } };

    const values = ArrayData{
        .data_type = value_type,
        .length = 0,
        .buffers = &[_]SharedBuffer{},
    };
    var dict_ref = try ArrayRef.fromBorrowed(allocator, values);
    defer dict_ref.release();

    const indices: [4]u8 = .{ 0, 0, 0, 0 };
    const data = ArrayData{
        .data_type = dict_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(indices[0..]) },
        .dictionary = dict_ref.retain(),
    };
    defer {
        var owned = data.dictionary.?;
        owned.release();
    }

    try std.testing.expectError(error.BufferTooSmall, data.validateLayout());
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
    const values_child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };

    const run_ends = [_]i32{ 2, 5 };
    var run_end_bytes: [run_ends.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));

    const run_ends_child = ArrayData{
        .data_type = .{ .int32 = {} },
        .length = run_ends.len,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(run_end_bytes[0..]) },
    };

    var run_ends_ref = try ArrayRef.fromBorrowed(std.testing.allocator, run_ends_child);
    defer run_ends_ref.release();
    var values_ref = try ArrayRef.fromBorrowed(std.testing.allocator, values_child);
    defer values_ref.release();
    const children = &[_]ArrayRef{ run_ends_ref.retain(), values_ref.retain() };
    defer {
        var run_ends_owned = children[0];
        run_ends_owned.release();
        var values_owned = children[1];
        values_owned.release();
    }

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
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
    const values_child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };

    const run_ends = [_]i32{ 3, 2 };
    var run_end_bytes: [run_ends.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));

    const run_ends_child = ArrayData{
        .data_type = .{ .int32 = {} },
        .length = run_ends.len,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(run_end_bytes[0..]) },
    };

    var run_ends_ref = try ArrayRef.fromBorrowed(std.testing.allocator, run_ends_child);
    defer run_ends_ref.release();
    var values_ref = try ArrayRef.fromBorrowed(std.testing.allocator, values_child);
    defer values_ref.release();
    const children = &[_]ArrayRef{ run_ends_ref.retain(), values_ref.retain() };
    defer {
        var run_ends_owned = children[0];
        run_ends_owned.release();
        var values_owned = children[1];
        values_owned.release();
    }

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data slice preserves offset length and recomputes null_count when needed" {
    const dtype = DataType{ .int32 = {} };
    var validity: [1]u8 = .{0b0000_1101}; // valid: [0,2,3], null: [1,4]
    const data = ArrayData{
        .data_type = dtype,
        .length = 5,
        .offset = 0,
        .null_count = 2,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(validity[0..])},
    };

    var sliced = data.slice(1, 3);
    try std.testing.expectEqual(@as(usize, 3), sliced.length);
    try std.testing.expectEqual(@as(usize, 1), sliced.offset);
    try std.testing.expectEqual(@as(?usize, null), sliced.null_count);

    try std.testing.expect(sliced.isNull(0));
    try std.testing.expect(!sliced.isNull(1));
    try std.testing.expect(!sliced.isNull(2));
    try std.testing.expectEqual(@as(usize, 1), sliced.nullCount());
    try std.testing.expectEqual(@as(?usize, 1), sliced.null_count);
}
