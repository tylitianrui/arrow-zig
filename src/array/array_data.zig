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

    fn layoutDataType(dt: DataType) DataType {
        return switch (dt) {
            .extension => |ext| layoutDataType(ext.storage_type.*),
            else => dt,
        };
    }

    fn hasTopLevelValidityBitmap(dt: DataType) bool {
        return switch (layoutDataType(dt)) {
            .null, .sparse_union, .dense_union => false,
            else => true,
        };
    }

    pub fn validity(self: Self) ?ValidityBitmap {
        if (!hasTopLevelValidityBitmap(self.data_type)) return null;
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

    pub fn nullCount(self: *Self) usize {
        if (self.null_count) |count| return count;
        const dt = layoutDataType(self.data_type);
        if (dt == .null) {
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

    fn offsetByteWidth(dt: DataType) ?usize {
        return switch (dt) {
            .string, .binary, .list, .list_view, .map => 4,
            .large_string, .large_binary, .large_list, .large_list_view => 8,
            else => null,
        };
    }

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

    fn readU32Le(bytes: []const u8) u32 {
        std.debug.assert(bytes.len >= 4);
        return @as(u32, bytes[0]) |
            (@as(u32, bytes[1]) << 8) |
            (@as(u32, bytes[2]) << 16) |
            (@as(u32, bytes[3]) << 24);
    }

    fn validateByteViews(view_bytes: []const u8, total_len: usize, variadic: []const SharedBuffer) ValidationError!void {
        if (view_bytes.len % 16 != 0) return error.InvalidOffsetBuffer;
        const view_count = view_bytes.len / 16;
        if (view_count < total_len) return error.BufferTooSmall;

        var i: usize = 0;
        while (i < total_len) : (i += 1) {
            const start = i * 16;
            const view = view_bytes[start .. start + 16];
            const len_u32 = readU32Le(view[0..4]);
            const len = std.math.cast(usize, len_u32) orelse return error.InvalidOffsets;

            if (len <= 12) {
                const pad_start = 4 + len;
                for (view[pad_start..16]) |b| {
                    if (b != 0) return error.InvalidOffsets;
                }
                continue;
            }

            const buffer_index = std.math.cast(usize, readU32Le(view[8..12])) orelse return error.InvalidOffsets;
            if (buffer_index >= variadic.len) return error.InvalidOffsets;

            const byte_offset = std.math.cast(usize, readU32Le(view[12..16])) orelse return error.InvalidOffsets;
            const end = std.math.add(usize, byte_offset, len) catch return error.InvalidOffsets;
            if (end > variadic[buffer_index].len()) return error.InvalidOffsets;
            if (!std.mem.eql(u8, view[4..8], variadic[buffer_index].data[byte_offset .. byte_offset + 4])) {
                return error.InvalidOffsets;
            }
        }
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

    fn unionTypeIdToChildIndex(uni: datatype.UnionType, type_id: i8) ?usize {
        if (uni.type_ids.len == 0) {
            if (type_id < 0) return null;
            const index = std.math.cast(usize, type_id) orelse return null;
            return if (index < uni.fields.len) index else null;
        }
        if (uni.type_ids.len != uni.fields.len) return null;
        for (uni.type_ids, 0..) |configured_id, child_index| {
            if (configured_id == type_id) return child_index;
        }
        return null;
    }

    fn validateUnionTypeIds(uni: datatype.UnionType, type_ids: []const i8, start: usize, end: usize) ValidationError!void {
        if (type_ids.len < end) return error.BufferTooSmall;
        var i = start;
        while (i < end) : (i += 1) {
            const type_id = type_ids[i];
            if (unionTypeIdToChildIndex(uni, type_id) == null) return error.InvalidOffsets;
        }
    }

    fn validateDenseUnionOffsets(
        uni: datatype.UnionType,
        type_ids: []const i8,
        offsets: []const i32,
        children: []const ArrayRef,
        start: usize,
        end: usize,
    ) ValidationError!void {
        if (offsets.len < end) return error.BufferTooSmall;
        var i = start;
        while (i < end) : (i += 1) {
            const child_index = unionTypeIdToChildIndex(uni, type_ids[i]) orelse return error.InvalidOffsets;
            const off = offsets[i];
            if (off < 0) return error.InvalidOffsets;
            const child_data = children[child_index].data();
            const child_total = std.math.add(usize, child_data.length, child_data.offset) catch return error.InvalidOffsets;
            const off_usize = std.math.cast(usize, off) orelse return error.InvalidOffsets;
            if (off_usize >= child_total) return error.InvalidOffsets;
        }
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

    pub fn validateLayout(self: Self) ValidationError!void {
        const dt = layoutDataType(self.data_type);

        if (self.null_count) |count| {
            // Null arrays have no validity bitmap; all elements are implicitly null.
            if (dt == .null) {
                if (count != self.length) return error.InvalidNullCount;
            } else if (!hasTopLevelValidityBitmap(dt)) {
                // Union arrays have no validity bitmap; null_count must be 0.
                if (count != 0) return error.InvalidNullCount;
            } else if (count != 0 and (self.buffers.len == 0 or self.buffers[0].isEmpty())) {
                return error.InvalidNullCount;
            }
        }

        const total_len = std.math.add(usize, self.offset, self.length) catch return error.InvalidOffsets;
        const has_top_level_validity = hasTopLevelValidityBitmap(dt);
        if (has_top_level_validity and self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
            const needed = bitmap.byteLength(total_len);
            if (self.buffers[0].len() < needed) return error.BufferTooSmall;
        }

        switch (dt) {
            .null => {},
            .string_view, .binary_view => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const variadic = if (self.buffers.len > 2) self.buffers[2..] else &[_]SharedBuffer{};
                try validateByteViews(self.buffers[1].data, total_len, variadic);
            },
            .bool => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const needed = bitmap.byteLength(total_len);
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
            },
            .list_view, .large_list_view => {
                if (self.buffers.len < 3) return error.InvalidBufferCount;
                if (self.children.len != 1) return error.InvalidChildren;

                const offset_width = offsetByteWidth(dt).?;
                if (self.buffers[1].len() % offset_width != 0) return error.InvalidOffsetBuffer;
                if (self.buffers[2].len() % offset_width != 0) return error.InvalidOffsetBuffer;

                // Views are validated against the child logical length.
                const child_data = self.children[0].data();
                const child_len = std.math.add(usize, child_data.length, child_data.offset) catch return error.InvalidChildren;
                if (offset_width == 4) {
                    const offsets = self.buffers[1].typedSlice(i32) catch return error.InvalidOffsetBuffer;
                    const sizes = self.buffers[2].typedSlice(i32) catch return error.InvalidOffsetBuffer;
                    try validateOffsetsSizesI32(offsets, sizes, total_len, child_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64) catch return error.InvalidOffsetBuffer;
                    const sizes = self.buffers[2].typedSlice(i64) catch return error.InvalidOffsetBuffer;
                    try validateOffsetsSizesI64(offsets, sizes, total_len, child_len);
                }
            },
            .string, .binary, .list, .map, .large_string, .large_binary, .large_list => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const offset_width = offsetByteWidth(dt).?;
                if (self.buffers[1].len() % offset_width != 0) return error.InvalidOffsetBuffer;

                const is_list = dt == .list or dt == .large_list or dt == .map;
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
                    const offsets = self.buffers[1].typedSlice(i32) catch return error.InvalidOffsetBuffer;
                    try validateOffsetsI32(offsets, total_len, data_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64) catch return error.InvalidOffsetBuffer;
                    try validateOffsetsI64(offsets, total_len, data_len);
                }
            },
            .fixed_size_list, .struct_ => {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                const expected = switch (dt) {
                    .fixed_size_list => 1,
                    .struct_ => |st| st.fields.len,
                    else => 0,
                };
                if (self.children.len != expected) return error.InvalidChildren;
                if (dt == .fixed_size_list) {
                    const list_size = std.math.cast(usize, dt.fixed_size_list.list_size) orelse return error.InvalidChildren;
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
                const type_ids = self.buffers[0].typedSlice(i8) catch return error.InvalidOffsetBuffer;
                try validateUnionTypeIds(uni, type_ids, self.offset, total_len);
            },
            .dense_union => |uni| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
                if (self.buffers[0].len() < total_len) return error.BufferTooSmall;
                const needed = std.math.mul(usize, total_len, @sizeOf(i32)) catch return error.BufferTooSmall;
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
                const type_ids = self.buffers[0].typedSlice(i8) catch return error.InvalidOffsetBuffer;
                const offsets = self.buffers[1].typedSlice(i32) catch return error.InvalidOffsetBuffer;
                try validateUnionTypeIds(uni, type_ids, self.offset, total_len);
                try validateDenseUnionOffsets(uni, type_ids, offsets, self.children, self.offset, total_len);
            },
            .run_end_encoded => {
                if (self.buffers.len != 0) return error.InvalidBufferCount;
                if (self.children.len != 2) return error.InvalidChildren;

                const run_ends = self.children[0].data();
                const values = self.children[1].data();
                const run_end_dt = switch (dt.run_end_encoded.run_end_type.bit_width) {
                    16 => if (dt.run_end_encoded.run_end_type.signed) DataType{ .int16 = {} } else return error.InvalidOffsetBuffer,
                    32 => if (dt.run_end_encoded.run_end_type.signed) DataType{ .int32 = {} } else return error.InvalidOffsetBuffer,
                    64 => if (dt.run_end_encoded.run_end_type.signed) DataType{ .int64 = {} } else return error.InvalidOffsetBuffer,
                    else => return error.InvalidOffsetBuffer,
                };
                if (!std.meta.eql(run_ends.data_type, run_end_dt)) return error.InvalidChildren;
                if (run_ends.buffers.len < 2) return error.InvalidChildren;

                const byte_width = switch (dt) {
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
                    2 => if (dt.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i16, (run_ends.buffers[1].typedSlice(i16) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u16, (run_ends.buffers[1].typedSlice(u16) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len),
                    4 => if (dt.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i32, (run_ends.buffers[1].typedSlice(i32) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u32, (run_ends.buffers[1].typedSlice(u32) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len),
                    8 => if (dt.run_end_encoded.run_end_type.signed)
                        try validateRunEndsSigned(i64, (run_ends.buffers[1].typedSlice(i64) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len)
                    else
                        try validateRunEndsUnsigned(u64, (run_ends.buffers[1].typedSlice(u64) catch return error.InvalidOffsetBuffer)[run_ends_slice_start..run_ends_slice_end], total_len),
                    else => return error.InvalidOffsetBuffer,
                }
            },
            else => {
                if (fixedWidthByteSize(dt)) |byte_width| {
                    if (self.buffers.len < 2) return error.InvalidBufferCount;
                    if (byte_width == 0) return error.InvalidOffsetBuffer;
                    const needed = std.math.mul(usize, total_len, byte_width) catch return error.BufferTooSmall;
                    if (self.buffers[1].len() < needed) return error.BufferTooSmall;
                }
            },
        }
    }

    pub fn validateFull(self: Self) ValidationError!void {
        try self.validateLayout();
        const dt = layoutDataType(self.data_type);

        if (self.null_count) |expected_count| {
            if (hasTopLevelValidityBitmap(dt) and self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
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

test "array data validateLayout accepts extension by delegating to storage layout" {
    const storage = DataType{ .int32 = {} };
    const ext = DataType{
        .extension = .{
            .name = "com.example.int32_ext",
            .storage_type = &storage,
            .metadata = "v1",
        },
    };
    var values_bytes: [3 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3 }));
    const data = ArrayData{
        .data_type = ext,
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

test "array data validateLayout rejects sparse union nonzero null count" {
    const value_type = DataType{ .int32 = {} };
    const union_fields = [_]datatype.Field{.{ .name = "i", .data_type = &value_type, .nullable = true }};
    const union_type_ids = [_]i8{5};
    const sparse_union_type = DataType{
        .sparse_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .sparse,
        },
    };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 11, 22 }));
    const child_data = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child_data);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const type_ids: [2]u8 = .{ 5, 5 };
    const data = ArrayData{
        .data_type = sparse_union_type,
        .length = 2,
        .null_count = 1,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(type_ids[0..])},
        .children = children,
    };

    try std.testing.expectError(error.InvalidNullCount, data.validateLayout());
}

test "array data validateLayout rejects sparse union invalid type id" {
    const value_type = DataType{ .int32 = {} };
    const union_fields = [_]datatype.Field{.{ .name = "i", .data_type = &value_type, .nullable = true }};
    const union_type_ids = [_]i8{5};
    const sparse_union_type = DataType{
        .sparse_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .sparse,
        },
    };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 11, 22 }));
    const child_data = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child_data);
    defer child_ref.release();
    const children = &[_]ArrayRef{child_ref.retain()};
    defer {
        var owned = children[0];
        owned.release();
    }

    const type_ids: [2]u8 = .{ 5, 6 };
    const data = ArrayData{
        .data_type = sparse_union_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{SharedBuffer.fromSlice(type_ids[0..])},
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data validateLayout accepts dense union with valid type ids and offsets" {
    const int_type = DataType{ .int32 = {} };
    const union_fields = [_]datatype.Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &int_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const dense_union_type = DataType{
        .dense_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .dense,
        },
    };

    var child0_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child0_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 11 }));
    const child0_data = ArrayData{
        .data_type = int_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child0_values[0..]) },
    };
    var child0_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child0_data);
    defer child0_ref.release();

    var child1_values: [1 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child1_values[0..], std.mem.sliceAsBytes(&[_]i32{20}));
    const child1_data = ArrayData{
        .data_type = int_type,
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child1_values[0..]) },
    };
    var child1_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child1_data);
    defer child1_ref.release();

    const children = &[_]ArrayRef{ child0_ref.retain(), child1_ref.retain() };
    defer {
        var owned0 = children[0];
        owned0.release();
        var owned1 = children[1];
        owned1.release();
    }

    const type_ids: [3]u8 = .{ 5, 7, 5 };
    const offsets = [_]i32{ 0, 0, 1 };
    var offsets_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offsets_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));

    const data = ArrayData{
        .data_type = dense_union_type,
        .length = 3,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.fromSlice(type_ids[0..]),
            SharedBuffer.fromSlice(offsets_bytes[0..]),
        },
        .children = children,
    };

    try data.validateLayout();
}

test "array data validateLayout rejects dense union invalid type id" {
    const int_type = DataType{ .int32 = {} };
    const union_fields = [_]datatype.Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &int_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const dense_union_type = DataType{
        .dense_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .dense,
        },
    };

    var child0_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child0_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 11 }));
    const child0_data = ArrayData{
        .data_type = int_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child0_values[0..]) },
    };
    var child0_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child0_data);
    defer child0_ref.release();

    var child1_values: [1 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child1_values[0..], std.mem.sliceAsBytes(&[_]i32{20}));
    const child1_data = ArrayData{
        .data_type = int_type,
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child1_values[0..]) },
    };
    var child1_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child1_data);
    defer child1_ref.release();

    const children = &[_]ArrayRef{ child0_ref.retain(), child1_ref.retain() };
    defer {
        var owned0 = children[0];
        owned0.release();
        var owned1 = children[1];
        owned1.release();
    }

    const type_ids: [3]u8 = .{ 5, 9, 5 };
    const offsets = [_]i32{ 0, 0, 1 };
    var offsets_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offsets_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));

    const data = ArrayData{
        .data_type = dense_union_type,
        .length = 3,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.fromSlice(type_ids[0..]),
            SharedBuffer.fromSlice(offsets_bytes[0..]),
        },
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data validateLayout rejects dense union offset out of bounds" {
    const int_type = DataType{ .int32 = {} };
    const union_fields = [_]datatype.Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &int_type, .nullable = true },
    };
    const union_type_ids = [_]i8{ 5, 7 };
    const dense_union_type = DataType{
        .dense_union = .{
            .type_ids = union_type_ids[0..],
            .fields = union_fields[0..],
            .mode = .dense,
        },
    };

    var child0_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child0_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 11 }));
    const child0_data = ArrayData{
        .data_type = int_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child0_values[0..]) },
    };
    var child0_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child0_data);
    defer child0_ref.release();

    var child1_values: [1 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child1_values[0..], std.mem.sliceAsBytes(&[_]i32{20}));
    const child1_data = ArrayData{
        .data_type = int_type,
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child1_values[0..]) },
    };
    var child1_ref = try ArrayRef.fromBorrowed(std.testing.allocator, child1_data);
    defer child1_ref.release();

    const children = &[_]ArrayRef{ child0_ref.retain(), child1_ref.retain() };
    defer {
        var owned0 = children[0];
        owned0.release();
        var owned1 = children[1];
        owned1.release();
    }

    const type_ids: [3]u8 = .{ 5, 7, 5 };
    const offsets = [_]i32{ 0, 1, 1 };
    var offsets_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offsets_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));

    const data = ArrayData{
        .data_type = dense_union_type,
        .length = 3,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.fromSlice(type_ids[0..]),
            SharedBuffer.fromSlice(offsets_bytes[0..]),
        },
        .children = children,
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
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

const ReeTestRefs = struct {
    run_ends_ref: ArrayRef,
    values_ref: ArrayRef,
    children: [2]ArrayRef,

    fn init(allocator: std.mem.Allocator, run_ends_child: ArrayData, values_child: ArrayData) !ReeTestRefs {
        var run_ends_ref = try ArrayRef.fromBorrowed(allocator, run_ends_child);
        errdefer run_ends_ref.release();
        var values_ref = try ArrayRef.fromBorrowed(allocator, values_child);
        errdefer values_ref.release();
        return .{
            .run_ends_ref = run_ends_ref,
            .values_ref = values_ref,
            .children = .{ run_ends_ref.retain(), values_ref.retain() },
        };
    }

    fn deinit(self: *ReeTestRefs) void {
        var run_ends_owned = self.children[0];
        run_ends_owned.release();
        var values_owned = self.children[1];
        values_owned.release();
        self.run_ends_ref.release();
        self.values_ref.release();
    }
};

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

    var ree_refs = try ReeTestRefs.init(std.testing.allocator, run_ends_child, values_child);
    defer ree_refs.deinit();

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
        .children = ree_refs.children[0..],
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

    var ree_refs = try ReeTestRefs.init(std.testing.allocator, run_ends_child, values_child);
    defer ree_refs.deinit();

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
        .children = ree_refs.children[0..],
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}

test "array data validateLayout rejects run_end_encoded int8 run_end type" {
    const value_type = DataType{ .int32 = {} };
    const run_end_type = datatype.IntType{ .bit_width = 8, .signed = true };
    const ree_type = DataType{ .run_end_encoded = .{ .run_end_type = run_end_type, .value_type = &value_type } };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 20 }));
    const values_child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };

    const run_ends = [_]i8{ 2, 5 };
    var run_end_bytes: [run_ends.len * @sizeOf(i8)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));
    const run_ends_child = ArrayData{
        .data_type = .{ .int8 = {} },
        .length = run_ends.len,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(run_end_bytes[0..]) },
    };

    var ree_refs = try ReeTestRefs.init(std.testing.allocator, run_ends_child, values_child);
    defer ree_refs.deinit();

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
        .children = ree_refs.children[0..],
    };

    try std.testing.expectError(error.InvalidOffsetBuffer, data.validateLayout());
}

test "array data validateLayout rejects run_end_encoded unsigned run_end type" {
    const value_type = DataType{ .int32 = {} };
    const run_end_type = datatype.IntType{ .bit_width = 32, .signed = false };
    const ree_type = DataType{ .run_end_encoded = .{ .run_end_type = run_end_type, .value_type = &value_type } };

    var child_values: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 10, 20 }));
    const values_child = ArrayData{
        .data_type = value_type,
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };

    const run_ends = [_]u32{ 2, 5 };
    var run_end_bytes: [run_ends.len * @sizeOf(u32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(run_end_bytes[0..], std.mem.sliceAsBytes(run_ends[0..]));
    const run_ends_child = ArrayData{
        .data_type = .{ .uint32 = {} },
        .length = run_ends.len,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(run_end_bytes[0..]) },
    };

    var ree_refs = try ReeTestRefs.init(std.testing.allocator, run_ends_child, values_child);
    defer ree_refs.deinit();

    const data = ArrayData{
        .data_type = ree_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{},
        .children = ree_refs.children[0..],
    };

    try std.testing.expectError(error.InvalidOffsetBuffer, data.validateLayout());
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

test "array data validateLayout accepts string_view with inline and variadic data" {
    var views: [2 * 16]u8 align(buffer.ALIGNMENT) = [_]u8{0} ** (2 * 16);
    const short = "hello";
    views[0] = @intCast(short.len);
    @memcpy(views[4 .. 4 + short.len], short);

    const long = "abcdefghijklmnop"; // 16 bytes
    views[16] = @intCast(long.len);
    @memcpy(views[20..24], long[0..4]); // prefix
    // buffer_index=0 and offset=0 are already zeroed.

    const data = ArrayData{
        .data_type = DataType{ .string_view = {} },
        .length = 2,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(views[0..]),
            SharedBuffer.fromSlice(long),
        },
    };
    try data.validateLayout();
}

test "array data validateLayout rejects string_view with mismatched prefix" {
    var views: [16]u8 align(buffer.ALIGNMENT) = [_]u8{0} ** 16;
    const long = "abcdefghijklmnop"; // 16 bytes
    views[0] = @intCast(long.len);
    @memcpy(views[4..8], "wxyz"); // wrong prefix

    const data = ArrayData{
        .data_type = DataType{ .string_view = {} },
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(views[0..]),
            SharedBuffer.fromSlice(long),
        },
    };
    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}
