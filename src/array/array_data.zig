const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");

pub const Buffer = buffer.Buffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const DataType = datatype.DataType;

/// Core Arrow array metadata and buffers.
///
/// Field semantics:
/// - data_type: logical Arrow type describing the array.
/// - length: number of logical elements.
/// - offset: logical slice offset into buffers.
/// - null_count: number of nulls (-1 means unknown; 0 means no nulls).
/// - buffers: Arrow buffers in type-specific order.
/// - children: child arrays for nested types (list/struct/union/map).
/// - dictionary: dictionary values for dictionary-encoded arrays.
///
/// Common buffer layouts:
/// - Primitive (int/float): [validity], [values].
/// - Boolean: [validity], [bit-packed values].
/// - String/Binary: [validity], [i32 offsets], [data bytes].
/// - List: [validity], [i32 offsets], children[0] = values.
/// - FixedSizeList: [validity], children[0] = values.
/// - Struct: [validity], children = fields in order.
/// - Dictionary: [validity], [indices], dictionary = values array.
/// - Union (sparse): [type_ids], children = fields; no offsets buffer.
/// - Union (dense): [type_ids], [offsets], children = fields.
/// - Map: [validity], [i32 offsets], children[0] = struct of (key, item).
/// - RunEndEncoded: [run_ends], children[0] = values.
pub const ArrayData = struct {
    data_type: *const DataType,
    length: usize,
    offset: usize = 0,
    null_count: isize = -1, // -1 means unknown; 0 means no nulls
    buffers: []const Buffer,
    children: []const ArrayData = &.{},
    dictionary: ?*const ArrayData = null,

    pub fn validity(self: ArrayData) ?ValidityBitmap {
        if (self.buffers.len == 0) return null;
        if (self.buffers[0].isEmpty()) return null;
        return ValidityBitmap.fromBuffer(self.buffers[0], self.length + self.offset);
    }

    pub fn isNull(self: ArrayData, i: usize) bool {
        std.debug.assert(i < self.length);
        if (self.null_count == 0) return false;
        const validity_bitmap = self.validity() orelse return false;
        return !validity_bitmap.isValid(self.offset + i);
    }

    pub fn isValid(self: ArrayData, i: usize) bool {
        return !self.isNull(i);
    }

    pub fn nullCount(self: *ArrayData) usize {
        if (self.null_count >= 0) return @intCast(self.null_count);
        const validity_bitmap = self.validity() orelse {
            self.null_count = 0;
            return 0;
        };
        var count: usize = 0;
        var i: usize = 0;
        while (i < self.length) : (i += 1) {
            if (!validity_bitmap.isValid(self.offset + i)) count += 1;
        }
        self.null_count = @intCast(count);
        return count;
    }
};
