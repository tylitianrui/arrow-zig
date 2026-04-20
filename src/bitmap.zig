const std = @import("std");
const SharedBuffer = @import("buffer.zig").SharedBuffer;
const OwnedBuffer = @import("buffer.zig").OwnedBuffer;

// Return the number of bytes required to store the requested bit count.
pub fn byteLength(bit_length: usize) usize {
    return (bit_length + 7) >> 3;
}

fn bitMask(bit_index: usize) u8 {
    return @as(u8, 1) << @as(u3, @intCast(bit_index & 7));
}

// Read a single bit using Arrow's least-significant-bit-first order.
pub fn bitIsSet(data: []const u8, bit_index: usize) bool {
    const byte = data[bit_index >> 3];
    return (byte & bitMask(bit_index)) != 0;
}

// Set a single bit in-place using Arrow's least-significant-bit-first bit order.
pub fn setBit(data: []u8, bit_index: usize) void {
    data[bit_index >> 3] |= bitMask(bit_index);
}

// Clear a single bit in-place using Arrow's least-significant-bit-first bit order.
pub fn clearBit(data: []u8, bit_index: usize) void {
    data[bit_index >> 3] &= ~bitMask(bit_index);
}

// Write a single bit in-place with a boolean value.
pub fn writeBit(data: []u8, bit_index: usize, value: bool) void {
    if (value) setBit(data, bit_index) else clearBit(data, bit_index);
}

// Count set bits across the logical length, ignoring padding bits.
pub fn countSetBit(data: []const u8, bit_len: usize) usize {
    if (bit_len == 0) return 0;
    var count: usize = 0;
    const full_bytes = bit_len >> 3;
    const remainder = bit_len & 7;
    for (data[0..full_bytes]) |byte| {
        count += @popCount(byte);
    }

    if (remainder > 0) {
        const last_byte = data[full_bytes];
        const mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
        count += @popCount(last_byte & mask);
    }

    return count;
}

// Count unset bits across the logical length, ignoring padding bits.
pub fn countUnsetBit(data: []const u8, bit_len: usize) usize {
    return bit_len - countSetBit(data, bit_len);
}

fn clearTrailingBits(bytes: []u8, len: usize, valid: bool) void {
    if (len == 0 or bytes.len == 0) return;

    const remainder = len & 7;
    if (remainder == 0) return;

    const last = bytes.len - 1;
    const keep_mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
    bytes[last] &= keep_mask;
    if (valid) bytes[last] |= keep_mask;
}

// Immutable Arrow validity bitmap view.
pub const ValidityBitmap = struct {
    data: []const u8,
    bit_len: usize,

    // Build a validity bitmap view from a buffer and logical length.
    pub fn fromBuffer(buf: SharedBuffer, bit_len: usize) ValidityBitmap {
        return .{ .data = buf.data, .bit_len = bit_len };
    }
    // Read the validity bit for an index.
    pub inline fn isValid(self: ValidityBitmap, i: usize) bool {
        std.debug.assert(i < self.bit_len);
        return bitIsSet(self.data, i);
    }
    // Invert the validity bit for convenience.
    pub inline fn isNull(self: ValidityBitmap, i: usize) bool {
        return !self.isValid(i);
    }
    // Count valid bits in the logical range.
    pub fn countValid(self: ValidityBitmap) usize {
        return countSetBit(self.data, self.bit_len);
    }
    // Count null bits in the logical range.
    pub fn countNulls(self: ValidityBitmap) usize {
        return self.bit_len - self.countValid();
    }
};

// MutableValidityBitmap owns writable storage for building or editing Arrow validity bits.
// Mutable Arrow validity bitmap with owned storage.
pub const MutableValidityBitmap = struct {
    buf: OwnedBuffer,
    bit_len: usize,

    // Allocate a bitmap with all values marked valid.
    pub fn initAllValid(allocator: std.mem.Allocator, len: usize) !MutableValidityBitmap {
        return initFilled(allocator, len, true);
    }

    // Allocate a bitmap with all values marked null.
    pub fn initAllNull(allocator: std.mem.Allocator, bit_len: usize) !MutableValidityBitmap {
        return initFilled(allocator, bit_len, false);
    }

    fn initFilled(allocator: std.mem.Allocator, bit_len: usize, valid: bool) !MutableValidityBitmap {
        const used_bytes = byteLength(bit_len);
        var buf = try OwnedBuffer.init(allocator, used_bytes);
        if (used_bytes > 0) {
            @memset(buf.data[0..used_bytes], if (valid) 0xFF else 0x00);
            clearTrailingBits(buf.data[0..used_bytes], bit_len, valid);
        }

        return .{
            .buf = buf,
            .bit_len = bit_len,
        };
    }

    // Release the owned storage.
    pub fn deinit(self: *MutableValidityBitmap) void {
        self.buf.deinit();
    }

    // Return the number of logical bits tracked by this bitmap.
    pub fn bitLength(self: MutableValidityBitmap) usize {
        return self.bit_len;
    }

    // Mark the indexed value as valid.
    pub fn setValid(self: *MutableValidityBitmap, index: usize) void {
        std.debug.assert(index < self.bit_len);
        setBit(self.buf.data[0..byteLength(self.bit_len)], index);
    }

    // Read the indexed validity bit from the mutable bitmap.
    pub inline fn isValid(self: MutableValidityBitmap, index: usize) bool {
        return ValidityBitmap.fromBuffer(self.toBuffer(), self.bit_len).isValid(index);
    }

    // Mark the indexed value as null.
    pub fn setNull(self: *MutableValidityBitmap, index: usize) void {
        std.debug.assert(index < self.bit_len);

        const byte_index = index / 8;
        const mask = bitMask(index);
        self.buf.data[byte_index] &= ~mask;
    }

    // Invert the validity bit for convenience.
    pub inline fn isNull(self: MutableValidityBitmap, index: usize) bool {
        return !self.isValid(index);
    }

    // Set the validity bit to a desired value.
    pub fn set(self: *MutableValidityBitmap, index: usize, valid: bool) void {
        if (valid) self.setValid(index) else self.setNull(index);
    }

    // Count valid bits in the logical range.
    pub fn countValid(self: MutableValidityBitmap) usize {
        return countSetBit(self.buf.data, self.bit_len);
    }

    // Count null bits in the logical range.
    pub fn countNulls(self: MutableValidityBitmap) usize {
        return countUnsetBit(self.buf.data, self.bit_len);
    }

    // Expose the logical bitmap bytes as a borrowed shared buffer view.
    pub fn toBuffer(self: MutableValidityBitmap) SharedBuffer {
        return SharedBuffer.fromSlice(self.buf.data[0..byteLength(self.bit_len)]);
    }
};

// Verify byte-length rounding rules.
test "bitmap byte length rounds up" {
    try std.testing.expectEqual(@as(usize, 0), byteLength(0));
    try std.testing.expectEqual(@as(usize, 1), byteLength(1));
    try std.testing.expectEqual(@as(usize, 1), byteLength(8));
    try std.testing.expectEqual(@as(usize, 2), byteLength(9));
}

// Exercise set, clear, and write operations.
test "bitmap bit operations set and clear" {
    var data: [2]u8 = .{ 0, 0 };

    setBit(data[0..], 0);
    setBit(data[0..], 9);
    try std.testing.expect(bitIsSet(data[0..], 0));
    try std.testing.expect(!bitIsSet(data[0..], 1));
    try std.testing.expect(bitIsSet(data[0..], 9));

    clearBit(data[0..], 0);
    try std.testing.expect(!bitIsSet(data[0..], 0));

    writeBit(data[0..], 1, true);
    writeBit(data[0..], 9, false);
    try std.testing.expect(bitIsSet(data[0..], 1));
    try std.testing.expect(!bitIsSet(data[0..], 9));
}

// Validate set/unset counting with partial-byte masks.
test "bitmap counts set and unset bits" {
    const data = [_]u8{ 0b1011_0101, 0b0000_1111 };

    try std.testing.expectEqual(@as(usize, 9), countSetBit(data[0..], 12));
    try std.testing.expectEqual(@as(usize, 3), countUnsetBit(data[0..], 12));
    try std.testing.expectEqual(@as(usize, 9), countSetBit(data[0..], 16));
    try std.testing.expectEqual(@as(usize, 7), countUnsetBit(data[0..], 16));
}

// Read-only validity bitmap behavior.
test "validity bitmap reads values" {
    const data = [_]u8{0b0000_0101};
    const bitmap = ValidityBitmap{ .data = data[0..], .bit_len = 5 };

    try std.testing.expect(bitmap.isValid(0));
    try std.testing.expect(bitmap.isNull(1));
    try std.testing.expect(bitmap.isValid(2));
    try std.testing.expect(bitmap.isNull(3));
    try std.testing.expect(bitmap.isNull(4));
    try std.testing.expectEqual(@as(usize, 2), bitmap.countValid());
    try std.testing.expectEqual(@as(usize, 3), bitmap.countNulls());
}

// Allocate, mutate, and export a mutable validity bitmap.
test "mutable validity bitmap init and mutate" {
    var bitmap = try MutableValidityBitmap.initAllNull(std.testing.allocator, 10);
    defer bitmap.deinit();

    try std.testing.expectEqual(@as(usize, 10), bitmap.bitLength());
    try std.testing.expectEqual(@as(usize, 0), bitmap.countValid());
    try std.testing.expectEqual(@as(usize, 10), bitmap.countNulls());

    bitmap.setValid(0);
    bitmap.setValid(9);
    try std.testing.expect(bitmap.isValid(0));
    try std.testing.expect(bitmap.isNull(1));
    try std.testing.expect(bitmap.isValid(9));

    bitmap.setNull(0);
    bitmap.set(1, true);
    try std.testing.expect(bitmap.isNull(0));
    try std.testing.expect(bitmap.isValid(1));

    const buf = bitmap.toBuffer();
    try std.testing.expectEqual(@as(usize, 2), buf.len());
}

// Ensure trailing padding bits are cleared for non-byte-aligned lengths.
test "mutable validity bitmap clears trailing bits" {
    var bitmap = try MutableValidityBitmap.initAllValid(std.testing.allocator, 10);
    defer bitmap.deinit();

    const buf = bitmap.toBuffer();
    try std.testing.expectEqual(@as(usize, 2), buf.len());
    try std.testing.expectEqual(@as(u8, 0b0000_0011), buf.data[1]);
    try std.testing.expectEqual(@as(usize, 10), bitmap.countValid());
}
