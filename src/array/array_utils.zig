const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");

// Shared helper routines used by multiple array builders.

pub const OwnedBuffer = buffer.OwnedBuffer;

/// Execute initValidityAllValid logic for this type.
pub fn initValidityAllValid(allocator: std.mem.Allocator, bit_len: usize) !OwnedBuffer {
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

/// Execute ensureBitmapCapacity logic for this type.
pub fn ensureBitmapCapacity(buf: *OwnedBuffer, bit_len: usize) !void {
    const needed = bitmap.byteLength(bit_len);
    if (needed <= buf.len()) return;
    try buf.resize(needed);
}

test "initValidityAllValid marks bits and clears padding" {
    const allocator = std.testing.allocator;

    var buf = try initValidityAllValid(allocator, 10);
    defer buf.deinit();

    try std.testing.expectEqual(@as(usize, 10), bitmap.countSetBit(buf.data, 10));
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        try std.testing.expect(bitmap.bitIsSet(buf.data, i));
    }

    const bit_cap = bitmap.byteLength(10) * 8;
    i = 10;
    while (i < bit_cap) : (i += 1) {
        try std.testing.expect(!bitmap.bitIsSet(buf.data, i));
    }
}

test "initValidityAllValid handles zero length" {
    const allocator = std.testing.allocator;

    var buf = try initValidityAllValid(allocator, 0);
    defer buf.deinit();

    try std.testing.expectEqual(@as(usize, 0), bitmap.countSetBit(buf.data, 0));
}

test "ensureBitmapCapacity grows buffer" {
    const allocator = std.testing.allocator;

    var buf = try OwnedBuffer.init(allocator, 1);
    defer buf.deinit();

    const before = buf.len();
    try ensureBitmapCapacity(&buf, (before + 1) * 8);
    try std.testing.expect(buf.len() > before);
}
