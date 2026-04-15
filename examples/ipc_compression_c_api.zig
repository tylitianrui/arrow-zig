const std = @import("std");

extern fn ZSTD_compressBound(src_size: usize) callconv(.c) usize;
extern fn ZSTD_compress(dst: ?*anyopaque, dst_capacity: usize, src: ?*const anyopaque, src_size: usize, compression_level: c_int) callconv(.c) usize;
extern fn ZSTD_decompress(dst: ?*anyopaque, dst_capacity: usize, src: ?*const anyopaque, compressed_size: usize) callconv(.c) usize;
extern fn ZSTD_isError(code: usize) callconv(.c) c_uint;

extern fn LZ4F_compressFrameBound(src_size: usize, prefs_ptr: ?*const anyopaque) callconv(.c) usize;
extern fn LZ4F_compressFrame(dst: ?*anyopaque, dst_capacity: usize, src: ?*const anyopaque, src_size: usize, prefs_ptr: ?*const anyopaque) callconv(.c) usize;
extern fn LZ4F_createDecompressionContext(ctx_ptr: *?*anyopaque, version: c_uint) callconv(.c) usize;
extern fn LZ4F_freeDecompressionContext(ctx: ?*anyopaque) callconv(.c) usize;
extern fn LZ4F_decompress(
    ctx: ?*anyopaque,
    dst: ?*anyopaque,
    dst_size_ptr: *usize,
    src: ?*const anyopaque,
    src_size_ptr: *usize,
    options_ptr: ?*const anyopaque,
) callconv(.c) usize;
extern fn LZ4F_isError(code: usize) callconv(.c) c_uint;

const LZ4F_VERSION: c_uint = 100;

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const payload =
        ("Arrow IPC BodyCompression C API probe. 0123456789abcdef\n" ** 256) ++
        ("This verifies vendored zstd/lz4 C functions are linked and working.\n" ** 64);

    try verifyZstdRoundTrip(allocator, payload);
    try verifyLz4FrameRoundTrip(allocator, payload);

    std.debug.print("examples/ipc_compression_c_api.zig | success (payload={d} bytes)\n", .{payload.len});
}

fn verifyZstdRoundTrip(allocator: std.mem.Allocator, input: []const u8) !void {
    const bound = ZSTD_compressBound(input.len);
    if (bound == 0 or ZSTD_isError(bound) != 0) return error.ZstdCompressBoundFailed;

    const compressed = try allocator.alloc(u8, bound);
    defer allocator.free(compressed);

    const written = ZSTD_compress(
        if (compressed.len == 0) null else @ptrCast(compressed.ptr),
        compressed.len,
        if (input.len == 0) null else @ptrCast(input.ptr),
        input.len,
        3,
    );
    if (ZSTD_isError(written) != 0) return error.ZstdCompressFailed;
    if (written == 0 or written > compressed.len) return error.ZstdCompressFailed;

    const compressed_slice = compressed[0..written];
    const decoded = try allocator.alloc(u8, input.len);
    defer allocator.free(decoded);

    const decoded_len = ZSTD_decompress(
        if (decoded.len == 0) null else @ptrCast(decoded.ptr),
        decoded.len,
        if (compressed_slice.len == 0) null else @ptrCast(compressed_slice.ptr),
        compressed_slice.len,
    );
    if (ZSTD_isError(decoded_len) != 0) return error.ZstdDecompressFailed;
    if (decoded_len != input.len) return error.ZstdDecompressSizeMismatch;
    if (!std.mem.eql(u8, decoded[0..decoded_len], input)) return error.ZstdRoundTripMismatch;

    std.debug.print("zstd c api ok | input={d} compressed={d}\n", .{ input.len, compressed_slice.len });
}

fn verifyLz4FrameRoundTrip(allocator: std.mem.Allocator, input: []const u8) !void {
    const bound = LZ4F_compressFrameBound(input.len, null);
    if (bound == 0 or LZ4F_isError(bound) != 0) return error.Lz4CompressBoundFailed;

    const compressed = try allocator.alloc(u8, bound);
    defer allocator.free(compressed);

    const written = LZ4F_compressFrame(
        if (compressed.len == 0) null else @ptrCast(compressed.ptr),
        compressed.len,
        if (input.len == 0) null else @ptrCast(input.ptr),
        input.len,
        null,
    );
    if (LZ4F_isError(written) != 0) return error.Lz4CompressFailed;
    if (written == 0 or written > compressed.len) return error.Lz4CompressFailed;

    const compressed_slice = compressed[0..written];
    const decoded = try allocator.alloc(u8, input.len);
    defer allocator.free(decoded);

    var dctx: ?*anyopaque = null;
    const create_rc = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(create_rc) != 0 or dctx == null) return error.Lz4CreateDctxFailed;
    defer _ = LZ4F_freeDecompressionContext(dctx);

    var src_pos: usize = 0;
    var dst_pos: usize = 0;
    while (true) {
        var src_size = compressed_slice.len - src_pos;
        var dst_size = decoded.len - dst_pos;

        const rc = LZ4F_decompress(
            dctx,
            if (dst_size == 0) null else @ptrCast(decoded.ptr + dst_pos),
            &dst_size,
            if (src_size == 0) null else @ptrCast(compressed_slice.ptr + src_pos),
            &src_size,
            null,
        );
        if (LZ4F_isError(rc) != 0) return error.Lz4DecompressFailed;

        src_pos += src_size;
        dst_pos += dst_size;

        if (rc == 0) break;
        if (src_size == 0 and dst_size == 0) return error.Lz4DecompressStalled;
        if (src_pos > compressed_slice.len or dst_pos > decoded.len) return error.Lz4DecompressBounds;
    }

    if (src_pos != compressed_slice.len) return error.Lz4SourceNotFullyConsumed;
    if (dst_pos != input.len) return error.Lz4DecodedSizeMismatch;
    if (!std.mem.eql(u8, decoded[0..dst_pos], input)) return error.Lz4RoundTripMismatch;

    std.debug.print("lz4_frame c api ok | input={d} compressed={d}\n", .{ input.len, compressed_slice.len });
}
