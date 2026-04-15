const std = @import("std");
const builtin = @import("builtin");

pub const ZstdSymbols = struct {
    compress_bound: *const fn (usize) callconv(.c) usize,
    compress: *const fn (?*anyopaque, usize, ?*const anyopaque, usize, c_int) callconv(.c) usize,
    is_error: *const fn (usize) callconv(.c) c_uint,
};

pub const Lz4Symbols = struct {
    create_decompression_context: *const fn (*?*anyopaque, c_uint) callconv(.c) usize,
    free_decompression_context: *const fn (?*anyopaque) callconv(.c) usize,
    decompress: *const fn (?*anyopaque, ?*anyopaque, *usize, ?*const anyopaque, *usize, ?*const anyopaque) callconv(.c) usize,
    is_error: *const fn (usize) callconv(.c) c_uint,
    compress_frame_bound: *const fn (usize, ?*const anyopaque) callconv(.c) usize,
    compress_frame: *const fn (?*anyopaque, usize, ?*const anyopaque, usize, ?*const anyopaque) callconv(.c) usize,
};

extern fn ZSTD_compressBound(src_size: usize) callconv(.c) usize;
extern fn ZSTD_compress(dst: ?*anyopaque, dst_capacity: usize, src: ?*const anyopaque, src_size: usize, compression_level: c_int) callconv(.c) usize;
extern fn ZSTD_isError(code: usize) callconv(.c) c_uint;

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
extern fn LZ4F_compressFrameBound(src_size: usize, prefs_ptr: ?*const anyopaque) callconv(.c) usize;
extern fn LZ4F_compressFrame(
    dst: ?*anyopaque,
    dst_capacity: usize,
    src: ?*const anyopaque,
    src_size: usize,
    prefs_ptr: ?*const anyopaque,
) callconv(.c) usize;

const linked_zstd_symbols: ZstdSymbols = .{
    .compress_bound = ZSTD_compressBound,
    .compress = ZSTD_compress,
    .is_error = ZSTD_isError,
};

const linked_lz4_symbols: Lz4Symbols = .{
    .create_decompression_context = LZ4F_createDecompressionContext,
    .free_decompression_context = LZ4F_freeDecompressionContext,
    .decompress = LZ4F_decompress,
    .is_error = LZ4F_isError,
    .compress_frame_bound = LZ4F_compressFrameBound,
    .compress_frame = LZ4F_compressFrame,
};

const ZstdState = enum { uninitialized, ready, missing };
const Lz4State = enum { uninitialized, ready, missing };

var zstd_mutex: std.Thread.Mutex = .{};
var zstd_state: ZstdState = .uninitialized;
var zstd_lib: ?std.DynLib = null;
var zstd_symbols: ?ZstdSymbols = null;

var lz4_mutex: std.Thread.Mutex = .{};
var lz4_state: Lz4State = .uninitialized;
var lz4_lib: ?std.DynLib = null;
var lz4_symbols: ?Lz4Symbols = null;

pub fn loadZstdSymbols() !*const ZstdSymbols {
    if (builtin.os.tag != .windows) return &linked_zstd_symbols;

    zstd_mutex.lock();
    defer zstd_mutex.unlock();

    switch (zstd_state) {
        .ready => return &(zstd_symbols orelse return error.FileNotFound),
        .missing => return error.FileNotFound,
        .uninitialized => {},
    }

    const env_path = std.process.getEnvVarOwned(std.heap.page_allocator, "ZARROW_ZSTD_LIB") catch null;
    defer if (env_path) |p| std.heap.page_allocator.free(p);

    var loaded = if (env_path) |p| loadOneZstd(p) else null;
    if (loaded == null) {
        const candidates = zstdDefaultCandidates();
        for (candidates) |candidate| {
            loaded = loadOneZstd(candidate);
            if (loaded != null) break;
        }
    }

    if (loaded) |entry| {
        zstd_lib = entry.lib;
        zstd_symbols = entry.syms;
        zstd_state = .ready;
        return &(zstd_symbols.?);
    }

    zstd_state = .missing;
    return error.FileNotFound;
}

pub fn loadLz4Symbols() !*const Lz4Symbols {
    if (builtin.os.tag != .windows) return &linked_lz4_symbols;

    lz4_mutex.lock();
    defer lz4_mutex.unlock();

    switch (lz4_state) {
        .ready => return &(lz4_symbols orelse return error.FileNotFound),
        .missing => return error.FileNotFound,
        .uninitialized => {},
    }

    const env_path = std.process.getEnvVarOwned(std.heap.page_allocator, "ZARROW_LZ4_LIB") catch null;
    defer if (env_path) |p| std.heap.page_allocator.free(p);

    var loaded = if (env_path) |p| loadOneLz4(p) else null;
    if (loaded == null) {
        const candidates = lz4DefaultCandidates();
        for (candidates) |candidate| {
            loaded = loadOneLz4(candidate);
            if (loaded != null) break;
        }
    }

    if (loaded) |entry| {
        lz4_lib = entry.lib;
        lz4_symbols = entry.syms;
        lz4_state = .ready;
        return &(lz4_symbols.?);
    }

    lz4_state = .missing;
    return error.FileNotFound;
}

const LoadedZstd = struct {
    lib: std.DynLib,
    syms: ZstdSymbols,
};

const LoadedLz4 = struct {
    lib: std.DynLib,
    syms: Lz4Symbols,
};

fn loadOneZstd(path: []const u8) ?LoadedZstd {
    var lib = std.DynLib.open(path) catch return null;

    const bound_fn = lib.lookup(*const fn (usize) callconv(.c) usize, "ZSTD_compressBound") orelse {
        lib.close();
        return null;
    };
    const compress_fn = lib.lookup(*const fn (?*anyopaque, usize, ?*const anyopaque, usize, c_int) callconv(.c) usize, "ZSTD_compress") orelse {
        lib.close();
        return null;
    };
    const is_error_fn = lib.lookup(*const fn (usize) callconv(.c) c_uint, "ZSTD_isError") orelse {
        lib.close();
        return null;
    };

    return .{
        .lib = lib,
        .syms = .{
            .compress_bound = bound_fn,
            .compress = compress_fn,
            .is_error = is_error_fn,
        },
    };
}

fn loadOneLz4(path: []const u8) ?LoadedLz4 {
    var lib = std.DynLib.open(path) catch return null;

    const create_dctx_fn = lib.lookup(*const fn (*?*anyopaque, c_uint) callconv(.c) usize, "LZ4F_createDecompressionContext") orelse {
        lib.close();
        return null;
    };
    const free_dctx_fn = lib.lookup(*const fn (?*anyopaque) callconv(.c) usize, "LZ4F_freeDecompressionContext") orelse {
        lib.close();
        return null;
    };
    const decompress_fn = lib.lookup(*const fn (?*anyopaque, ?*anyopaque, *usize, ?*const anyopaque, *usize, ?*const anyopaque) callconv(.c) usize, "LZ4F_decompress") orelse {
        lib.close();
        return null;
    };
    const is_error_fn = lib.lookup(*const fn (usize) callconv(.c) c_uint, "LZ4F_isError") orelse {
        lib.close();
        return null;
    };
    const bound_fn = lib.lookup(*const fn (usize, ?*const anyopaque) callconv(.c) usize, "LZ4F_compressFrameBound") orelse {
        lib.close();
        return null;
    };
    const compress_fn = lib.lookup(*const fn (?*anyopaque, usize, ?*const anyopaque, usize, ?*const anyopaque) callconv(.c) usize, "LZ4F_compressFrame") orelse {
        lib.close();
        return null;
    };

    return .{
        .lib = lib,
        .syms = .{
            .create_decompression_context = create_dctx_fn,
            .free_decompression_context = free_dctx_fn,
            .decompress = decompress_fn,
            .is_error = is_error_fn,
            .compress_frame_bound = bound_fn,
            .compress_frame = compress_fn,
        },
    };
}

fn zstdDefaultCandidates() []const []const u8 {
    return switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "libzstd.dylib",
            "libzstd.1.dylib",
            "/opt/homebrew/lib/libzstd.dylib",
            "/usr/local/lib/libzstd.dylib",
        },
        .linux => &[_][]const u8{
            "libzstd.so.1",
            "libzstd.so",
            "/lib/x86_64-linux-gnu/libzstd.so.1",
            "/usr/lib/x86_64-linux-gnu/libzstd.so.1",
            "/lib/aarch64-linux-gnu/libzstd.so.1",
            "/usr/lib/aarch64-linux-gnu/libzstd.so.1",
            "/lib64/libzstd.so.1",
            "/usr/lib64/libzstd.so.1",
            "/lib/libzstd.so.1",
            "/usr/lib/libzstd.so.1",
        },
        .windows => &[_][]const u8{
            "zstd.dll",
            "libzstd.dll",
        },
        else => &[_][]const u8{},
    };
}

fn lz4DefaultCandidates() []const []const u8 {
    return switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "liblz4.dylib",
            "liblz4.1.dylib",
            "/opt/homebrew/lib/liblz4.dylib",
            "/usr/local/lib/liblz4.dylib",
        },
        .linux => &[_][]const u8{
            "liblz4.so.1",
            "liblz4.so",
            "/lib/x86_64-linux-gnu/liblz4.so.1",
            "/usr/lib/x86_64-linux-gnu/liblz4.so.1",
            "/lib/aarch64-linux-gnu/liblz4.so.1",
            "/usr/lib/aarch64-linux-gnu/liblz4.so.1",
            "/lib64/liblz4.so.1",
            "/usr/lib64/liblz4.so.1",
            "/lib/liblz4.so.1",
            "/usr/lib/liblz4.so.1",
        },
        .windows => &[_][]const u8{
            "lz4.dll",
            "liblz4.dll",
        },
        else => &[_][]const u8{},
    };
}
