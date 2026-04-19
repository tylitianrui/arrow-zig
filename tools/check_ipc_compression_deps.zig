const std = @import("std");

const RequiredVendorFile = struct {
    path: []const u8,
    sha256_hex: []const u8,
};

const required_vendor_files = [_]RequiredVendorFile{
    .{ .path = "vendor/zstd/zstd.c", .sha256_hex = "a9102d29368cc32ab6c95285f7a178cf0d87bdeac9a732b16fd97aca0b579dc7" },
    .{ .path = "vendor/zstd/zstd.h", .sha256_hex = "9b4bc8245565c98ccfc61c07749928b57e7c0f6fddb0530c4f6aa1971893d88b" },
    .{ .path = "vendor/zstd/zstd_errors.h", .sha256_hex = "66a8c3f71d12ea6e797e4f622f31f3f8f81c41b36f48cad4f5de7d8bfb6aac0a" },
    .{ .path = "vendor/zstd.h", .sha256_hex = "9b4bc8245565c98ccfc61c07749928b57e7c0f6fddb0530c4f6aa1971893d88b" },
    .{ .path = "vendor/zstd_errors.h", .sha256_hex = "66a8c3f71d12ea6e797e4f622f31f3f8f81c41b36f48cad4f5de7d8bfb6aac0a" },
    .{ .path = "vendor/lz4/lz4.c", .sha256_hex = "b6a85fd8f9be0fedb568abd1338719b23b999583ccda6f3404d5ae11e4ce7b8e" },
    .{ .path = "vendor/lz4/lz4.h", .sha256_hex = "c1614ecf7ada7b0be1acb560d4239595f96fbb7aa6a79a7c40cb358753830be6" },
    .{ .path = "vendor/lz4/lz4_all.c", .sha256_hex = "5192c7d93a8d5d7f50edf7101c116a5eecef70b3583de7d6967b0dc630057c94" },
    .{ .path = "vendor/lz4/lz4hc.c", .sha256_hex = "bca2cf3a014b21919da4fa92db5adbfbfc9aee2f0938107885a3a8f49a750aa8" },
    .{ .path = "vendor/lz4/lz4hc.h", .sha256_hex = "6bc1efeb79571807da2ca084809de1760e0cd87064e40eccf9624d95088dce7d" },
    .{ .path = "vendor/lz4/lz4frame.c", .sha256_hex = "80812286293760e032b254fb1c5bfd03ed099879dbecf1503286c0bb3c342574" },
    .{ .path = "vendor/lz4/lz4frame.h", .sha256_hex = "47501e4925d60c0f87c7bfc68c8b9e0e4d942eea786e35d2c5bfbf5e9deab561" },
    .{ .path = "vendor/lz4/xxhash.c", .sha256_hex = "7ae8273e2eae674db237c4d0c300e4a69bff8832b0e5b4e08552255fde006fa2" },
    .{ .path = "vendor/lz4/xxhash.h", .sha256_hex = "aefdd236f35130495c18764cabed3f7b216906855fc5e6a9025cd2040bc84444" },
};

fn fileExists(path: []const u8) bool {
    std.fs.cwd().access(path, .{}) catch return false;
    return true;
}

fn sha256Hex(path: []const u8) ![64]u8 {
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = try file.read(&buf);
        if (n == 0) break;
        hasher.update(buf[0..n]);
    }

    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var missing = std.ArrayList([]const u8).empty;
    defer missing.deinit(allocator);
    var mismatched = std.ArrayList([]const u8).empty;
    defer {
        for (mismatched.items) |line| allocator.free(line);
        mismatched.deinit(allocator);
    }

    for (required_vendor_files) |entry| {
        if (!fileExists(entry.path)) {
            try missing.append(allocator, entry.path);
            continue;
        }

        const actual_hash = sha256Hex(entry.path) catch |err| {
            const line = try std.fmt.allocPrint(allocator, "{s}: hash read failed ({s})", .{
                entry.path,
                @errorName(err),
            });
            try mismatched.append(allocator, line);
            continue;
        };
        if (!std.mem.eql(u8, actual_hash[0..], entry.sha256_hex)) {
            const line = try std.fmt.allocPrint(allocator, "{s}: expected {s}, got {s}", .{
                entry.path,
                entry.sha256_hex,
                actual_hash,
            });
            try mismatched.append(allocator, line);
        }
    }

    if (missing.items.len == 0 and mismatched.items.len == 0) return;

    std.debug.print("error: required vendored IPC compression sources failed dependency integrity checks\n", .{});
    std.debug.print("zarrow now builds zstd/lz4 from vendor/ and does not fall back to system libraries.\n", .{});
    if (missing.items.len > 0) {
        std.debug.print("missing files:\n", .{});
        for (missing.items) |path| {
            std.debug.print("  - {s}\n", .{path});
        }
    }
    if (mismatched.items.len > 0) {
        std.debug.print("hash mismatches:\n", .{});
        for (mismatched.items) |line| {
            std.debug.print("  - {s}\n", .{line});
        }
    }
    std.debug.print("hint: restore vendor/zstd and vendor/lz4 sources (pinned upstream versions) before building.\n", .{});

    return error.MissingCompressionDependency;
}
