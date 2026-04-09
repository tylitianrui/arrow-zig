const std = @import("std");

pub const CsvMeta = struct {
    git_sha: []const u8,
    timestamp: i64,
    owns_git_sha: bool,

    pub fn deinit(self: CsvMeta, allocator: std.mem.Allocator) void {
        if (self.owns_git_sha) allocator.free(self.git_sha);
    }
};

pub fn resolve(allocator: std.mem.Allocator) !CsvMeta {
    const timestamp = std.time.timestamp();
    const git_sha = std.process.getEnvVarOwned(allocator, "ZARROW_GIT_SHA") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };

    if (git_sha) |sha| {
        return .{ .git_sha = sha, .timestamp = timestamp, .owns_git_sha = true };
    }

    return .{ .git_sha = "unknown", .timestamp = timestamp, .owns_git_sha = false };
}
