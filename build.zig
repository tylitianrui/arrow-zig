const std = @import("std");

// Configure the zarrow package as a reusable module plus a dedicated test step.
pub fn build(b: *std.Build) void {
    // Allow downstream consumers and tests to select their own target and optimization mode.
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Expose zarrow as a reusable Zig module for downstream dependencies.
    _ = b.addModule("zarrow", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Tests still build a runnable artifact, but the package itself does not.
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Wire the test artifact into a named build step for `zig build test`.
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run zarrow unit tests");
    test_step.dependOn(&run_tests.step);
}
