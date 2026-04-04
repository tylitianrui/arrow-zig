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

    // Discover example files in the `examples` directory and wire them into the build.
    const examples_dir = b.path("examples");
    var dir = std.fs.openDirAbsolute(examples_dir.getPath(b), .{ .iterate = true }) catch |err| {
        std.debug.print("warning: failed to open examples directory: {s}\n", .{@errorName(err)});
        return;
    };
    defer dir.close();

    // Wire the test artifact into a named build step for `zig build test`.
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run zarrow unit tests");
    test_step.dependOn(&run_tests.step);

    var run_default: ?*std.Build.Step = null;
    var run_all_step = b.step("examples", "Run all examples");
    var it = dir.iterate();
    while (it.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;

        const base_name = entry.name[0 .. entry.name.len - 4];
        const example_path = b.fmt("examples/{s}", .{entry.name});
        const exe = b.addExecutable(.{
            .name = b.fmt("example-{s}", .{base_name}),
            .root_module = b.createModule(.{
                .root_source_file = b.path(example_path),
                .target = target,
                .optimize = optimize,
            }),
        });

        exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

        const run_exe = b.addRunArtifact(exe);
        const run_step = b.step(b.fmt("example-{s}", .{base_name}), b.fmt("Run example {s}", .{base_name}));
        run_step.dependOn(&run_exe.step);
        run_all_step.dependOn(&run_exe.step);

        if (run_default == null) run_default = &run_exe.step;
    }

    if (run_default) |step| {
        const run_step = b.step("run", "Run the default example");
        run_step.dependOn(step);
    }
}
