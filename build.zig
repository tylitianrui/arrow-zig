const std = @import("std");
const builtin = @import("builtin");

// Enforce supported Zig version range at compile time.
// Zig 0.16+ is not yet supported; update this check when compatibility is confirmed.
comptime {
    const ver = builtin.zig_version;
    if (ver.major == 0 and ver.minor >= 16) {
        @compileError("zarrow requires Zig 0.15.x. Zig 0.16+ is not yet supported.");
    }
}

fn configureIpcCompression(b: *std.Build, step: *std.Build.Step.Compile, deps_check: *std.Build.Step) void {
    step.step.dependOn(deps_check);

    step.addIncludePath(b.path("vendor/zstd"));
    step.addIncludePath(b.path("vendor/lz4"));

    step.addCSourceFile(.{
        .file = b.path("vendor/zstd/zstd.c"),
        .flags = &.{"-DZSTD_LEGACY_SUPPORT=0"},
    });
    step.addCSourceFile(.{
        .file = b.path("vendor/lz4/lz4_all.c"),
        .flags = &.{},
    });

    step.linkLibC();
}

// Configure the zarrow package as a reusable module plus a dedicated test step.
pub fn build(b: *std.Build) !void {
    // Allow downstream consumers and tests to select their own target and optimization mode.
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Expose zarrow as a reusable Zig module for downstream dependencies.
    const zarrow_module = b.addModule("zarrow", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Core-only module for downstream libraries that do not require IPC/FFI.
    _ = b.addModule("zarrow-core", .{
        .root_source_file = b.path("src/core.zig"),
        .target = target,
        .optimize = optimize,
    });

    const fbz_module = b.createModule(.{
        .root_source_file = b.path("src/fbs_runtime/lib.zig"),
    });

    const compression_deps_check_exe = b.addExecutable(.{
        .name = "check-ipc-compression-deps",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/check_ipc_compression_deps.zig"),
            .target = b.graph.host,
            .optimize = .ReleaseSafe,
        }),
    });
    const run_compression_deps_check = b.addRunArtifact(compression_deps_check_exe);
    const compression_deps_check_step = b.step(
        "check-ipc-compression-deps",
        "Verify vendored zstd/lz4 sources required for IPC BodyCompression",
    );
    compression_deps_check_step.dependOn(&run_compression_deps_check.step);

    const arrow_fbs_module = b.createModule(.{
        .root_source_file = b.path("src/ipc_schema/lib.zig"),
        .imports = &.{.{ .name = "fbs_runtime", .module = fbz_module }},
    });

    zarrow_module.addImport("fbs_runtime", fbz_module);
    zarrow_module.addImport("ipc_schema", arrow_fbs_module);

    // Tests still build a runnable artifact, but the package itself does not.
    const test_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_module.addImport("fbs_runtime", fbz_module);
    test_module.addImport("ipc_schema", arrow_fbs_module);

    const tests = b.addTest(.{ .root_module = test_module });
    configureIpcCompression(b, tests, &run_compression_deps_check.step);

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

    // Fuzz harnesses for parser/layout robustness.
    const fuzz_layout_exe = b.addExecutable(.{
        .name = "fuzz-array-validate-layout",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/fuzz_array_validate_layout.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    configureIpcCompression(b, fuzz_layout_exe, &run_compression_deps_check.step);
    fuzz_layout_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

    const run_fuzz_layout = b.addRunArtifact(fuzz_layout_exe);
    if (b.args) |args| run_fuzz_layout.addArgs(args);
    const fuzz_layout_step = b.step("fuzz-array-layout", "Run ArrayData.validateLayout fuzz harness");
    fuzz_layout_step.dependOn(&run_fuzz_layout.step);

    const fuzz_ipc_exe = b.addExecutable(.{
        .name = "fuzz-ipc-reader",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/fuzz_ipc_reader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    configureIpcCompression(b, fuzz_ipc_exe, &run_compression_deps_check.step);
    fuzz_ipc_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

    const run_fuzz_ipc = b.addRunArtifact(fuzz_ipc_exe);
    if (b.args) |args| run_fuzz_ipc.addArgs(args);
    const fuzz_ipc_step = b.step("fuzz-ipc-reader", "Run IPC reader fuzz harness");
    fuzz_ipc_step.dependOn(&run_fuzz_ipc.step);

    const fuzz_corpus_step = b.step("fuzz-corpus", "Replay built-in fuzz seed corpus");
    const fuzz_corpus_root = b.path("fuzz/corpus").getPath(b);

    var corpus_layout_dir = std.fs.openDirAbsolute(
        b.pathJoin(&.{ fuzz_corpus_root, "array-validate-layout" }),
        .{ .iterate = true },
    ) catch null;
    if (corpus_layout_dir) |*dir_handle| {
        defer dir_handle.close();
        var it_corpus_layout = dir_handle.iterate();
        while (it_corpus_layout.next() catch null) |entry| {
            if (entry.kind != .file) continue;
            const run_one = b.addRunArtifact(fuzz_layout_exe);
            run_one.addFileArg(b.path(b.fmt("fuzz/corpus/array-validate-layout/{s}", .{entry.name})));
            fuzz_corpus_step.dependOn(&run_one.step);
        }
    } else {
        std.debug.print("warning: fuzz corpus missing: fuzz/corpus/array-validate-layout\n", .{});
    }

    var corpus_ipc_dir = std.fs.openDirAbsolute(
        b.pathJoin(&.{ fuzz_corpus_root, "ipc-reader" }),
        .{ .iterate = true },
    ) catch null;
    if (corpus_ipc_dir) |*dir_handle| {
        defer dir_handle.close();
        var it_corpus_ipc = dir_handle.iterate();
        while (it_corpus_ipc.next() catch null) |entry| {
            if (entry.kind != .file) continue;
            const run_one = b.addRunArtifact(fuzz_ipc_exe);
            run_one.addFileArg(b.path(b.fmt("fuzz/corpus/ipc-reader/{s}", .{entry.name})));
            fuzz_corpus_step.dependOn(&run_one.step);
        }
    } else {
        std.debug.print("warning: fuzz corpus missing: fuzz/corpus/ipc-reader\n", .{});
    }

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
        configureIpcCompression(b, exe, &run_compression_deps_check.step);

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

    // IPC interop helper tools used by CI compatibility matrix checks.
    const interop_writer_exe = b.addExecutable(.{
        .name = "interop-fixture-writer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/interop_fixture_writer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    configureIpcCompression(b, interop_writer_exe, &run_compression_deps_check.step);
    interop_writer_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

    const run_interop_writer = b.addRunArtifact(interop_writer_exe);
    if (b.args) |args| run_interop_writer.addArgs(args);
    const interop_writer_step = b.step("interop-fixture-writer", "Write canonical IPC fixture with zarrow");
    interop_writer_step.dependOn(&run_interop_writer.step);

    const interop_check_exe = b.addExecutable(.{
        .name = "interop-fixture-check",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/interop_fixture_check.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    configureIpcCompression(b, interop_check_exe, &run_compression_deps_check.step);
    interop_check_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

    const run_interop_check = b.addRunArtifact(interop_check_exe);
    if (b.args) |args| run_interop_check.addArgs(args);
    const interop_check_step = b.step("interop-fixture-check", "Validate canonical IPC fixture with zarrow");
    interop_check_step.dependOn(&run_interop_check.step);

    // Compute compatibility runner used by PyArrow compute alignment checks.
    const compute_compat_exe = b.addExecutable(.{
        .name = "compute-compat-runner",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/compute_compat_runner.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    compute_compat_exe.root_module.addImport("zarrow-core", b.modules.get("zarrow-core").?);

    const run_compute_compat = b.addRunArtifact(compute_compat_exe);
    if (b.args) |args| run_compute_compat.addArgs(args);
    const compute_compat_step = b.step("compute-compat-check", "Run compute compatibility check cases");
    compute_compat_step.dependOn(&run_compute_compat.step);

    // Discover benchmark files in `benchmarks` and wire dedicated run steps.
    const benches_dir = b.path("benchmarks");
    var benches = std.fs.openDirAbsolute(benches_dir.getPath(b), .{ .iterate = true }) catch |err| {
        std.debug.print("warning: failed to open benchmarks directory: {s}\n", .{@errorName(err)});
        return;
    };
    defer benches.close();

    var bench_all_step = b.step("benchmark", "Run all benchmarks (default mode)");
    var bench_smoke_step = b.step("benchmark-smoke", "Run all benchmarks in smoke mode");
    var bench_full_step = b.step("benchmark-full", "Run all benchmarks in full mode");
    var bench_matrix_step = b.step("benchmark-matrix", "Run all benchmarks in matrix CSV mode");
    var bench_ci_step = b.step("benchmark-ci", "Run all benchmarks in CI CSV mode");
    var matrix_header_done = false;
    var ci_header_done = false;
    var prev_matrix_run: ?*std.Build.Step = null;
    var prev_ci_run: ?*std.Build.Step = null;
    var bench_it = benches.iterate();
    while (bench_it.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;
        if (!std.mem.endsWith(u8, entry.name, "_benchmark.zig")) continue;

        const base_name = entry.name[0 .. entry.name.len - 4];
        const bench_path = b.fmt("benchmarks/{s}", .{entry.name});
        const bench_exe = b.addExecutable(.{
            .name = b.fmt("benchmark-{s}", .{base_name}),
            .root_module = b.createModule(.{
                .root_source_file = b.path(bench_path),
                .target = target,
                .optimize = optimize,
            }),
        });
        configureIpcCompression(b, bench_exe, &run_compression_deps_check.step);
        bench_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

        const run_bench = b.addRunArtifact(bench_exe);
        const bench_step = b.step(b.fmt("benchmark-{s}", .{base_name}), b.fmt("Run benchmark {s}", .{base_name}));
        bench_step.dependOn(&run_bench.step);
        bench_all_step.dependOn(&run_bench.step);

        const run_bench_smoke = b.addRunArtifact(bench_exe);
        run_bench_smoke.addArg("smoke");
        bench_smoke_step.dependOn(&run_bench_smoke.step);

        const run_bench_full = b.addRunArtifact(bench_exe);
        run_bench_full.addArg("full");
        bench_full_step.dependOn(&run_bench_full.step);

        const run_bench_matrix = b.addRunArtifact(bench_exe);
        if (prev_matrix_run) |prev| run_bench_matrix.step.dependOn(prev);
        if (!matrix_header_done) {
            run_bench_matrix.addArg("matrix");
            matrix_header_done = true;
        } else {
            run_bench_matrix.addArg("matrix-no-header");
        }
        prev_matrix_run = &run_bench_matrix.step;
        bench_matrix_step.dependOn(&run_bench_matrix.step);

        const run_bench_ci = b.addRunArtifact(bench_exe);
        if (prev_ci_run) |prev| run_bench_ci.step.dependOn(prev);
        if (!ci_header_done) {
            run_bench_ci.addArg("ci");
            ci_header_done = true;
        } else {
            run_bench_ci.addArg("ci-no-header");
        }
        prev_ci_run = &run_bench_ci.step;
        bench_ci_step.dependOn(&run_bench_ci.step);
    }
}
