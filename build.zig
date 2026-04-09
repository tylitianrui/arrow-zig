const std = @import("std");

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

    const fbz_dep = b.dependency("flatbufferz", .{
        .target = target,
        .optimize = optimize,
    });

    const is_windows = b.graph.host.result.os.tag == .windows;
    const format_dir = if (is_windows) "src/format" else b.path("src/format").getPath(b);
    const message_fbs = if (is_windows) "src/format/Message.fbs" else b.path("src/format/Message.fbs").getPath(b);
    const schema_fbs = if (is_windows) "src/format/Schema.fbs" else b.path("src/format/Schema.fbs").getPath(b);
    const tensor_fbs = if (is_windows) "src/format/Tensor.fbs" else b.path("src/format/Tensor.fbs").getPath(b);
    const sparse_tensor_fbs = if (is_windows) "src/format/SparseTensor.fbs" else b.path("src/format/SparseTensor.fbs").getPath(b);
    const gen_step = try @import("flatbufferz").GenStep.create(
        b,
        fbz_dep.artifact("flatc-zig"),
        &.{
            message_fbs,
            schema_fbs,
            tensor_fbs,
            sparse_tensor_fbs,
        },
        &.{ "-I", format_dir },
        "flatc-zig",
    );

    // Work around a flatc-zig Windows path bug where generated lib.zig can use
    // backslashes inside @import string literals.
    const fix_flatc_lib = b.addExecutable(.{
        .name = "fix-flatc-lib",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/fix_flatc_lib.zig"),
            .target = b.graph.host,
            .optimize = .ReleaseSafe,
        }),
    });
    const run_fix_flatc_lib = b.addRunArtifact(fix_flatc_lib);
    run_fix_flatc_lib.step.dependOn(&gen_step.step);
    run_fix_flatc_lib.addFileArg(gen_step.module.root_source_file.?);

    const arrow_fbs_module = b.createModule(.{
        .root_source_file = gen_step.module.root_source_file,
        .imports = &.{.{ .name = "flatbufferz", .module = fbz_dep.module("flatbufferz") }},
    });

    zarrow_module.addImport("flatbufferz", fbz_dep.module("flatbufferz"));
    zarrow_module.addImport("arrow_fbs", arrow_fbs_module);

    // Tests still build a runnable artifact, but the package itself does not.
    const test_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_module.addImport("flatbufferz", fbz_dep.module("flatbufferz"));
    test_module.addImport("arrow_fbs", arrow_fbs_module);

    const tests = b.addTest(.{ .root_module = test_module });
    tests.step.dependOn(&run_fix_flatc_lib.step);

    // Discover example files in the `examples` directory and wire them into the build.
    const examples_dir = b.path("examples");
    var dir = std.fs.openDirAbsolute(examples_dir.getPath(b), .{ .iterate = true }) catch |err| {
        std.debug.print("warning: failed to open examples directory: {s}\n", .{@errorName(err)});
        return;
    };
    defer dir.close();

    // Wire the test artifact into a named build step for `zig build test`.
    const run_tests = b.addRunArtifact(tests);
    run_tests.step.dependOn(&gen_step.step);
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
    fuzz_layout_exe.step.dependOn(&run_fix_flatc_lib.step);
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
    fuzz_ipc_exe.step.dependOn(&run_fix_flatc_lib.step);
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
        exe.step.dependOn(&run_fix_flatc_lib.step);

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
    interop_writer_exe.step.dependOn(&run_fix_flatc_lib.step);
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
    interop_check_exe.step.dependOn(&run_fix_flatc_lib.step);
    interop_check_exe.root_module.addImport("zarrow", b.modules.get("zarrow").?);

    const run_interop_check = b.addRunArtifact(interop_check_exe);
    if (b.args) |args| run_interop_check.addArgs(args);
    const interop_check_step = b.step("interop-fixture-check", "Validate canonical IPC fixture with zarrow");
    interop_check_step.dependOn(&run_interop_check.step);

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
    var bench_it = benches.iterate();
    while (bench_it.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;

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
        bench_exe.step.dependOn(&run_fix_flatc_lib.step);
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
        if (!matrix_header_done) {
            run_bench_matrix.addArg("matrix");
            matrix_header_done = true;
        } else {
            run_bench_matrix.addArg("matrix-no-header");
        }
        bench_matrix_step.dependOn(&run_bench_matrix.step);

        const run_bench_ci = b.addRunArtifact(bench_exe);
        if (!ci_header_done) {
            run_bench_ci.addArg("ci");
            ci_header_done = true;
        } else {
            run_bench_ci.addArg("ci-no-header");
        }
        bench_ci_step.dependOn(&run_bench_ci.step);
    }
}
