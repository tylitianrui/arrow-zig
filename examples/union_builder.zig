const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Union of i32 (type_id=0) and f32 (type_id=1).
    const i32_type = zarrow.DataType{ .int32 = {} };
    const f32_type = zarrow.DataType{ .float = {} };
    const union_fields = [_]zarrow.Field{
        .{ .name = "int_val", .data_type = &i32_type, .nullable = false },
        .{ .name = "float_val", .data_type = &f32_type, .nullable = false },
    };
    const type_ids = [_]i8{ 0, 1 };

    // ── Sparse union ─────────────────────────────────────────────────────────
    // All children have length == union length.
    // Sequence: i32(10), f32(3.14), i32(20)
    const sparse_union_type = zarrow.UnionType{
        .type_ids = type_ids[0..],
        .fields = union_fields[0..],
        .mode = .sparse,
    };

    var sparse_i32 = try zarrow.Int32Builder.init(allocator, 3);
    defer sparse_i32.deinit();
    try sparse_i32.append(10); // slot 0 used by row 0
    try sparse_i32.append(0); // slot 1 unused (row 1 is float)
    try sparse_i32.append(20); // slot 2 used by row 2
    var sparse_i32_ref = try sparse_i32.finish();
    defer sparse_i32_ref.release();

    var sparse_f32 = try zarrow.Float32Builder.init(allocator, 3);
    defer sparse_f32.deinit();
    try sparse_f32.append(0.0); // slot 0 unused
    try sparse_f32.append(3.14); // slot 1 used by row 1
    try sparse_f32.append(0.0); // slot 2 unused
    var sparse_f32_ref = try sparse_f32.finish();
    defer sparse_f32_ref.release();

    var sparse_builder = try zarrow.SparseUnionBuilder.init(allocator, sparse_union_type, 3);
    defer sparse_builder.deinit();
    try sparse_builder.appendTypeId(0); // row 0 → i32
    try sparse_builder.appendTypeId(1); // row 1 → f32
    try sparse_builder.appendTypeId(0); // row 2 → i32

    var sparse_ref = try sparse_builder.finish(&[_]zarrow.ArrayRef{ sparse_i32_ref, sparse_f32_ref });
    defer sparse_ref.release();

    const sparse = zarrow.SparseUnionArray{ .data = sparse_ref.data() };
    std.debug.print("examples/union_builder.zig | sparse union length={d}\n", .{sparse.len()});
    for (0..sparse.len()) |i| {
        if (sparse.typeId(i) == 0) {
            var v = try sparse.value(i);
            defer v.release();
            const a = zarrow.Int32Array{ .data = v.data() };
            std.debug.print("  [{d}] i32({d})\n", .{ i, a.value(0) });
        } else {
            var v = try sparse.value(i);
            defer v.release();
            const a = zarrow.Float32Array{ .data = v.data() };
            std.debug.print("  [{d}] f32({d:.2})\n", .{ i, a.value(0) });
        }
    }

    // ── Dense union ──────────────────────────────────────────────────────────
    // Children only hold the values referenced by each row.
    // Sequence: i32(100), f32(2.71), f32(1.41), i32(200)
    const dense_union_type = zarrow.UnionType{
        .type_ids = type_ids[0..],
        .fields = union_fields[0..],
        .mode = .dense,
    };

    var dense_i32 = try zarrow.Int32Builder.init(allocator, 2);
    defer dense_i32.deinit();
    try dense_i32.append(100);
    try dense_i32.append(200);
    var dense_i32_ref = try dense_i32.finish();
    defer dense_i32_ref.release();

    var dense_f32 = try zarrow.Float32Builder.init(allocator, 2);
    defer dense_f32.deinit();
    try dense_f32.append(2.71);
    try dense_f32.append(1.41);
    var dense_f32_ref = try dense_f32.finish();
    defer dense_f32_ref.release();

    var dense_builder = try zarrow.DenseUnionBuilder.init(allocator, dense_union_type, 4);
    defer dense_builder.deinit();
    try dense_builder.append(0, 0); // row 0 → i32 child offset 0
    try dense_builder.append(1, 0); // row 1 → f32 child offset 0
    try dense_builder.append(1, 1); // row 2 → f32 child offset 1
    try dense_builder.append(0, 1); // row 3 → i32 child offset 1

    var dense_ref = try dense_builder.finish(&[_]zarrow.ArrayRef{ dense_i32_ref, dense_f32_ref });
    defer dense_ref.release();

    const dense = zarrow.DenseUnionArray{ .data = dense_ref.data() };
    std.debug.print("examples/union_builder.zig | dense union length={d}\n", .{dense.len()});
    for (0..dense.len()) |i| {
        if (dense.typeId(i) == 0) {
            var v = try dense.value(i);
            defer v.release();
            const a = zarrow.Int32Array{ .data = v.data() };
            std.debug.print("  [{d}] i32({d})\n", .{ i, a.value(0) });
        } else {
            var v = try dense.value(i);
            defer v.release();
            const a = zarrow.Float32Array{ .data = v.data() };
            std.debug.print("  [{d}] f32({d:.2})\n", .{ i, a.value(0) });
        }
    }
}
