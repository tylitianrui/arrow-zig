const std = @import("std");
const array_mod = @import("../array/array.zig");
const chunked_array_mod = @import("../chunked_array.zig");
const concat_array_refs = @import("../concat_array_refs.zig");
const core = @import("core.zig");
const iterators = @import("iterators.zig");
const accessors = @import("datum_accessors.zig");
const builders = @import("datum_builders.zig");
const common = @import("datum_common.zig");

const DataType = core.DataType;
const ArrayRef = core.ArrayRef;
const ChunkedArray = core.ChunkedArray;
const Datum = core.Datum;
const KernelError = core.KernelError;
const FilterOptions = core.FilterOptions;

const inferBinaryExecLen = iterators.inferBinaryExecLen;
const inferNaryExecLen = iterators.inferNaryExecLen;
const sameDataTypes = iterators.sameDataTypes;

const mapArrayReadError = common.mapArrayReadError;

const ChunkLookup = struct {
    chunk: *const ArrayRef,
    local_index: usize,
};

fn lookupChunkAt(chunks: ChunkedArray, logical_index: usize) ?ChunkLookup {
    if (logical_index >= chunks.len()) return null;

    var remaining = logical_index;
    var chunk_index: usize = 0;
    while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
        const chunk_ref = chunks.chunk(chunk_index);
        const chunk_len = chunk_ref.data().length;
        if (remaining < chunk_len) {
            return .{
                .chunk = chunk_ref,
                .local_index = remaining,
            };
        }
        remaining -= chunk_len;
    }
    return null;
}

const PredicateDecision = enum {
    keep,
    drop,
    emit_null,
};

fn appendOwnedArrayRef(
    allocator: std.mem.Allocator,
    pieces: *std.ArrayList(ArrayRef),
    piece: ArrayRef,
) KernelError!void {
    pieces.append(allocator, piece) catch {
        var owned = piece;
        owned.release();
        return error.OutOfMemory;
    };
}

fn flushTakeContiguousRun(
    allocator: std.mem.Allocator,
    out_chunks: *std.ArrayList(ArrayRef),
    chunks: ChunkedArray,
    chunk_index: usize,
    run_start_local: usize,
    run_len: usize,
) KernelError!void {
    if (run_len == 0) return;
    const source = chunks.chunk(chunk_index).*;
    const source_len = source.data().length;
    const piece = if (run_start_local == 0 and run_len == source_len)
        source.retain()
    else
        source.slice(run_start_local, run_len) catch |err| return mapArrayReadError(err);
    try appendOwnedArrayRef(allocator, out_chunks, piece);
}

fn flushTakeNullRun(
    allocator: std.mem.Allocator,
    out_chunks: *std.ArrayList(ArrayRef),
    out_type: DataType,
    null_run_len: usize,
) KernelError!void {
    if (null_run_len == 0) return;
    const nulls = try builders.buildNullLikeArray(allocator, out_type, null_run_len);
    try appendOwnedArrayRef(allocator, out_chunks, nulls);
}

fn datumTakeChunkedNullable(
    allocator: std.mem.Allocator,
    chunks: ChunkedArray,
    out_type: DataType,
    indices: []const ?usize,
) KernelError!Datum {
    if (indices.len == 0) {
        return Datum.fromChunked(chunks.slice(allocator, 0, 0) catch |err| return common.mapChunkedError(err));
    }

    var out_chunks: std.ArrayList(ArrayRef) = .{};
    defer {
        for (out_chunks.items) |*chunk| chunk.release();
        out_chunks.deinit(allocator);
    }

    var non_null_count: usize = 0;
    for (indices) |idx| {
        if (idx != null) {
            non_null_count = std.math.add(usize, non_null_count, 1) catch return error.Overflow;
        }
    }

    var resolved: []accessors.ChunkLocalIndex = &[_]accessors.ChunkLocalIndex{};
    var non_null_indices: []usize = &[_]usize{};
    if (non_null_count > 0) {
        non_null_indices = allocator.alloc(usize, non_null_count) catch return error.OutOfMemory;
        defer allocator.free(non_null_indices);

        var fill_i: usize = 0;
        for (indices) |idx| {
            if (idx) |logical| {
                non_null_indices[fill_i] = logical;
                fill_i += 1;
            }
        }
        resolved = try accessors.chunkedResolveLogicalIndices(allocator, chunks, non_null_indices);
        defer allocator.free(resolved);
    }

    var run_active = false;
    var run_chunk_index: usize = 0;
    var run_start_local: usize = 0;
    var run_len: usize = 0;
    var null_run_len: usize = 0;
    var resolved_i: usize = 0;

    for (indices) |maybe_index| {
        if (maybe_index == null) {
            if (run_active) {
                try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
                run_active = false;
                run_len = 0;
            }
            null_run_len = std.math.add(usize, null_run_len, 1) catch return error.Overflow;
            continue;
        }

        if (resolved_i >= resolved.len) return error.InvalidInput;
        const located = resolved[resolved_i];
        resolved_i += 1;

        if (null_run_len > 0) {
            try flushTakeNullRun(allocator, &out_chunks, out_type, null_run_len);
            null_run_len = 0;
        }

        if (!run_active) {
            run_active = true;
            run_chunk_index = located.chunk_index;
            run_start_local = located.index_in_chunk;
            run_len = 1;
            continue;
        }

        const expected_next = run_start_local + run_len;
        if (located.chunk_index == run_chunk_index and located.index_in_chunk == expected_next) {
            run_len = std.math.add(usize, run_len, 1) catch return error.Overflow;
            continue;
        }

        try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
        run_chunk_index = located.chunk_index;
        run_start_local = located.index_in_chunk;
        run_len = 1;
    }

    if (resolved_i != resolved.len) return error.InvalidInput;

    if (run_active) {
        try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
    }
    if (null_run_len > 0) {
        try flushTakeNullRun(allocator, &out_chunks, out_type, null_run_len);
    }

    const out = ChunkedArray.init(allocator, out_type, out_chunks.items) catch |err| return common.mapChunkedError(err);
    return Datum.fromChunked(out);
}

fn datumTakeArrayLikeNullable(
    allocator: std.mem.Allocator,
    datum: Datum,
    out_type: DataType,
    indices: []const ?usize,
) KernelError!Datum {
    if (indices.len == 0) return builders.datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices) |choice| {
        const piece = if (choice) |idx|
            try accessors.datumElementArrayAt(allocator, datum, idx)
        else
            try builders.buildNullLikeArray(allocator, out_type, 1);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return common.mapConcatError(err);
    return Datum.fromArray(out);
}

fn predicateDecisionAt(predicate: Datum, logical_index: usize, options: FilterOptions) KernelError!PredicateDecision {
    return switch (predicate) {
        .scalar => |s| blk: {
            if (s.data_type != .bool) break :blk error.InvalidInput;
            if (s.isNull()) break :blk if (options.drop_nulls) .drop else .emit_null;
            break :blk if (s.value.bool) .keep else .drop;
        },
        .array => |arr| blk: {
            if (arr.data().data_type != .bool) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            if (arr.data().isNull(logical_index)) break :blk if (options.drop_nulls) .drop else .emit_null;
            const bool_array = array_mod.BooleanArray{ .data = arr.data() };
            break :blk if (bool_array.value(logical_index)) .keep else .drop;
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .bool) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            if (located.chunk.data().isNull(located.local_index)) break :blk if (options.drop_nulls) .drop else .emit_null;
            const bool_array = array_mod.BooleanArray{ .data = located.chunk.data() };
            break :blk if (bool_array.value(located.local_index)) .keep else .drop;
        },
    };
}

/// Build nullable selection indices from a boolean predicate.
///
/// `keep` emits the logical input index, `drop` emits nothing, and `emit_null`
/// emits `null` to preserve output row count for null-predicate semantics.
pub fn datumFilterSelectionIndices(
    allocator: std.mem.Allocator,
    predicate: Datum,
    logical_len: usize,
    options: FilterOptions,
) KernelError![]?usize {
    var selections: std.ArrayList(?usize) = .{};
    defer selections.deinit(allocator);

    var i: usize = 0;
    while (i < logical_len) : (i += 1) {
        switch (try predicateDecisionAt(predicate, i, options)) {
            .drop => {},
            .keep => try selections.append(allocator, i),
            .emit_null => try selections.append(allocator, null),
        }
    }
    return selections.toOwnedSlice(allocator);
}

/// Gather/take rows from a datum according to logical indices.
///
/// For chunked inputs this helper preserves chunked output and avoids forcing
/// concat pre-normalization.
pub fn datumTake(datum: Datum, indices: []const usize) KernelError!Datum {
    const allocator = common.inferDatumAllocator(datum) orelse std.heap.page_allocator;
    const out_type = datum.dataType();

    if (indices.len == 0) {
        return switch (datum) {
            .chunked => |chunks| Datum.fromChunked(chunks.slice(allocator, 0, 0) catch |err| return common.mapChunkedError(err)),
            else => builders.datumBuildEmptyLikeWithAllocator(allocator, out_type),
        };
    }

    return switch (datum) {
        .chunked => |chunks| blk: {
            var nullable = allocator.alloc(?usize, indices.len) catch break :blk error.OutOfMemory;
            defer allocator.free(nullable);
            for (indices, 0..) |index, i| nullable[i] = index;
            break :blk try datumTakeChunkedNullable(allocator, chunks, out_type, nullable);
        },
        else => blk: {
            var nullable = allocator.alloc(?usize, indices.len) catch break :blk error.OutOfMemory;
            defer allocator.free(nullable);
            for (indices, 0..) |index, i| nullable[i] = index;
            break :blk try datumTakeArrayLikeNullable(allocator, datum, out_type, nullable);
        },
    };
}

/// Gather/take rows with nullable indices where `null` emits an all-null row.
///
/// For chunked inputs this helper preserves chunked output and avoids forcing
/// concat pre-normalization.
pub fn datumTakeNullable(datum: Datum, indices: []const ?usize) KernelError!Datum {
    const allocator = common.inferDatumAllocator(datum) orelse std.heap.page_allocator;
    const out_type = datum.dataType();
    return switch (datum) {
        .chunked => |chunks| datumTakeChunkedNullable(allocator, chunks, out_type, indices),
        else => datumTakeArrayLikeNullable(allocator, datum, out_type, indices),
    };
}

/// Row-wise selection primitive shared by choose/case_when/filter style operators.
///
/// `indices[i]` selects which entry in `values` contributes output row `i`.
/// Each candidate value may be array/chunked/scalar (scalar broadcasts).
pub fn datumSelect(indices: []const usize, values: []const Datum) KernelError!Datum {
    if (values.len == 0) return error.InvalidArity;
    if (!sameDataTypes(values)) return error.InvalidInput;

    const output_len = try inferNaryExecLen(values);
    if (indices.len != output_len) return error.InvalidInput;

    const allocator = common.inferDatumsAllocator(values);
    const out_type = values[0].dataType();
    if (indices.len == 0) return builders.datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices, 0..) |choice, logical_index| {
        if (choice >= values.len) return error.InvalidInput;
        const piece = try accessors.datumElementArrayAt(allocator, values[choice], logical_index);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return common.mapConcatError(err);
    return Datum.fromArray(out);
}

/// Row-wise selection primitive with nullable index support.
///
/// `indices[i] == null` emits a null row at output position `i`.
pub fn datumSelectNullable(indices: []const ?usize, values: []const Datum) KernelError!Datum {
    if (values.len == 0) return error.InvalidArity;
    if (!sameDataTypes(values)) return error.InvalidInput;

    const output_len = try inferNaryExecLen(values);
    if (indices.len != output_len) return error.InvalidInput;

    const allocator = common.inferDatumsAllocator(values);
    const out_type = values[0].dataType();
    if (indices.len == 0) return builders.datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices, 0..) |choice, logical_index| {
        const piece = if (choice) |idx| blk: {
            if (idx >= values.len) return error.InvalidInput;
            break :blk try accessors.datumElementArrayAt(allocator, values[idx], logical_index);
        } else try builders.buildNullLikeArray(allocator, out_type, 1);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return common.mapConcatError(err);
    return Datum.fromArray(out);
}

/// Chunk-aware filter helper for array/chunked/scalar datums with bool predicates.
///
/// This helper composes `datumFilterSelectionIndices` + `datumTakeNullable`,
/// preserving chunked output when the input datum is chunked.
pub fn datumFilterChunkAware(datum: Datum, predicate: Datum, options: FilterOptions) KernelError!Datum {
    const input_len = try inferBinaryExecLen(datum, predicate);
    const allocator = common.inferDatumsAllocator(&[_]Datum{ datum, predicate });
    const selections = try datumFilterSelectionIndices(allocator, predicate, input_len, options);
    defer allocator.free(selections);
    return datumTakeNullable(datum, selections);
}

/// Generic filter primitive for array/chunked/scalar datums with bool predicates.
///
/// This preserves the historical array-output shape for compatibility.
pub fn datumFilter(datum: Datum, predicate: Datum, options: FilterOptions) KernelError!Datum {
    const output_len = try inferBinaryExecLen(datum, predicate);
    const allocator = common.inferDatumsAllocator(&[_]Datum{ datum, predicate });
    const out_type = datum.dataType();

    if (output_len == 0) return builders.datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }

    var i: usize = 0;
    while (i < output_len) : (i += 1) {
        const decision = try predicateDecisionAt(predicate, i, options);
        const piece = switch (decision) {
            .drop => continue,
            .keep => try accessors.datumElementArrayAt(allocator, datum, i),
            .emit_null => try builders.buildNullLikeArray(allocator, out_type, 1),
        };
        try appendOwnedArrayRef(allocator, &pieces, piece);
    }

    if (pieces.items.len == 0) return builders.datumBuildEmptyLikeWithAllocator(allocator, out_type);

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return common.mapConcatError(err);
    return Datum.fromArray(out);
}
