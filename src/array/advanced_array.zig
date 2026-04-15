const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

// Map/Union/RunEndEncoded array views and minimal builders.

const SharedBuffer = buffer.SharedBuffer;
const OwnedBuffer = buffer.OwnedBuffer;
const ArrayData = array_data.ArrayData;
const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const IntType = datatype.IntType;
const ArrayRef = array_ref.ArrayRef;
const BuilderState = builder_state.BuilderState;


pub const MapArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: MapArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: MapArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn entriesRef(self: MapArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: MapArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        std.debug.assert(self.data.children.len == 1);

        const offsets = self.data.buffers[1].typedSlice(i32);
        const base = self.data.offset + i;
        const start: usize = @intCast(offsets[base]);
        const end: usize = @intCast(offsets[base + 1]);
        return self.data.children[0].slice(start, end - start);
    }
};

pub const SparseUnionArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: SparseUnionArray) usize {
        return self.data.length;
    }

    pub fn typeId(self: SparseUnionArray, i: usize) i8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 1);
        return self.data.buffers[0].typedSlice(i8)[self.data.offset + i];
    }

    pub fn childRef(self: SparseUnionArray, child_index: usize) *const ArrayRef {
        std.debug.assert(child_index < self.data.children.len);
        return &self.data.children[child_index];
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: SparseUnionArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        const type_id = self.typeId(i);
        const uni = self.data.data_type.sparse_union;

        var child_index: ?usize = null;
        for (uni.type_ids, 0..) |id, idx| {
            if (id == type_id) {
                child_index = idx;
                break;
            }
        }
        const idx = child_index orelse return error.InvalidChildren;
        return self.data.children[idx].slice(self.data.offset + i, 1);
    }
};

pub const DenseUnionArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: DenseUnionArray) usize {
        return self.data.length;
    }

    pub fn typeId(self: DenseUnionArray, i: usize) i8 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 1);
        return self.data.buffers[0].typedSlice(i8)[self.data.offset + i];
    }

    pub fn childOffset(self: DenseUnionArray, i: usize) i32 {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.buffers.len >= 2);
        return self.data.buffers[1].typedSlice(i32)[self.data.offset + i];
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: DenseUnionArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        const type_id = self.typeId(i);
        const uni = self.data.data_type.dense_union;

        var child_index: ?usize = null;
        for (uni.type_ids, 0..) |id, idx| {
            if (id == type_id) {
                child_index = idx;
                break;
            }
        }
        const idx = child_index orelse return error.InvalidChildren;
        const off: usize = @intCast(self.childOffset(i));
        return self.data.children[idx].slice(off, 1);
    }
};

pub const RunEndEncodedArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: RunEndEncodedArray) usize {
        return self.data.length;
    }

    fn runCount(self: RunEndEncodedArray) usize {
        std.debug.assert(self.data.children.len == 2);
        return self.data.children[0].data().length;
    }

    fn runEndAt(self: RunEndEncodedArray, run_index: usize) !i64 {
        std.debug.assert(self.data.children.len == 2);
        const run_ends = self.data.children[0].data();
        const run_ty = self.data.data_type.run_end_encoded.run_end_type;
        return switch (run_ty.bit_width) {
            8 => if (run_ty.signed)
                @as(i64, run_ends.buffers[1].typedSlice(i8)[run_ends.offset + run_index])
            else
                @as(i64, @intCast(run_ends.buffers[1].typedSlice(u8)[run_ends.offset + run_index])),
            16 => if (run_ty.signed)
                @as(i64, run_ends.buffers[1].typedSlice(i16)[run_ends.offset + run_index])
            else
                @as(i64, @intCast(run_ends.buffers[1].typedSlice(u16)[run_ends.offset + run_index])),
            32 => if (run_ty.signed)
                @as(i64, run_ends.buffers[1].typedSlice(i32)[run_ends.offset + run_index])
            else
                @as(i64, @intCast(run_ends.buffers[1].typedSlice(u32)[run_ends.offset + run_index])),
            64 => if (run_ty.signed)
                run_ends.buffers[1].typedSlice(i64)[run_ends.offset + run_index]
            else
                @as(i64, @intCast(run_ends.buffers[1].typedSlice(u64)[run_ends.offset + run_index])),
            else => error.InvalidRunEnds,
        };
    }

    fn runIndexFor(self: RunEndEncodedArray, logical_index: usize) !usize {
        var lo: usize = 0;
        var hi: usize = self.runCount();
        const target = std.math.cast(i64, logical_index + 1) orelse return error.InvalidRunEnds;

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if ((try self.runEndAt(mid)) >= target) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /// Return the logical value view at the requested index.
    pub fn value(self: RunEndEncodedArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.children.len == 2);
        const run_idx = try self.runIndexFor(self.data.offset + i);
        if (run_idx >= self.runCount()) return error.InvalidRunEnds;
        return self.data.children[1].slice(run_idx, 1);
    }

    pub fn valuesRef(self: RunEndEncodedArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 2);
        return &self.data.children[1];
    }
};

fn dataTypeFromIntType(int_type: IntType) ?DataType {
    return switch (int_type.bit_width) {
        8 => if (int_type.signed) .{ .int8 = {} } else .{ .uint8 = {} },
        16 => if (int_type.signed) .{ .int16 = {} } else .{ .uint16 = {} },
        32 => if (int_type.signed) .{ .int32 = {} } else .{ .uint32 = {} },
        64 => if (int_type.signed) .{ .int64 = {} } else .{ .uint64 = {} },
        else => null,
    };
}

pub const MapBuilder = struct {
    allocator: std.mem.Allocator,
    key_field: Field,
    item_field: Field,
    keys_sorted: bool = false,
    offsets: OwnedBuffer,
    validity: ?OwnedBuffer = null,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    values_len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, OffsetOverflow, InvalidChildLength, InvalidEntriesType, InvalidEntriesSchema };

    fn fieldMatches(expected: Field, actual: Field) bool {
        return std.mem.eql(u8, expected.name, actual.name) and
            expected.nullable == actual.nullable and
            std.meta.eql(expected.data_type.*, actual.data_type.*);
    }

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, key_field: Field, item_field: Field, keys_sorted: bool) !MapBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i32));
        std.mem.bytesAsSlice(i32, offsets.data)[0] = 0;
        return .{ .allocator = allocator, .key_field = key_field, .item_field = item_field, .keys_sorted = keys_sorted, .offsets = offsets };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *MapBuilder) void {
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *MapBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (!self.offsets.isEmpty()) {
            std.mem.bytesAsSlice(i32, self.offsets.data)[0] = 0;
        }
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *MapBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.values_len = 0;
        self.state = .ready;
    }

    fn ensureOffsetsCapacity(self: *MapBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i32);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i32));
        if (was_empty) std.mem.bytesAsSlice(i32, self.offsets.data)[0] = 0;
    }

    pub fn appendLen(self: *MapBuilder, value_len: usize) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const next_offset = self.values_len + value_len;
        const cast_offset = std.math.cast(i32, next_offset) orelse return BuilderError.OffsetOverflow;
        std.mem.bytesAsSlice(i32, self.offsets.data)[next_len] = cast_offset;
        try array_utils.setValidBit(&self.validity, self.len);
        self.len = next_len;
        self.values_len = next_offset;
    }

    /// Append a null entry into the builder.
    pub fn appendNull(self: *MapBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        std.mem.bytesAsSlice(i32, self.offsets.data)[next_len] = std.math.cast(i32, self.values_len) orelse return BuilderError.OffsetOverflow;
        try array_utils.ensureValidityForNull(self.allocator, &self.validity, &self.null_count, next_len);
        self.len = next_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *MapBuilder, entries: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (entries.data().length != self.values_len) return BuilderError.InvalidChildLength;

        const st = entries.data().data_type;
        if (st != .struct_) return BuilderError.InvalidEntriesType;
        if (st.struct_.fields.len != 2) return BuilderError.InvalidEntriesSchema;
        if (!fieldMatches(self.key_field, st.struct_.fields[0])) return BuilderError.InvalidEntriesSchema;
        if (!fieldMatches(self.item_field, st.struct_.fields[1])) return BuilderError.InvalidEntriesSchema;
        const map_type = DataType{ .map = .{ .key_field = self.key_field, .item_field = self.item_field, .keys_sorted = self.keys_sorted } };

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;
        self.buffers[1] = try self.offsets.toShared((self.len + 1) * @sizeOf(i32));

        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];

        const children = try self.allocator.alloc(ArrayRef, 1);
        children[0] = entries.retain();

        const data = ArrayData{
            .data_type = map_type,
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .children = children,
        };
        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }
};

pub const SparseUnionBuilder = struct {
    allocator: std.mem.Allocator,
    union_type: datatype.UnionType,
    type_ids: OwnedBuffer,
    buffers: [1]SharedBuffer = undefined,
    len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidTypeId, InvalidChildCount, InvalidChildLength, InvalidChildType };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, union_type: datatype.UnionType, capacity: usize) !SparseUnionBuilder {
        return .{ .allocator = allocator, .union_type = union_type, .type_ids = try OwnedBuffer.init(allocator, capacity) };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *SparseUnionBuilder) void {
        self.type_ids.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *SparseUnionBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *SparseUnionBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.type_ids.deinit();
        self.len = 0;
        self.state = .ready;
    }

    fn ensureCapacity(self: *SparseUnionBuilder, needed_len: usize) !void {
        if (needed_len <= self.type_ids.len()) return;
        try self.type_ids.resize(needed_len);
    }

    pub fn appendTypeId(self: *SparseUnionBuilder, type_id: i8) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        var known = false;
        for (self.union_type.type_ids) |id| {
            if (id == type_id) known = true;
        }
        if (!known) return BuilderError.InvalidTypeId;

        const next_len = self.len + 1;
        try self.ensureCapacity(next_len);
        self.type_ids.data[self.len] = @bitCast(type_id);
        self.len = next_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *SparseUnionBuilder, children: []const ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (children.len != self.union_type.fields.len) return BuilderError.InvalidChildCount;
        for (children, 0..) |child, i| {
            if (child.data().length != self.len) return BuilderError.InvalidChildLength;
            if (!std.meta.eql(child.data().data_type, self.union_type.fields[i].data_type.*)) return BuilderError.InvalidChildType;
        }

        self.buffers[0] = try self.type_ids.toShared(self.len);
        const buffers = try self.allocator.alloc(SharedBuffer, 1);
        buffers[0] = self.buffers[0];

        const out_children = try self.allocator.alloc(ArrayRef, children.len);
        for (children, 0..) |child, i| out_children[i] = child.retain();

        const data = ArrayData{
            .data_type = DataType{ .sparse_union = self.union_type },
            .length = self.len,
            .buffers = buffers,
            .children = out_children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }
};

pub const DenseUnionBuilder = struct {
    allocator: std.mem.Allocator,
    union_type: datatype.UnionType,
    type_ids: OwnedBuffer,
    offsets: OwnedBuffer,
    buffers: [2]SharedBuffer = undefined,
    len: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidTypeId, InvalidChildCount, InvalidChildOffset, InvalidChildType };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, union_type: datatype.UnionType, capacity: usize) !DenseUnionBuilder {
        return .{
            .allocator = allocator,
            .union_type = union_type,
            .type_ids = try OwnedBuffer.init(allocator, capacity),
            .offsets = try OwnedBuffer.init(allocator, capacity * @sizeOf(i32)),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *DenseUnionBuilder) void {
        self.type_ids.deinit();
        self.offsets.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *DenseUnionBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *DenseUnionBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.type_ids.deinit();
        self.offsets.deinit();
        self.len = 0;
        self.state = .ready;
    }

    fn ensureCapacity(self: *DenseUnionBuilder, needed_len: usize) !void {
        if (needed_len > self.type_ids.len()) try self.type_ids.resize(needed_len);
        if (needed_len * @sizeOf(i32) > self.offsets.len()) try self.offsets.resize(needed_len * @sizeOf(i32));
    }

    /// Append one logical value into the builder.
    pub fn append(self: *DenseUnionBuilder, type_id: i8, child_offset: i32) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (child_offset < 0) return BuilderError.InvalidChildOffset;

        var known = false;
        for (self.union_type.type_ids) |id| {
            if (id == type_id) known = true;
        }
        if (!known) return BuilderError.InvalidTypeId;

        const next_len = self.len + 1;
        try self.ensureCapacity(next_len);
        self.type_ids.data[self.len] = @bitCast(type_id);
        std.mem.bytesAsSlice(i32, self.offsets.data)[self.len] = child_offset;
        self.len = next_len;
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *DenseUnionBuilder, children: []const ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (children.len != self.union_type.fields.len) return BuilderError.InvalidChildCount;
        for (children, 0..) |child, i| {
            if (!std.meta.eql(child.data().data_type, self.union_type.fields[i].data_type.*)) return BuilderError.InvalidChildType;
        }

        const offs = std.mem.bytesAsSlice(i32, self.offsets.data)[0..self.len];
        const type_ids = std.mem.bytesAsSlice(i8, self.type_ids.data)[0..self.len];
        for (type_ids, offs) |tid, off| {
            var child_idx: ?usize = null;
            for (self.union_type.type_ids, 0..) |id, idx| {
                if (id == tid) child_idx = idx;
            }
            const idx = child_idx orelse return BuilderError.InvalidTypeId;
            if (@as(usize, @intCast(off)) >= children[idx].data().length) return BuilderError.InvalidChildOffset;
        }

        self.buffers[0] = try self.type_ids.toShared(self.len);
        self.buffers[1] = try self.offsets.toShared(self.len * @sizeOf(i32));
        const buffers = try self.allocator.alloc(SharedBuffer, 2);
        buffers[0] = self.buffers[0];
        buffers[1] = self.buffers[1];

        const out_children = try self.allocator.alloc(ArrayRef, children.len);
        for (children, 0..) |child, i| out_children[i] = child.retain();

        const data = ArrayData{
            .data_type = DataType{ .dense_union = self.union_type },
            .length = self.len,
            .buffers = buffers,
            .children = out_children,
        };
        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }
};

pub const RunEndEncodedBuilder = struct {
    allocator: std.mem.Allocator,
    run_end_type: IntType,
    value_type: *const DataType,
    run_ends: OwnedBuffer,
    run_count: usize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidRunEndType, InvalidRunEnd, InvalidChildLength, Overflow };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, run_end_type: IntType, value_type: *const DataType, capacity: usize) !RunEndEncodedBuilder {
        if (run_end_type.bit_width != 8 and run_end_type.bit_width != 16 and run_end_type.bit_width != 32 and run_end_type.bit_width != 64) {
            return BuilderError.InvalidRunEndType;
        }
        return .{
            .allocator = allocator,
            .run_end_type = run_end_type,
            .value_type = value_type,
            .run_ends = try OwnedBuffer.init(allocator, capacity * (@as(usize, run_end_type.bit_width) / 8)),
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(self: *RunEndEncodedBuilder) void {
        self.run_ends.deinit();
    }

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *RunEndEncodedBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.run_count = 0;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *RunEndEncodedBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.run_ends.deinit();
        self.run_count = 0;
        self.state = .ready;
    }

    fn ensureCapacity(self: *RunEndEncodedBuilder, needed_runs: usize) !void {
        const bytes = needed_runs * (@as(usize, self.run_end_type.bit_width) / 8);
        if (bytes <= self.run_ends.len()) return;
        try self.run_ends.resize(bytes);
    }

    pub fn appendRunEnd(self: *RunEndEncodedBuilder, run_end: i64) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (run_end <= 0) return BuilderError.InvalidRunEnd;

        const next_runs = self.run_count + 1;
        try self.ensureCapacity(next_runs);

        if (self.run_count > 0) {
            const prev = try self.getRunEnd(self.run_count - 1);
            if (run_end <= prev) return BuilderError.InvalidRunEnd;
        }

        switch (self.run_end_type.bit_width) {
            8 => {
                if (self.run_end_type.signed) {
                    std.mem.bytesAsSlice(i8, self.run_ends.data)[self.run_count] = std.math.cast(i8, run_end) orelse return BuilderError.Overflow;
                } else {
                    std.mem.bytesAsSlice(u8, self.run_ends.data)[self.run_count] = std.math.cast(u8, @as(u64, @intCast(run_end))) orelse return BuilderError.Overflow;
                }
            },
            16 => {
                if (self.run_end_type.signed) {
                    std.mem.bytesAsSlice(i16, self.run_ends.data)[self.run_count] = std.math.cast(i16, run_end) orelse return BuilderError.Overflow;
                } else {
                    std.mem.bytesAsSlice(u16, self.run_ends.data)[self.run_count] = std.math.cast(u16, @as(u64, @intCast(run_end))) orelse return BuilderError.Overflow;
                }
            },
            32 => {
                if (self.run_end_type.signed) {
                    std.mem.bytesAsSlice(i32, self.run_ends.data)[self.run_count] = std.math.cast(i32, run_end) orelse return BuilderError.Overflow;
                } else {
                    std.mem.bytesAsSlice(u32, self.run_ends.data)[self.run_count] = std.math.cast(u32, @as(u64, @intCast(run_end))) orelse return BuilderError.Overflow;
                }
            },
            64 => {
                if (self.run_end_type.signed) {
                    std.mem.bytesAsSlice(i64, self.run_ends.data)[self.run_count] = run_end;
                } else {
                    std.mem.bytesAsSlice(u64, self.run_ends.data)[self.run_count] = @intCast(run_end);
                }
            },
            else => return BuilderError.InvalidRunEndType,
        }

        self.run_count = next_runs;
    }

    fn getRunEnd(self: *RunEndEncodedBuilder, index: usize) BuilderError!i64 {
        return switch (self.run_end_type.bit_width) {
            8 => if (self.run_end_type.signed)
                std.mem.bytesAsSlice(i8, self.run_ends.data)[index]
            else
                @intCast(std.mem.bytesAsSlice(u8, self.run_ends.data)[index]),
            16 => if (self.run_end_type.signed)
                std.mem.bytesAsSlice(i16, self.run_ends.data)[index]
            else
                @intCast(std.mem.bytesAsSlice(u16, self.run_ends.data)[index]),
            32 => if (self.run_end_type.signed)
                std.mem.bytesAsSlice(i32, self.run_ends.data)[index]
            else
                @intCast(std.mem.bytesAsSlice(u32, self.run_ends.data)[index]),
            64 => if (self.run_end_type.signed)
                std.mem.bytesAsSlice(i64, self.run_ends.data)[index]
            else
                @intCast(std.mem.bytesAsSlice(u64, self.run_ends.data)[index]),
            else => BuilderError.InvalidRunEndType,
        };
    }

    /// Finalize builder state and return an immutable array reference.
    pub fn finish(self: *RunEndEncodedBuilder, values: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (!std.meta.eql(values.data().data_type, self.value_type.*)) return BuilderError.InvalidChildLength;
        if (values.data().length != self.run_count) return BuilderError.InvalidChildLength;

        const total_len: usize = if (self.run_count == 0)
            0
        else
            std.math.cast(usize, try self.getRunEnd(self.run_count - 1)) orelse return BuilderError.InvalidRunEnd;
        const bytes = self.run_count * (@as(usize, self.run_end_type.bit_width) / 8);
        const run_ends_buf = try self.run_ends.toShared(bytes);

        const run_end_dt = dataTypeFromIntType(self.run_end_type) orelse return BuilderError.InvalidRunEndType;
        const run_end_buffers = try self.allocator.alloc(SharedBuffer, 2);
        run_end_buffers[0] = SharedBuffer.empty;
        run_end_buffers[1] = run_ends_buf;

        var run_end_ref = try ArrayRef.fromOwnedUnsafe(self.allocator, .{
            .data_type = run_end_dt,
            .length = self.run_count,
            .null_count = 0,
            .buffers = run_end_buffers,
        });
        errdefer run_end_ref.release();

        const buffers = try self.allocator.alloc(SharedBuffer, 0);

        const children = try self.allocator.alloc(ArrayRef, 2);
        children[0] = run_end_ref;
        children[1] = values.retain();

        const data = ArrayData{
            .data_type = DataType{ .run_end_encoded = .{ .run_end_type = self.run_end_type, .value_type = self.value_type } },
            .length = total_len,
            .buffers = buffers,
            .children = children,
        };

        self.state = .finished;
        return ArrayRef.fromOwnedUnsafe(self.allocator, data);
    }
};

test "map builder and map array basic path" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };

    var key_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer key_builder.deinit();
    try key_builder.append(1);
    try key_builder.append(2);
    try key_builder.append(3);
    var key_ref = try key_builder.finish();
    defer key_ref.release();

    var item_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer item_builder.deinit();
    try item_builder.append(10);
    try item_builder.append(20);
    try item_builder.append(30);
    var item_ref = try item_builder.finish();
    defer item_ref.release();

    var entries_builder = @import("struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ key_ref, item_ref });
    defer entries_ref.release();

    var map_builder = try MapBuilder.init(allocator, 2, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendLen(1);

    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    const map_array = MapArray{ .data = map_ref.data() };
    try std.testing.expectEqual(@as(usize, 2), map_array.len());
    var first = try map_array.value(0);
    defer first.release();
    try std.testing.expectEqual(@as(usize, 2), first.data().length);
}

test "map builder rejects entries schema mismatch" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const key_field = Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };

    var key_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer key_builder.deinit();
    try key_builder.append(1);
    var key_ref = try key_builder.finish();
    defer key_ref.release();

    var wrong_item_builder = try @import("boolean_array.zig").BooleanBuilder.init(allocator, 1);
    defer wrong_item_builder.deinit();
    try wrong_item_builder.append(true);
    var wrong_item_ref = try wrong_item_builder.finish();
    defer wrong_item_ref.release();

    const wrong_item_field = Field{ .name = "item", .data_type = &bool_type, .nullable = true };
    var entries_builder = @import("struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, wrong_item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ key_ref, wrong_item_ref });
    defer entries_ref.release();

    var map_builder = try MapBuilder.init(allocator, 1, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(1);

    try std.testing.expectError(error.InvalidEntriesSchema, map_builder.finish(entries_ref));
}

test "sparse union builder validates child count/length" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .sparse };

    var builder = try SparseUnionBuilder.init(allocator, union_type, 2);
    defer builder.deinit();
    try builder.appendTypeId(0);
    try builder.appendTypeId(0);

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    try std.testing.expectError(error.InvalidChildLength, builder.finish(&[_]ArrayRef{child_ref}));
}

test "sparse union builder rejects child type mismatch" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .sparse };

    var builder = try SparseUnionBuilder.init(allocator, union_type, 1);
    defer builder.deinit();
    try builder.appendTypeId(0);

    var bool_builder = try @import("boolean_array.zig").BooleanBuilder.init(allocator, 1);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    try std.testing.expectError(error.InvalidChildType, builder.finish(&[_]ArrayRef{bool_ref}));
}

test "dense union builder validates child offsets" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .dense };

    var builder = try DenseUnionBuilder.init(allocator, union_type, 1);
    defer builder.deinit();
    try builder.append(0, 2);

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    try std.testing.expectError(error.InvalidChildOffset, builder.finish(&[_]ArrayRef{child_ref}));
}

test "dense union builder rejects child type mismatch" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .dense };

    var builder = try DenseUnionBuilder.init(allocator, union_type, 1);
    defer builder.deinit();
    try builder.append(0, 0);

    var bool_builder = try @import("boolean_array.zig").BooleanBuilder.init(allocator, 1);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    try std.testing.expectError(error.InvalidChildType, builder.finish(&[_]ArrayRef{bool_ref}));
}

test "run end encoded builder and array basic path" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .int32 = {} };
    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(100);
    try values_builder.append(200);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try RunEndEncodedBuilder.init(allocator, .{ .bit_width = 32, .signed = true }, &value_type, 2);
    defer builder.deinit();
    try builder.appendRunEnd(2);
    try builder.appendRunEnd(5);

    var out = try builder.finish(values_ref);
    defer out.release();

    try std.testing.expectEqual(@as(usize, 0), out.data().buffers.len);
    try std.testing.expectEqual(@as(usize, 2), out.data().children.len);
    try std.testing.expect(out.data().children[0].data().data_type == .int32);

    const ree = RunEndEncodedArray{ .data = out.data() };
    try std.testing.expectEqual(@as(usize, 5), ree.len());

    var v0 = try ree.value(0);
    defer v0.release();
    var v4 = try ree.value(4);
    defer v4.release();
    const a0 = @import("primitive_array.zig").PrimitiveArray(i32){ .data = v0.data() };
    const a4 = @import("primitive_array.zig").PrimitiveArray(i32){ .data = v4.data() };
    try std.testing.expectEqual(@as(i32, 100), a0.value(0));
    try std.testing.expectEqual(@as(i32, 200), a4.value(0));
}

test "map builder preserves nullability invariants" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };

    var key_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer key_builder.deinit();
    try key_builder.append(1);
    try key_builder.append(2);
    var key_ref = try key_builder.finish();
    defer key_ref.release();

    var item_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer item_builder.deinit();
    try item_builder.append(10);
    try item_builder.append(20);
    var item_ref = try item_builder.finish();
    defer item_ref.release();

    var entries_builder = @import("struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ key_ref, item_ref });
    defer entries_ref.release();

    var map_builder = try MapBuilder.init(allocator, 2, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendNull();

    var out = try map_builder.finish(entries_ref);
    defer out.release();

    const map = MapArray{ .data = out.data() };
    try std.testing.expectEqual(@as(usize, 2), map.len());
    try std.testing.expect(!map.isNull(0));
    try std.testing.expect(map.isNull(1));

    var first = try map.value(0);
    defer first.release();
    try std.testing.expectEqual(@as(usize, 2), first.data().length);

    var second = try map.value(1);
    defer second.release();
    try std.testing.expectEqual(@as(usize, 0), second.data().length);
}

test "map builder reset and clear support reuse" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &int_type, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &int_type, .nullable = true };

    var key_builder_1 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer key_builder_1.deinit();
    try key_builder_1.append(1);
    var key_ref_1 = try key_builder_1.finish();
    defer key_ref_1.release();

    var item_builder_1 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer item_builder_1.deinit();
    try item_builder_1.append(10);
    var item_ref_1 = try item_builder_1.finish();
    defer item_ref_1.release();

    var entries_builder_1 = @import("struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder_1.deinit();
    try entries_builder_1.appendValid();
    var entries_ref_1 = try entries_builder_1.finish(&[_]ArrayRef{ key_ref_1, item_ref_1 });
    defer entries_ref_1.release();

    var key_builder_0 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 0);
    defer key_builder_0.deinit();
    var key_ref_0 = try key_builder_0.finish();
    defer key_ref_0.release();

    var item_builder_0 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 0);
    defer item_builder_0.deinit();
    var item_ref_0 = try item_builder_0.finish();
    defer item_ref_0.release();

    var entries_builder_0 = @import("struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder_0.deinit();
    var entries_ref_0 = try entries_builder_0.finish(&[_]ArrayRef{ key_ref_0, item_ref_0 });
    defer entries_ref_0.release();

    var map_builder = try MapBuilder.init(allocator, 1, key_field, item_field, false);
    defer map_builder.deinit();

    try std.testing.expectError(error.NotFinished, map_builder.reset());
    try std.testing.expectError(error.NotFinished, map_builder.clear());

    try map_builder.appendLen(1);
    var out_1 = try map_builder.finish(entries_ref_1);
    defer out_1.release();

    try map_builder.reset();
    try map_builder.appendNull();
    var out_2 = try map_builder.finish(entries_ref_0);
    defer out_2.release();

    try map_builder.clear();
    try map_builder.appendLen(1);
    var out_3 = try map_builder.finish(entries_ref_1);
    defer out_3.release();
}

test "sparse union builder reset and clear support reuse" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .sparse };

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    var builder = try SparseUnionBuilder.init(allocator, union_type, 1);
    defer builder.deinit();

    try std.testing.expectError(error.NotFinished, builder.reset());
    try std.testing.expectError(error.NotFinished, builder.clear());

    try builder.appendTypeId(0);
    var out_1 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_1.release();

    try builder.reset();
    try builder.appendTypeId(0);
    var out_2 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_2.release();

    try builder.clear();
    try builder.appendTypeId(0);
    var out_3 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_3.release();
}

test "dense union builder reset and clear support reuse" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "a", .data_type = &int_type, .nullable = true }};
    const union_type = datatype.UnionType{ .type_ids = &[_]i8{0}, .fields = fields[0..], .mode = .dense };

    var child_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);
    var child_ref = try child_builder.finish();
    defer child_ref.release();

    var builder = try DenseUnionBuilder.init(allocator, union_type, 1);
    defer builder.deinit();

    try std.testing.expectError(error.NotFinished, builder.reset());
    try std.testing.expectError(error.NotFinished, builder.clear());

    try builder.append(0, 0);
    var out_1 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_1.release();

    try builder.reset();
    try builder.append(0, 0);
    var out_2 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_2.release();

    try builder.clear();
    try builder.append(0, 0);
    var out_3 = try builder.finish(&[_]ArrayRef{child_ref});
    defer out_3.release();
}

test "run end encoded builder reset and clear support reuse" {
    const allocator = std.testing.allocator;

    const value_type = DataType{ .int32 = {} };
    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder.deinit();
    try values_builder.append(11);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var builder = try RunEndEncodedBuilder.init(allocator, .{ .bit_width = 32, .signed = true }, &value_type, 1);
    defer builder.deinit();

    try std.testing.expectError(error.NotFinished, builder.reset());
    try std.testing.expectError(error.NotFinished, builder.clear());

    try builder.appendRunEnd(1);
    var out_1 = try builder.finish(values_ref);
    defer out_1.release();

    try builder.reset();
    try builder.appendRunEnd(1);
    var out_2 = try builder.finish(values_ref);
    defer out_2.release();

    try builder.clear();
    try builder.appendRunEnd(1);
    var out_3 = try builder.finish(values_ref);
    defer out_3.release();
}
