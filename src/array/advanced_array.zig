const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_utils = @import("array_utils.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const IntType = datatype.IntType;
pub const ArrayRef = array_ref.ArrayRef;
pub const BuilderState = builder_state.BuilderState;

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

pub const MapArray = struct {
    data: *const ArrayData,

    pub fn len(self: MapArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: MapArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn entriesRef(self: MapArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }

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

    pub fn len(self: RunEndEncodedArray) usize {
        return self.data.length;
    }

    fn runCount(self: RunEndEncodedArray) usize {
        const byte_width = self.data.data_type.run_end_encoded.run_end_type.bit_width / 8;
        return self.data.buffers[0].len() / byte_width;
    }

    fn runEndAt(self: RunEndEncodedArray, run_index: usize) i64 {
        const run_ty = self.data.data_type.run_end_encoded.run_end_type;
        return switch (run_ty.bit_width) {
            8 => if (run_ty.signed)
                @as(i64, self.data.buffers[0].typedSlice(i8)[run_index])
            else
                @as(i64, @intCast(self.data.buffers[0].typedSlice(u8)[run_index])),
            16 => if (run_ty.signed)
                @as(i64, self.data.buffers[0].typedSlice(i16)[run_index])
            else
                @as(i64, @intCast(self.data.buffers[0].typedSlice(u16)[run_index])),
            32 => if (run_ty.signed)
                @as(i64, self.data.buffers[0].typedSlice(i32)[run_index])
            else
                @as(i64, @intCast(self.data.buffers[0].typedSlice(u32)[run_index])),
            64 => if (run_ty.signed)
                self.data.buffers[0].typedSlice(i64)[run_index]
            else
                @as(i64, @intCast(self.data.buffers[0].typedSlice(u64)[run_index])),
            else => unreachable,
        };
    }

    fn runIndexFor(self: RunEndEncodedArray, logical_index: usize) usize {
        var lo: usize = 0;
        var hi: usize = self.runCount();
        const target = @as(i64, @intCast(logical_index + 1));

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.runEndAt(mid) >= target) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    pub fn value(self: RunEndEncodedArray, i: usize) !ArrayRef {
        std.debug.assert(i < self.data.length);
        std.debug.assert(self.data.children.len == 1);
        const run_idx = self.runIndexFor(self.data.offset + i);
        if (run_idx >= self.runCount()) return error.InvalidRunEnds;
        return self.data.children[0].slice(run_idx, 1);
    }

    pub fn valuesRef(self: RunEndEncodedArray) *const ArrayRef {
        std.debug.assert(self.data.children.len == 1);
        return &self.data.children[0];
    }
};

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

    pub fn init(allocator: std.mem.Allocator, capacity: usize, key_field: Field, item_field: Field, keys_sorted: bool) !MapBuilder {
        const offsets = try OwnedBuffer.init(allocator, (capacity + 1) * @sizeOf(i32));
        std.mem.bytesAsSlice(i32, offsets.data)[0] = 0;
        return .{ .allocator = allocator, .key_field = key_field, .item_field = item_field, .keys_sorted = keys_sorted, .offsets = offsets };
    }

    pub fn deinit(self: *MapBuilder) void {
        self.offsets.deinit();
        if (self.validity) |*valid| valid.deinit();
    }

    fn ensureOffsetsCapacity(self: *MapBuilder, needed_len: usize) !void {
        const capacity = self.offsets.len() / @sizeOf(i32);
        if (needed_len <= capacity) return;
        const was_empty = capacity == 0;
        try self.offsets.resize(needed_len * @sizeOf(i32));
        if (was_empty) std.mem.bytesAsSlice(i32, self.offsets.data)[0] = 0;
    }

    fn ensureValidityForNull(self: *MapBuilder, new_len: usize) !void {
        if (self.validity == null) {
            var buf = try initValidityAllValid(self.allocator, new_len);
            bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
            self.validity = buf;
            self.null_count += 1;
            return;
        }
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, new_len);
        bitmap.clearBit(buf.data[0..bitmap.byteLength(new_len)], new_len - 1);
        self.null_count += 1;
    }

    fn setValidBit(self: *MapBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn appendLen(self: *MapBuilder, value_len: usize) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        const next_offset = self.values_len + value_len;
        const cast_offset = std.math.cast(i32, next_offset) orelse return BuilderError.OffsetOverflow;
        std.mem.bytesAsSlice(i32, self.offsets.data)[next_len] = cast_offset;
        try self.setValidBit(self.len);
        self.len = next_len;
        self.values_len = next_offset;
    }

    pub fn appendNull(self: *MapBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureOffsetsCapacity(next_len + 1);
        std.mem.bytesAsSlice(i32, self.offsets.data)[next_len] = std.math.cast(i32, self.values_len) orelse return BuilderError.OffsetOverflow;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

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

    pub fn init(allocator: std.mem.Allocator, union_type: datatype.UnionType, capacity: usize) !SparseUnionBuilder {
        return .{ .allocator = allocator, .union_type = union_type, .type_ids = try OwnedBuffer.init(allocator, capacity) };
    }

    pub fn deinit(self: *SparseUnionBuilder) void {
        self.type_ids.deinit();
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

    pub fn init(allocator: std.mem.Allocator, union_type: datatype.UnionType, capacity: usize) !DenseUnionBuilder {
        return .{
            .allocator = allocator,
            .union_type = union_type,
            .type_ids = try OwnedBuffer.init(allocator, capacity),
            .offsets = try OwnedBuffer.init(allocator, capacity * @sizeOf(i32)),
        };
    }

    pub fn deinit(self: *DenseUnionBuilder) void {
        self.type_ids.deinit();
        self.offsets.deinit();
    }

    fn ensureCapacity(self: *DenseUnionBuilder, needed_len: usize) !void {
        if (needed_len > self.type_ids.len()) try self.type_ids.resize(needed_len);
        if (needed_len * @sizeOf(i32) > self.offsets.len()) try self.offsets.resize(needed_len * @sizeOf(i32));
    }

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

    pub fn deinit(self: *RunEndEncodedBuilder) void {
        self.run_ends.deinit();
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
            const prev = self.getRunEnd(self.run_count - 1);
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

    fn getRunEnd(self: *RunEndEncodedBuilder, index: usize) i64 {
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
            else => unreachable,
        };
    }

    pub fn finish(self: *RunEndEncodedBuilder, values: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (!std.meta.eql(values.data().data_type, self.value_type.*)) return BuilderError.InvalidChildLength;
        if (values.data().length != self.run_count) return BuilderError.InvalidChildLength;

        const total_len: usize = if (self.run_count == 0) 0 else @intCast(self.getRunEnd(self.run_count - 1));
        const bytes = self.run_count * (@as(usize, self.run_end_type.bit_width) / 8);
        const run_ends_buf = try self.run_ends.toShared(bytes);

        const buffers = try self.allocator.alloc(SharedBuffer, 1);
        buffers[0] = run_ends_buf;

        const children = try self.allocator.alloc(ArrayRef, 1);
        children[0] = values.retain();

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
