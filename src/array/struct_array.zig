const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;
pub const ArrayData = array_data.ArrayData;
pub const ArrayRef = array_ref.ArrayRef;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const BuilderState = builder_state.BuilderState;

fn initValidityAllValid(allocator: std.mem.Allocator, bit_len: usize) !OwnedBuffer {
    const used_bytes = bitmap.byteLength(bit_len);
    var buf = try OwnedBuffer.init(allocator, used_bytes);
    if (used_bytes > 0) {
        @memset(buf.data[0..used_bytes], 0xFF);
        const remainder = bit_len & 7;
        if (remainder != 0) {
            const keep_mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
            buf.data[used_bytes - 1] &= keep_mask;
        }
    }
    return buf;
}

fn ensureBitmapCapacity(buf: *OwnedBuffer, bit_len: usize) !void {
    const needed = bitmap.byteLength(bit_len);
    if (needed <= buf.len()) return;
    try buf.resize(needed);
}

pub const StructArray = struct {
    data: *const ArrayData,

    pub fn len(self: StructArray) usize {
        return self.data.length;
    }

    pub fn isNull(self: StructArray, i: usize) bool {
        return self.data.isNull(i);
    }

    pub fn fieldCount(self: StructArray) usize {
        return self.data.children.len;
    }

    pub fn fieldRef(self: StructArray, index: usize) *const ArrayRef {
        std.debug.assert(index < self.data.children.len);
        return &self.data.children[index];
    }

    pub fn field(self: StructArray, index: usize) !ArrayRef {
        std.debug.assert(index < self.data.children.len);
        const child = self.data.children[index];
        const child_data = child.data();
        if (child_data.length == self.data.length and child_data.offset == self.data.offset) {
            return child.retain();
        }
        return child.slice(self.data.offset, self.data.length);
    }
};

pub const StructBuilder = struct {
    allocator: std.mem.Allocator,
    fields: []const Field,
    validity: ?OwnedBuffer = null,
    buffers: [1]SharedBuffer = undefined,
    len: usize = 0,
    null_count: isize = 0,
    state: BuilderState = .ready,

    const BuilderError = error{ AlreadyFinished, NotFinished, InvalidChildCount, InvalidChildLength };

    pub fn init(allocator: std.mem.Allocator, fields: []const Field) StructBuilder {
        return .{ .allocator = allocator, .fields = fields };
    }

    pub fn deinit(self: *StructBuilder) void {
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *StructBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    pub fn clear(self: *StructBuilder) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        if (self.validity) |*valid| valid.deinit();
        self.validity = null;
        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    fn ensureValidityForNull(self: *StructBuilder, new_len: usize) !void {
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

    fn setValidBit(self: *StructBuilder, index: usize) !void {
        if (self.validity == null) return;
        var buf = &self.validity.?;
        try ensureBitmapCapacity(buf, index + 1);
        bitmap.setBit(buf.data[0..bitmap.byteLength(index + 1)], index);
    }

    pub fn appendValid(self: *StructBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.setValidBit(self.len);
        self.len = next_len;
    }

    pub fn appendNull(self: *StructBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn finish(self: *StructBuilder, children: []const ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (children.len != self.fields.len) return BuilderError.InvalidChildCount;
        for (children) |child| {
            if (child.data().length != self.len) return BuilderError.InvalidChildLength;
        }

        const validity_buf = if (self.validity) |*buf| try buf.toShared(bitmap.byteLength(self.len)) else SharedBuffer.empty;
        self.buffers[0] = validity_buf;

        const buffers = try self.allocator.alloc(SharedBuffer, 1);
        var filled: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < filled) : (i += 1) {
                var owned = buffers[i];
                owned.release();
            }
            self.allocator.free(buffers);
        }
        buffers[0] = self.buffers[0];
        filled = 1;

        const child_refs = try self.allocator.alloc(ArrayRef, children.len);
        var child_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < child_count) : (i += 1) {
                child_refs[i].release();
            }
            self.allocator.free(child_refs);
        }
        for (children, 0..) |child, i| {
            child_refs[i] = child.retain();
            child_count += 1;
        }

        const data = ArrayData{
            .data_type = DataType{ .struct_ = .{ .fields = self.fields } },
            .length = self.len,
            .null_count = self.null_count,
            .buffers = buffers,
            .children = child_refs,
        };

        const array_ref_out = try ArrayRef.fromOwnedUnsafe(self.allocator, data);
        self.state = .finished;
        return array_ref_out;
    }

    pub fn finishReset(self: *StructBuilder, children: []const ArrayRef) !ArrayRef {
        const array_ref_out = try self.finish(children);
        try self.reset();
        return array_ref_out;
    }
};

test "struct array fields follow parent slice" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };

    var values_bytes: [4 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4 }));

    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 4,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = SharedBuffer.empty;

    const struct_layout = ArrayData{
        .data_type = DataType{ .struct_ = .{ .fields = &[_]datatype.Field{.{ .name = "a", .data_type = &value_type, .nullable = true }} } },
        .length = 2,
        .offset = 1,
        .buffers = buffers,
        .children = children,
    };

    var struct_ref = try ArrayRef.fromOwnedUnsafe(allocator, struct_layout);
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    var sliced_child = try struct_array.field(0);
    defer sliced_child.release();

    const child_view = @import("primitive_array.zig").PrimitiveArray(i32){ .data = sliced_child.data() };
    try std.testing.expectEqual(@as(usize, 2), child_view.len());
    try std.testing.expectEqual(@as(i32, 2), child_view.value(0));
    try std.testing.expectEqual(@as(i32, 3), child_view.value(1));
}

test "struct array fieldRef returns child" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };

    var values_bytes: [2 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 9, 11 }));

    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 2,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(values_bytes[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = SharedBuffer.empty;

    const struct_layout = ArrayData{
        .data_type = DataType{ .struct_ = .{ .fields = &[_]datatype.Field{.{ .name = "a", .data_type = &value_type, .nullable = true }} } },
        .length = 2,
        .buffers = buffers,
        .children = children,
    };

    var struct_ref = try ArrayRef.fromOwnedUnsafe(allocator, struct_layout);
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    const child_view = @import("primitive_array.zig").PrimitiveArray(i32){ .data = struct_array.fieldRef(0).data() };
    try std.testing.expectEqual(@as(usize, 2), child_view.len());
    try std.testing.expectEqual(@as(i32, 9), child_view.value(0));
    try std.testing.expectEqual(@as(i32, 11), child_view.value(1));
}

test "struct builder builds arrays" {
    const allocator = std.testing.allocator;
    const int_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &int_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };

    var int_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(1);
    try int_builder.append(2);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var bool_builder = try @import("boolean_array.zig").BooleanBuilder.init(allocator, 2);
    defer bool_builder.deinit();
    try bool_builder.append(true);
    try bool_builder.append(false);
    var bool_ref = try bool_builder.finish();
    defer bool_ref.release();

    var builder = StructBuilder.init(allocator, fields);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendNull();

    const children = &[_]ArrayRef{ int_ref, bool_ref };
    var struct_ref = try builder.finish(children);
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    try std.testing.expectEqual(@as(usize, 2), struct_array.len());
    try std.testing.expect(!struct_array.isNull(0));
    try std.testing.expect(struct_array.isNull(1));
}

test "struct builder finishReset allows reuse" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    var builder = StructBuilder.init(allocator, fields);
    defer builder.deinit();
    try builder.appendValid();

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder.deinit();
    try values_builder.append(5);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var struct_ref = try builder.finishReset(&[_]ArrayRef{values_ref});
    defer struct_ref.release();
    const struct_array = StructArray{ .data = struct_ref.data() };
    try std.testing.expectEqual(@as(usize, 1), struct_array.len());

    try builder.appendValid();
    var values_builder2 = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 1);
    defer values_builder2.deinit();
    try values_builder2.append(9);
    var values_ref2 = try values_builder2.finish();
    defer values_ref2.release();

    var struct_ref2 = try builder.finish(&[_]ArrayRef{values_ref2});
    defer struct_ref2.release();
    const struct_array2 = StructArray{ .data = struct_ref2.data() };
    try std.testing.expectEqual(@as(usize, 1), struct_array2.len());
}
