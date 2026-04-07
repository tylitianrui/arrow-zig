const std = @import("std");

const BuilderError = error{
    AlreadyFinished,
    NotFinished,
    InvalidChildCount,
    InvalidChildLength,
    InvalidChildType,
    InvalidChildOffset,
    MissingChildBuilders,
    ChildResetUnsupported,
    ChildClearUnsupported,
};
pub const ChildBuilderError = error{
    ResetFailed,
    ClearFailed,
};
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
pub const ArrayRef = array_ref.ArrayRef;
pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const BuilderState = builder_state.BuilderState;

pub const ChildBuilder = struct {
    ctx: *anyopaque,
    finishFn: *const fn (*anyopaque) anyerror!ArrayRef,
    lenFn: *const fn (*anyopaque) usize,
    resetFn: ?*const fn (*anyopaque) ChildBuilderError!void = null,
    clearFn: ?*const fn (*anyopaque) ChildBuilderError!void = null,
};

const FromChildrenError = BuilderError || ChildBuilderError;
const RecycleAction = enum {
    reset,
    clear,
};

const initValidityAllValid = array_utils.initValidityAllValid;
const ensureBitmapCapacity = array_utils.ensureBitmapCapacity;

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
    child_builders: ?[]const ChildBuilder = null,
    validity: ?OwnedBuffer = null,
    buffers: [1]SharedBuffer = undefined,
    len: usize = 0,
    null_count: usize = 0,
    state: BuilderState = .ready,

    fn resetChildren(self: *StructBuilder) FromChildrenError!void {
        if (self.child_builders == null) return;
        const builders = self.child_builders.?;
        if (builders.len != self.fields.len) return BuilderError.InvalidChildCount;
        for (builders) |builder| {
            if (builder.resetFn == null) return BuilderError.ChildResetUnsupported;
        }
        for (builders) |builder| {
            try builder.resetFn.?(builder.ctx);
        }
    }

    fn clearChildren(self: *StructBuilder) FromChildrenError!void {
        if (self.child_builders == null) return;
        const builders = self.child_builders.?;
        if (builders.len != self.fields.len) return BuilderError.InvalidChildCount;
        for (builders) |builder| {
            if (builder.clearFn == null) return BuilderError.ChildClearUnsupported;
        }
        for (builders) |builder| {
            try builder.clearFn.?(builder.ctx);
        }
    }

    fn materializeChildren(self: *StructBuilder, builders: []const ChildBuilder) ![]ArrayRef {
        if (builders.len != self.fields.len) return BuilderError.InvalidChildCount;

        const children = try self.allocator.alloc(ArrayRef, builders.len);
        var child_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < child_count) : (i += 1) {
                children[i].release();
            }
            self.allocator.free(children);
        }

        for (builders, 0..) |builder, i| {
            if (builder.lenFn(builder.ctx) != self.len) return BuilderError.InvalidChildLength;
            children[i] = try builder.finishFn(builder.ctx);
            child_count += 1;
        }

        return children;
    }

    fn releaseMaterializedChildren(self: *StructBuilder, children: []ArrayRef) void {
        var i: usize = 0;
        while (i < children.len) : (i += 1) {
            children[i].release();
        }
        self.allocator.free(children);
    }

    fn recycle(self: *StructBuilder, action: RecycleAction) FromChildrenError!void {
        // Any-state recycle: caller can always reset/clear the parent builder.
        // Child coordination is only attempted for finished batches so we don't
        // force child builders to support ready-state reset/clear.
        const should_coordinate_children = self.state == .finished and self.child_builders != null;

        // Strategy: children first, parent second.
        // If child coordination fails, parent state is left unchanged.
        if (should_coordinate_children) {
            switch (action) {
                .reset => try self.resetChildren(),
                .clear => try self.clearChildren(),
            }
        }

        if (action == .clear) {
            if (self.validity) |*valid| valid.deinit();
            self.validity = null;
        }

        self.len = 0;
        self.null_count = 0;
        self.state = .ready;
    }

    pub fn init(allocator: std.mem.Allocator, fields: []const Field) StructBuilder {
        return .{ .allocator = allocator, .fields = fields };
    }

    pub fn initWithChildren(allocator: std.mem.Allocator, fields: []const Field, child_builders: []const ChildBuilder) StructBuilder {
        return .{ .allocator = allocator, .fields = fields, .child_builders = child_builders };
    }

    pub fn deinit(self: *StructBuilder) void {
        if (self.validity) |*valid| valid.deinit();
    }

    pub fn reset(self: *StructBuilder) FromChildrenError!void {
        try self.recycle(.reset);
    }

    pub fn clear(self: *StructBuilder) FromChildrenError!void {
        try self.recycle(.clear);
    }

    pub fn reserve(self: *StructBuilder, additional: usize) !void {
        if (self.validity == null) return;
        try ensureBitmapCapacity(&self.validity.?, self.len + additional);
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

    pub fn appendPresent(self: *StructBuilder) !void {
        try self.appendValid();
    }

    pub fn appendNull(self: *StructBuilder) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        const next_len = self.len + 1;
        try self.ensureValidityForNull(next_len);
        self.len = next_len;
    }

    pub fn appendMany(self: *StructBuilder, present: []const bool) !void {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        for (present) |is_present| {
            if (is_present) {
                try self.appendValid();
            } else {
                try self.appendNull();
            }
        }
    }

    pub fn finish(self: *StructBuilder, children: []const ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (children.len != self.fields.len) return BuilderError.InvalidChildCount;
        for (children, 0..) |child, i| {
            const child_data = child.data();
            if (child_data.length != self.len) return BuilderError.InvalidChildLength;
            if (child_data.offset != 0) return BuilderError.InvalidChildOffset;
            if (!std.meta.eql(child_data.data_type, self.fields[i].data_type.*)) return BuilderError.InvalidChildType;
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

    pub fn finishFromChildren(self: *StructBuilder) !ArrayRef {
        if (self.child_builders == null) return BuilderError.MissingChildBuilders;
        const builders = self.child_builders.?;

        const children = try self.materializeChildren(builders);
        defer self.releaseMaterializedChildren(children);
        return self.finish(children);
    }

    pub fn finishFromChildrenReset(self: *StructBuilder) (FromChildrenError || anyerror)!ArrayRef {
        if (self.child_builders == null) return BuilderError.MissingChildBuilders;
        const builders = self.child_builders.?;

        for (builders) |builder| {
            if (builder.resetFn == null) return BuilderError.ChildResetUnsupported;
        }

        const children = try self.materializeChildren(builders);
        defer self.releaseMaterializedChildren(children);

        var array_ref_out = try self.finish(children);
        errdefer array_ref_out.release();

        try self.reset();
        return array_ref_out;
    }

    pub fn finishFromChildrenClear(self: *StructBuilder) (FromChildrenError || anyerror)!ArrayRef {
        if (self.child_builders == null) return BuilderError.MissingChildBuilders;
        const builders = self.child_builders.?;

        for (builders) |builder| {
            if (builder.clearFn == null) return BuilderError.ChildClearUnsupported;
        }

        const children = try self.materializeChildren(builders);
        defer self.releaseMaterializedChildren(children);

        var array_ref_out = try self.finish(children);
        errdefer array_ref_out.release();

        try self.clear();
        return array_ref_out;
    }

    pub fn finishReset(self: *StructBuilder, children: []const ArrayRef) !ArrayRef {
        const array_ref_out = try self.finish(children);
        try self.reset();
        return array_ref_out;
    }

    pub fn finishClear(self: *StructBuilder, children: []const ArrayRef) !ArrayRef {
        const array_ref_out = try self.finish(children);
        try self.clear();
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

test "struct builder finishFromChildren builds" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 2);
    defer child_builder.deinit();
    try child_builder.append(1);
    try child_builder.append(2);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
        },
    });
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();

    var struct_ref = try builder.finishFromChildren();
    defer struct_ref.release();

    const struct_array = StructArray{ .data = struct_ref.data() };
    try std.testing.expectEqual(@as(usize, 2), struct_array.len());
}

test "struct builder finishFromChildrenReset resets child builders" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 2);
    defer child_builder.deinit();
    try child_builder.append(1);
    try child_builder.append(2);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();

    var struct_ref = try builder.finishFromChildrenReset();
    defer struct_ref.release();

    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expectEqual(@as(usize, 0), child_builder.len);
}

test "struct builder reset reuses builders when children configured" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(7);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendValid();

    var struct_ref = try builder.finishFromChildren();
    defer struct_ref.release();

    try builder.reset();
    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expectEqual(@as(usize, 0), child_builder.len);
}

test "struct builder clear frees validity and clears children when configured" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 2);
    defer child_builder.deinit();
    try child_builder.append(3);
    try child_builder.append(5);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendNull(); // forces validity bitmap allocation

    var struct_ref = try builder.finishFromChildren();
    defer struct_ref.release();

    try builder.clear();
    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expectEqual(@as(usize, 0), child_builder.len);
    try std.testing.expect(builder.validity == null);
}

test "struct builder finishFromChildrenClear clears after finish" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 2);
    defer child_builder.deinit();
    try child_builder.append(10);
    try child_builder.append(20);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendNull(); // validity bitmap
    try builder.appendValid();

    var struct_ref = try builder.finishFromChildrenClear();
    defer struct_ref.release();

    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expectEqual(@as(usize, 0), child_builder.len);
    try std.testing.expect(builder.validity == null);
}

test "struct builder finishClear reuses builder without children" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    var builder = StructBuilder.init(allocator, fields);
    defer builder.deinit();
    try builder.appendNull();
    try builder.appendValid();

    var values_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(1);
    try values_builder.append(2);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    var struct_ref = try builder.finishClear(&[_]ArrayRef{values_ref});
    defer struct_ref.release();

    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expect(builder.validity == null);
}

test "struct builder reset works in ready state" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    var builder = StructBuilder.init(allocator, fields);
    defer builder.deinit();
    try builder.appendValid();
    try builder.reset();

    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expectEqual(@as(usize, 0), builder.null_count);
    try std.testing.expectEqual(BuilderState.ready, builder.state);
}

test "struct builder clear works in ready state" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    var builder = StructBuilder.init(allocator, fields);
    defer builder.deinit();
    try builder.appendNull();
    try std.testing.expect(builder.validity != null);

    try builder.clear();
    try std.testing.expectEqual(@as(usize, 0), builder.len);
    try std.testing.expect(builder.validity == null);
    try std.testing.expectEqual(BuilderState.ready, builder.state);
}

test "struct builder reset child failure leaves parent unchanged" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(1);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(_: *anyopaque) ChildBuilderError!void {
                    return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendValid();

    var struct_ref = try builder.finishFromChildren();
    defer struct_ref.release();

    try std.testing.expectError(ChildBuilderError.ResetFailed, builder.reset());
    try std.testing.expectEqual(@as(usize, 1), builder.len);
    try std.testing.expectEqual(BuilderState.finished, builder.state);
}

test "struct builder clear child failure leaves parent unchanged" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(1);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(_: *anyopaque) ChildBuilderError!void {
                    return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendNull();

    var struct_ref = try builder.finishFromChildren();
    defer struct_ref.release();

    try std.testing.expect(builder.validity != null);
    try std.testing.expectError(ChildBuilderError.ClearFailed, builder.clear());
    try std.testing.expectEqual(@as(usize, 1), builder.len);
    try std.testing.expect(builder.validity != null);
    try std.testing.expectEqual(BuilderState.finished, builder.state);
}

test "struct builder finishFromChildrenReset does not partially recycle on child reset failure" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(9);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(_: *anyopaque) ChildBuilderError!void {
                    return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.clear() catch return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendValid();

    try std.testing.expectError(ChildBuilderError.ResetFailed, builder.finishFromChildrenReset());
    try std.testing.expectEqual(@as(usize, 1), builder.len);
    try std.testing.expectEqual(BuilderState.finished, builder.state);
}

test "struct builder finishFromChildrenClear does not partially recycle on child clear failure" {
    const allocator = std.testing.allocator;
    const value_type = DataType{ .int32 = {} };
    const fields = &[_]Field{
        .{ .name = "a", .data_type = &value_type, .nullable = true },
    };

    const IntBuilder = @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} });

    var child_builder = try IntBuilder.init(allocator, 1);
    defer child_builder.deinit();
    try child_builder.append(9);

    var builder = StructBuilder.initWithChildren(allocator, fields, &[_]ChildBuilder{
        .{
            .ctx = &child_builder,
            .finishFn = struct {
                fn finish(ctx: *anyopaque) !ArrayRef {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.finish();
                }
            }.finish,
            .lenFn = struct {
                fn len(ctx: *anyopaque) usize {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    return ptr.len;
                }
            }.len,
            .resetFn = struct {
                fn reset(ctx: *anyopaque) ChildBuilderError!void {
                    const ptr: *IntBuilder = @ptrCast(@alignCast(ctx));
                    ptr.reset() catch return ChildBuilderError.ResetFailed;
                }
            }.reset,
            .clearFn = struct {
                fn clear(_: *anyopaque) ChildBuilderError!void {
                    return ChildBuilderError.ClearFailed;
                }
            }.clear,
        },
    });
    defer builder.deinit();
    try builder.appendNull();

    try std.testing.expectError(ChildBuilderError.ClearFailed, builder.finishFromChildrenClear());
    try std.testing.expectEqual(@as(usize, 1), builder.len);
    try std.testing.expect(builder.validity != null);
    try std.testing.expectEqual(BuilderState.finished, builder.state);
}
