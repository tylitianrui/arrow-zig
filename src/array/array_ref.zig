const std = @import("std");
const array_data = @import("array_data.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");

pub const ArrayData = array_data.ArrayData;
pub const SharedBuffer = buffer.SharedBuffer;

const empty_buffers: [0]SharedBuffer = .{};
const empty_children: [0]ArrayRef = .{};

fn isOwnedLayout(layout: ArrayData) bool {
    if (layout.buffers.len == 0 and @intFromPtr(layout.buffers.ptr) == @intFromPtr(empty_buffers[0..].ptr)) return false;
    if (layout.children.len == 0 and @intFromPtr(layout.children.ptr) == @intFromPtr(empty_children[0..].ptr)) return false;
    return true;
}

const ArrayNode = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    data: ArrayData,
};

pub const ArrayRef = struct {
    node: *ArrayNode,

    pub fn retain(self: ArrayRef) ArrayRef {
        _ = self.node.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn release(self: *ArrayRef) void {
        if (self.node.ref_count.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.node.allocator;

        for (self.node.data.buffers) |buf| {
            var owned = buf;
            owned.release();
        }
        allocator.free(self.node.data.buffers);

        for (self.node.data.children) |child| {
            var owned = child;
            owned.release();
        }
        allocator.free(self.node.data.children);

        if (self.node.data.dictionary) |dict| {
            var owned = dict;
            owned.release();
        }

        allocator.destroy(self.node);
    }

    pub fn data(self: ArrayRef) *const ArrayData {
        return &self.node.data;
    }

    pub fn slice(self: ArrayRef, offset: usize, length: usize) !ArrayRef {
        var sliced = self.node.data.slice(offset, length);
        const allocator = self.node.allocator;

        const buffers = try allocator.alloc(SharedBuffer, sliced.buffers.len);
        errdefer {
            for (buffers) |buf| {
                var owned = buf;
                owned.release();
            }
            allocator.free(buffers);
        }
        for (sliced.buffers, 0..) |buf, i| {
            buffers[i] = buf.retain();
        }

        const children = try allocator.alloc(ArrayRef, sliced.children.len);
        var child_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < child_count) : (i += 1) {
                children[i].release();
            }
            allocator.free(children);
        }

        var dict_ref: ?ArrayRef = null;
        if (sliced.dictionary) |dict| dict_ref = dict.retain();

        switch (sliced.data_type) {
            .list, .large_list, .map => {
                const total_offset = self.node.data.offset + offset;
                const total_end = total_offset + length;
                const offsets_buf = self.node.data.buffers[1];
                const child = self.node.data.children[0];
                const start: usize = if (sliced.data_type == .large_list)
                    @intCast(offsets_buf.typedSlice(i64)[total_offset])
                else
                    @intCast(offsets_buf.typedSlice(i32)[total_offset]);
                const end: usize = if (sliced.data_type == .large_list)
                    @intCast(offsets_buf.typedSlice(i64)[total_end])
                else
                    @intCast(offsets_buf.typedSlice(i32)[total_end]);
                children[0] = try child.slice(start, end - start);
                child_count = 1;
            },
            .struct_ => {
                var i: usize = 0;
                while (i < sliced.children.len) : (i += 1) {
                    children[i] = try sliced.children[i].slice(offset, length);
                    child_count += 1;
                }
            },
            .dictionary => {
                var i: usize = 0;
                while (i < sliced.children.len) : (i += 1) {
                    children[i] = sliced.children[i].retain();
                    child_count += 1;
                }
            },
            else => {
                var i: usize = 0;
                while (i < sliced.children.len) : (i += 1) {
                    children[i] = sliced.children[i].retain();
                    child_count += 1;
                }
            },
        }

        sliced.buffers = buffers;
        sliced.children = children;
        sliced.dictionary = dict_ref;

        return ArrayRef.fromOwned(allocator, sliced);
    }

    /// Create an ArrayRef that takes ownership of the ArrayData layout.
    ///
    /// Requirements:
    /// - buffers/children/dictionary must be allocator-owned and releasable.
    /// - Use `fromBorrowed` if the layout borrows or uses stack/static slices.
    /// - This is an unsafe entry point for callers who already normalized slices.
    pub fn fromOwnedUnsafe(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        std.debug.assert(isOwnedLayout(layout));

        const node = try allocator.create(ArrayNode);
        node.* = .{
            .allocator = allocator,
            .ref_count = std.atomic.Value(u32).init(1),
            .data = layout,
        };
        return .{ .node = node };
    }

    /// Create an ArrayRef that owns the layout and normalizes empty slices.
    pub fn fromOwned(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        var owned = layout;
        if (owned.buffers.len == 0 and @intFromPtr(owned.buffers.ptr) == @intFromPtr(empty_buffers[0..].ptr)) {
            owned.buffers = try allocator.alloc(SharedBuffer, 0);
        }
        if (owned.children.len == 0 and @intFromPtr(owned.children.ptr) == @intFromPtr(empty_children[0..].ptr)) {
            owned.children = try allocator.alloc(ArrayRef, 0);
        }
        std.debug.assert(owned.buffers.len != 0 or @intFromPtr(owned.buffers.ptr) != @intFromPtr(empty_buffers[0..].ptr));
        std.debug.assert(owned.children.len != 0 or @intFromPtr(owned.children.ptr) != @intFromPtr(empty_children[0..].ptr));
        return fromOwnedUnsafe(allocator, owned);
    }

    /// Create an ArrayRef by retaining shared buffers and child refs.
    pub fn fromBorrowed(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        const buffers = try allocator.alloc(SharedBuffer, layout.buffers.len);
        for (layout.buffers, 0..) |buf, i| {
            buffers[i] = buf.retain();
        }

        const children = try allocator.alloc(ArrayRef, layout.children.len);
        for (layout.children, 0..) |child, i| {
            children[i] = child.retain();
        }

        var dict_ref: ?ArrayRef = null;
        if (layout.dictionary) |dict| dict_ref = dict.retain();

        var owned = layout;
        owned.buffers = buffers;
        owned.children = children;
        owned.dictionary = dict_ref;

        return ArrayRef.fromOwned(allocator, owned);
    }
};

test "array ref release handles empty slices" {
    const allocator = std.testing.allocator;
    const dtype = array_data.DataType{ .int32 = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = &[_]SharedBuffer{},
        .children = &[_]ArrayRef{},
    };

    var array_ref = try ArrayRef.fromOwned(allocator, layout);
    array_ref.release();
}

test "array ref owned layout check rejects static empties" {
    const dtype = array_data.DataType{ .int32 = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = empty_buffers[0..],
        .children = empty_children[0..],
    };

    try std.testing.expect(!isOwnedLayout(layout));
}

test "array ref owned layout check accepts allocator empties" {
    const allocator = std.testing.allocator;
    const dtype = array_data.DataType{ .int32 = {} };
    const buffers = try allocator.alloc(SharedBuffer, 0);
    const children = try allocator.alloc(ArrayRef, 0);
    defer allocator.free(buffers);
    defer allocator.free(children);

    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = buffers,
        .children = children,
    };

    try std.testing.expect(isOwnedLayout(layout));
}

test "array ref fromOwnedUnsafe requires owned layout" {
    const dtype = array_data.DataType{ .int32 = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = empty_buffers[0..],
        .children = empty_children[0..],
    };

    // In debug builds, fromOwnedUnsafe asserts on non-owned layouts.
    try std.testing.expect(!isOwnedLayout(layout));
}

test "array ref slice retains buffers" {
    const allocator = std.testing.allocator;

    var owned = try buffer.OwnedBuffer.init(allocator, 4);
    defer owned.deinit();
    @memcpy(owned.data[0..4], "data");
    var shared = try owned.toShared(4);
    defer shared.release();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = shared.retain();

    const dtype = array_data.DataType{ .binary = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 1,
        .buffers = buffers,
    };

    var array_ref = try ArrayRef.fromOwned(allocator, layout);
    defer array_ref.release();

    var sliced = try array_ref.slice(0, 1);
    defer sliced.release();

    const view = sliced.data();
    try std.testing.expectEqualStrings("data", view.buffers[0].data);
}

test "array ref fromBorrowed retains shared buffers" {
    const allocator = std.testing.allocator;

    var owned = try buffer.OwnedBuffer.init(allocator, 3);
    defer owned.deinit();
    @memcpy(owned.data[0..3], "abc");
    var shared = try owned.toShared(3);
    defer shared.release();

    const buffers = &[_]SharedBuffer{shared};
    const dtype = array_data.DataType{ .binary = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 1,
        .buffers = buffers,
    };

    var array_ref = try ArrayRef.fromBorrowed(allocator, layout);
    array_ref.release();

    try std.testing.expectEqualStrings("abc", shared.data[0..3]);
    shared.release();
}

test "array ref releases children and dictionary" {
    const allocator = std.testing.allocator;
    const dtype = array_data.DataType{ .binary = {} };

    var owned = try buffer.OwnedBuffer.init(allocator, 2);
    defer owned.deinit();
    @memcpy(owned.data[0..2], "ok");
    var shared = try owned.toShared(2);
    defer shared.release();

    const child_buffers = try allocator.alloc(SharedBuffer, 1);
    child_buffers[0] = shared.retain();
    const child_layout = ArrayData{ .data_type = dtype, .length = 1, .buffers = child_buffers };
    var child_ref = try ArrayRef.fromOwned(allocator, child_layout);
    var child_hold = child_ref.retain();

    const dict_buffers = try allocator.alloc(SharedBuffer, 1);
    dict_buffers[0] = shared.retain();
    const dict_layout = ArrayData{ .data_type = dtype, .length = 1, .buffers = dict_buffers };
    var dict_ref = try ArrayRef.fromOwned(allocator, dict_layout);
    var dict_hold = dict_ref.retain();

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref;
    const parent_buffers = try allocator.alloc(SharedBuffer, 0);
    const parent_layout = ArrayData{
        .data_type = array_data.DataType{ .null = {} },
        .length = 1,
        .buffers = parent_buffers,
        .children = children,
        .dictionary = dict_ref,
    };

    var parent = try ArrayRef.fromOwnedUnsafe(allocator, parent_layout);
    parent.release();

    child_hold.release();
    dict_hold.release();
}

test "array ref slice is shallow for list_view" {
    const allocator = std.testing.allocator;
    const value_type = array_data.DataType{ .int32 = {} };
    const field = datatype.Field{ .name = "item", .data_type = &value_type, .nullable = true };
    const list_view_type = array_data.DataType{ .list_view = .{ .value_field = field } };

    var child_values: [5 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4, 5 }));
    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 5,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const offsets = [_]i32{ 0, 2, 4 };
    const sizes = [_]i32{ 2, 2, 1 };
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    var size_bytes: [sizes.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    @memcpy(size_bytes[0..], std.mem.sliceAsBytes(sizes[0..]));

    const buffers = try allocator.alloc(SharedBuffer, 3);
    buffers[0] = SharedBuffer.empty;
    buffers[1] = SharedBuffer.fromSlice(offset_bytes[0..]);
    buffers[2] = SharedBuffer.fromSlice(size_bytes[0..]);

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    const layout = ArrayData{
        .data_type = list_view_type,
        .length = 3,
        .buffers = buffers,
        .children = children,
    };
    var array_ref = try ArrayRef.fromOwnedUnsafe(allocator, layout);
    defer array_ref.release();

    var sliced = try array_ref.slice(1, 1);
    defer sliced.release();

    const child_data = sliced.data().children[0].data();
    try std.testing.expectEqual(@as(usize, 5), child_data.length);
    try std.testing.expectEqual(@as(usize, 0), child_data.offset);
}
