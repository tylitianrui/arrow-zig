const std = @import("std");
const array_data = @import("array_data.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");

// Shared ownership handle for ArrayData with retain/release semantics.

pub const ArrayData = array_data.ArrayData;
pub const SharedBuffer = buffer.SharedBuffer;

const ArrayNode = struct {
    // Allocator used to free this node and all owned containers.
    allocator: std.mem.Allocator,
    // Shared reference count for ArrayRef clones.
    ref_count: std.atomic.Value(u32),
    // Owned array payload (buffers/children/dictionary containers).
    data: ArrayData,
};

pub const ArrayRef = struct {
    // Pointer to shared array node.
    node: *ArrayNode,

    /// Increment reference count and return another handle to the same array.
    pub fn retain(self: ArrayRef) ArrayRef {
        _ = self.node.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    /// Release one handle; destroy node when last reference is dropped.
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

    /// Borrow immutable access to the underlying array layout.
    pub fn data(self: ArrayRef) *const ArrayData {
        return &self.node.data;
    }

    /// Create a sliced ArrayRef while preserving ownership correctness.
    ///
    /// The slice semantics are type-aware:
    /// - list/map keep child in parent coordinate space (offset-driven view)
    /// - struct slices children to keep element alignment
    /// - dictionary keeps child/dictionary shared
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
        errdefer if (dict_ref) |*owned| owned.release();

        switch (sliced.data_type) {
            .list, .large_list, .map => {
                // Keep child shared: offsets buffer defines logical subranges.
                // Child is not physically re-sliced for list/map shallow slices.

                const child = self.node.data.children[0];
                children[0] = child.retain();
                child_count = 1;
            },
            .struct_ => {
                // Struct requires child-wise slicing to keep row alignment.
                const total_offset = self.node.data.offset + offset;
                var i: usize = 0;
                while (i < sliced.children.len) : (i += 1) {
                    children[i] = try sliced.children[i].slice(total_offset, length);
                    child_count += 1;
                }
            },
            .dictionary => {
                // Dictionary arrays keep the same dictionary and shared children.
                var i: usize = 0;
                while (i < sliced.children.len) : (i += 1) {
                    children[i] = sliced.children[i].retain();
                    child_count += 1;
                }
            },
            else => {
                // Default shallow slice for primitive and most nested representations.
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

        return ArrayRef.fromOwnedUnsafe(allocator, sliced);
    }

    /// Create an ArrayRef by wrapping an owned ArrayData layout.
    ///
    /// This is a minimal wrapper. Callers must ensure the layout is safe to
    /// release with the provided allocator.
    pub fn fromOwnedUnsafe(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
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

        const buffers = try allocator.alloc(SharedBuffer, owned.buffers.len);
        errdefer allocator.free(buffers);
        @memcpy(buffers, owned.buffers);

        const children = try allocator.alloc(ArrayRef, owned.children.len);
        errdefer allocator.free(children);
        @memcpy(children, owned.children);

        owned.buffers = buffers;
        owned.children = children;
        return fromOwnedUnsafe(allocator, owned);
    }

    /// Create an ArrayRef by retaining shared buffers and child refs.
    pub fn fromBorrowed(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        const buffers = try allocator.alloc(SharedBuffer, layout.buffers.len);
        var buf_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < buf_count) : (i += 1) {
                var owned = buffers[i];
                owned.release();
            }
            allocator.free(buffers);
        }
        for (layout.buffers, 0..) |buf, i| {
            buffers[i] = buf.retain();
            buf_count += 1;
        }

        const children = try allocator.alloc(ArrayRef, layout.children.len);
        var child_count: usize = 0;
        errdefer {
            var i: usize = 0;
            while (i < child_count) : (i += 1) {
                children[i].release();
            }
            allocator.free(children);
        }
        for (layout.children, 0..) |child, i| {
            children[i] = child.retain();
            child_count += 1;
        }

        var dict_ref: ?ArrayRef = null;
        if (layout.dictionary) |dict| dict_ref = dict.retain();
        errdefer if (dict_ref) |*owned| owned.release();

        var owned = layout;
        owned.buffers = buffers;
        owned.children = children;
        owned.dictionary = dict_ref;

        return ArrayRef.fromOwnedUnsafe(allocator, owned);
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

    var array_ref = try ArrayRef.fromOwnedUnsafe(allocator, layout);
    array_ref.release();
}

test "array ref fromOwned handles static empty containers safely" {
    const allocator = std.testing.allocator;
    const dtype = array_data.DataType{ .int32 = {} };
    const static_buffers = &[_]SharedBuffer{};
    const static_children = &[_]ArrayRef{};
    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = static_buffers,
        .children = static_children,
    };

    var array_ref = try ArrayRef.fromOwned(allocator, layout);
    defer array_ref.release();

    const owned = array_ref.data();
    try std.testing.expect(owned.buffers.len == 0);
    try std.testing.expect(owned.children.len == 0);
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

    var array_ref = try ArrayRef.fromOwnedUnsafe(allocator, layout);
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
    var child_ref = try ArrayRef.fromOwnedUnsafe(allocator, child_layout);
    var child_hold = child_ref.retain();

    const dict_buffers = try allocator.alloc(SharedBuffer, 1);
    dict_buffers[0] = shared.retain();
    const dict_layout = ArrayData{ .data_type = dtype, .length = 1, .buffers = dict_buffers };
    var dict_ref = try ArrayRef.fromOwnedUnsafe(allocator, dict_layout);
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

test "array ref nested retain/release stress stays balanced" {
    const allocator = std.testing.allocator;
    const value_type = array_data.DataType{ .int32 = {} };
    const field = datatype.Field{ .name = "item", .data_type = &value_type, .nullable = true };
    const list_type = array_data.DataType{ .list = .{ .value_field = field } };

    var child_values: [6 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(child_values[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3, 4, 5, 6 }));
    const child_layout = ArrayData{
        .data_type = value_type,
        .length = 6,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_values[0..]) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const offsets = [_]i32{ 0, 2, 4, 6 };
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));

    const buffers = try allocator.alloc(SharedBuffer, 2);
    buffers[0] = SharedBuffer.empty;
    buffers[1] = SharedBuffer.fromSlice(offset_bytes[0..]);

    const children = try allocator.alloc(ArrayRef, 1);
    children[0] = child_ref.retain();

    var parent = try ArrayRef.fromOwnedUnsafe(allocator, .{
        .data_type = list_type,
        .length = 3,
        .buffers = buffers,
        .children = children,
    });
    defer parent.release();

    var i: usize = 0;
    while (i < 512) : (i += 1) {
        var sliced = try parent.slice(1, 2);
        var held = sliced.retain();
        held.release();

        var nested = try sliced.slice(0, 1);
        nested.release();
        sliced.release();
    }
}
