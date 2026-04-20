const std = @import("std");
const datatype = @import("datatype.zig");

// Lightweight schema container used by arrays and record batches.

pub const Endianness = datatype.Endianness;
pub const Field = datatype.Field;
pub const KeyValue = datatype.KeyValue;
pub const DataType = datatype.DataType;

pub const Schema = struct {
    fields: []const Field,
    endianness: Endianness = .little,
    metadata: ?[]const KeyValue = null,

    pub fn fieldIndex(self: Schema, name: []const u8) ?usize {
        for (self.fields, 0..) |field, index| {
            if (std.mem.eql(u8, field.name, name)) return index;
        }
        return null;
    }

    pub fn eql(self: Schema, other: Schema) bool {
        if (self.endianness != other.endianness) return false;
        if (!schemaMetadataEql(self.metadata, other.metadata)) return false;
        if (self.fields.len != other.fields.len) return false;
        for (self.fields, other.fields) |lhs, rhs| {
            if (!lhs.eql(rhs)) return false;
        }
        return true;
    }
};

// ──────────────────────────────────────────────────────────────────────────────
// SchemaRef — shared ownership handle for Schema (mirrors ArrayRef/ArrayNode).
// ──────────────────────────────────────────────────────────────────────────────

// Internal control block. Not exported; callers use SchemaRef exclusively.
const SchemaNode = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    arena: std.heap.ArenaAllocator,
    field_index: std.StringHashMapUnmanaged(usize),
    schema: Schema,
};

pub const SchemaRef = struct {
    node: *SchemaNode,

    /// Increment reference count and return another handle to the same schema.
    pub fn retain(self: SchemaRef) SchemaRef {
        _ = self.node.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    /// Release one handle; frees arena and node when last reference drops.
    pub fn release(self: *SchemaRef) void {
        if (self.node.ref_count.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.node.allocator;
        self.node.field_index.deinit(allocator);
        self.node.arena.deinit();
        allocator.destroy(self.node);
    }

    /// Borrow immutable access to the underlying schema.
    pub fn schema(self: SchemaRef) *const Schema {
        return &self.node.schema;
    }

    pub fn fieldIndex(self: SchemaRef, name: []const u8) ?usize {
        return self.node.field_index.get(name);
    }

    pub fn project(self: SchemaRef, allocator: std.mem.Allocator, indices: []const usize) !SchemaRef {
        const source = self.schema().*;
        const projected_fields = try allocator.alloc(Field, indices.len);
        defer allocator.free(projected_fields);

        for (indices, 0..) |index, out_i| {
            if (index >= source.fields.len) return error.IndexOutOfBounds;
            projected_fields[out_i] = source.fields[index];
        }

        return SchemaRef.fromBorrowed(allocator, .{
            .fields = projected_fields,
            .endianness = source.endianness,
            .metadata = source.metadata,
        });
    }

    /// Deep-clone a borrowed (stack/static) Schema into an owned SchemaRef.
    /// All field names, data types, and metadata are duplicated into an arena.
    pub fn fromBorrowed(allocator: std.mem.Allocator, s: Schema) !SchemaRef {
        const node = try allocator.create(SchemaNode);
        errdefer allocator.destroy(node);
        node.allocator = allocator;
        node.ref_count = std.atomic.Value(u32).init(1);
        node.field_index = .{};
        errdefer node.field_index.deinit(allocator);
        node.arena = std.heap.ArenaAllocator.init(allocator);
        errdefer node.arena.deinit();
        const a = node.arena.allocator();
        node.schema = try cloneSchema(a, s);
        try initFieldIndex(node);
        return SchemaRef{ .node = node };
    }

    /// Wrap an already-owned arena + schema into a SchemaRef (takes arena ownership).
    /// Used by IPC readers that already built the schema inside an arena.
    pub fn fromArena(allocator: std.mem.Allocator, arena: std.heap.ArenaAllocator, s: Schema) !SchemaRef {
        const node = try allocator.create(SchemaNode);
        errdefer allocator.destroy(node);
        node.allocator = allocator;
        node.ref_count = std.atomic.Value(u32).init(1);
        node.field_index = .{};
        errdefer node.field_index.deinit(allocator);
        node.arena = arena;
        errdefer node.arena.deinit();
        node.schema = s;
        try initFieldIndex(node);
        return SchemaRef{ .node = node };
    }
};

// ── deep clone helpers (arena-allocated) ─────────────────────────────────────

fn cloneSchema(a: std.mem.Allocator, s: Schema) !Schema {
    const fields = try a.alloc(Field, s.fields.len);
    for (s.fields, 0..) |f, i| fields[i] = try cloneField(a, f);
    return Schema{
        .fields = fields,
        .endianness = s.endianness,
        .metadata = try cloneMetadata(a, s.metadata),
    };
}

fn cloneField(a: std.mem.Allocator, f: Field) !Field {
    const name = try a.dupe(u8, f.name);
    const dt_ptr = try a.create(DataType);
    dt_ptr.* = try cloneDataType(a, f.data_type.*);
    const meta = try cloneMetadata(a, f.metadata);
    return Field{ .name = name, .data_type = dt_ptr, .nullable = f.nullable, .metadata = meta };
}

fn cloneDataType(a: std.mem.Allocator, dt: DataType) error{OutOfMemory}!DataType {
    return switch (dt) {
        .list => |lt| DataType{ .list = .{ .value_field = try cloneField(a, lt.value_field) } },
        .large_list => |lt| DataType{ .large_list = .{ .value_field = try cloneField(a, lt.value_field) } },
        .list_view => |lt| DataType{ .list_view = .{ .value_field = try cloneField(a, lt.value_field) } },
        .large_list_view => |lt| DataType{ .large_list_view = .{ .value_field = try cloneField(a, lt.value_field) } },
        .fixed_size_list => |fl| DataType{ .fixed_size_list = .{
            .value_field = try cloneField(a, fl.value_field),
            .list_size = fl.list_size,
        } },
        .struct_ => |st| blk: {
            const fields = try a.alloc(Field, st.fields.len);
            for (st.fields, 0..) |f, i| fields[i] = try cloneField(a, f);
            break :blk DataType{ .struct_ = .{ .fields = fields } };
        },
        .map => |mt| blk: {
            const kf = try cloneField(a, mt.key_field);
            const vf = try cloneField(a, mt.item_field);
            var entries_type: ?*const DataType = null;
            if (mt.entries_type) |et| {
                const ep = try a.create(DataType);
                ep.* = try cloneDataType(a, et.*);
                entries_type = ep;
            }
            break :blk DataType{ .map = .{
                .key_field = kf,
                .item_field = vf,
                .keys_sorted = mt.keys_sorted,
                .entries_type = entries_type,
            } };
        },
        .sparse_union => |ut| DataType{ .sparse_union = try cloneUnionType(a, ut) },
        .dense_union => |ut| DataType{ .dense_union = try cloneUnionType(a, ut) },
        .dictionary => |dict| blk: {
            const vp = try a.create(DataType);
            vp.* = try cloneDataType(a, dict.value_type.*);
            break :blk DataType{ .dictionary = .{
                .id = dict.id,
                .index_type = dict.index_type,
                .value_type = vp,
                .ordered = dict.ordered,
            } };
        },
        .run_end_encoded => |ree| blk: {
            const vp = try a.create(DataType);
            vp.* = try cloneDataType(a, ree.value_type.*);
            break :blk DataType{ .run_end_encoded = .{
                .run_end_type = ree.run_end_type,
                .value_type = vp,
            } };
        },
        .timestamp => |ts| DataType{ .timestamp = .{
            .unit = ts.unit,
            .timezone = if (ts.timezone) |tz| try a.dupe(u8, tz) else null,
        } },
        .extension => |ext| blk: {
            const sp = try a.create(DataType);
            sp.* = try cloneDataType(a, ext.storage_type.*);
            break :blk DataType{ .extension = .{
                .name = try a.dupe(u8, ext.name),
                .storage_type = sp,
                .metadata = if (ext.metadata) |m| try a.dupe(u8, m) else null,
            } };
        },
        // All other variants carry no heap pointers — copy by value.
        else => dt,
    };
}

fn cloneUnionType(a: std.mem.Allocator, ut: datatype.UnionType) !datatype.UnionType {
    const fields = try a.alloc(Field, ut.fields.len);
    for (ut.fields, 0..) |f, i| fields[i] = try cloneField(a, f);
    const type_ids = try a.dupe(i8, ut.type_ids);
    return datatype.UnionType{ .type_ids = type_ids, .fields = fields, .mode = ut.mode };
}

fn cloneMetadata(a: std.mem.Allocator, meta: ?[]const KeyValue) !?[]const KeyValue {
    const m = meta orelse return null;
    const out = try a.alloc(KeyValue, m.len);
    for (m, 0..) |kv, i| out[i] = .{
        .key = try a.dupe(u8, kv.key),
        .value = try a.dupe(u8, kv.value),
    };
    return out;
}

fn initFieldIndex(node: *SchemaNode) !void {
    const capacity = std.math.cast(u32, node.schema.fields.len) orelse return error.OutOfMemory;
    try node.field_index.ensureTotalCapacity(node.allocator, capacity);
    for (node.schema.fields, 0..) |field, index| {
        const gop = try node.field_index.getOrPut(node.allocator, field.name);
        if (!gop.found_existing) gop.value_ptr.* = index;
    }
}

fn schemaMetadataEql(lhs: ?[]const KeyValue, rhs: ?[]const KeyValue) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    const l = lhs.?;
    const r = rhs.?;
    if (l.len != r.len) return false;
    for (l, r) |lv, rv| {
        if (!std.mem.eql(u8, lv.key, rv.key)) return false;
        if (!std.mem.eql(u8, lv.value, rv.value)) return false;
    }
    return true;
}

test "schema holds fields and defaults" {
    const name_type = datatype.DataType{ .string = {} };
    const id_type = datatype.DataType{ .int64 = {} };

    const fields = [_]Field{
        Field{ .name = "name", .data_type = &name_type, .nullable = false },
        Field{ .name = "id", .data_type = &id_type, .nullable = true },
    };

    const schema = Schema{ .fields = fields[0..] };

    try std.testing.expectEqual(@as(usize, 2), schema.fields.len);
    try std.testing.expectEqual(datatype.Endianness.little, schema.endianness);
}

test "schema supports explicit endianness" {
    const id_type = datatype.DataType{ .int64 = {} };
    const fields = [_]Field{
        Field{ .name = "id", .data_type = &id_type, .nullable = false },
    };

    const schema = Schema{
        .fields = fields[0..],
        .endianness = .little,
    };

    try std.testing.expectEqual(datatype.Endianness.little, schema.endianness);
    try std.testing.expectEqual(@as(usize, 1), schema.fields.len);
}

test "schema keeps metadata reference" {
    const id_type = datatype.DataType{ .int64 = {} };
    const fields = [_]Field{
        Field{ .name = "id", .data_type = &id_type, .nullable = false },
    };

    const metadata = [_]KeyValue{
        .{ .key = "owner", .value = "analytics" },
        .{ .key = "version", .value = "1" },
    };

    const schema = Schema{
        .fields = fields[0..],
        .metadata = metadata[0..],
    };

    try std.testing.expect(schema.metadata != null);
    const md = schema.metadata.?;
    try std.testing.expectEqual(@as(usize, 2), md.len);
    try std.testing.expectEqualStrings("owner", md[0].key);
    try std.testing.expectEqualStrings("analytics", md[0].value);
    try std.testing.expectEqualStrings("version", md[1].key);
    try std.testing.expectEqualStrings("1", md[1].value);
}

test "schema ref retain and release" {
    const allocator = std.testing.allocator;
    const id_type = datatype.DataType{ .int32 = {} };
    const fields = [_]Field{.{ .name = "id", .data_type = &id_type, .nullable = false }};
    const s = Schema{ .fields = fields[0..] };

    var ref = try SchemaRef.fromBorrowed(allocator, s);
    var ref2 = ref.retain();
    ref2.release();
    ref.release();
    // No leak if memory is freed correctly (detected by testing allocator).
}

test "schema ref fromBorrowed deep clones fields" {
    const allocator = std.testing.allocator;
    const id_type = datatype.DataType{ .int32 = {} };
    const name_type = datatype.DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const s = Schema{ .fields = fields[0..] };

    var ref = try SchemaRef.fromBorrowed(allocator, s);
    defer ref.release();

    const sc = ref.schema();
    try std.testing.expectEqual(@as(usize, 2), sc.fields.len);
    try std.testing.expectEqualStrings("id", sc.fields[0].name);
    try std.testing.expect(sc.fields[0].data_type.* == .int32);
    try std.testing.expectEqualStrings("name", sc.fields[1].name);
    try std.testing.expect(sc.fields[1].data_type.* == .string);
}

test "schema ref fieldIndex provides hashed lookup and keeps first duplicate" {
    const allocator = std.testing.allocator;
    const id_type = datatype.DataType{ .int32 = {} };
    const name_type = datatype.DataType{ .string = {} };
    const score_type = datatype.DataType{ .double = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
        .{ .name = "name", .data_type = &score_type, .nullable = true },
    };
    const s = Schema{ .fields = fields[0..] };

    var ref = try SchemaRef.fromBorrowed(allocator, s);
    defer ref.release();

    try std.testing.expectEqual(@as(?usize, 0), ref.fieldIndex("id"));
    try std.testing.expectEqual(@as(?usize, 1), ref.fieldIndex("name"));
    try std.testing.expectEqual(@as(?usize, null), ref.fieldIndex("missing"));
}

test "schema fieldIndex finds and misses fields" {
    const id_type = datatype.DataType{ .int64 = {} };
    const name_type = datatype.DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const s = Schema{ .fields = fields[0..] };

    try std.testing.expectEqual(@as(?usize, 0), s.fieldIndex("id"));
    try std.testing.expectEqual(@as(?usize, 1), s.fieldIndex("name"));
    try std.testing.expectEqual(@as(?usize, null), s.fieldIndex("missing"));
}

test "schema eql compares endianness fields and metadata" {
    const id_type = datatype.DataType{ .int32 = {} };
    const fields_a = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const fields_b = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const metadata_a = [_]KeyValue{.{ .key = "k", .value = "v" }};
    const metadata_b = [_]KeyValue{.{ .key = "k", .value = "v" }};

    const lhs = Schema{
        .fields = fields_a[0..],
        .endianness = .little,
        .metadata = metadata_a[0..],
    };
    const rhs = Schema{
        .fields = fields_b[0..],
        .endianness = .little,
        .metadata = metadata_b[0..],
    };
    const different_endian = Schema{
        .fields = fields_b[0..],
        .endianness = .big,
        .metadata = metadata_b[0..],
    };

    try std.testing.expect(lhs.eql(rhs));
    try std.testing.expect(!lhs.eql(different_endian));
}

test "schema ref project selects ordered fields and deep clones" {
    const allocator = std.testing.allocator;
    const id_type = datatype.DataType{ .int64 = {} };
    const name_type = datatype.DataType{ .string = {} };
    const score_type = datatype.DataType{ .double = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
        .{ .name = "score", .data_type = &score_type, .nullable = true },
    };
    const metadata = [_]KeyValue{
        .{ .key = "owner", .value = "analytics" },
    };
    const s = Schema{ .fields = fields[0..], .metadata = metadata[0..] };

    var schema_ref = try SchemaRef.fromBorrowed(allocator, s);
    defer schema_ref.release();

    var projected = try schema_ref.project(allocator, &[_]usize{ 2, 0 });
    defer projected.release();

    const out = projected.schema();
    try std.testing.expectEqual(@as(usize, 2), out.fields.len);
    try std.testing.expectEqualStrings("score", out.fields[0].name);
    try std.testing.expectEqualStrings("id", out.fields[1].name);
    try std.testing.expect(out.metadata != null);
    try std.testing.expectEqual(@as(usize, 1), out.metadata.?.len);
    try std.testing.expect(out.fields[0].data_type != schema_ref.schema().fields[2].data_type);

    try std.testing.expectError(error.IndexOutOfBounds, schema_ref.project(allocator, &[_]usize{3}));
}
