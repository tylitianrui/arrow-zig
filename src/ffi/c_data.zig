const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const array_data_mod = @import("../array/array_data.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const bitmap = @import("../bitmap.zig");

pub const DataType = datatype.DataType;
pub const Field = datatype.Field;
pub const Schema = schema_mod.Schema;
pub const ArrayData = array_data_mod.ArrayData;
pub const ArrayRef = array_ref_mod.ArrayRef;
pub const SharedBuffer = array_data_mod.SharedBuffer;
pub const Endianness = datatype.Endianness;
pub const TimeUnit = datatype.TimeUnit;
pub const IntType = datatype.IntType;

pub const ArrowSchema = extern struct {
    format: [*c]const u8,
    name: [*c]const u8,
    metadata: [*c]const u8,
    flags: i64,
    n_children: i64,
    children: [*c]?*ArrowSchema,
    dictionary: ?*ArrowSchema,
    release: ?*const fn (?*ArrowSchema) callconv(.c) void,
    private_data: ?*anyopaque,
};

pub const ArrowArray = extern struct {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: [*c]?*const anyopaque,
    children: [*c]?*ArrowArray,
    dictionary: ?*ArrowArray,
    release: ?*const fn (?*ArrowArray) callconv(.c) void,
    private_data: ?*anyopaque,
};

pub const OwnedSchema = struct {
    arena: std.heap.ArenaAllocator,
    schema: Schema,

    pub fn deinit(self: *OwnedSchema) void {
        self.arena.deinit();
    }
};

pub const Error = error{
    UnsupportedType,
    InvalidFormat,
    TopLevelSchemaMustBeStruct,
    InvalidChildren,
    InvalidBufferCount,
    InvalidLength,
    InvalidOffset,
    InvalidNullCount,
    MissingDictionary,
    Released,
    OutOfMemory,
};

const ARROW_FLAG_DICTIONARY_ORDERED: i64 = 1;
const ARROW_FLAG_NULLABLE: i64 = 2;
const ARROW_FLAG_MAP_KEYS_SORTED: i64 = 4;

const ExportedSchemaPrivate = struct {
    allocator: std.mem.Allocator,
    format_z: [:0]u8,
    name_z: ?[:0]u8,
    children_storage: []ArrowSchema,
    children_ptrs: []?*ArrowSchema,
    dict_storage: ?*ArrowSchema,
};

const ExportedArrayPrivate = struct {
    allocator: std.mem.Allocator,
    retained_ref: ArrayRef,
    buffers_ptrs: []?*const anyopaque,
    children_storage: []ArrowArray,
    children_ptrs: []?*ArrowArray,
    dict_storage: ?*ArrowArray,
};

const ImportedArrayOwner = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    owned_array: ArrowArray,
};

fn ownerRetain(ctx: ?*anyopaque) void {
    const owner: *ImportedArrayOwner = @ptrCast(@alignCast(ctx.?));
    _ = owner.ref_count.fetchAdd(1, .monotonic);
}

fn ownerRelease(ctx: ?*anyopaque) void {
    const owner: *ImportedArrayOwner = @ptrCast(@alignCast(ctx.?));
    if (owner.ref_count.fetchSub(1, .acq_rel) != 1) return;
    if (owner.owned_array.release) |release_fn| {
        var moved = owner.owned_array;
        release_fn(&moved);
        owner.owned_array.release = null;
        owner.owned_array.private_data = null;
    }
    owner.allocator.destroy(owner);
}

pub fn exportSchema(allocator: std.mem.Allocator, schema: Schema) Error!ArrowSchema {
    if (schema.endianness != .little) return error.UnsupportedType;
    const out = try exportRootSchema(allocator, schema);
    return out;
}

pub fn importSchemaOwned(allocator: std.mem.Allocator, c_schema: *ArrowSchema) Error!OwnedSchema {
    if (c_schema.release == null) return error.Released;

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();

    const fmt = cString(c_schema.format) orelse return error.InvalidFormat;
    if (!std.mem.eql(u8, fmt, "+s")) return error.TopLevelSchemaMustBeStruct;

    const children = childPtrsSchema(c_schema) orelse return error.InvalidChildren;
    const fields = try a.alloc(Field, children.len);
    for (children, 0..) |child_ptr, i| {
        if (child_ptr == null) return error.InvalidChildren;
        fields[i] = try importField(a, child_ptr.?);
    }

    const schema = Schema{
        .fields = fields,
        .endianness = .little,
        .metadata = null,
    };

    if (c_schema.release) |release_fn| release_fn(c_schema);
    return .{ .arena = arena, .schema = schema };
}

pub fn exportArray(allocator: std.mem.Allocator, arr: ArrayRef) Error!ArrowArray {
    return exportArrayRecursive(allocator, arr);
}

pub fn importArray(allocator: std.mem.Allocator, data_type: *const DataType, c_array: *ArrowArray) Error!ArrayRef {
    if (c_array.release == null) return error.Released;

    const owner = try allocator.create(ImportedArrayOwner);
    owner.* = .{
        .allocator = allocator,
        .ref_count = std.atomic.Value(u32).init(1),
        .owned_array = c_array.*,
    };
    errdefer ownerRelease(owner);

    // C Data Interface ownership transfer: importer takes release responsibility.
    c_array.release = null;
    c_array.private_data = null;

    var out = try importArrayRecursive(allocator, data_type, c_array, owner);
    errdefer out.release();

    // Drop bootstrap owner ref.
    ownerRelease(owner);
    return out;
}

fn exportRootSchema(allocator: std.mem.Allocator, schema: Schema) Error!ArrowSchema {
    const priv = try allocator.create(ExportedSchemaPrivate);
    errdefer allocator.destroy(priv);

    const format_z = try allocator.dupeZ(u8, "+s");
    errdefer allocator.free(format_z);

    priv.* = .{
        .allocator = allocator,
        .format_z = format_z,
        .name_z = null,
        .children_storage = &.{},
        .children_ptrs = &.{},
        .dict_storage = null,
    };

    if (schema.fields.len > 0) {
        priv.children_storage = try allocator.alloc(ArrowSchema, schema.fields.len);
        errdefer allocator.free(priv.children_storage);

        priv.children_ptrs = try allocator.alloc(?*ArrowSchema, schema.fields.len);
        errdefer allocator.free(priv.children_ptrs);

        for (schema.fields, 0..) |field, i| {
            priv.children_storage[i] = try exportFieldSchema(allocator, field);
            priv.children_ptrs[i] = &priv.children_storage[i];
        }
    }

    return ArrowSchema{
        .format = priv.format_z.ptr,
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = @intCast(schema.fields.len),
        .children = if (priv.children_ptrs.len == 0) null else priv.children_ptrs.ptr,
        .dictionary = null,
        .release = releaseExportedSchema,
        .private_data = priv,
    };
}

fn exportFieldSchema(allocator: std.mem.Allocator, field: Field) Error!ArrowSchema {
    const priv = try allocator.create(ExportedSchemaPrivate);
    errdefer allocator.destroy(priv);

    const fmt = try formatFromDataType(allocator, field.data_type.*);
    errdefer allocator.free(fmt);

    const name_z = try allocator.dupeZ(u8, field.name);
    errdefer allocator.free(name_z);

    priv.* = .{
        .allocator = allocator,
        .format_z = fmt,
        .name_z = name_z,
        .children_storage = &.{},
        .children_ptrs = &.{},
        .dict_storage = null,
    };

    if (field.data_type.* == .map) {
        const map_t = field.data_type.map;
        priv.children_storage = try allocator.alloc(ArrowSchema, 1);
        errdefer allocator.free(priv.children_storage);
        priv.children_ptrs = try allocator.alloc(?*ArrowSchema, 1);
        errdefer allocator.free(priv.children_ptrs);

        if (map_t.entries_type) |entries_type| {
            const entries_field = Field{ .name = "entries", .data_type = entries_type, .nullable = false };
            priv.children_storage[0] = try exportFieldSchema(allocator, entries_field);
        } else {
            const entry_fields = [_]Field{
                map_t.key_field,
                map_t.item_field,
            };
            const entries_dt = DataType{ .struct_ = .{ .fields = entry_fields[0..] } };
            const entries_field = Field{ .name = "entries", .data_type = &entries_dt, .nullable = false };
            priv.children_storage[0] = try exportFieldSchema(allocator, entries_field);
        }
        priv.children_ptrs[0] = &priv.children_storage[0];
    } else {
        const children = try childFieldsForDataType(allocator, field.data_type.*);
        defer allocator.free(children);

        if (children.len > 0) {
            priv.children_storage = try allocator.alloc(ArrowSchema, children.len);
            errdefer allocator.free(priv.children_storage);

            priv.children_ptrs = try allocator.alloc(?*ArrowSchema, children.len);
            errdefer allocator.free(priv.children_ptrs);

            for (children, 0..) |child, i| {
                priv.children_storage[i] = try exportFieldSchema(allocator, child);
                priv.children_ptrs[i] = &priv.children_storage[i];
            }
        }
    }

    var dict_ptr: ?*ArrowSchema = null;
    if (field.data_type.* == .dictionary) {
        const dict_dt = field.data_type.dictionary.value_type;
        const dict_field = Field{ .name = "dictionary", .data_type = dict_dt, .nullable = true };
        priv.dict_storage = try allocator.create(ArrowSchema);
        errdefer allocator.destroy(priv.dict_storage.?);
        priv.dict_storage.?.* = try exportFieldSchema(allocator, dict_field);
        dict_ptr = priv.dict_storage;
    }

    var flags: i64 = if (field.nullable) ARROW_FLAG_NULLABLE else 0;
    if (field.data_type.* == .dictionary and field.data_type.dictionary.ordered) {
        flags |= ARROW_FLAG_DICTIONARY_ORDERED;
    }
    if (field.data_type.* == .map and field.data_type.map.keys_sorted) {
        flags |= ARROW_FLAG_MAP_KEYS_SORTED;
    }

    return ArrowSchema{
        .format = priv.format_z.ptr,
        .name = if (priv.name_z) |n| n.ptr else null,
        .metadata = null,
        .flags = flags,
        .n_children = @intCast(priv.children_ptrs.len),
        .children = if (priv.children_ptrs.len == 0) null else priv.children_ptrs.ptr,
        .dictionary = dict_ptr,
        .release = releaseExportedSchema,
        .private_data = priv,
    };
}

fn releaseExportedSchema(raw: ?*ArrowSchema) callconv(.c) void {
    if (raw == null) return;
    const schema = raw.?;
    if (schema.release == null) return;

    const priv: *ExportedSchemaPrivate = @ptrCast(@alignCast(schema.private_data.?));

    for (priv.children_storage) |*child| {
        if (child.release) |release_fn| {
            release_fn(child);
        }
    }
    if (priv.dict_storage) |dict| {
        if (dict.release) |release_fn| {
            release_fn(dict);
        }
        priv.allocator.destroy(dict);
    }

    if (priv.children_ptrs.len > 0) priv.allocator.free(priv.children_ptrs);
    if (priv.children_storage.len > 0) priv.allocator.free(priv.children_storage);
    if (priv.name_z) |name_z| priv.allocator.free(name_z);
    priv.allocator.free(priv.format_z);
    priv.allocator.destroy(priv);

    schema.release = null;
    schema.private_data = null;
}

fn exportArrayRecursive(allocator: std.mem.Allocator, arr: ArrayRef) Error!ArrowArray {
    const data = arr.data();
    const priv = try allocator.create(ExportedArrayPrivate);
    errdefer allocator.destroy(priv);

    priv.* = .{
        .allocator = allocator,
        .retained_ref = arr.retain(),
        .buffers_ptrs = &.{},
        .children_storage = &.{},
        .children_ptrs = &.{},
        .dict_storage = null,
    };

    const n_buffers = data.buffers.len;
    if (n_buffers > 0) {
        priv.buffers_ptrs = try allocator.alloc(?*const anyopaque, n_buffers);
        errdefer allocator.free(priv.buffers_ptrs);
        for (data.buffers, 0..) |buf, i| {
            priv.buffers_ptrs[i] = if (buf.len() == 0) null else @ptrCast(buf.data.ptr);
        }
    }

    if (data.children.len > 0) {
        priv.children_storage = try allocator.alloc(ArrowArray, data.children.len);
        errdefer allocator.free(priv.children_storage);

        priv.children_ptrs = try allocator.alloc(?*ArrowArray, data.children.len);
        errdefer allocator.free(priv.children_ptrs);

        for (data.children, 0..) |child, i| {
            priv.children_storage[i] = try exportArrayRecursive(allocator, child);
            priv.children_ptrs[i] = &priv.children_storage[i];
        }
    }

    var dict_ptr: ?*ArrowArray = null;
    if (data.dictionary) |dict| {
        priv.dict_storage = try allocator.create(ArrowArray);
        errdefer allocator.destroy(priv.dict_storage.?);
        priv.dict_storage.?.* = try exportArrayRecursive(allocator, dict);
        dict_ptr = priv.dict_storage;
    }

    return ArrowArray{
        .length = @intCast(data.length),
        .null_count = if (data.null_count) |n| @intCast(n) else -1,
        .offset = @intCast(data.offset),
        .n_buffers = @intCast(n_buffers),
        .n_children = @intCast(data.children.len),
        .buffers = if (n_buffers == 0) null else priv.buffers_ptrs.ptr,
        .children = if (data.children.len == 0) null else priv.children_ptrs.ptr,
        .dictionary = dict_ptr,
        .release = releaseExportedArray,
        .private_data = priv,
    };
}

fn releaseExportedArray(raw: ?*ArrowArray) callconv(.c) void {
    if (raw == null) return;
    const arr = raw.?;
    if (arr.release == null) return;

    const priv: *ExportedArrayPrivate = @ptrCast(@alignCast(arr.private_data.?));

    for (priv.children_storage) |*child| {
        if (child.release) |release_fn| {
            release_fn(child);
        }
    }
    if (priv.dict_storage) |dict| {
        if (dict.release) |release_fn| {
            release_fn(dict);
        }
        priv.allocator.destroy(dict);
    }

    if (priv.children_ptrs.len > 0) priv.allocator.free(priv.children_ptrs);
    if (priv.children_storage.len > 0) priv.allocator.free(priv.children_storage);
    if (priv.buffers_ptrs.len > 0) priv.allocator.free(priv.buffers_ptrs);

    var retained = priv.retained_ref;
    retained.release();

    priv.allocator.destroy(priv);

    arr.release = null;
    arr.private_data = null;
}

fn importField(allocator: std.mem.Allocator, c_schema: *ArrowSchema) Error!Field {
    const name = if (c_schema.name == null) "" else try allocator.dupe(u8, cString(c_schema.name).?);
    const dt = try allocator.create(DataType);
    dt.* = try importDataType(allocator, c_schema);

    return Field{
        .name = name,
        .data_type = dt,
        .nullable = (c_schema.flags & ARROW_FLAG_NULLABLE) != 0,
        .metadata = null,
    };
}

fn importDataType(allocator: std.mem.Allocator, c_schema: *ArrowSchema) Error!DataType {
    const fmt = cString(c_schema.format) orelse return error.InvalidFormat;

    if (std.mem.eql(u8, fmt, "n")) return DataType{ .null = {} };
    if (std.mem.eql(u8, fmt, "b")) return DataType{ .bool = {} };
    if (std.mem.eql(u8, fmt, "c")) return DataType{ .int8 = {} };
    if (std.mem.eql(u8, fmt, "C")) return DataType{ .uint8 = {} };
    if (std.mem.eql(u8, fmt, "s")) return DataType{ .int16 = {} };
    if (std.mem.eql(u8, fmt, "S")) return DataType{ .uint16 = {} };
    if (std.mem.eql(u8, fmt, "i")) return DataType{ .int32 = {} };
    if (std.mem.eql(u8, fmt, "I")) return DataType{ .uint32 = {} };
    if (std.mem.eql(u8, fmt, "l")) return DataType{ .int64 = {} };
    if (std.mem.eql(u8, fmt, "L")) return DataType{ .uint64 = {} };
    if (std.mem.eql(u8, fmt, "e")) return DataType{ .half_float = {} };
    if (std.mem.eql(u8, fmt, "f")) return DataType{ .float = {} };
    if (std.mem.eql(u8, fmt, "g")) return DataType{ .double = {} };
    if (std.mem.eql(u8, fmt, "u")) return DataType{ .string = {} };
    if (std.mem.eql(u8, fmt, "U")) return DataType{ .large_string = {} };
    if (std.mem.eql(u8, fmt, "z")) return DataType{ .binary = {} };
    if (std.mem.eql(u8, fmt, "Z")) return DataType{ .large_binary = {} };
    if (std.mem.eql(u8, fmt, "tdD")) return DataType{ .date32 = {} };
    if (std.mem.eql(u8, fmt, "tdm")) return DataType{ .date64 = {} };
    if (std.mem.eql(u8, fmt, "tts")) return DataType{ .time32 = .{ .unit = .second } };
    if (std.mem.eql(u8, fmt, "ttm")) return DataType{ .time32 = .{ .unit = .millisecond } };
    if (std.mem.eql(u8, fmt, "ttu")) return DataType{ .time64 = .{ .unit = .microsecond } };
    if (std.mem.eql(u8, fmt, "ttn")) return DataType{ .time64 = .{ .unit = .nanosecond } };
    if (std.mem.eql(u8, fmt, "tDs")) return DataType{ .duration = .{ .unit = .second } };
    if (std.mem.eql(u8, fmt, "tDm")) return DataType{ .duration = .{ .unit = .millisecond } };
    if (std.mem.eql(u8, fmt, "tDu")) return DataType{ .duration = .{ .unit = .microsecond } };
    if (std.mem.eql(u8, fmt, "tDn")) return DataType{ .duration = .{ .unit = .nanosecond } };
    if (std.mem.eql(u8, fmt, "tiM")) return DataType{ .interval_months = .{ .unit = .months } };
    if (std.mem.eql(u8, fmt, "tDt")) return DataType{ .interval_day_time = .{ .unit = .day_time } };
    if (std.mem.eql(u8, fmt, "tin")) return DataType{ .interval_month_day_nano = .{ .unit = .month_day_nano } };
    if (std.mem.startsWith(u8, fmt, "ts")) {
        const parsed = parseTimestampFormat(fmt) orelse return error.InvalidFormat;
        const tz = if (parsed.timezone.len == 0) null else try allocator.dupe(u8, parsed.timezone);
        return DataType{ .timestamp = .{ .unit = parsed.unit, .timezone = tz } };
    }
    if (std.mem.startsWith(u8, fmt, "d:")) {
        const parsed = parseDecimalFormat(fmt) orelse return error.InvalidFormat;
        return switch (parsed.bit_width) {
            32 => DataType{ .decimal32 = .{ .precision = parsed.precision, .scale = parsed.scale } },
            64 => DataType{ .decimal64 = .{ .precision = parsed.precision, .scale = parsed.scale } },
            128 => DataType{ .decimal128 = .{ .precision = parsed.precision, .scale = parsed.scale } },
            256 => DataType{ .decimal256 = .{ .precision = parsed.precision, .scale = parsed.scale } },
            else => error.InvalidFormat,
        };
    }

    if (std.mem.startsWith(u8, fmt, "w:")) {
        const bw = std.fmt.parseInt(i32, fmt[2..], 10) catch return error.InvalidFormat;
        return DataType{ .fixed_size_binary = .{ .byte_width = bw } };
    }

    if (std.mem.eql(u8, fmt, "+l")) {
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const child_field = try importField(allocator, child);
        return DataType{ .list = .{ .value_field = child_field } };
    }
    if (std.mem.eql(u8, fmt, "+L")) {
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const child_field = try importField(allocator, child);
        return DataType{ .large_list = .{ .value_field = child_field } };
    }
    if (std.mem.eql(u8, fmt, "+m")) {
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const entries_field = try importField(allocator, child);
        if (entries_field.data_type.* != .struct_) return error.InvalidChildren;
        const entry_fields = entries_field.data_type.struct_.fields;
        if (entry_fields.len != 2) return error.InvalidChildren;
        return DataType{
            .map = .{
                .key_field = entry_fields[0],
                .item_field = entry_fields[1],
                .keys_sorted = (c_schema.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0,
                .entries_type = entries_field.data_type,
            },
        };
    }
    if (std.mem.startsWith(u8, fmt, "+us:") or std.mem.startsWith(u8, fmt, "+ud:")) {
        const mode: datatype.UnionMode = if (std.mem.startsWith(u8, fmt, "+us:")) .sparse else .dense;
        const ids = try parseUnionTypeIds(allocator, fmt[4..]);
        const child_ptrs = childPtrsSchema(c_schema) orelse return error.InvalidChildren;
        if (child_ptrs.len != ids.len) return error.InvalidChildren;
        const fields = try allocator.alloc(Field, child_ptrs.len);
        for (child_ptrs, 0..) |child_ptr, i| {
            if (child_ptr == null) return error.InvalidChildren;
            fields[i] = try importField(allocator, child_ptr.?);
        }
        const union_ty = datatype.UnionType{
            .type_ids = ids,
            .fields = fields,
            .mode = mode,
        };
        return switch (mode) {
            .sparse => DataType{ .sparse_union = union_ty },
            .dense => DataType{ .dense_union = union_ty },
        };
    }
    if (std.mem.eql(u8, fmt, "+s")) {
        const child_ptrs = childPtrsSchema(c_schema) orelse return error.InvalidChildren;
        const fields = try allocator.alloc(Field, child_ptrs.len);
        for (child_ptrs, 0..) |child_ptr, i| {
            if (child_ptr == null) return error.InvalidChildren;
            fields[i] = try importField(allocator, child_ptr.?);
        }
        return DataType{ .struct_ = .{ .fields = fields } };
    }
    if (std.mem.startsWith(u8, fmt, "+w:")) {
        const sz = std.fmt.parseInt(i32, fmt[3..], 10) catch return error.InvalidFormat;
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const child_field = try importField(allocator, child);
        return DataType{ .fixed_size_list = .{ .value_field = child_field, .list_size = sz } };
    }

    if (c_schema.dictionary != null) {
        const idx = intTypeFromFormat(fmt) orelse return error.InvalidFormat;
        const dict_field = try importField(allocator, c_schema.dictionary.?);
        const value_type = try allocator.create(DataType);
        value_type.* = dict_field.data_type.*;
        return DataType{ .dictionary = .{
            .id = null,
            .index_type = idx,
            .value_type = value_type,
            .ordered = (c_schema.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0,
        } };
    }

    return error.UnsupportedType;
}

fn importArrayRecursive(
    allocator: std.mem.Allocator,
    data_type: *const DataType,
    c_array: *ArrowArray,
    owner: *ImportedArrayOwner,
) Error!ArrayRef {
    const len = toUsize(c_array.length) orelse return error.InvalidLength;
    const off = toUsize(c_array.offset) orelse return error.InvalidOffset;
    const null_count: ?usize = if (c_array.null_count < 0)
        null
    else
        toUsize(c_array.null_count) orelse return error.InvalidNullCount;

    const expected_buffers = expectedBufferCount(data_type.*) orelse return error.UnsupportedType;
    const expected_children = expectedChildrenCount(data_type.*);

    if (c_array.n_buffers != expected_buffers) return error.InvalidBufferCount;
    if (c_array.n_children != expected_children) return error.InvalidChildren;

    const n_buffers = toUsize(c_array.n_buffers).?;
    const n_children = toUsize(c_array.n_children).?;

    const total_len = off + len;

    const buffers = try allocator.alloc(SharedBuffer, n_buffers);
    var filled_buffers: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < filled_buffers) : (i += 1) {
            var b = buffers[i];
            b.release();
        }
        allocator.free(buffers);
    }

    if (n_buffers > 0 and c_array.buffers == null) return error.InvalidBufferCount;

    var data_buffer_len: usize = 0;
    var offsets_i32: ?[]const i32 = null;
    var offsets_i64: ?[]const i64 = null;

    var i: usize = 0;
    while (i < n_buffers) : (i += 1) {
        const needed = neededBufferLen(data_type.*, i, total_len, offsets_i32, offsets_i64, data_buffer_len) orelse return error.UnsupportedType;
        const ptr_any = c_array.buffers[i];
        buffers[i] = try importBuffer(ptr_any, needed);
        filled_buffers += 1;

        if ((data_type.* == .string or data_type.* == .binary or data_type.* == .list) and i == 1) {
            offsets_i32 = buffers[i].typedSlice(i32);
            data_buffer_len = @intCast(offsets_i32.?[total_len]);
        }
        if ((data_type.* == .large_string or data_type.* == .large_binary or data_type.* == .large_list) and i == 1) {
            offsets_i64 = buffers[i].typedSlice(i64);
            data_buffer_len = @intCast(offsets_i64.?[total_len]);
        }
    }

    const children = try allocator.alloc(ArrayRef, n_children);
    var filled_children: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < filled_children) : (j += 1) {
            children[j].release();
        }
        allocator.free(children);
    }

    const child_ptrs = if (n_children == 0) &[_]?*ArrowArray{} else childPtrsArray(c_array) orelse return error.InvalidChildren;
    const child_types = try childTypesForDataType(allocator, data_type.*);
    defer allocator.free(child_types);

    for (child_types, 0..) |child_ty, idx| {
        const child_ptr = child_ptrs[idx] orelse return error.InvalidChildren;
        children[idx] = try importArrayRecursive(allocator, child_ty, child_ptr, owner);
        filled_children += 1;
    }

    var dict: ?ArrayRef = null;
    errdefer if (dict) |*d| d.release();
    if (data_type.* == .dictionary) {
        if (c_array.dictionary == null) return error.MissingDictionary;
        dict = try importArrayRecursive(allocator, data_type.dictionary.value_type, c_array.dictionary.?, owner);
    }

    const layout = ArrayData{
        .data_type = data_type.*,
        .length = len,
        .offset = off,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = dict,
    };

    layout.validateLayout() catch |err| switch (err) {
        error.InvalidBufferCount, error.BufferTooSmall => return error.InvalidBufferCount,
        error.InvalidOffsets, error.InvalidOffsetBuffer => return error.InvalidOffset,
        error.InvalidNullCount => return error.InvalidNullCount,
        error.MissingDictionary => return error.MissingDictionary,
        error.InvalidChildren => return error.InvalidChildren,
    };

    return ArrayRef.fromOwnedWithOwner(allocator, layout, owner, ownerRetain, ownerRelease);
}

fn importBuffer(ptr_any: ?*const anyopaque, needed_len: usize) Error!SharedBuffer {
    if (needed_len == 0 or ptr_any == null) return SharedBuffer.empty;
    const ptr: [*]const u8 = @ptrCast(ptr_any.?);
    return SharedBuffer.init(ptr[0..needed_len]);
}

fn neededBufferLen(
    dt: DataType,
    idx: usize,
    total_len: usize,
    offsets_i32: ?[]const i32,
    offsets_i64: ?[]const i64,
    data_buffer_len: usize,
) ?usize {
    _ = data_buffer_len;

    if (dt == .null) return if (idx == 0) 0 else null;

    if (idx == 0 and hasValidity(dt)) {
        return bitmap.byteLength(total_len);
    }

    return switch (dt) {
        .bool => if (idx == 1) bitmap.byteLength(total_len) else null,
        .int8, .uint8 => if (idx == 1) total_len else null,
        .int16, .uint16, .half_float => if (idx == 1) total_len * 2 else null,
        .int32, .uint32, .float, .date32, .time32, .interval_months, .decimal32 => if (idx == 1) total_len * 4 else null,
        .int64, .uint64, .double, .date64, .timestamp, .time64, .duration, .interval_day_time, .decimal64 => if (idx == 1) total_len * 8 else null,
        .interval_month_day_nano, .decimal128 => if (idx == 1) total_len * 16 else null,
        .decimal256 => if (idx == 1) total_len * 32 else null,
        .fixed_size_binary => |fsb| if (idx == 1) total_len * @as(usize, @intCast(fsb.byte_width)) else null,
        .string, .binary, .list => {
            if (idx == 1) return (total_len + 1) * @sizeOf(i32);
            if (idx == 2) {
                const offs = offsets_i32 orelse return null;
                return @intCast(offs[total_len]);
            }
            return null;
        },
        .map => {
            if (idx == 1) return (total_len + 1) * @sizeOf(i32);
            return null;
        },
        .sparse_union => {
            if (idx == 0) return total_len;
            return null;
        },
        .dense_union => {
            if (idx == 0) return total_len;
            if (idx == 1) return total_len * @sizeOf(i32);
            return null;
        },
        .large_string, .large_binary, .large_list => {
            if (idx == 1) return (total_len + 1) * @sizeOf(i64);
            if (idx == 2) {
                const offs = offsets_i64 orelse return null;
                return @intCast(offs[total_len]);
            }
            return null;
        },
        .struct_, .fixed_size_list => null,
        .dictionary => |dict| if (idx == 1) total_len * (@as(usize, dict.index_type.bit_width) / 8) else null,
        else => null,
    };
}

fn hasValidity(dt: DataType) bool {
    return switch (dt) {
        .null, .sparse_union, .dense_union => false,
        else => true,
    };
}

fn expectedBufferCount(dt: DataType) ?i64 {
    return switch (dt) {
        .null => 0,
        .struct_, .fixed_size_list => 1,
        .sparse_union => 1,
        .dense_union => 2,
        .bool => 2,
        .int8,
        .uint8,
        .int16,
        .uint16,
        .int32,
        .uint32,
        .int64,
        .uint64,
        .half_float,
        .float,
        .double,
        .date32,
        .date64,
        .timestamp,
        .time32,
        .time64,
        .duration,
        .interval_months,
        .interval_day_time,
        .interval_month_day_nano,
        .decimal32,
        .decimal64,
        .decimal128,
        .decimal256,
        .fixed_size_binary,
        .dictionary,
        .map,
        => 2,
        .string, .binary, .large_string, .large_binary, .list, .large_list => 3,
        else => null,
    };
}

fn expectedChildrenCount(dt: DataType) i64 {
    return switch (dt) {
        .list, .large_list, .fixed_size_list, .map => 1,
        .struct_ => |s| @intCast(s.fields.len),
        .sparse_union => |u| @intCast(u.fields.len),
        .dense_union => |u| @intCast(u.fields.len),
        else => 0,
    };
}

fn childTypesForDataType(allocator: std.mem.Allocator, dt: DataType) Error![]const *const DataType {
    return switch (dt) {
        .list => |l| blk: {
            const out = try allocator.alloc(*const DataType, 1);
            out[0] = l.value_field.data_type;
            break :blk out;
        },
        .large_list => |l| blk: {
            const out = try allocator.alloc(*const DataType, 1);
            out[0] = l.value_field.data_type;
            break :blk out;
        },
        .fixed_size_list => |l| blk: {
            const out = try allocator.alloc(*const DataType, 1);
            out[0] = l.value_field.data_type;
            break :blk out;
        },
        .struct_ => |s| blk: {
            const out = try allocator.alloc(*const DataType, s.fields.len);
            for (s.fields, 0..) |f, i| out[i] = f.data_type;
            break :blk out;
        },
        .map => |m| blk: {
            const entries_type = m.entries_type orelse return error.UnsupportedType;
            const out = try allocator.alloc(*const DataType, 1);
            out[0] = entries_type;
            break :blk out;
        },
        .sparse_union => |u| blk: {
            const out = try allocator.alloc(*const DataType, u.fields.len);
            for (u.fields, 0..) |f, i| out[i] = f.data_type;
            break :blk out;
        },
        .dense_union => |u| blk: {
            const out = try allocator.alloc(*const DataType, u.fields.len);
            for (u.fields, 0..) |f, i| out[i] = f.data_type;
            break :blk out;
        },
        else => try allocator.alloc(*const DataType, 0),
    };
}

fn childFieldsForDataType(allocator: std.mem.Allocator, dt: DataType) Error![]const Field {
    return switch (dt) {
        .list => |l| blk: {
            const out = try allocator.alloc(Field, 1);
            out[0] = l.value_field;
            break :blk out;
        },
        .large_list => |l| blk: {
            const out = try allocator.alloc(Field, 1);
            out[0] = l.value_field;
            break :blk out;
        },
        .fixed_size_list => |l| blk: {
            const out = try allocator.alloc(Field, 1);
            out[0] = l.value_field;
            break :blk out;
        },
        .struct_ => |s| blk: {
            const out = try allocator.alloc(Field, s.fields.len);
            @memcpy(out, s.fields);
            break :blk out;
        },
        .map => |m| blk: {
            const out = try allocator.alloc(Field, 1);
            if (m.entries_type) |entries_type| {
                out[0] = Field{ .name = "entries", .data_type = entries_type, .nullable = false };
            } else {
                return error.UnsupportedType;
            }
            break :blk out;
        },
        .sparse_union => |u| blk: {
            const out = try allocator.alloc(Field, u.fields.len);
            @memcpy(out, u.fields);
            break :blk out;
        },
        .dense_union => |u| blk: {
            const out = try allocator.alloc(Field, u.fields.len);
            @memcpy(out, u.fields);
            break :blk out;
        },
        else => try allocator.alloc(Field, 0),
    };
}

fn formatFromDataType(allocator: std.mem.Allocator, dt: DataType) Error![:0]u8 {
    return switch (dt) {
        .null => try allocator.dupeZ(u8, "n"),
        .bool => try allocator.dupeZ(u8, "b"),
        .int8 => try allocator.dupeZ(u8, "c"),
        .uint8 => try allocator.dupeZ(u8, "C"),
        .int16 => try allocator.dupeZ(u8, "s"),
        .uint16 => try allocator.dupeZ(u8, "S"),
        .int32 => try allocator.dupeZ(u8, "i"),
        .uint32 => try allocator.dupeZ(u8, "I"),
        .int64 => try allocator.dupeZ(u8, "l"),
        .uint64 => try allocator.dupeZ(u8, "L"),
        .half_float => try allocator.dupeZ(u8, "e"),
        .float => try allocator.dupeZ(u8, "f"),
        .double => try allocator.dupeZ(u8, "g"),
        .string => try allocator.dupeZ(u8, "u"),
        .large_string => try allocator.dupeZ(u8, "U"),
        .binary => try allocator.dupeZ(u8, "z"),
        .large_binary => try allocator.dupeZ(u8, "Z"),
        .date32 => try allocator.dupeZ(u8, "tdD"),
        .date64 => try allocator.dupeZ(u8, "tdm"),
        .timestamp => |ts| formatTimestamp(allocator, ts.unit, ts.timezone),
        .time32 => |t| switch (t.unit) {
            .second => try allocator.dupeZ(u8, "tts"),
            .millisecond => try allocator.dupeZ(u8, "ttm"),
            else => return error.UnsupportedType,
        },
        .time64 => |t| switch (t.unit) {
            .microsecond => try allocator.dupeZ(u8, "ttu"),
            .nanosecond => try allocator.dupeZ(u8, "ttn"),
            else => return error.UnsupportedType,
        },
        .duration => |d| switch (d.unit) {
            .second => try allocator.dupeZ(u8, "tDs"),
            .millisecond => try allocator.dupeZ(u8, "tDm"),
            .microsecond => try allocator.dupeZ(u8, "tDu"),
            .nanosecond => try allocator.dupeZ(u8, "tDn"),
        },
        .interval_months => try allocator.dupeZ(u8, "tiM"),
        .interval_day_time => try allocator.dupeZ(u8, "tDt"),
        .interval_month_day_nano => try allocator.dupeZ(u8, "tin"),
        .fixed_size_binary => |fsb| allocPrintZ(allocator, "w:{d}", .{fsb.byte_width}),
        .list => try allocator.dupeZ(u8, "+l"),
        .large_list => try allocator.dupeZ(u8, "+L"),
        .map => try allocator.dupeZ(u8, "+m"),
        .sparse_union => |u| formatUnion(allocator, true, u.type_ids),
        .dense_union => |u| formatUnion(allocator, false, u.type_ids),
        .struct_ => try allocator.dupeZ(u8, "+s"),
        .fixed_size_list => |fsl| allocPrintZ(allocator, "+w:{d}", .{fsl.list_size}),
        .dictionary => |d| formatFromIntType(allocator, d.index_type),
        .decimal32 => |d| allocPrintZ(allocator, "d:{d},{d},32", .{ d.precision, d.scale }),
        .decimal64 => |d| allocPrintZ(allocator, "d:{d},{d},64", .{ d.precision, d.scale }),
        .decimal128 => |d| allocPrintZ(allocator, "d:{d},{d}", .{ d.precision, d.scale }),
        .decimal256 => |d| allocPrintZ(allocator, "d:{d},{d},256", .{ d.precision, d.scale }),
        else => error.UnsupportedType,
    };
}

fn formatFromIntType(allocator: std.mem.Allocator, int_type: IntType) Error![:0]u8 {
    const s: []const u8 = switch (int_type.bit_width) {
        8 => if (int_type.signed) "c" else "C",
        16 => if (int_type.signed) "s" else "S",
        32 => if (int_type.signed) "i" else "I",
        64 => if (int_type.signed) "l" else "L",
        else => return error.InvalidFormat,
    };
    return allocator.dupeZ(u8, s) catch error.OutOfMemory;
}

fn intTypeFromFormat(fmt: []const u8) ?IntType {
    if (std.mem.eql(u8, fmt, "c")) return .{ .bit_width = 8, .signed = true };
    if (std.mem.eql(u8, fmt, "C")) return .{ .bit_width = 8, .signed = false };
    if (std.mem.eql(u8, fmt, "s")) return .{ .bit_width = 16, .signed = true };
    if (std.mem.eql(u8, fmt, "S")) return .{ .bit_width = 16, .signed = false };
    if (std.mem.eql(u8, fmt, "i")) return .{ .bit_width = 32, .signed = true };
    if (std.mem.eql(u8, fmt, "I")) return .{ .bit_width = 32, .signed = false };
    if (std.mem.eql(u8, fmt, "l")) return .{ .bit_width = 64, .signed = true };
    if (std.mem.eql(u8, fmt, "L")) return .{ .bit_width = 64, .signed = false };
    return null;
}

fn childPtrsSchema(c_schema: *ArrowSchema) ?[]?*ArrowSchema {
    const n = toUsize(c_schema.n_children) orelse return null;
    if (n == 0) return &[_]?*ArrowSchema{};
    if (c_schema.children == null) return null;
    return c_schema.children[0..n];
}

fn childPtrsArray(c_array: *ArrowArray) ?[]?*ArrowArray {
    const n = toUsize(c_array.n_children) orelse return null;
    if (n == 0) return &[_]?*ArrowArray{};
    if (c_array.children == null) return null;
    return c_array.children[0..n];
}

fn singleChildSchema(c_schema: *ArrowSchema) ?*ArrowSchema {
    const children = childPtrsSchema(c_schema) orelse return null;
    if (children.len != 1) return null;
    return children[0];
}

fn cString(ptr: [*c]const u8) ?[]const u8 {
    if (ptr == null) return null;
    return std.mem.span(ptr);
}

fn toUsize(v: i64) ?usize {
    if (v < 0) return null;
    return std.math.cast(usize, v);
}

fn formatTimestamp(allocator: std.mem.Allocator, unit: TimeUnit, timezone: ?[]const u8) Error![:0]u8 {
    const unit_c: u8 = switch (unit) {
        .second => 's',
        .millisecond => 'm',
        .microsecond => 'u',
        .nanosecond => 'n',
    };
    const tz = timezone orelse "";
    return allocPrintZ(allocator, "ts{c}:{s}", .{ unit_c, tz });
}

const ParsedTimestamp = struct {
    unit: TimeUnit,
    timezone: []const u8,
};

fn parseTimestampFormat(fmt: []const u8) ?ParsedTimestamp {
    // C Data Interface timestamp: ts{unit}:{timezone}
    if (fmt.len < 4) return null;
    if (!std.mem.startsWith(u8, fmt, "ts")) return null;
    const unit = switch (fmt[2]) {
        's' => TimeUnit.second,
        'm' => TimeUnit.millisecond,
        'u' => TimeUnit.microsecond,
        'n' => TimeUnit.nanosecond,
        else => return null,
    };
    if (fmt[3] != ':') return null;
    return .{ .unit = unit, .timezone = fmt[4..] };
}

const ParsedDecimal = struct {
    precision: u8,
    scale: i32,
    bit_width: u16,
};

fn parseDecimalFormat(fmt: []const u8) ?ParsedDecimal {
    // C Data Interface decimal:
    // - decimal128: d:precision,scale
    // - explicit widths: d:precision,scale,32|64|128|256
    if (!std.mem.startsWith(u8, fmt, "d:")) return null;
    const payload = fmt[2..];
    var it = std.mem.splitScalar(u8, payload, ',');
    const precision_s = it.next() orelse return null;
    const scale_s = it.next() orelse return null;
    const width_s = it.next();
    if (it.next() != null) return null;

    const precision = std.fmt.parseInt(u8, precision_s, 10) catch return null;
    const scale = std.fmt.parseInt(i32, scale_s, 10) catch return null;
    const bit_width: u16 = if (width_s) |w|
        std.fmt.parseInt(u16, w, 10) catch return null
    else
        128;

    return .{
        .precision = precision,
        .scale = scale,
        .bit_width = bit_width,
    };
}

fn parseUnionTypeIds(allocator: std.mem.Allocator, payload: []const u8) Error![]i8 {
    if (payload.len == 0) return error.InvalidFormat;
    var count: usize = 1;
    for (payload) |c| if (c == ',') count += 1;
    const out = allocator.alloc(i8, count) catch return error.OutOfMemory;
    errdefer allocator.free(out);
    var it = std.mem.splitScalar(u8, payload, ',');
    var i: usize = 0;
    while (it.next()) |part| : (i += 1) {
        if (part.len == 0) return error.InvalidFormat;
        out[i] = std.fmt.parseInt(i8, part, 10) catch return error.InvalidFormat;
    }
    if (i != count) return error.InvalidFormat;
    return out;
}

fn formatUnion(allocator: std.mem.Allocator, sparse: bool, ids: []const i8) Error![:0]u8 {
    if (ids.len == 0) return error.InvalidFormat;
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    try list.appendSlice(if (sparse) "+us:" else "+ud:");
    for (ids, 0..) |id, i| {
        if (i != 0) try list.append(',');
        const s = std.fmt.allocPrint(allocator, "{d}", .{id}) catch return error.OutOfMemory;
        defer allocator.free(s);
        try list.appendSlice(s);
    }
    return allocator.dupeZ(u8, list.items) catch error.OutOfMemory;
}

fn allocPrintZ(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) Error![:0]u8 {
    const tmp = std.fmt.allocPrint(allocator, fmt, args) catch return error.OutOfMemory;
    defer allocator.free(tmp);
    return allocator.dupeZ(u8, tmp) catch error.OutOfMemory;
}

test "c data schema export/import roundtrip" {
    const allocator = std.testing.allocator;

    const int_ty = DataType{ .int32 = {} };
    const str_ty = DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_ty, .nullable = false },
        .{ .name = "name", .data_type = &str_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();

    try std.testing.expectEqual(@as(usize, 2), imported.schema.fields.len);
    try std.testing.expectEqualStrings("id", imported.schema.fields[0].name);
    try std.testing.expect(imported.schema.fields[0].data_type.* == .int32);
    try std.testing.expect(!imported.schema.fields[0].nullable);
    try std.testing.expectEqualStrings("name", imported.schema.fields[1].name);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .string);
    try std.testing.expect(imported.schema.fields[1].nullable);
    try std.testing.expect(c_schema.release == null);
}

test "c data array export/import zero-copy smoke" {
    const allocator = std.testing.allocator;

    var builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 4);
    defer builder.deinit();
    try builder.append(10);
    try builder.appendNull();
    try builder.append(30);
    var arr = try builder.finish();
    defer arr.release();

    var c_array = try exportArray(allocator, arr);

    {
        var imported = try importArray(allocator, &arr.data().data_type, &c_array);
        defer imported.release();

        const view = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = imported.data() };
        try std.testing.expectEqual(@as(usize, 3), view.len());
        try std.testing.expectEqual(@as(i32, 10), view.value(0));
        try std.testing.expect(view.isNull(1));
        try std.testing.expectEqual(@as(i32, 30), view.value(2));

        // Zero-copy: values buffer pointer should be shared.
        try std.testing.expectEqual(
            @intFromPtr(arr.data().buffers[1].data.ptr),
            @intFromPtr(imported.data().buffers[1].data.ptr),
        );
    }

    // importArray should transfer ownership and clear producer release hook.
    try std.testing.expect(c_array.release == null);
}

test "c data schema supports timestamp and decimal128 formats" {
    const allocator = std.testing.allocator;

    const ts_ty = DataType{ .timestamp = .{ .unit = .microsecond, .timezone = "UTC" } };
    const dec_ty = DataType{ .decimal128 = .{ .precision = 38, .scale = 10 } };
    const fields = [_]Field{
        .{ .name = "event_time", .data_type = &ts_ty, .nullable = true },
        .{ .name = "amount", .data_type = &dec_ty, .nullable = false },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();

    try std.testing.expectEqual(@as(usize, 2), imported.schema.fields.len);
    try std.testing.expect(imported.schema.fields[0].data_type.* == .timestamp);
    try std.testing.expectEqual(TimeUnit.microsecond, imported.schema.fields[0].data_type.timestamp.unit);
    try std.testing.expectEqualStrings("UTC", imported.schema.fields[0].data_type.timestamp.timezone.?);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .decimal128);
    try std.testing.expectEqual(@as(u8, 38), imported.schema.fields[1].data_type.decimal128.precision);
    try std.testing.expectEqual(@as(i32, 10), imported.schema.fields[1].data_type.decimal128.scale);
}

test "c data schema supports duration and intervals formats" {
    const allocator = std.testing.allocator;

    const dur_ty = DataType{ .duration = .{ .unit = .nanosecond } };
    const int_m_ty = DataType{ .interval_months = .{ .unit = .months } };
    const int_dt_ty = DataType{ .interval_day_time = .{ .unit = .day_time } };
    const int_mdn_ty = DataType{ .interval_month_day_nano = .{ .unit = .month_day_nano } };
    const fields = [_]Field{
        .{ .name = "dur", .data_type = &dur_ty, .nullable = true },
        .{ .name = "im", .data_type = &int_m_ty, .nullable = true },
        .{ .name = "idt", .data_type = &int_dt_ty, .nullable = true },
        .{ .name = "imdn", .data_type = &int_mdn_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();

    try std.testing.expectEqual(@as(usize, 4), imported.schema.fields.len);
    try std.testing.expect(imported.schema.fields[0].data_type.* == .duration);
    try std.testing.expectEqual(TimeUnit.nanosecond, imported.schema.fields[0].data_type.duration.unit);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .interval_months);
    try std.testing.expect(imported.schema.fields[2].data_type.* == .interval_day_time);
    try std.testing.expect(imported.schema.fields[3].data_type.* == .interval_month_day_nano);
}

test "c data export emits canonical temporal and decimal formats" {
    const allocator = std.testing.allocator;

    const ts_ty = DataType{ .timestamp = .{ .unit = .second, .timezone = "UTC" } };
    const dur_ty = DataType{ .duration = .{ .unit = .microsecond } };
    const int_m_ty = DataType{ .interval_months = .{ .unit = .months } };
    const int_dt_ty = DataType{ .interval_day_time = .{ .unit = .day_time } };
    const int_mdn_ty = DataType{ .interval_month_day_nano = .{ .unit = .month_day_nano } };
    const dec_ty = DataType{ .decimal128 = .{ .precision = 20, .scale = 4 } };

    const fields = [_]Field{
        .{ .name = "ts", .data_type = &ts_ty, .nullable = true },
        .{ .name = "dur", .data_type = &dur_ty, .nullable = true },
        .{ .name = "im", .data_type = &int_m_ty, .nullable = true },
        .{ .name = "idt", .data_type = &int_dt_ty, .nullable = true },
        .{ .name = "imdn", .data_type = &int_mdn_ty, .nullable = true },
        .{ .name = "dec", .data_type = &dec_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const children = childPtrsSchema(&c_schema).?;
    try std.testing.expectEqual(@as(usize, 6), children.len);
    try std.testing.expectEqualStrings("tss:UTC", cString(children[0].?.format).?);
    try std.testing.expectEqualStrings("tDu", cString(children[1].?.format).?);
    try std.testing.expectEqualStrings("tiM", cString(children[2].?.format).?);
    try std.testing.expectEqualStrings("tDt", cString(children[3].?.format).?);
    try std.testing.expectEqualStrings("tin", cString(children[4].?.format).?);
    try std.testing.expectEqualStrings("d:20,4", cString(children[5].?.format).?);
}

test "c data import parses explicit decimal widths and empty timestamp timezone" {
    const allocator = std.testing.allocator;

    const ts_schema = ArrowSchema{
        .format = "tsn:",
        .name = null,
        .metadata = null,
        .flags = ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    var ts_schema_mut = ts_schema;
    const ts_dt = try importDataType(allocator, &ts_schema_mut);
    try std.testing.expect(ts_dt == .timestamp);
    try std.testing.expectEqual(TimeUnit.nanosecond, ts_dt.timestamp.unit);
    try std.testing.expect(ts_dt.timestamp.timezone == null);

    const dec32_schema = ArrowSchema{
        .format = "d:9,2,32",
        .name = null,
        .metadata = null,
        .flags = ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    var dec32_schema_mut = dec32_schema;
    const dec32_dt = try importDataType(allocator, &dec32_schema_mut);
    try std.testing.expect(dec32_dt == .decimal32);
    try std.testing.expectEqual(@as(u8, 9), dec32_dt.decimal32.precision);
    try std.testing.expectEqual(@as(i32, 2), dec32_dt.decimal32.scale);

    const dec64_schema = ArrowSchema{
        .format = "d:18,3,64",
        .name = null,
        .metadata = null,
        .flags = ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    var dec64_schema_mut = dec64_schema;
    const dec64_dt = try importDataType(allocator, &dec64_schema_mut);
    try std.testing.expect(dec64_dt == .decimal64);
    try std.testing.expectEqual(@as(u8, 18), dec64_dt.decimal64.precision);
    try std.testing.expectEqual(@as(i32, 3), dec64_dt.decimal64.scale);

    const dec256_schema = ArrowSchema{
        .format = "d:76,20,256",
        .name = null,
        .metadata = null,
        .flags = ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    var dec256_schema_mut = dec256_schema;
    const dec256_dt = try importDataType(allocator, &dec256_schema_mut);
    try std.testing.expect(dec256_dt == .decimal256);
    try std.testing.expectEqual(@as(u8, 76), dec256_dt.decimal256.precision);
    try std.testing.expectEqual(@as(i32, 20), dec256_dt.decimal256.scale);
}

test "c data schema supports map format" {
    const allocator = std.testing.allocator;

    const key_ty = DataType{ .int32 = {} };
    const item_ty = DataType{ .string = {} };
    const key_field = Field{ .name = "key", .data_type = &key_ty, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };
    const map_ty = DataType{
        .map = .{
            .key_field = key_field,
            .item_field = item_field,
            .keys_sorted = true,
            .entries_type = null,
        },
    };
    const fields = [_]Field{
        .{ .name = "attrs", .data_type = &map_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const root_children = childPtrsSchema(&c_schema).?;
    try std.testing.expectEqual(@as(usize, 1), root_children.len);
    const map_schema = root_children[0].?;
    try std.testing.expectEqualStrings("+m", cString(map_schema.format).?);
    try std.testing.expect((map_schema.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0);

    const map_children = childPtrsSchema(map_schema).?;
    try std.testing.expectEqual(@as(usize, 1), map_children.len);
    try std.testing.expectEqualStrings("+s", cString(map_children[0].?.format).?);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();
    try std.testing.expect(imported.schema.fields[0].data_type.* == .map);
    try std.testing.expect(imported.schema.fields[0].data_type.map.keys_sorted);
    try std.testing.expect(imported.schema.fields[0].data_type.map.key_field.data_type.* == .int32);
    try std.testing.expect(imported.schema.fields[0].data_type.map.item_field.data_type.* == .string);
}

test "c data array import supports map with entries_type" {
    const allocator = std.testing.allocator;

    const key_ty = DataType{ .int32 = {} };
    const item_ty = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &key_ty, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };

    var keys_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer keys_builder.deinit();
    try keys_builder.append(1);
    try keys_builder.append(2);
    try keys_builder.append(3);
    var keys_ref = try keys_builder.finish();
    defer keys_ref.release();

    var items_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer items_builder.deinit();
    try items_builder.append(10);
    try items_builder.appendNull();
    try items_builder.append(30);
    var items_ref = try items_builder.finish();
    defer items_ref.release();

    var entries_builder = @import("../array/struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ keys_ref, items_ref });
    defer entries_ref.release();

    var map_builder = try @import("../array/advanced_array.zig").MapBuilder.init(allocator, 2, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendLen(1);
    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    var c_array = try exportArray(allocator, map_ref);
    const entries_ty_ptr: *const DataType = &entries_ref.data().data_type;
    const map_ty = DataType{
        .map = .{
            .key_field = key_field,
            .item_field = item_field,
            .keys_sorted = false,
            .entries_type = entries_ty_ptr,
        },
    };

    var imported = try importArray(allocator, &map_ty, &c_array);
    defer imported.release();

    try std.testing.expect(imported.data().data_type == .map);
    try std.testing.expectEqual(@as(usize, 2), imported.data().length);
    try std.testing.expectEqual(@as(usize, 1), imported.data().children.len);
    try std.testing.expect(c_array.release == null);
}

test "c data map import validates offsets and entries values explicitly" {
    const allocator = std.testing.allocator;

    const key_ty = DataType{ .int32 = {} };
    const item_ty = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &key_ty, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };

    // entries: (1,10), (2,null), (3,30)
    var keys_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer keys_builder.deinit();
    try keys_builder.append(1);
    try keys_builder.append(2);
    try keys_builder.append(3);
    var keys_ref = try keys_builder.finish();
    defer keys_ref.release();

    var items_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer items_builder.deinit();
    try items_builder.append(10);
    try items_builder.appendNull();
    try items_builder.append(30);
    var items_ref = try items_builder.finish();
    defer items_ref.release();

    var entries_builder = @import("../array/struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ keys_ref, items_ref });
    defer entries_ref.release();

    // map rows:
    // row0 => 2 entries (0..2)
    // row1 => null, 0 entries (2..2)
    // row2 => 1 entry  (2..3)
    var map_builder = try @import("../array/advanced_array.zig").MapBuilder.init(allocator, 3, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    try map_builder.appendNull();
    try map_builder.appendLen(1);
    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    var c_array = try exportArray(allocator, map_ref);
    const entries_ty_ptr: *const DataType = &entries_ref.data().data_type;
    const map_ty = DataType{
        .map = .{
            .key_field = key_field,
            .item_field = item_field,
            .keys_sorted = false,
            .entries_type = entries_ty_ptr,
        },
    };

    var imported = try importArray(allocator, &map_ty, &c_array);
    defer imported.release();

    const map_view = @import("../array/advanced_array.zig").MapArray{ .data = imported.data() };
    try std.testing.expectEqual(@as(usize, 3), map_view.len());
    try std.testing.expect(!map_view.isNull(0));
    try std.testing.expect(map_view.isNull(1));
    try std.testing.expect(!map_view.isNull(2));

    const offsets = imported.data().buffers[1].typedSlice(i32);
    try std.testing.expectEqual(@as(i32, 0), offsets[0]);
    try std.testing.expectEqual(@as(i32, 2), offsets[1]);
    try std.testing.expectEqual(@as(i32, 2), offsets[2]);
    try std.testing.expectEqual(@as(i32, 3), offsets[3]);

    var row0 = try map_view.value(0);
    defer row0.release();
    var row2 = try map_view.value(2);
    defer row2.release();
    try std.testing.expectEqual(@as(usize, 2), row0.data().length);
    try std.testing.expectEqual(@as(usize, 1), row2.data().length);

    const entries_struct = @import("../array/struct_array.zig").StructArray{ .data = imported.data().children[0].data() };
    var keys_all = try entries_struct.field(0);
    defer keys_all.release();
    var items_all = try entries_struct.field(1);
    defer items_all.release();
    const keys_arr = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = keys_all.data() };
    const items_arr = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = items_all.data() };

    try std.testing.expectEqual(@as(i32, 1), keys_arr.value(0));
    try std.testing.expectEqual(@as(i32, 2), keys_arr.value(1));
    try std.testing.expectEqual(@as(i32, 3), keys_arr.value(2));
    try std.testing.expectEqual(@as(i32, 10), items_arr.value(0));
    try std.testing.expect(items_arr.isNull(1));
    try std.testing.expectEqual(@as(i32, 30), items_arr.value(2));

    try std.testing.expect(c_array.release == null);
}

fn testNoopReleaseSchema(raw: ?*ArrowSchema) callconv(.c) void {
    if (raw == null) return;
    raw.?.release = null;
    raw.?.private_data = null;
}

test "c data import schema returns explicit top-level struct error" {
    const allocator = std.testing.allocator;

    var c_schema = ArrowSchema{
        .format = "i",
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = testNoopReleaseSchema,
        .private_data = null,
    };

    try std.testing.expectError(error.TopLevelSchemaMustBeStruct, importSchemaOwned(allocator, &c_schema));
}

test "c data import schema rejects released input" {
    const allocator = std.testing.allocator;

    var c_schema = ArrowSchema{
        .format = "+s",
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    try std.testing.expectError(error.Released, importSchemaOwned(allocator, &c_schema));
}

test "c data export schema rejects non-little-endian" {
    const int_ty = DataType{ .int32 = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_ty, .nullable = true },
    };
    const schema = Schema{
        .fields = fields[0..],
        .endianness = .big,
    };

    try std.testing.expectError(error.UnsupportedType, exportSchema(std.testing.allocator, schema));
}

test "c data import array rejects released input" {
    const allocator = std.testing.allocator;

    const int_ty = DataType{ .int32 = {} };
    var c_array = ArrowArray{
        .length = 0,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = null,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    try std.testing.expectError(error.Released, importArray(allocator, &int_ty, &c_array));
}

test "c data map import requires entries_type in target datatype" {
    const allocator = std.testing.allocator;

    const key_ty = DataType{ .int32 = {} };
    const item_ty = DataType{ .int32 = {} };
    const key_field = Field{ .name = "key", .data_type = &key_ty, .nullable = false };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };

    var keys_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer keys_builder.deinit();
    try keys_builder.append(1);
    try keys_builder.append(2);
    var keys_ref = try keys_builder.finish();
    defer keys_ref.release();

    var items_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer items_builder.deinit();
    try items_builder.append(10);
    try items_builder.append(20);
    var items_ref = try items_builder.finish();
    defer items_ref.release();

    var entries_builder = @import("../array/struct_array.zig").StructBuilder.init(allocator, &[_]Field{ key_field, item_field });
    defer entries_builder.deinit();
    try entries_builder.appendValid();
    try entries_builder.appendValid();
    var entries_ref = try entries_builder.finish(&[_]ArrayRef{ keys_ref, items_ref });
    defer entries_ref.release();

    var map_builder = try @import("../array/advanced_array.zig").MapBuilder.init(allocator, 1, key_field, item_field, false);
    defer map_builder.deinit();
    try map_builder.appendLen(2);
    var map_ref = try map_builder.finish(entries_ref);
    defer map_ref.release();

    var c_array = try exportArray(allocator, map_ref);
    const map_ty_no_entries = DataType{
        .map = .{
            .key_field = key_field,
            .item_field = item_field,
            .keys_sorted = false,
            .entries_type = null,
        },
    };

    try std.testing.expectError(error.UnsupportedType, importArray(allocator, &map_ty_no_entries, &c_array));
    try std.testing.expect(c_array.release == null);
}
