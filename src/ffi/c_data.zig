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
const extension_name_key = "ARROW:extension:name";
const extension_metadata_key = "ARROW:extension:metadata";

const ExportedSchemaPrivate = struct {
    allocator: std.mem.Allocator,
    format_z: [:0]u8,
    name_z: ?[:0]u8,
    metadata_storage: ?[]u8,
    children_storage: []ArrowSchema,
    children_ptrs: []?*ArrowSchema,
    dict_storage: ?*ArrowSchema,
};

const ParsedFieldMetadata = struct {
    user_metadata: ?[]const datatype.KeyValue,
    extension_name: ?[]const u8,
    extension_metadata: ?[]const u8,
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
        .metadata = try decodeMetadataBlob(a, c_schema.metadata),
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
        .metadata_storage = try encodeMetadataBlob(allocator, schema.metadata),
        .children_storage = &.{},
        .children_ptrs = &.{},
        .dict_storage = null,
    };
    errdefer if (priv.metadata_storage) |m| allocator.free(m);

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
        .metadata = if (priv.metadata_storage) |m| @ptrCast(m.ptr) else null,
        .flags = 0,
        .n_children = std.math.cast(i64, schema.fields.len) orelse return error.InvalidChildren,
        .children = if (priv.children_ptrs.len == 0) null else priv.children_ptrs.ptr,
        .dictionary = null,
        .release = releaseExportedSchema,
        .private_data = priv,
    };
}

fn exportFieldSchema(allocator: std.mem.Allocator, field: Field) Error!ArrowSchema {
    const priv = try allocator.create(ExportedSchemaPrivate);
    errdefer allocator.destroy(priv);

    const dt = field.data_type.*;
    const ext_meta = switch (dt) {
        .extension => |ext| ext,
        else => null,
    };
    const layout_dt = switch (dt) {
        .extension => |ext| ext.storage_type.*,
        else => dt,
    };
    const fmt = try formatFromDataType(allocator, layout_dt);
    errdefer allocator.free(fmt);

    const name_z = try allocator.dupeZ(u8, field.name);
    errdefer allocator.free(name_z);

    priv.* = .{
        .allocator = allocator,
        .format_z = fmt,
        .name_z = name_z,
        .metadata_storage = try encodeFieldMetadataBlob(allocator, field.metadata, ext_meta),
        .children_storage = &.{},
        .children_ptrs = &.{},
        .dict_storage = null,
    };
    errdefer if (priv.metadata_storage) |m| allocator.free(m);

    if (layout_dt == .map) {
        const map_t = layout_dt.map;
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
    } else if (layout_dt == .run_end_encoded) {
        const ree = layout_dt.run_end_encoded;
        const run_end_dt = dataTypeFromIntType(ree.run_end_type) orelse return error.UnsupportedType;
        const run_end_field = Field{ .name = "run_ends", .data_type = &run_end_dt, .nullable = false };
        const values_field = Field{ .name = "values", .data_type = ree.value_type, .nullable = true };

        priv.children_storage = try allocator.alloc(ArrowSchema, 2);
        errdefer allocator.free(priv.children_storage);
        priv.children_ptrs = try allocator.alloc(?*ArrowSchema, 2);
        errdefer allocator.free(priv.children_ptrs);

        priv.children_storage[0] = try exportFieldSchema(allocator, run_end_field);
        priv.children_storage[1] = try exportFieldSchema(allocator, values_field);
        priv.children_ptrs[0] = &priv.children_storage[0];
        priv.children_ptrs[1] = &priv.children_storage[1];
    } else {
        const children = try childFieldsForDataType(allocator, layout_dt);
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
    if (layout_dt == .dictionary) {
        const dict_dt = layout_dt.dictionary.value_type;
        const dict_field = Field{ .name = "dictionary", .data_type = dict_dt, .nullable = true };
        priv.dict_storage = try allocator.create(ArrowSchema);
        errdefer allocator.destroy(priv.dict_storage.?);
        priv.dict_storage.?.* = try exportFieldSchema(allocator, dict_field);
        dict_ptr = priv.dict_storage;
    }

    var flags: i64 = if (field.nullable) ARROW_FLAG_NULLABLE else 0;
    if (layout_dt == .dictionary and layout_dt.dictionary.ordered) {
        flags |= ARROW_FLAG_DICTIONARY_ORDERED;
    }
    if (layout_dt == .map and layout_dt.map.keys_sorted) {
        flags |= ARROW_FLAG_MAP_KEYS_SORTED;
    }

    return ArrowSchema{
        .format = priv.format_z.ptr,
        .name = if (priv.name_z) |n| n.ptr else null,
        .metadata = if (priv.metadata_storage) |m| @ptrCast(m.ptr) else null,
        .flags = flags,
        .n_children = std.math.cast(i64, priv.children_ptrs.len) orelse return error.InvalidChildren,
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
    if (priv.metadata_storage) |m| priv.allocator.free(m);
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
        .length = std.math.cast(i64, data.length) orelse return error.InvalidLength,
        .null_count = if (data.null_count) |n| (std.math.cast(i64, n) orelse return error.InvalidLength) else -1,
        .offset = std.math.cast(i64, data.offset) orelse return error.InvalidLength,
        .n_buffers = std.math.cast(i64, n_buffers) orelse return error.InvalidLength,
        .n_children = std.math.cast(i64, data.children.len) orelse return error.InvalidLength,
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
    const parsed_md = try parseFieldMetadata(allocator, c_schema);
    var dt_value = try importDataType(allocator, c_schema);
    if (parsed_md.extension_name) |ext_name| {
        if (dt_value == .dictionary) {
            const dict = dt_value.dictionary;
            const storage_ptr = try allocator.create(DataType);
            storage_ptr.* = dict.value_type.*;
            const ext_ptr = try allocator.create(DataType);
            ext_ptr.* = .{
                .extension = .{
                    .name = ext_name,
                    .storage_type = storage_ptr,
                    .metadata = parsed_md.extension_metadata,
                },
            };
            dt_value = .{
                .dictionary = .{
                    .id = dict.id,
                    .index_type = dict.index_type,
                    .value_type = ext_ptr,
                    .ordered = dict.ordered,
                },
            };
        } else {
            const storage_ptr = try allocator.create(DataType);
            storage_ptr.* = dt_value;
            dt_value = .{
                .extension = .{
                    .name = ext_name,
                    .storage_type = storage_ptr,
                    .metadata = parsed_md.extension_metadata,
                },
            };
        }
    }

    const dt = try allocator.create(DataType);
    dt.* = dt_value;

    return Field{
        .name = name,
        .data_type = dt,
        .nullable = (c_schema.flags & ARROW_FLAG_NULLABLE) != 0,
        .metadata = parsed_md.user_metadata,
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
    if (std.mem.eql(u8, fmt, "vu")) return DataType{ .string_view = {} };
    if (std.mem.eql(u8, fmt, "vz")) return DataType{ .binary_view = {} };
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
    if (std.mem.eql(u8, fmt, "tiD") or std.mem.eql(u8, fmt, "tDt")) {
        return DataType{ .interval_day_time = .{ .unit = .day_time } };
    }
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
    if (std.mem.eql(u8, fmt, "+vl")) {
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const child_field = try importField(allocator, child);
        return DataType{ .list_view = .{ .value_field = child_field } };
    }
    if (std.mem.eql(u8, fmt, "+vL")) {
        const child = singleChildSchema(c_schema) orelse return error.InvalidChildren;
        const child_field = try importField(allocator, child);
        return DataType{ .large_list_view = .{ .value_field = child_field } };
    }
    if (std.mem.eql(u8, fmt, "+r")) {
        const children = childPtrsSchema(c_schema) orelse return error.InvalidChildren;
        if (children.len != 2) return error.InvalidChildren;
        const run_end_field = try importField(allocator, children[0] orelse return error.InvalidChildren);
        const value_field = try importField(allocator, children[1] orelse return error.InvalidChildren);
        const run_end_type = intTypeFromDataType(run_end_field.data_type.*) orelse return error.InvalidFormat;
        if (!run_end_type.signed) return error.InvalidFormat;
        if (run_end_type.bit_width != 16 and run_end_type.bit_width != 32 and run_end_type.bit_width != 64) {
            return error.InvalidFormat;
        }
        const value_ptr = try allocator.create(DataType);
        value_ptr.* = value_field.data_type.*;
        return DataType{
            .run_end_encoded = .{
                .run_end_type = run_end_type,
                .value_type = value_ptr,
            },
        };
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
    const layout_dt = storageDataType(data_type.*);
    const len = toUsize(c_array.length) orelse return error.InvalidLength;
    const off = toUsize(c_array.offset) orelse return error.InvalidOffset;
    const null_count: ?usize = if (c_array.null_count < 0)
        null
    else
        toUsize(c_array.null_count) orelse return error.InvalidNullCount;

    const is_view_type = layout_dt == .string_view or layout_dt == .binary_view;
    if (is_view_type) {
        if (c_array.n_buffers < 3) return error.InvalidBufferCount;
    } else {
        const expected_buffers = expectedBufferCount(layout_dt) orelse return error.UnsupportedType;
        if (c_array.n_buffers != expected_buffers) return error.InvalidBufferCount;
    }
    const expected_children = expectedChildrenCount(layout_dt);

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
    var view_variadic_lengths: ?[]const i64 = null;
    if (is_view_type) {
        const variadic_count = n_buffers - 3;
        const lengths_len = variadic_count * @sizeOf(i64);
        const lengths_ptr_any = c_array.buffers[n_buffers - 1];
        if (lengths_len > 0 and lengths_ptr_any == null) return error.InvalidBufferCount;
        if (variadic_count == 0) {
            view_variadic_lengths = &[_]i64{};
        } else {
            const lengths_ptr: [*]const u8 = @ptrCast(lengths_ptr_any.?);
            const lengths_buf: []align(@alignOf(i64)) const u8 = @alignCast(lengths_ptr[0..lengths_len]);
            const lengths = std.mem.bytesAsSlice(i64, lengths_buf);
            for (lengths) |one| {
                if (one < 0) return error.InvalidBufferCount;
            }
            view_variadic_lengths = lengths;
        }
    }

    var i: usize = 0;
    while (i < n_buffers) : (i += 1) {
        const needed = neededBufferLen(layout_dt, i, total_len, offsets_i32, offsets_i64, data_buffer_len, view_variadic_lengths, n_buffers) orelse return error.UnsupportedType;
        const ptr_any = c_array.buffers[i];
        buffers[i] = try importBuffer(ptr_any, needed);
        filled_buffers += 1;

        if ((layout_dt == .string or layout_dt == .binary or layout_dt == .list) and i == 1) {
            offsets_i32 = buffers[i].typedSlice(i32);
            data_buffer_len = std.math.cast(usize, offsets_i32.?[total_len]) orelse return error.InvalidOffset;
        }
        if ((layout_dt == .large_string or layout_dt == .large_binary or layout_dt == .large_list) and i == 1) {
            offsets_i64 = buffers[i].typedSlice(i64);
            data_buffer_len = std.math.cast(usize, offsets_i64.?[total_len]) orelse return error.InvalidOffset;
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
    if (layout_dt == .run_end_encoded) {
        const ree = layout_dt.run_end_encoded;
        const run_end_dt = dataTypeFromIntType(ree.run_end_type) orelse return error.UnsupportedType;
        children[0] = try importArrayRecursive(allocator, &run_end_dt, child_ptrs[0] orelse return error.InvalidChildren, owner);
        filled_children += 1;
        children[1] = try importArrayRecursive(allocator, ree.value_type, child_ptrs[1] orelse return error.InvalidChildren, owner);
        filled_children += 1;
    } else {
        const child_types = try childTypesForDataType(allocator, layout_dt);
        defer allocator.free(child_types);

        for (child_types, 0..) |child_ty, idx| {
            const child_ptr = child_ptrs[idx] orelse return error.InvalidChildren;
            children[idx] = try importArrayRecursive(allocator, child_ty, child_ptr, owner);
            filled_children += 1;
        }
    }

    var dict: ?ArrayRef = null;
    errdefer if (dict) |*d| d.release();
    if (layout_dt == .dictionary) {
        if (c_array.dictionary == null) return error.MissingDictionary;
        dict = try importArrayRecursive(allocator, layout_dt.dictionary.value_type, c_array.dictionary.?, owner);
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
    return SharedBuffer.fromSlice(ptr[0..needed_len]);
}

fn neededBufferLen(
    dt: DataType,
    idx: usize,
    total_len: usize,
    offsets_i32: ?[]const i32,
    offsets_i64: ?[]const i64,
    data_buffer_len: usize,
    view_variadic_lengths: ?[]const i64,
    n_buffers: usize,
) ?usize {
    _ = data_buffer_len;
    const layout_dt = storageDataType(dt);

    if (layout_dt == .null) return if (idx == 0) 0 else null;

    if (idx == 0 and hasValidity(layout_dt)) {
        return bitmap.byteLength(total_len);
    }

    return switch (layout_dt) {
        .bool => if (idx == 1) bitmap.byteLength(total_len) else null,
        .int8, .uint8 => if (idx == 1) total_len else null,
        .int16, .uint16, .half_float => if (idx == 1) total_len * 2 else null,
        .int32, .uint32, .float, .date32, .time32, .interval_months, .decimal32 => if (idx == 1) total_len * 4 else null,
        .int64, .uint64, .double, .date64, .timestamp, .time64, .duration, .interval_day_time, .decimal64 => if (idx == 1) total_len * 8 else null,
        .interval_month_day_nano, .decimal128 => if (idx == 1) total_len * 16 else null,
        .decimal256 => if (idx == 1) total_len * 32 else null,
        .fixed_size_binary => |fsb| if (idx == 1) total_len * (std.math.cast(usize, fsb.byte_width) orelse return null) else null,
        .string, .binary, .list => {
            if (idx == 1) return (total_len + 1) * @sizeOf(i32);
            if (idx == 2) {
                const offs = offsets_i32 orelse return null;
                return std.math.cast(usize, offs[total_len]) orelse return null;
            }
            return null;
        },
        .map => {
            if (idx == 1) return (total_len + 1) * @sizeOf(i32);
            return null;
        },
        .list_view => {
            if (idx == 1) return total_len * @sizeOf(i32);
            if (idx == 2) return total_len * @sizeOf(i32);
            return null;
        },
        .large_list_view => {
            if (idx == 1) return total_len * @sizeOf(i64);
            if (idx == 2) return total_len * @sizeOf(i64);
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
                return std.math.cast(usize, offs[total_len]) orelse return null;
            }
            return null;
        },
        .string_view, .binary_view => {
            if (n_buffers < 3) return null;
            const variadic_count = n_buffers - 3;
            if (idx == 1) return total_len * @sizeOf(u128);
            if (idx >= 2 and idx < 2 + variadic_count) {
                const lengths = view_variadic_lengths orelse return null;
                const one = lengths[idx - 2];
                if (one < 0) return null;
                return std.math.cast(usize, one);
            }
            if (idx == n_buffers - 1) return variadic_count * @sizeOf(i64);
            return null;
        },
        .struct_, .fixed_size_list => null,
        .dictionary => |dict| if (idx == 1) total_len * (@as(usize, dict.index_type.bit_width) / 8) else null,
        else => null,
    };
}

fn hasValidity(dt: DataType) bool {
    return switch (storageDataType(dt)) {
        .null, .sparse_union, .dense_union => false,
        else => true,
    };
}

fn expectedBufferCount(dt: DataType) ?i64 {
    return switch (storageDataType(dt)) {
        .null => 0,
        .run_end_encoded => 0,
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
        .string, .binary, .large_string, .large_binary, .list, .large_list, .list_view, .large_list_view => 3,
        else => null,
    };
}

fn expectedChildrenCount(dt: DataType) i64 {
    return switch (storageDataType(dt)) {
        .list, .large_list, .fixed_size_list, .map, .list_view, .large_list_view => 1,
        .run_end_encoded => 2,
        .struct_ => |s| @intCast(s.fields.len),
        .sparse_union => |u| @intCast(u.fields.len),
        .dense_union => |u| @intCast(u.fields.len),
        else => 0,
    };
}

fn childTypesForDataType(allocator: std.mem.Allocator, dt: DataType) Error![]const *const DataType {
    return switch (storageDataType(dt)) {
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
        .list_view => |l| blk: {
            const out = try allocator.alloc(*const DataType, 1);
            out[0] = l.value_field.data_type;
            break :blk out;
        },
        .large_list_view => |l| blk: {
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
    return switch (storageDataType(dt)) {
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
        .list_view => |l| blk: {
            const out = try allocator.alloc(Field, 1);
            out[0] = l.value_field;
            break :blk out;
        },
        .large_list_view => |l| blk: {
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
    const layout_dt = storageDataType(dt);
    return switch (layout_dt) {
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
        .string_view => try allocator.dupeZ(u8, "vu"),
        .binary_view => try allocator.dupeZ(u8, "vz"),
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
        .interval_day_time => try allocator.dupeZ(u8, "tiD"),
        .interval_month_day_nano => try allocator.dupeZ(u8, "tin"),
        .fixed_size_binary => |fsb| allocPrintZ(allocator, "w:{d}", .{fsb.byte_width}),
        .list => try allocator.dupeZ(u8, "+l"),
        .large_list => try allocator.dupeZ(u8, "+L"),
        .list_view => try allocator.dupeZ(u8, "+vl"),
        .large_list_view => try allocator.dupeZ(u8, "+vL"),
        .map => try allocator.dupeZ(u8, "+m"),
        .run_end_encoded => try allocator.dupeZ(u8, "+r"),
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

fn dataTypeFromIntType(int_type: IntType) ?DataType {
    const id = int_type.toTypeId() orelse return null;
    return switch (id) {
        .int8 => DataType{ .int8 = {} },
        .uint8 => DataType{ .uint8 = {} },
        .int16 => DataType{ .int16 = {} },
        .uint16 => DataType{ .uint16 = {} },
        .int32 => DataType{ .int32 = {} },
        .uint32 => DataType{ .uint32 = {} },
        .int64 => DataType{ .int64 = {} },
        .uint64 => DataType{ .uint64 = {} },
        else => null,
    };
}

fn storageDataType(dt: DataType) DataType {
    return switch (dt) {
        .extension => |ext| storageDataType(ext.storage_type.*),
        else => dt,
    };
}

fn intTypeFromDataType(dt: DataType) ?IntType {
    return IntType.fromTypeId(dt.id());
}

fn appendI32Le(list: *std.array_list.Managed(u8), value: i32) Error!void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, bytes[0..4], value, .little);
    try list.appendSlice(bytes[0..4]);
}

fn appendMetadataEntry(list: *std.array_list.Managed(u8), key: []const u8, value: []const u8) Error!void {
    const key_len = std.math.cast(i32, key.len) orelse return error.InvalidFormat;
    const value_len = std.math.cast(i32, value.len) orelse return error.InvalidFormat;
    try appendI32Le(list, key_len);
    try list.appendSlice(key);
    try appendI32Le(list, value_len);
    try list.appendSlice(value);
}

fn encodeMetadataBlob(allocator: std.mem.Allocator, metadata: ?[]const datatype.KeyValue) Error!?[]u8 {
    const md = metadata orelse return null;
    if (md.len == 0) return null;

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    const count = std.math.cast(i32, md.len) orelse return error.InvalidFormat;
    try appendI32Le(&out, count);
    for (md) |entry| {
        try appendMetadataEntry(&out, entry.key, entry.value);
    }
    return @as(?[]u8, try out.toOwnedSlice());
}

fn encodeFieldMetadataBlob(
    allocator: std.mem.Allocator,
    metadata: ?[]const datatype.KeyValue,
    extension_meta: ?datatype.ExtensionType,
) Error!?[]u8 {
    const base_len: usize = if (metadata) |m| m.len else 0;
    const ext_len: usize = if (extension_meta) |ext| if (ext.metadata != null) 2 else 1 else 0;
    const total = base_len + ext_len;
    if (total == 0) return null;

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    const count = std.math.cast(i32, total) orelse return error.InvalidFormat;
    try appendI32Le(&out, count);
    if (metadata) |md| {
        for (md) |entry| {
            try appendMetadataEntry(&out, entry.key, entry.value);
        }
    }
    if (extension_meta) |ext| {
        try appendMetadataEntry(&out, extension_name_key, ext.name);
        if (ext.metadata) |ext_md| {
            try appendMetadataEntry(&out, extension_metadata_key, ext_md);
        }
    }
    return @as(?[]u8, try out.toOwnedSlice());
}

fn readI32LeFromMetadata(ptr: [*]const u8, cursor: *usize) i32 {
    var bytes: [4]u8 = undefined;
    bytes[0] = ptr[cursor.* + 0];
    bytes[1] = ptr[cursor.* + 1];
    bytes[2] = ptr[cursor.* + 2];
    bytes[3] = ptr[cursor.* + 3];
    cursor.* += 4;
    return std.mem.readInt(i32, bytes[0..4], .little);
}

fn decodeMetadataBlob(allocator: std.mem.Allocator, ptr: [*c]const u8) Error!?[]const datatype.KeyValue {
    if (ptr == null) return null;

    const bytes: [*]const u8 = @ptrCast(ptr);
    var cursor: usize = 0;
    const count_i32 = readI32LeFromMetadata(bytes, &cursor);
    if (count_i32 < 0) return error.InvalidFormat;
    const count = std.math.cast(usize, count_i32) orelse return error.InvalidFormat;
    if (count == 0) return null;

    const out = try allocator.alloc(datatype.KeyValue, count);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |kv| {
            allocator.free(kv.key);
            allocator.free(kv.value);
        }
        allocator.free(out);
    }

    while (filled < count) : (filled += 1) {
        const key_len_i32 = readI32LeFromMetadata(bytes, &cursor);
        if (key_len_i32 < 0) return error.InvalidFormat;
        const key_len = std.math.cast(usize, key_len_i32) orelse return error.InvalidFormat;
        const key = try allocator.dupe(u8, bytes[cursor .. cursor + key_len]);
        cursor += key_len;

        const value_len_i32 = readI32LeFromMetadata(bytes, &cursor);
        if (value_len_i32 < 0) return error.InvalidFormat;
        const value_len = std.math.cast(usize, value_len_i32) orelse return error.InvalidFormat;
        const value = try allocator.dupe(u8, bytes[cursor .. cursor + value_len]);
        cursor += value_len;

        out[filled] = .{ .key = key, .value = value };
    }

    return out;
}

fn parseFieldMetadata(allocator: std.mem.Allocator, c_schema: *ArrowSchema) Error!ParsedFieldMetadata {
    const all_md = try decodeMetadataBlob(allocator, c_schema.metadata);
    const metadata = all_md orelse return .{
        .user_metadata = null,
        .extension_name = null,
        .extension_metadata = null,
    };

    var extension_name: ?[]const u8 = null;
    var extension_metadata: ?[]const u8 = null;
    var user_count: usize = 0;
    for (metadata) |entry| {
        if (std.mem.eql(u8, entry.key, extension_name_key)) {
            if (extension_name != null) return error.InvalidFormat;
            extension_name = entry.value;
            continue;
        }
        if (std.mem.eql(u8, entry.key, extension_metadata_key)) {
            if (extension_metadata != null) return error.InvalidFormat;
            extension_metadata = entry.value;
            continue;
        }
        user_count += 1;
    }
    if (extension_metadata != null and extension_name == null) return error.InvalidFormat;

    if (user_count == metadata.len) {
        return .{
            .user_metadata = metadata,
            .extension_name = extension_name,
            .extension_metadata = extension_metadata,
        };
    }
    if (user_count == 0) {
        return .{
            .user_metadata = null,
            .extension_name = extension_name,
            .extension_metadata = extension_metadata,
        };
    }

    const user_md = try allocator.alloc(datatype.KeyValue, user_count);
    var idx: usize = 0;
    for (metadata) |entry| {
        if (std.mem.eql(u8, entry.key, extension_name_key) or std.mem.eql(u8, entry.key, extension_metadata_key)) {
            continue;
        }
        user_md[idx] = entry;
        idx += 1;
    }

    return .{
        .user_metadata = user_md,
        .extension_name = extension_name,
        .extension_metadata = extension_metadata,
    };
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
    for (payload) |c| {
        if (c == ',') count += 1;
    }
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
    var list = std.array_list.Managed(u8).init(allocator);
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

test "c data schema export/import roundtrip preserves extension metadata" {
    const allocator = std.testing.allocator;

    const storage_ty = DataType{ .int32 = {} };
    const ext_ty = DataType{
        .extension = .{
            .name = "com.example.int32_ext",
            .storage_type = &storage_ty,
            .metadata = "v1",
        },
    };
    const field_md = [_]datatype.KeyValue{
        .{ .key = "owner", .value = "ffi" },
    };
    const fields = [_]Field{
        .{ .name = "ext_i32", .data_type = &ext_ty, .nullable = true, .metadata = field_md[0..] },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();

    try std.testing.expectEqual(@as(usize, 1), imported.schema.fields.len);
    try std.testing.expect(imported.schema.fields[0].data_type.* == .extension);
    const ext = imported.schema.fields[0].data_type.extension;
    try std.testing.expectEqualStrings("com.example.int32_ext", ext.name);
    try std.testing.expectEqualStrings("v1", ext.metadata.?);
    try std.testing.expect(ext.storage_type.* == .int32);
    try std.testing.expect(imported.schema.fields[0].metadata != null);
    try std.testing.expectEqual(@as(usize, 1), imported.schema.fields[0].metadata.?.len);
    try std.testing.expectEqualStrings("owner", imported.schema.fields[0].metadata.?[0].key);
    try std.testing.expectEqualStrings("ffi", imported.schema.fields[0].metadata.?[0].value);
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

test "c data array export/import handles extension layout via storage type" {
    const allocator = std.testing.allocator;

    const storage_ty = DataType{ .int32 = {} };
    const ext_ty = DataType{
        .extension = .{
            .name = "com.example.int32_ext",
            .storage_type = &storage_ty,
            .metadata = "v1",
        },
    };

    var values_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer values_builder.deinit();
    try values_builder.append(7);
    try values_builder.appendNull();
    try values_builder.append(11);
    var values = try values_builder.finish();
    defer values.release();

    var ext_builder = try @import("../array/extension_array.zig").ExtensionBuilder.init(allocator, ext_ty.extension);
    defer ext_builder.deinit();
    var ext_arr = try ext_builder.finish(values);
    defer ext_arr.release();

    var c_array = try exportArray(allocator, ext_arr);
    var imported = try importArray(allocator, &ext_ty, &c_array);
    defer imported.release();

    try std.testing.expect(imported.data().data_type == .extension);
    const ext = imported.data().data_type.extension;
    try std.testing.expectEqualStrings("com.example.int32_ext", ext.name);
    try std.testing.expectEqualStrings("v1", ext.metadata.?);
    try std.testing.expect(ext.storage_type.* == .int32);

    const ints = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = imported.data() };
    try std.testing.expectEqual(@as(i32, 7), ints.value(0));
    try std.testing.expect(ints.isNull(1));
    try std.testing.expectEqual(@as(i32, 11), ints.value(2));

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
    try std.testing.expectEqualStrings("tiD", cString(children[3].?.format).?);
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

test "c data schema supports sparse and dense union formats" {
    const allocator = std.testing.allocator;

    const a_ty = DataType{ .int32 = {} };
    const b_ty = DataType{ .string = {} };
    const c_ty = DataType{ .bool = {} };
    const children = [_]Field{
        .{ .name = "a", .data_type = &a_ty, .nullable = true },
        .{ .name = "b", .data_type = &b_ty, .nullable = true },
        .{ .name = "c", .data_type = &c_ty, .nullable = true },
    };
    const type_ids = [_]i8{ 0, 1, 2 };
    const sparse_ty = DataType{
        .sparse_union = .{
            .type_ids = type_ids[0..],
            .fields = children[0..],
            .mode = .sparse,
        },
    };
    const dense_ty = DataType{
        .dense_union = .{
            .type_ids = type_ids[0..],
            .fields = children[0..],
            .mode = .dense,
        },
    };
    const root_fields = [_]Field{
        .{ .name = "su", .data_type = &sparse_ty, .nullable = false },
        .{ .name = "du", .data_type = &dense_ty, .nullable = false },
    };
    const schema = Schema{ .fields = root_fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const top_children = childPtrsSchema(&c_schema).?;
    try std.testing.expectEqual(@as(usize, 2), top_children.len);
    try std.testing.expectEqualStrings("+us:0,1,2", cString(top_children[0].?.format).?);
    try std.testing.expectEqualStrings("+ud:0,1,2", cString(top_children[1].?.format).?);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();
    try std.testing.expect(imported.schema.fields[0].data_type.* == .sparse_union);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .dense_union);
    try std.testing.expectEqual(@as(usize, 3), imported.schema.fields[0].data_type.sparse_union.type_ids.len);
    try std.testing.expectEqual(@as(i8, 0), imported.schema.fields[0].data_type.sparse_union.type_ids[0]);
    try std.testing.expectEqual(@as(i8, 1), imported.schema.fields[0].data_type.sparse_union.type_ids[1]);
    try std.testing.expectEqual(@as(i8, 2), imported.schema.fields[0].data_type.sparse_union.type_ids[2]);
    try std.testing.expectEqual(@as(usize, 3), imported.schema.fields[1].data_type.dense_union.type_ids.len);
}

test "c data import union rejects invalid type id payload" {
    const allocator = std.testing.allocator;

    var c_schema = ArrowSchema{
        .format = "+us:0,,2",
        .name = "u",
        .metadata = null,
        .flags = ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    try std.testing.expectError(error.InvalidFormat, importDataType(allocator, &c_schema));
}

test "c data schema supports run-end-encoded format" {
    const allocator = std.testing.allocator;

    const value_ty = DataType{ .int32 = {} };
    const ree_ty = DataType{
        .run_end_encoded = .{
            .run_end_type = .{ .bit_width = 32, .signed = true },
            .value_type = &value_ty,
        },
    };
    const fields = [_]Field{
        .{ .name = "ree", .data_type = &ree_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const root_children = childPtrsSchema(&c_schema).?;
    const ree_schema = root_children[0].?;
    try std.testing.expectEqualStrings("+r", cString(ree_schema.format).?);
    const ree_children = childPtrsSchema(ree_schema).?;
    try std.testing.expectEqual(@as(usize, 2), ree_children.len);
    try std.testing.expectEqualStrings("i", cString(ree_children[0].?.format).?);
    try std.testing.expectEqualStrings("i", cString(ree_children[1].?.format).?);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();
    try std.testing.expect(imported.schema.fields[0].data_type.* == .run_end_encoded);
    try std.testing.expectEqual(@as(u8, 32), imported.schema.fields[0].data_type.run_end_encoded.run_end_type.bit_width);
    try std.testing.expect(imported.schema.fields[0].data_type.run_end_encoded.run_end_type.signed);
    try std.testing.expect(imported.schema.fields[0].data_type.run_end_encoded.value_type.* == .int32);
}

test "c data array import supports run-end-encoded" {
    const allocator = std.testing.allocator;

    var values_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer values_builder.deinit();
    try values_builder.append(7);
    try values_builder.append(9);
    var values_ref = try values_builder.finish();
    defer values_ref.release();

    const value_ty = DataType{ .int32 = {} };
    var ree_builder = try @import("../array/advanced_array.zig").RunEndEncodedBuilder.init(
        allocator,
        .{ .bit_width = 32, .signed = true },
        &value_ty,
        2,
    );
    defer ree_builder.deinit();
    try ree_builder.appendRunEnd(2);
    try ree_builder.appendRunEnd(5);
    var ree_ref = try ree_builder.finish(values_ref);
    defer ree_ref.release();

    var c_array = try exportArray(allocator, ree_ref);
    var imported = try importArray(allocator, &ree_ref.data().data_type, &c_array);
    defer imported.release();

    try std.testing.expect(imported.data().data_type == .run_end_encoded);
    try std.testing.expectEqual(@as(usize, 5), imported.data().length);
    try std.testing.expectEqual(@as(usize, 0), imported.data().buffers.len);
    try std.testing.expectEqual(@as(usize, 2), imported.data().children.len);

    const run_ends = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = imported.data().children[0].data() };
    const values = @import("../array/primitive_array.zig").PrimitiveArray(i32){ .data = imported.data().children[1].data() };
    try std.testing.expectEqual(@as(usize, 2), run_ends.len());
    try std.testing.expectEqual(@as(i32, 2), run_ends.value(0));
    try std.testing.expectEqual(@as(i32, 5), run_ends.value(1));
    try std.testing.expectEqual(@as(usize, 2), values.len());
    try std.testing.expectEqual(@as(i32, 7), values.value(0));
    try std.testing.expectEqual(@as(i32, 9), values.value(1));
    try std.testing.expect(c_array.release == null);
}

test "c data schema supports list_view and large_list_view formats" {
    const allocator = std.testing.allocator;

    const item_ty = DataType{ .int32 = {} };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };
    const lv_ty = DataType{ .list_view = .{ .value_field = item_field } };
    const llv_ty = DataType{ .large_list_view = .{ .value_field = item_field } };
    const fields = [_]Field{
        .{ .name = "lv", .data_type = &lv_ty, .nullable = true },
        .{ .name = "llv", .data_type = &llv_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const root_children = childPtrsSchema(&c_schema).?;
    try std.testing.expectEqual(@as(usize, 2), root_children.len);
    try std.testing.expectEqualStrings("+vl", cString(root_children[0].?.format).?);
    try std.testing.expectEqualStrings("+vL", cString(root_children[1].?.format).?);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();
    try std.testing.expect(imported.schema.fields[0].data_type.* == .list_view);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .large_list_view);
}

test "c data schema supports string_view and binary_view formats" {
    const allocator = std.testing.allocator;

    const sv_ty = DataType{ .string_view = {} };
    const bv_ty = DataType{ .binary_view = {} };
    const fields = [_]Field{
        .{ .name = "sv", .data_type = &sv_ty, .nullable = true },
        .{ .name = "bv", .data_type = &bv_ty, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var c_schema = try exportSchema(allocator, schema);
    defer if (c_schema.release) |release_fn| release_fn(&c_schema);

    const root_children = childPtrsSchema(&c_schema).?;
    try std.testing.expectEqual(@as(usize, 2), root_children.len);
    try std.testing.expectEqualStrings("vu", cString(root_children[0].?.format).?);
    try std.testing.expectEqualStrings("vz", cString(root_children[1].?.format).?);

    var imported = try importSchemaOwned(allocator, &c_schema);
    defer imported.deinit();
    try std.testing.expect(imported.schema.fields[0].data_type.* == .string_view);
    try std.testing.expect(imported.schema.fields[1].data_type.* == .binary_view);
}

test "c data array import supports list_view and large_list_view" {
    const allocator = std.testing.allocator;

    var child_values = [_]i32{ 11, 22, 33, 44, 55 };
    const child_bytes = std.mem.sliceAsBytes(child_values[0..]);
    const child_layout = ArrayData{
        .data_type = DataType{ .int32 = {} },
        .length = child_values.len,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{ SharedBuffer.empty, SharedBuffer.fromSlice(child_bytes) },
    };
    var child_ref = try ArrayRef.fromBorrowed(allocator, child_layout);
    defer child_ref.release();

    const item_ty = DataType{ .int32 = {} };
    const item_field = Field{ .name = "item", .data_type = &item_ty, .nullable = true };

    var lv_offsets = [_]i32{ 0, 2 };
    var lv_sizes = [_]i32{ 2, 3 };
    const lv_layout = ArrayData{
        .data_type = DataType{ .list_view = .{ .value_field = item_field } },
        .length = 2,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(lv_offsets[0..])),
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(lv_sizes[0..])),
        },
        .children = &[_]ArrayRef{child_ref},
    };
    var lv_ref = try ArrayRef.fromBorrowed(allocator, lv_layout);
    defer lv_ref.release();

    var lv_c_array = try exportArray(allocator, lv_ref);
    var lv_imported = try importArray(allocator, &lv_ref.data().data_type, &lv_c_array);
    defer lv_imported.release();
    try std.testing.expect(lv_imported.data().data_type == .list_view);
    try std.testing.expectEqual(@as(usize, 2), lv_imported.data().length);
    try std.testing.expectEqual(@as(usize, 3), lv_imported.data().buffers.len);
    const lv_offs = lv_imported.data().buffers[1].typedSlice(i32);
    const lv_szs = lv_imported.data().buffers[2].typedSlice(i32);
    try std.testing.expectEqual(@as(i32, 0), lv_offs[0]);
    try std.testing.expectEqual(@as(i32, 2), lv_offs[1]);
    try std.testing.expectEqual(@as(i32, 2), lv_szs[0]);
    try std.testing.expectEqual(@as(i32, 3), lv_szs[1]);
    try std.testing.expect(lv_c_array.release == null);

    var llv_offsets = [_]i64{ 1, 3 };
    var llv_sizes = [_]i64{ 2, 2 };
    const llv_layout = ArrayData{
        .data_type = DataType{ .large_list_view = .{ .value_field = item_field } },
        .length = 2,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.empty,
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(llv_offsets[0..])),
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(llv_sizes[0..])),
        },
        .children = &[_]ArrayRef{child_ref},
    };
    var llv_ref = try ArrayRef.fromBorrowed(allocator, llv_layout);
    defer llv_ref.release();

    var llv_c_array = try exportArray(allocator, llv_ref);
    var llv_imported = try importArray(allocator, &llv_ref.data().data_type, &llv_c_array);
    defer llv_imported.release();
    try std.testing.expect(llv_imported.data().data_type == .large_list_view);
    try std.testing.expectEqual(@as(usize, 2), llv_imported.data().length);
    try std.testing.expectEqual(@as(usize, 3), llv_imported.data().buffers.len);
    const llv_offs = llv_imported.data().buffers[1].typedSlice(i64);
    const llv_szs = llv_imported.data().buffers[2].typedSlice(i64);
    try std.testing.expectEqual(@as(i64, 1), llv_offs[0]);
    try std.testing.expectEqual(@as(i64, 3), llv_offs[1]);
    try std.testing.expectEqual(@as(i64, 2), llv_szs[0]);
    try std.testing.expectEqual(@as(i64, 2), llv_szs[1]);
    try std.testing.expect(llv_c_array.release == null);
}

test "c data array import supports string_view and binary_view buffers" {
    const allocator = std.testing.allocator;

    var valid_bits = [_]u8{0x01};
    var sv_views = [_]u8{0} ** 16;
    var sv_data = [_]u8{'x'};
    var sv_lens = [_]i64{@as(i64, sv_data.len)};
    const sv_layout = ArrayData{
        .data_type = DataType{ .string_view = {} },
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.fromSlice(valid_bits[0..]),
            SharedBuffer.fromSlice(sv_views[0..]),
            SharedBuffer.fromSlice(sv_data[0..]),
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(sv_lens[0..])),
        },
    };
    var sv_ref = try ArrayRef.fromBorrowed(allocator, sv_layout);
    defer sv_ref.release();

    var sv_c_array = try exportArray(allocator, sv_ref);
    var sv_imported = try importArray(allocator, &sv_ref.data().data_type, &sv_c_array);
    defer sv_imported.release();
    try std.testing.expect(sv_imported.data().data_type == .string_view);
    try std.testing.expectEqual(@as(usize, 4), sv_imported.data().buffers.len);
    try std.testing.expectEqual(@as(usize, 16), sv_imported.data().buffers[1].len());
    try std.testing.expectEqual(@as(usize, 1), sv_imported.data().buffers[2].len());
    try std.testing.expectEqual(@as(usize, 8), sv_imported.data().buffers[3].len());
    try std.testing.expect(sv_c_array.release == null);

    var bv_views = [_]u8{0} ** 16;
    const empty_lens = [_]i64{};
    const bv_layout = ArrayData{
        .data_type = DataType{ .binary_view = {} },
        .length = 1,
        .null_count = 0,
        .buffers = &[_]SharedBuffer{
            SharedBuffer.fromSlice(valid_bits[0..]),
            SharedBuffer.fromSlice(bv_views[0..]),
            SharedBuffer.fromSlice(std.mem.sliceAsBytes(empty_lens[0..])),
        },
    };
    var bv_ref = try ArrayRef.fromBorrowed(allocator, bv_layout);
    defer bv_ref.release();

    var bv_c_array = try exportArray(allocator, bv_ref);
    var bv_imported = try importArray(allocator, &bv_ref.data().data_type, &bv_c_array);
    defer bv_imported.release();
    try std.testing.expect(bv_imported.data().data_type == .binary_view);
    try std.testing.expectEqual(@as(usize, 3), bv_imported.data().buffers.len);
    try std.testing.expectEqual(@as(usize, 16), bv_imported.data().buffers[1].len());
    try std.testing.expectEqual(@as(usize, 0), bv_imported.data().buffers[2].len());
    try std.testing.expect(bv_c_array.release == null);
}
