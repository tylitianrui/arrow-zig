const std = @import("std");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const stream_reader = @import("stream_reader.zig");
const format = @import("format.zig");
const file_writer = @import("file_writer.zig");
const array_data = @import("../array/array_data.zig");
const fb = @import("flatbufferz");
const arrow_fbs = @import("arrow_fbs");

pub const FileMagic = file_writer.FileMagic;

pub const Schema = schema_mod.Schema;
pub const RecordBatch = record_batch.RecordBatch;

pub const FileError = stream_reader.StreamError || array_data.ValidationError || record_batch.RecordBatchError || fb.common.PackError || error{
    OutOfMemory,
    InvalidFile,
    FooterTooLarge,
};

const fbs = struct {
    const Footer = arrow_fbs.org_apache_arrow_flatbuf_Footer.Footer;
    const FooterT = arrow_fbs.org_apache_arrow_flatbuf_Footer.FooterT;
};

const SliceState = struct {
    data: []const u8,
    cursor: usize = 0,
};

const SliceReader = struct {
    state: *SliceState,

    pub const Error = error{EndOfStream};

    pub fn readNoEof(self: @This(), dest: []u8) Error!void {
        const end = std.math.add(usize, self.state.cursor, dest.len) catch return error.EndOfStream;
        if (end > self.state.data.len) return error.EndOfStream;
        @memcpy(dest, self.state.data[self.state.cursor..end]);
        self.state.cursor = end;
    }
};

pub fn FileReader(comptime ReaderType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        loaded: bool = false,
        backing_bytes: []u8 = &.{},
        stream_bytes: []u8 = &.{},
        slice_state: ?*SliceState = null,
        stream: ?stream_reader.StreamReader(SliceReader) = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return .{
                .allocator = allocator,
                .reader = reader,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.stream) |*s| s.deinit();
            self.stream = null;
            if (self.slice_state) |state| self.allocator.destroy(state);
            self.slice_state = null;
            if (self.stream_bytes.len > 0) self.allocator.free(self.stream_bytes);
            self.stream_bytes = &.{};
            if (self.backing_bytes.len > 0) self.allocator.free(self.backing_bytes);
            self.backing_bytes = &.{};
        }

        pub fn readSchema(self: *Self) (FileError || @TypeOf(self.reader).Error || SliceReader.Error)!Schema {
            try self.ensureLoaded();
            const s = &self.stream.?;
            return try s.readSchema();
        }

        pub fn nextRecordBatch(self: *Self) (FileError || @TypeOf(self.reader).Error || SliceReader.Error)!?RecordBatch {
            try self.ensureLoaded();
            const s = &self.stream.?;
            return try s.nextRecordBatch();
        }

        fn ensureLoaded(self: *Self) (FileError || @TypeOf(self.reader).Error)!void {
            if (self.loaded) return;

            var all = std.ArrayList(u8){};
            defer all.deinit(self.allocator);

            var chunk: [4096]u8 = undefined;
            while (true) {
                const read_n = try self.reader.read(&chunk);
                if (read_n == 0) break;
                try all.appendSlice(self.allocator, chunk[0..read_n]);
            }
            self.backing_bytes = try all.toOwnedSlice(self.allocator);

            const message_region = try parseAndValidateFileContainer(self.allocator, self.backing_bytes);
            self.stream_bytes = try self.allocator.alloc(u8, message_region.len + 8);
            @memcpy(self.stream_bytes[0..message_region.len], message_region);

            var eos: [8]u8 = undefined;
            std.mem.writeInt(u32, eos[0..4], format.ContinuationMarker, .little);
            std.mem.writeInt(u32, eos[4..8], 0, .little);
            @memcpy(self.stream_bytes[message_region.len .. message_region.len + eos.len], eos[0..]);

            const state = try self.allocator.create(SliceState);
            state.* = .{ .data = self.stream_bytes };
            self.slice_state = state;

            const reader = SliceReader{ .state = state };
            self.stream = stream_reader.StreamReader(SliceReader).init(self.allocator, reader);
            self.loaded = true;
        }
    };
}

fn parseAndValidateFileContainer(allocator: std.mem.Allocator, bytes: []const u8) FileError![]const u8 {
    // Arrow IPC file header: magic (6 bytes) + 2 padding bytes = 8 bytes total.
    const header_len = FileMagic.len + 2;
    const trailer_len = 4 + FileMagic.len;
    if (bytes.len < header_len + trailer_len) return error.InvalidFile;
    if (!std.mem.eql(u8, bytes[0..FileMagic.len], FileMagic)) return error.InvalidFile;
    if (!std.mem.eql(u8, bytes[bytes.len - FileMagic.len .. bytes.len], FileMagic)) return error.InvalidFile;

    const footer_len_pos = bytes.len - trailer_len;
    const footer_len_u32 = readU32Le(bytes[footer_len_pos .. footer_len_pos + 4]);
    const footer_len = std.math.cast(usize, footer_len_u32) orelse return error.FooterTooLarge;
    const footer_end = footer_len_pos;
    if (footer_len > footer_end - header_len) return error.InvalidFile;
    const footer_start = footer_end - footer_len;
    if (footer_start < header_len) return error.InvalidFile;

    const footer_bytes = bytes[footer_start..footer_end];
    if (!isSaneFlatbufferTable(footer_bytes)) return error.InvalidFile;

    const footer = fbs.Footer.GetRootAs(@constCast(footer_bytes), 0);
    const opts: fb.common.PackOptions = .{ .allocator = allocator };
    var footer_t = try fbs.FooterT.Unpack(footer, opts);
    defer footer_t.deinit(allocator);
    if (footer_t.schema == null) return error.InvalidFile;

    return bytes[header_len..footer_start];
}

fn isSaneFlatbufferTable(buf: []const u8) bool {
    if (buf.len < 8) return false;

    const root_u32 = std.mem.readInt(u32, @ptrCast(buf[0..4]), .little);
    const root = std.math.cast(usize, root_u32) orelse return false;
    if (root > buf.len - 4) return false;

    const rel = std.mem.readInt(i32, @ptrCast(buf[root .. root + 4]), .little);
    if (rel <= 0) return false;
    const rel_usize = std.math.cast(usize, rel) orelse return false;
    if (rel_usize > root) return false;

    const vtable = root - rel_usize;
    if (vtable > buf.len - 4) return false;

    const vtable_len = std.mem.readInt(u16, @ptrCast(buf[vtable .. vtable + 2]), .little);
    const object_len = std.mem.readInt(u16, @ptrCast(buf[vtable + 2 .. vtable + 4]), .little);
    if (vtable_len < 4) return false;

    const vtable_len_usize = @as(usize, vtable_len);
    const object_len_usize = @as(usize, object_len);
    if (vtable + vtable_len_usize > buf.len) return false;
    if (root + object_len_usize > buf.len) return false;

    return true;
}

fn readU32Le(bytes: []const u8) u32 {
    var buf: [4]u8 = undefined;
    @memcpy(buf[0..], bytes[0..4]);
    return std.mem.readInt(u32, &buf, .little);
}

test "ipc file reader roundtrips batches via stream reader" {
    const allocator = std.testing.allocator;

    const zarray = @import("../array/array_ref.zig");
    const prim = @import("../array/primitive_array.zig");
    const str = @import("../array/string_array.zig");
    const DataType = @import("../datatype.zig").DataType;
    const Field = @import("../datatype.zig").Field;

    const id_type = DataType{ .int32 = {} };
    const name_type = DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var id_builder = try prim.PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(10);
    try id_builder.append(20);
    try id_builder.append(30);
    var ids = try id_builder.finish();
    defer ids.release();

    var name_builder = try str.StringBuilder.init(allocator, 3, 16);
    defer name_builder.deinit();
    try name_builder.append("aa");
    try name_builder.appendNull();
    try name_builder.append("cc");
    var names = try name_builder.finish();
    defer names.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]zarray.ArrayRef{ ids, names });
    defer batch.deinit();

    var file_bytes = std.ArrayList(u8){};
    defer file_bytes.deinit(allocator);
    const Sink = struct {
        allocator: std.mem.Allocator,
        out: *std.ArrayList(u8),
        pub const Error = error{OutOfMemory};
        pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
            try self.out.appendSlice(self.allocator, bytes);
        }
    };

    var fw = try file_writer.FileWriter(Sink).init(allocator, .{ .allocator = allocator, .out = &file_bytes });
    defer fw.deinit();
    try fw.writeSchema(schema);
    try fw.writeRecordBatch(batch);
    try fw.writeEnd();

    var fixed = std.io.fixedBufferStream(file_bytes.items);
    var fr = FileReader(@TypeOf(fixed.reader())).init(allocator, fixed.reader());
    defer fr.deinit();

    const out_schema = try fr.readSchema();
    try std.testing.expectEqual(@as(usize, 2), out_schema.fields.len);
    try std.testing.expectEqualStrings("id", out_schema.fields[0].name);
    try std.testing.expectEqualStrings("name", out_schema.fields[1].name);

    const out_batch_opt = try fr.nextRecordBatch();
    try std.testing.expect(out_batch_opt != null);
    var out_batch = out_batch_opt.?;
    defer out_batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), out_batch.numRows());

    const id_arr = prim.PrimitiveArray(i32){ .data = out_batch.columns[0].data() };
    const name_arr = str.StringArray{ .data = out_batch.columns[1].data() };
    try std.testing.expectEqual(@as(i32, 10), id_arr.value(0));
    try std.testing.expectEqual(@as(i32, 20), id_arr.value(1));
    try std.testing.expectEqual(@as(i32, 30), id_arr.value(2));
    try std.testing.expectEqualStrings("aa", name_arr.value(0));
    try std.testing.expect(name_arr.isNull(1));
    try std.testing.expectEqualStrings("cc", name_arr.value(2));

    try std.testing.expect((try fr.nextRecordBatch()) == null);
}
