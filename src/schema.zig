const std = @import("std");
const datatype = @import("datatype.zig");

pub const Endianness = datatype.Endianness;
pub const Field = datatype.Field;
pub const KeyValue = datatype.KeyValue;

pub const Schema = struct {
    fields: []const Field,
    endianness: Endianness = .native,
    metadata: ?[]const KeyValue = null,
};

test "schema holds fields and defaults" {
    const name_type = datatype.DataType{ .string = {} };
    const id_type = datatype.DataType{ .int64 = {} };

    const fields = [_]Field{
        Field{ .name = "name", .data_type = &name_type, .nullable = false },
        Field{ .name = "id", .data_type = &id_type, .nullable = true },
    };

    const schema = Schema{ .fields = fields[0..] };

    try std.testing.expectEqual(@as(usize, 2), schema.fields.len);
    try std.testing.expectEqual(datatype.Endianness.native, schema.endianness);
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

    const md = schema.metadata orelse unreachable;
    try std.testing.expectEqual(@as(usize, 2), md.len);
    try std.testing.expectEqualStrings("owner", md[0].key);
    try std.testing.expectEqualStrings("analytics", md[0].value);
    try std.testing.expectEqualStrings("version", md[1].key);
    try std.testing.expectEqualStrings("1", md[1].value);
}
