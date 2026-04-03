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
