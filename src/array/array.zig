const std = @import("std");
const datatype = @import("../datatype.zig");
const buffer = @import("../buffer.zig");

// Public aggregation module that re-exports all array views/builders.

pub const SharedBuffer = buffer.SharedBuffer;
pub const OwnedBuffer = buffer.OwnedBuffer;

pub const DataType = datatype.DataType;
pub const ArrayData = @import("array_data.zig").ArrayData;
pub const ArrayRef = @import("array_ref.zig").ArrayRef;
pub const PrimitiveArray = @import("primitive_array.zig").PrimitiveArray;
pub const PrimitiveBuilder = @import("primitive_array.zig").PrimitiveBuilder;
pub const Int8Array = PrimitiveArray(i8);
pub const Int16Array = PrimitiveArray(i16);
pub const Int32Array = PrimitiveArray(i32);
pub const Int64Array = PrimitiveArray(i64);
pub const Date32Array = PrimitiveArray(i32);
pub const Date64Array = PrimitiveArray(i64);
pub const Time32Array = PrimitiveArray(i32);
pub const Time64Array = PrimitiveArray(i64);
pub const TimestampArray = PrimitiveArray(i64);
pub const DurationArray = PrimitiveArray(i64);
pub const UInt8Array = PrimitiveArray(u8);
pub const UInt16Array = PrimitiveArray(u16);
pub const UInt32Array = PrimitiveArray(u32);
pub const UInt64Array = PrimitiveArray(u64);
pub const Float32Array = PrimitiveArray(f32);
pub const Float64Array = PrimitiveArray(f64);
pub const Int8Builder = PrimitiveBuilder(i8, DataType{ .int8 = {} });
pub const Int16Builder = PrimitiveBuilder(i16, DataType{ .int16 = {} });
pub const Int32Builder = PrimitiveBuilder(i32, DataType{ .int32 = {} });
pub const Int64Builder = PrimitiveBuilder(i64, DataType{ .int64 = {} });
pub const Date32Builder = PrimitiveBuilder(i32, DataType{ .date32 = {} });
pub const Date64Builder = PrimitiveBuilder(i64, DataType{ .date64 = {} });
pub fn Time32Builder(comptime unit: datatype.TimeUnit) type {
    return PrimitiveBuilder(i32, DataType{ .time32 = .{ .unit = unit } });
}
pub fn Time64Builder(comptime unit: datatype.TimeUnit) type {
    return PrimitiveBuilder(i64, DataType{ .time64 = .{ .unit = unit } });
}
pub fn TimestampBuilder(comptime unit: datatype.TimeUnit, comptime timezone: ?[]const u8) type {
    return PrimitiveBuilder(i64, DataType{ .timestamp = .{ .unit = unit, .timezone = timezone } });
}
pub fn DurationBuilder(comptime unit: datatype.TimeUnit) type {
    return PrimitiveBuilder(i64, DataType{ .duration = .{ .unit = unit } });
}
pub const UInt8Builder = PrimitiveBuilder(u8, DataType{ .uint8 = {} });
pub const UInt16Builder = PrimitiveBuilder(u16, DataType{ .uint16 = {} });
pub const UInt32Builder = PrimitiveBuilder(u32, DataType{ .uint32 = {} });
pub const UInt64Builder = PrimitiveBuilder(u64, DataType{ .uint64 = {} });
pub const Float32Builder = PrimitiveBuilder(f32, DataType{ .float = {} });
pub const Float64Builder = PrimitiveBuilder(f64, DataType{ .double = {} });
pub const BooleanArray = @import("boolean_array.zig").BooleanArray;
pub const BooleanBuilder = @import("boolean_array.zig").BooleanBuilder;
pub const StringArray = @import("string_array.zig").StringArray;
pub const LargeStringArray = @import("string_array.zig").LargeStringArray;
pub const StringBuilder = @import("string_array.zig").StringBuilder;
pub const LargeStringBuilder = @import("string_array.zig").LargeStringBuilder;
pub const BinaryArray = @import("binary_array.zig").BinaryArray;
pub const LargeBinaryArray = @import("binary_array.zig").LargeBinaryArray;
pub const BinaryBuilder = @import("binary_array.zig").BinaryBuilder;
pub const LargeBinaryBuilder = @import("binary_array.zig").LargeBinaryBuilder;
pub const ListArray = @import("list_array.zig").ListArray;
pub const LargeListArray = @import("list_array.zig").LargeListArray;
pub const ListBuilder = @import("list_array.zig").ListBuilder;
pub const LargeListBuilder = @import("list_array.zig").LargeListBuilder;
pub const StructArray = @import("struct_array.zig").StructArray;
pub const StructBuilder = @import("struct_array.zig").StructBuilder;
pub const FixedSizeBinaryArray = @import("fixed_size_array.zig").FixedSizeBinaryArray;
pub const FixedSizeBinaryBuilder = @import("fixed_size_array.zig").FixedSizeBinaryBuilder;
pub const FixedSizeListArray = @import("fixed_size_array.zig").FixedSizeListArray;
pub const FixedSizeListBuilder = @import("fixed_size_array.zig").FixedSizeListBuilder;
pub const DictionaryArray = @import("dictionary_array.zig").DictionaryArray;
pub const DictionaryBuilder = @import("dictionary_array.zig").DictionaryBuilder;
pub const MapArray = @import("advanced_array.zig").MapArray;
pub const SparseUnionArray = @import("advanced_array.zig").SparseUnionArray;
pub const DenseUnionArray = @import("advanced_array.zig").DenseUnionArray;
pub const RunEndEncodedArray = @import("advanced_array.zig").RunEndEncodedArray;
pub const MapBuilder = @import("advanced_array.zig").MapBuilder;
pub const SparseUnionBuilder = @import("advanced_array.zig").SparseUnionBuilder;
pub const DenseUnionBuilder = @import("advanced_array.zig").DenseUnionBuilder;
pub const RunEndEncodedBuilder = @import("advanced_array.zig").RunEndEncodedBuilder;

test "date32 aliases build primitive i32 with date32 logical type" {
    var builder = try Date32Builder.init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(18_628);
    try builder.appendNull();
    try builder.append(18_630);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = Date32Array{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i32, 18_630), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .date32);
}

test "date64 aliases build primitive i64 with date64 logical type" {
    var builder = try Date64Builder.init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(1_609_459_200_000);
    try builder.appendNull();
    try builder.append(1_609_545_600_000);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = Date64Array{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i64, 1_609_545_600_000), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .date64);
}

test "time32 builder alias builds primitive i32 with configured unit" {
    var builder = try Time32Builder(.millisecond).init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(1000);
    try builder.appendNull();
    try builder.append(2500);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = Time32Array{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i32, 2500), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .time32);
    try std.testing.expectEqual(datatype.TimeUnit.millisecond, array_handle.data().data_type.time32.unit);
}

test "time64 builder alias builds primitive i64 with configured unit" {
    var builder = try Time64Builder(.nanosecond).init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(1_000_000);
    try builder.appendNull();
    try builder.append(2_500_000);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = Time64Array{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i64, 2_500_000), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .time64);
    try std.testing.expectEqual(datatype.TimeUnit.nanosecond, array_handle.data().data_type.time64.unit);
}

test "timestamp builder alias builds primitive i64 with configured unit/timezone" {
    var builder = try TimestampBuilder(.microsecond, "UTC").init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(1_700_000_000_000_000);
    try builder.appendNull();
    try builder.append(1_700_000_000_123_456);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = TimestampArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i64, 1_700_000_000_123_456), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .timestamp);
    try std.testing.expectEqual(datatype.TimeUnit.microsecond, array_handle.data().data_type.timestamp.unit);
    try std.testing.expectEqualStrings("UTC", array_handle.data().data_type.timestamp.timezone.?);
}

test "duration builder alias builds primitive i64 with configured unit" {
    var builder = try DurationBuilder(.nanosecond).init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(42);
    try builder.appendNull();
    try builder.append(99);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = DurationArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 3), built.len());
    try std.testing.expect(built.isNull(1));
    try std.testing.expectEqual(@as(i64, 99), built.value(2));
    try std.testing.expect(array_handle.data().data_type == .duration);
    try std.testing.expectEqual(datatype.TimeUnit.nanosecond, array_handle.data().data_type.duration.unit);
}

test "timestamp builder alias supports null timezone" {
    var builder = try TimestampBuilder(.second, null).init(std.testing.allocator, 1);
    defer builder.deinit();

    try builder.append(1_700_000_000);

    var array_handle = try builder.finish();
    defer array_handle.release();

    const built = TimestampArray{ .data = array_handle.data() };
    try std.testing.expectEqual(@as(usize, 1), built.len());
    try std.testing.expectEqual(@as(i64, 1_700_000_000), built.value(0));
    try std.testing.expect(array_handle.data().data_type == .timestamp);
    try std.testing.expectEqual(datatype.TimeUnit.second, array_handle.data().data_type.timestamp.unit);
    try std.testing.expect(array_handle.data().data_type.timestamp.timezone == null);
}

test "duration builder alias keeps unit across finishReset reuse" {
    var builder = try DurationBuilder(.millisecond).init(std.testing.allocator, 2);
    defer builder.deinit();

    try builder.append(7);
    var first = try builder.finishReset();
    defer first.release();
    try std.testing.expect(first.data().data_type == .duration);
    try std.testing.expectEqual(datatype.TimeUnit.millisecond, first.data().data_type.duration.unit);

    try builder.append(11);
    var second = try builder.finish();
    defer second.release();

    const built = DurationArray{ .data = second.data() };
    try std.testing.expectEqual(@as(usize, 1), built.len());
    try std.testing.expectEqual(@as(i64, 11), built.value(0));
    try std.testing.expect(second.data().data_type == .duration);
    try std.testing.expectEqual(datatype.TimeUnit.millisecond, second.data().data_type.duration.unit);
}
