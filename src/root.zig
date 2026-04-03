// Re-export the public buffer types from the package root.
const buffer = @import("buffer.zig");
const bitmap = @import("bitmap.zig");
const datatype = @import("datatype.zig");
const schema = @import("schema.zig");
const array = @import("array/array.zig");

pub const Buffer = buffer.Buffer;
pub const MutableBuffer = buffer.MutableBuffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const MutableValidityBitmap = bitmap.MutableValidityBitmap;
pub const DataType = datatype.DataType;
pub const TypeId = datatype.TypeId;
pub const TimeUnit = datatype.TimeUnit;
pub const IntervalUnit = datatype.IntervalUnit;
pub const UnionMode = datatype.UnionMode;
pub const FloatPrecision = datatype.FloatPrecision;
pub const Endianness = datatype.Endianness;
pub const IntType = datatype.IntType;
pub const DecimalParams = datatype.DecimalParams;
pub const FixedSizeBinaryType = datatype.FixedSizeBinaryType;
pub const TimestampType = datatype.TimestampType;
pub const TimeType = datatype.TimeType;
pub const DurationType = datatype.DurationType;
pub const IntervalType = datatype.IntervalType;
pub const KeyValue = datatype.KeyValue;
pub const Field = datatype.Field;
pub const StructType = datatype.StructType;
pub const ListType = datatype.ListType;
pub const ListViewType = datatype.ListViewType;
pub const FixedSizeListType = datatype.FixedSizeListType;
pub const MapType = datatype.MapType;
pub const UnionType = datatype.UnionType;
pub const DictionaryType = datatype.DictionaryType;
pub const RunEndEncodedType = datatype.RunEndEncodedType;
pub const ExtensionType = datatype.ExtensionType;
pub const Schema = schema.Schema;
pub const ArrayData = array.ArrayData;
pub const PrimitiveArray = array.PrimitiveArray;
pub const PrimitiveBuilder = array.PrimitiveBuilder;
pub const Int32Array = array.Int32Array;
pub const Float64Array = array.Float64Array;
pub const BooleanArray = array.BooleanArray;
pub const StringArray = array.StringArray;
pub const BinaryArray = array.BinaryArray;
pub const Int32Builder = array.Int32Builder;
pub const BooleanBuilder = array.BooleanBuilder;
pub const StringBuilder = array.StringBuilder;

// Pull buffer tests into the root test target.
test {
    _ = @import("buffer.zig");
    _ = @import("bitmap.zig");
    _ = @import("datatype.zig");
    _ = @import("schema.zig");
    _ = @import("array/array.zig");
    _ = @import("array/array_data.zig");
    _ = @import("array/primitive_array.zig");
    _ = @import("array/boolean_array.zig");
    _ = @import("array/string_array.zig");
    _ = @import("array/binary_array.zig");
}
