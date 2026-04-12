#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/config.h>

#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace {

#if !defined(ARROW_VERSION_MAJOR) || (ARROW_VERSION_MAJOR < 23)
#error "tools/interop/cpp/interop.cpp requires Arrow C++ >= 23.0.0 (view + complex interop paths)"
#endif

enum class ContainerMode {
  kStream,
  kFile,
};

std::shared_ptr<arrow::Schema> CanonicalSchema() {
  return arrow::schema({
      arrow::field("id", arrow::int32(), false),
      arrow::field("name", arrow::utf8(), true),
  });
}

arrow::Status Generate(const std::string& path, ContainerMode container) {
  // Writes one stream with:
  // - schema: id: int32 (non-null), name: utf8 (nullable)
  // - one record batch (3 rows)
  //   id=[1, 2, 3]
  //   name=["alice", null, "bob"]
  auto schema = CanonicalSchema();

  arrow::Int32Builder id_builder;
  ARROW_RETURN_NOT_OK(id_builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> ids;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&ids));

  arrow::StringBuilder name_builder;
  ARROW_RETURN_NOT_OK(name_builder.Append("alice"));
  ARROW_RETURN_NOT_OK(name_builder.AppendNull());
  ARROW_RETURN_NOT_OK(name_builder.Append("bob"));
  std::shared_ptr<arrow::Array> names;
  ARROW_RETURN_NOT_OK(name_builder.Finish(&names));

  std::vector<std::shared_ptr<arrow::Array>> columns = {ids, names};
  auto batch = arrow::RecordBatch::Make(schema, 3, std::move(columns));
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

arrow::Status GenerateDictDelta(const std::string& path, ContainerMode container) {
  // Writes one stream with dictionary-encoded column "color":
  // - schema: color: dictionary<int32, utf8>
  // - two record batches to exercise dictionary delta behavior
  //   batch1 decoded values=["red", "blue"]
  //   batch2 decoded values=["green"]
  auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8(), false);
  auto schema = arrow::schema({arrow::field("color", dict_type, false)});

  arrow::StringBuilder first_strings;
  ARROW_RETURN_NOT_OK(first_strings.Append("red"));
  ARROW_RETURN_NOT_OK(first_strings.Append("blue"));
  std::shared_ptr<arrow::Array> first_plain;
  ARROW_RETURN_NOT_OK(first_strings.Finish(&first_plain));
  ARROW_ASSIGN_OR_RAISE(auto first_encoded_datum, arrow::compute::CallFunction("dictionary_encode", {first_plain}));
  auto first_encoded = first_encoded_datum.make_array();
  std::vector<std::shared_ptr<arrow::Array>> first_columns = {first_encoded};
  auto first_batch = arrow::RecordBatch::Make(schema, 2, std::move(first_columns));

  arrow::StringBuilder second_strings;
  ARROW_RETURN_NOT_OK(second_strings.Append("green"));
  std::shared_ptr<arrow::Array> second_plain;
  ARROW_RETURN_NOT_OK(second_strings.Finish(&second_plain));
  ARROW_ASSIGN_OR_RAISE(auto second_encoded_datum, arrow::compute::CallFunction("dictionary_encode", {second_plain}));
  auto second_encoded = second_encoded_datum.make_array();
  std::vector<std::shared_ptr<arrow::Array>> second_columns = {second_encoded};
  auto second_batch = arrow::RecordBatch::Make(schema, 1, std::move(second_columns));

  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*first_batch));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*second_batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*first_batch));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*second_batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

arrow::Status GenerateRee(const std::string& path, ContainerMode container) {
  // Writes one stream with:
  // - schema: ree: run_end_encoded<int32, int32>
  // - one record batch (5 rows)
  //   run_ends=[2, 5], values=[100, 200]
  //   decoded logical values=[100, 100, 200, 200, 200]
  auto ree_type = arrow::run_end_encoded(arrow::int32(), arrow::int32());
  auto schema = arrow::schema({arrow::field("ree", ree_type, true)});

  arrow::Int32Builder run_ends_builder;
  ARROW_RETURN_NOT_OK(run_ends_builder.AppendValues({2, 5}));
  std::shared_ptr<arrow::Array> run_ends;
  ARROW_RETURN_NOT_OK(run_ends_builder.Finish(&run_ends));

  arrow::Int32Builder values_builder;
  ARROW_RETURN_NOT_OK(values_builder.AppendValues({100, 200}));
  std::shared_ptr<arrow::Array> values;
  ARROW_RETURN_NOT_OK(values_builder.Finish(&values));

  ARROW_ASSIGN_OR_RAISE(auto ree_arr, arrow::RunEndEncodedArray::Make(5, run_ends, values, 0));
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.push_back(std::static_pointer_cast<arrow::Array>(ree_arr));
  auto batch = arrow::RecordBatch::Make(schema, 5, std::move(columns));

  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

std::shared_ptr<arrow::Schema> ExtensionSchema() {
  auto md = arrow::key_value_metadata(
      {"ARROW:extension:name", "ARROW:extension:metadata", "owner"},
      {"com.example.int32_ext", "v1", "interop"});
  return arrow::schema({arrow::field("ext_i32", arrow::int32(), true, md)});
}

std::shared_ptr<arrow::Schema> ViewSchema() {
  return arrow::schema({
      arrow::field("sv", arrow::utf8_view(), true),
      arrow::field("bv", arrow::binary_view(), true),
  });
}

std::shared_ptr<arrow::Schema> ComplexSchema() {
  auto list_type = arrow::list(arrow::field("item", arrow::int32(), true));
  auto struct_type = arrow::struct_({
      arrow::field("id", arrow::int32(), false),
      arrow::field("name", arrow::utf8(), true),
  });
  auto map_type = arrow::map(arrow::int32(), arrow::int32(), false);
  auto union_type = arrow::dense_union({
      arrow::field("i", arrow::int32(), true),
      arrow::field("b", arrow::boolean(), true),
  }, {5, 7});
  auto dec_type = arrow::decimal128(10, 2);
  auto ts_type = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");

  return arrow::schema({
      arrow::field("list_i32", list_type, true),
      arrow::field("struct_pair", struct_type, true),
      arrow::field("map_i32_i32", map_type, true),
      arrow::field("u_dense", union_type, true),
      arrow::field("dec", dec_type, false),
      arrow::field("ts", ts_type, false),
  });
}

arrow::Status GenerateComplex(const std::string& path, ContainerMode container) {
  auto schema = ComplexSchema();
  auto* pool = arrow::default_memory_pool();

  auto list_value_builder = std::make_shared<arrow::Int32Builder>(pool);
  arrow::ListBuilder list_builder(pool, list_value_builder);
  auto* list_values = static_cast<arrow::Int32Builder*>(list_builder.value_builder());
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(list_values->Append(1));
  ARROW_RETURN_NOT_OK(list_values->Append(2));
  ARROW_RETURN_NOT_OK(list_builder.AppendNull());
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(list_values->Append(3));
  std::shared_ptr<arrow::Array> list_col;
  ARROW_RETURN_NOT_OK(list_builder.Finish(&list_col));

  auto struct_type = schema->field(1)->type();
  auto struct_id_builder = std::make_shared<arrow::Int32Builder>(pool);
  auto struct_name_builder = std::make_shared<arrow::StringBuilder>(pool);
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> struct_children = {struct_id_builder, struct_name_builder};
  arrow::StructBuilder struct_builder(struct_type, pool, std::move(struct_children));
  auto* struct_ids = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(0));
  auto* struct_names = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(1));
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(struct_ids->Append(10));
  ARROW_RETURN_NOT_OK(struct_names->Append("aa"));
  ARROW_RETURN_NOT_OK(struct_builder.AppendNull());
  ARROW_RETURN_NOT_OK(struct_ids->Append(0));
  ARROW_RETURN_NOT_OK(struct_names->AppendNull());
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(struct_ids->Append(30));
  ARROW_RETURN_NOT_OK(struct_names->Append("cc"));
  std::shared_ptr<arrow::Array> struct_col;
  ARROW_RETURN_NOT_OK(struct_builder.Finish(&struct_col));

  auto map_key_builder = std::make_shared<arrow::Int32Builder>(pool);
  auto map_item_builder = std::make_shared<arrow::Int32Builder>(pool);
  arrow::MapBuilder map_builder(pool, map_key_builder, map_item_builder);
  auto* map_keys = static_cast<arrow::Int32Builder*>(map_builder.key_builder());
  auto* map_items = static_cast<arrow::Int32Builder*>(map_builder.item_builder());
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(map_keys->Append(1));
  ARROW_RETURN_NOT_OK(map_items->Append(10));
  ARROW_RETURN_NOT_OK(map_keys->Append(2));
  ARROW_RETURN_NOT_OK(map_items->Append(20));
  ARROW_RETURN_NOT_OK(map_builder.AppendNull());
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(map_keys->Append(3));
  ARROW_RETURN_NOT_OK(map_items->Append(30));
  std::shared_ptr<arrow::Array> map_col;
  ARROW_RETURN_NOT_OK(map_builder.Finish(&map_col));

  arrow::Int8Builder union_type_ids_builder(pool);
  ARROW_RETURN_NOT_OK(union_type_ids_builder.AppendValues({5, 7, 5}));
  std::shared_ptr<arrow::Array> union_type_ids;
  ARROW_RETURN_NOT_OK(union_type_ids_builder.Finish(&union_type_ids));

  arrow::Int32Builder union_offsets_builder(pool);
  ARROW_RETURN_NOT_OK(union_offsets_builder.AppendValues({0, 0, 1}));
  std::shared_ptr<arrow::Array> union_offsets;
  ARROW_RETURN_NOT_OK(union_offsets_builder.Finish(&union_offsets));

  arrow::Int32Builder union_i_builder(pool);
  ARROW_RETURN_NOT_OK(union_i_builder.AppendValues({100, 200}));
  std::shared_ptr<arrow::Array> union_i_values;
  ARROW_RETURN_NOT_OK(union_i_builder.Finish(&union_i_values));

  arrow::BooleanBuilder union_b_builder(pool);
  ARROW_RETURN_NOT_OK(union_b_builder.Append(true));
  std::shared_ptr<arrow::Array> union_b_values;
  ARROW_RETURN_NOT_OK(union_b_builder.Finish(&union_b_values));

  auto union_type = arrow::dense_union({
      arrow::field("i", arrow::int32(), true),
      arrow::field("b", arrow::boolean(), true),
  }, {5, 7});
  std::vector<std::shared_ptr<arrow::Buffer>> union_buffers = {
      nullptr,
      union_type_ids->data()->buffers[1],
      union_offsets->data()->buffers[1],
  };
  std::vector<std::shared_ptr<arrow::ArrayData>> union_children = {
      union_i_values->data(),
      union_b_values->data(),
  };
  auto union_data = arrow::ArrayData::Make(union_type, 3, std::move(union_buffers),
                                           std::move(union_children), 0, 0);
  auto union_col = arrow::MakeArray(union_data);

  arrow::Decimal128Builder dec_builder(arrow::decimal128(10, 2), pool);
  ARROW_RETURN_NOT_OK(dec_builder.Append(arrow::Decimal128(12345)));
  ARROW_RETURN_NOT_OK(dec_builder.Append(arrow::Decimal128(-42)));
  ARROW_RETURN_NOT_OK(dec_builder.Append(arrow::Decimal128(0)));
  std::shared_ptr<arrow::Array> dec_col;
  ARROW_RETURN_NOT_OK(dec_builder.Finish(&dec_col));

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), pool);
  ARROW_RETURN_NOT_OK(ts_builder.Append(1700000000000LL));
  ARROW_RETURN_NOT_OK(ts_builder.Append(1700000001000LL));
  ARROW_RETURN_NOT_OK(ts_builder.Append(1700000002000LL));
  std::shared_ptr<arrow::Array> ts_col;
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_col));

  std::vector<std::shared_ptr<arrow::Array>> columns = {
      list_col, struct_col, map_col, union_col, dec_col, ts_col};
  auto batch = arrow::RecordBatch::Make(schema, 3, std::move(columns));
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

arrow::Status GenerateView(const std::string& path, ContainerMode container) {
  auto schema = ViewSchema();

  arrow::StringViewBuilder sv_builder;
  ARROW_RETURN_NOT_OK(sv_builder.Append("short"));
  ARROW_RETURN_NOT_OK(sv_builder.AppendNull());
  ARROW_RETURN_NOT_OK(sv_builder.Append("tiny"));
  ARROW_RETURN_NOT_OK(sv_builder.Append("this string is longer than twelve"));
  std::shared_ptr<arrow::Array> sv;
  ARROW_RETURN_NOT_OK(sv_builder.Finish(&sv));

  arrow::BinaryViewBuilder bv_builder;
  ARROW_RETURN_NOT_OK(bv_builder.Append("ab", 2));
  ARROW_RETURN_NOT_OK(bv_builder.Append("this-binary-view-is-long", 24));
  ARROW_RETURN_NOT_OK(bv_builder.AppendNull());
  ARROW_RETURN_NOT_OK(bv_builder.Append("xy", 2));
  std::shared_ptr<arrow::Array> bv;
  ARROW_RETURN_NOT_OK(bv_builder.Finish(&bv));

  std::vector<std::shared_ptr<arrow::Array>> columns = {sv, bv};
  auto batch = arrow::RecordBatch::Make(schema, 4, std::move(columns));
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

arrow::Status GenerateExtension(const std::string& path, ContainerMode container) {
  auto schema = ExtensionSchema();

  arrow::Int32Builder values_builder;
  ARROW_RETURN_NOT_OK(values_builder.Append(7));
  ARROW_RETURN_NOT_OK(values_builder.AppendNull());
  ARROW_RETURN_NOT_OK(values_builder.Append(11));
  std::shared_ptr<arrow::Array> values;
  ARROW_RETURN_NOT_OK(values_builder.Finish(&values));

  std::vector<std::shared_ptr<arrow::Array>> columns = {values};
  auto batch = arrow::RecordBatch::Make(schema, 3, std::move(columns));
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  } else {
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out.get(), schema));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
  }
  return out->Close();
}

arrow::Status Validate(const std::string& path, ContainerMode container) {
  // Expected decoded content:
  // - one batch with 3 rows
  // - id=[1, 2, 3]
  // - name=["alice", null, "bob"]
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(batch, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(1));
    }
  }
  if (schema->num_fields() != 2) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "id" || schema->field(0)->type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("invalid id field");
  }
  if (schema->field(1)->name() != "name" || schema->field(1)->type()->id() != arrow::Type::STRING) {
    return arrow::Status::Invalid("invalid name field");
  }

  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 3) return arrow::Status::Invalid("invalid row count");

  if (extra) return arrow::Status::Invalid("unexpected extra batch");

  auto ids = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
  auto names = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

  if (ids->Value(0) != 1 || ids->Value(1) != 2 || ids->Value(2) != 3) {
    return arrow::Status::Invalid("invalid id values");
  }
  if (names->GetString(0) != "alice" || !names->IsNull(1) || names->GetString(2) != "bob") {
    return arrow::Status::Invalid("invalid name values");
  }
  return arrow::Status::OK();
}

arrow::Result<std::string> DecodeDictionaryString(const std::shared_ptr<arrow::Array>& column,
                                                  int64_t row) {
  auto dict_arr = std::static_pointer_cast<arrow::DictionaryArray>(column);
  auto values = std::static_pointer_cast<arrow::StringArray>(dict_arr->dictionary());
  const auto keys = dict_arr->indices();
  if (keys->IsNull(row)) return arrow::Status::Invalid("dictionary key is null");

  int64_t key = -1;
  switch (keys->type_id()) {
    case arrow::Type::INT8:
      key = std::static_pointer_cast<arrow::Int8Array>(keys)->Value(row);
      break;
    case arrow::Type::INT16:
      key = std::static_pointer_cast<arrow::Int16Array>(keys)->Value(row);
      break;
    case arrow::Type::INT32:
      key = std::static_pointer_cast<arrow::Int32Array>(keys)->Value(row);
      break;
    case arrow::Type::INT64:
      key = std::static_pointer_cast<arrow::Int64Array>(keys)->Value(row);
      break;
    case arrow::Type::UINT8:
      key = std::static_pointer_cast<arrow::UInt8Array>(keys)->Value(row);
      break;
    case arrow::Type::UINT16:
      key = std::static_pointer_cast<arrow::UInt16Array>(keys)->Value(row);
      break;
    case arrow::Type::UINT32:
      key = std::static_pointer_cast<arrow::UInt32Array>(keys)->Value(row);
      break;
    case arrow::Type::UINT64: {
      const auto key_u64 = std::static_pointer_cast<arrow::UInt64Array>(keys)->Value(row);
      if (key_u64 > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return arrow::Status::Invalid("dictionary key out of int64 range");
      }
      key = static_cast<int64_t>(key_u64);
      break;
    }
    default:
      return arrow::Status::Invalid("unsupported dictionary index type");
  }

  if (key < 0 || key >= values->length()) return arrow::Status::Invalid("dictionary key out of range");
  return values->GetString(key);
}

arrow::Status ValidateDictDelta(const std::string& path, ContainerMode container) {
  // Expected decoded content:
  // - two batches
  //   batch1 values=["red", "blue"]
  //   batch2 values=["green"]
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> first;
  std::shared_ptr<arrow::RecordBatch> second;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&first));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&second));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(first, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(second, reader->ReadRecordBatch(1));
    }
    if (reader->num_record_batches() > 2) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(2));
    }
  }
  if (schema->num_fields() != 1) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "color") return arrow::Status::Invalid("invalid color field");
  if (schema->field(0)->type()->id() != arrow::Type::DICTIONARY) {
    return arrow::Status::Invalid("color field must be dictionary type");
  }

  if (!first || !second) return arrow::Status::Invalid("missing batches");
  if (first->num_rows() != 2 || second->num_rows() != 1) return arrow::Status::Invalid("invalid row counts");

  ARROW_ASSIGN_OR_RAISE(auto first0, DecodeDictionaryString(first->column(0), 0));
  ARROW_ASSIGN_OR_RAISE(auto first1, DecodeDictionaryString(first->column(0), 1));
  ARROW_ASSIGN_OR_RAISE(auto second0, DecodeDictionaryString(second->column(0), 0));
  if (first0 != "red" || first1 != "blue") return arrow::Status::Invalid("invalid first batch values");
  if (second0 != "green") return arrow::Status::Invalid("invalid second batch values");

  if (extra) return arrow::Status::Invalid("unexpected extra batch");
  return arrow::Status::OK();
}

arrow::Status ValidateRee(const std::string& path, ContainerMode container) {
  // Expected decoded content:
  // - one batch with 5 rows
  // - logical values=[100, 100, 200, 200, 200]
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(batch, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(1));
    }
  }
  if (schema->num_fields() != 1) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "ree") return arrow::Status::Invalid("invalid ree field");
  if (schema->field(0)->type()->id() != arrow::Type::RUN_END_ENCODED) {
    return arrow::Status::Invalid("ree field must be run_end_encoded type");
  }

  auto ree_type = std::static_pointer_cast<arrow::RunEndEncodedType>(schema->field(0)->type());
  const auto run_end_id = ree_type->run_end_type()->id();
  if (run_end_id != arrow::Type::INT16 && run_end_id != arrow::Type::INT32 &&
      run_end_id != arrow::Type::INT64) {
    return arrow::Status::Invalid("ree run_end_type must be int16/int32/int64");
  }
  if (ree_type->value_type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("ree value_type must be int32");
  }

  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 5) return arrow::Status::Invalid("invalid row count");
  ARROW_ASSIGN_OR_RAISE(auto decoded_any, arrow::compute::CallFunction("run_end_decode", {batch->column(0)}));
  auto decoded = std::dynamic_pointer_cast<arrow::Int32Array>(decoded_any.make_array());
  if (!decoded) return arrow::Status::Invalid("decoded ree values type must be int32");
  if (decoded->length() != 5) return arrow::Status::Invalid("decoded ree values length must be 5");
  if (decoded->Value(0) != 100 || decoded->Value(1) != 100 || decoded->Value(2) != 200 ||
      decoded->Value(3) != 200 || decoded->Value(4) != 200) {
    return arrow::Status::Invalid("invalid ree values");
  }

  if (extra) return arrow::Status::Invalid("unexpected extra batch");
  return arrow::Status::OK();
}

arrow::Status ValidateExtension(const std::string& path, ContainerMode container) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(batch, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(1));
    }
  }
  if (schema->num_fields() != 1) return arrow::Status::Invalid("invalid field count");
  auto field = schema->field(0);
  if (field->name() != "ext_i32" || field->type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("invalid extension field");
  }
  auto md = field->metadata();
  if (!md) return arrow::Status::Invalid("missing extension metadata");
  const int name_idx = md->FindKey("ARROW:extension:name");
  const int meta_idx = md->FindKey("ARROW:extension:metadata");
  const int owner_idx = md->FindKey("owner");
  if (name_idx < 0 || md->value(name_idx) != "com.example.int32_ext") {
    return arrow::Status::Invalid("invalid extension name");
  }
  if (meta_idx < 0 || md->value(meta_idx) != "v1") {
    return arrow::Status::Invalid("invalid extension metadata");
  }
  if (owner_idx < 0 || md->value(owner_idx) != "interop") {
    return arrow::Status::Invalid("invalid owner metadata");
  }

  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 3) return arrow::Status::Invalid("invalid row count");
  if (extra) return arrow::Status::Invalid("unexpected extra batch");

  auto values = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
  if (values->Value(0) != 7 || !values->IsNull(1) || values->Value(2) != 11) {
    return arrow::Status::Invalid("invalid extension values");
  }
  return arrow::Status::OK();
}

arrow::Status ValidateComplex(const std::string& path, ContainerMode container) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(batch, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(1));
    }
  }

  if (schema->num_fields() != 6) return arrow::Status::Invalid("invalid schema field count");
  if (schema->field(0)->name() != "list_i32" || schema->field(0)->type()->id() != arrow::Type::LIST) {
    return arrow::Status::Invalid("invalid list_i32 field");
  }
  if (schema->field(1)->name() != "struct_pair" || schema->field(1)->type()->id() != arrow::Type::STRUCT) {
    return arrow::Status::Invalid("invalid struct_pair field");
  }
  if (schema->field(2)->name() != "map_i32_i32" || schema->field(2)->type()->id() != arrow::Type::MAP) {
    return arrow::Status::Invalid("invalid map_i32_i32 field");
  }
  if (schema->field(3)->name() != "u_dense" || schema->field(3)->type()->id() != arrow::Type::DENSE_UNION) {
    return arrow::Status::Invalid("invalid u_dense field");
  }
  if (schema->field(4)->name() != "dec" || schema->field(4)->type()->id() != arrow::Type::DECIMAL128) {
    return arrow::Status::Invalid("invalid dec field");
  }
  if (schema->field(5)->name() != "ts" || schema->field(5)->type()->id() != arrow::Type::TIMESTAMP) {
    return arrow::Status::Invalid("invalid ts field");
  }

  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 3) return arrow::Status::Invalid("invalid row count");
  if (extra) return arrow::Status::Invalid("unexpected extra batch");

  auto list_col = std::static_pointer_cast<arrow::ListArray>(batch->column(0));
  if (list_col->IsNull(0) || !list_col->IsNull(1) || list_col->IsNull(2)) {
    return arrow::Status::Invalid("invalid list nulls");
  }
  auto l0_any = list_col->value_slice(0);
  auto l2_any = list_col->value_slice(2);
  auto l0 = std::static_pointer_cast<arrow::Int32Array>(l0_any);
  auto l2 = std::static_pointer_cast<arrow::Int32Array>(l2_any);
  if (l0->length() != 2 || l0->Value(0) != 1 || l0->Value(1) != 2 || l2->length() != 1 ||
      l2->Value(0) != 3) {
    return arrow::Status::Invalid("invalid list values");
  }

  auto struct_col = std::static_pointer_cast<arrow::StructArray>(batch->column(1));
  if (struct_col->IsNull(0) || !struct_col->IsNull(1) || struct_col->IsNull(2)) {
    return arrow::Status::Invalid("invalid struct nulls");
  }
  auto sid = std::static_pointer_cast<arrow::Int32Array>(struct_col->field(0));
  auto sname = std::static_pointer_cast<arrow::StringArray>(struct_col->field(1));
  if (sid->Value(0) != 10 || sid->Value(2) != 30 || sname->GetString(0) != "aa" ||
      sname->GetString(2) != "cc") {
    return arrow::Status::Invalid("invalid struct values");
  }

  auto map_col = std::static_pointer_cast<arrow::MapArray>(batch->column(2));
  if (map_col->IsNull(0) || !map_col->IsNull(1) || map_col->IsNull(2)) {
    return arrow::Status::Invalid("invalid map nulls");
  }
  auto m0_any = map_col->value_slice(0);
  auto m2_any = map_col->value_slice(2);
  auto m0 = std::static_pointer_cast<arrow::StructArray>(m0_any);
  auto m2 = std::static_pointer_cast<arrow::StructArray>(m2_any);
  auto m0k = std::static_pointer_cast<arrow::Int32Array>(m0->field(0));
  auto m0v = std::static_pointer_cast<arrow::Int32Array>(m0->field(1));
  auto m2k = std::static_pointer_cast<arrow::Int32Array>(m2->field(0));
  auto m2v = std::static_pointer_cast<arrow::Int32Array>(m2->field(1));
  if (m0k->length() != 2 || m0k->Value(0) != 1 || m0k->Value(1) != 2 || m0v->Value(0) != 10 ||
      m0v->Value(1) != 20 || m2k->length() != 1 || m2k->Value(0) != 3 || m2v->Value(0) != 30) {
    return arrow::Status::Invalid("invalid map values");
  }

  auto union_col = std::static_pointer_cast<arrow::DenseUnionArray>(batch->column(3));
  auto union_type = std::static_pointer_cast<arrow::DenseUnionType>(union_col->type());
  if (union_type->num_fields() != 2) {
    return arrow::Status::Invalid("invalid union child count");
  }
  int int_child = -1;
  int bool_child = -1;
  for (int i = 0; i < union_type->num_fields(); ++i) {
    if (union_type->fields()[static_cast<size_t>(i)]->type()->id() == arrow::Type::INT32) int_child = i;
    if (union_type->fields()[static_cast<size_t>(i)]->type()->id() == arrow::Type::BOOL) bool_child = i;
  }
  if (int_child < 0 || bool_child < 0) {
    return arrow::Status::Invalid("invalid union child types");
  }
  const auto& type_codes = union_type->type_codes();
  const auto int_code = type_codes[static_cast<size_t>(int_child)];
  const auto bool_code = type_codes[static_cast<size_t>(bool_child)];
  if (union_col->type_code(0) != int_code || union_col->type_code(1) != bool_code ||
      union_col->type_code(2) != int_code) {
    return arrow::Status::Invalid("invalid union type ids");
  }
  if (union_col->value_offset(0) != 0 || union_col->value_offset(1) != 0 ||
      union_col->value_offset(2) != 1) {
    return arrow::Status::Invalid("invalid union value offsets");
  }
  auto union_i = std::static_pointer_cast<arrow::Int32Array>(union_col->field(int_child));
  auto union_b = std::static_pointer_cast<arrow::BooleanArray>(union_col->field(bool_child));
  if (union_i->Value(0) != 100 || !union_b->Value(0) || union_i->Value(1) != 200) {
    return arrow::Status::Invalid("invalid union values");
  }

  auto dec = std::static_pointer_cast<arrow::Decimal128Array>(batch->column(4));
  ARROW_ASSIGN_OR_RAISE(auto dec0_scalar_any, dec->GetScalar(0));
  ARROW_ASSIGN_OR_RAISE(auto dec1_scalar_any, dec->GetScalar(1));
  ARROW_ASSIGN_OR_RAISE(auto dec2_scalar_any, dec->GetScalar(2));
  auto dec0_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(dec0_scalar_any);
  auto dec1_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(dec1_scalar_any);
  auto dec2_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(dec2_scalar_any);
  if (dec0_scalar->value != arrow::Decimal128(12345) || dec1_scalar->value != arrow::Decimal128(-42) ||
      dec2_scalar->value != arrow::Decimal128(0)) {
    return arrow::Status::Invalid("invalid decimal values");
  }

  auto ts = std::static_pointer_cast<arrow::TimestampArray>(batch->column(5));
  if (ts->Value(0) != 1700000000000LL || ts->Value(1) != 1700000001000LL ||
      ts->Value(2) != 1700000002000LL) {
    return arrow::Status::Invalid("invalid timestamp values");
  }

  return arrow::Status::OK();
}

arrow::Status ValidateView(const std::string& path, ContainerMode container) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<arrow::RecordBatch> extra;
  if (container == ContainerMode::kStream) {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
    schema = reader->schema();
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(in));
    schema = reader->schema();
    if (reader->num_record_batches() > 0) {
      ARROW_ASSIGN_OR_RAISE(batch, reader->ReadRecordBatch(0));
    }
    if (reader->num_record_batches() > 1) {
      ARROW_ASSIGN_OR_RAISE(extra, reader->ReadRecordBatch(1));
    }
  }

  if (schema->num_fields() != 2) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "sv" || schema->field(0)->type()->id() != arrow::Type::STRING_VIEW) {
    return arrow::Status::Invalid("invalid sv field");
  }
  if (schema->field(1)->name() != "bv" || schema->field(1)->type()->id() != arrow::Type::BINARY_VIEW) {
    return arrow::Status::Invalid("invalid bv field");
  }
  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 4) return arrow::Status::Invalid("invalid row count");
  if (extra) return arrow::Status::Invalid("unexpected extra batch");

  auto sv = std::static_pointer_cast<arrow::StringViewArray>(batch->column(0));
  auto bv = std::static_pointer_cast<arrow::BinaryViewArray>(batch->column(1));
  if (sv->GetString(0) != "short" || !sv->IsNull(1) || sv->GetString(2) != "tiny" ||
      sv->GetString(3) != "this string is longer than twelve") {
    return arrow::Status::Invalid("invalid sv values");
  }

  const auto b0 = bv->GetView(0);
  const auto b1 = bv->GetView(1);
  const auto b3 = bv->GetView(3);
  if (b0 != "ab" || b1 != "this-binary-view-is-long" || !bv->IsNull(2) || b3 != "xy") {
    return arrow::Status::Invalid("invalid bv values");
  }
  return arrow::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3 || argc > 5) {
    std::cerr
        << "usage: interop_cpp <generate|validate> <path.arrow> [canonical|dict-delta|ree|complex|extension|view] [stream|file]\n";
    return 2;
  }
  const std::string mode = argv[1];
  const std::string path = argv[2];
  std::string case_name = "canonical";
  ContainerMode container = ContainerMode::kStream;
  if (argc >= 4) {
    const std::string arg3 = argv[3];
    if (arg3 == "stream" || arg3 == "file") {
      container = arg3 == "file" ? ContainerMode::kFile : ContainerMode::kStream;
    } else {
      case_name = arg3;
      if (argc == 5) {
        const std::string arg4 = argv[4];
        if (arg4 == "stream")
          container = ContainerMode::kStream;
        else if (arg4 == "file")
          container = ContainerMode::kFile;
        else {
          std::cerr
              << "usage: interop_cpp <generate|validate> <path.arrow> [canonical|dict-delta|ree|complex|extension|view] [stream|file]\n";
          return 2;
        }
      }
    }
  }
  if (case_name == "dict-delta" && container == ContainerMode::kFile) {
    std::cerr
        << "dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches\n";
    return 2;
  }

  arrow::Status st = arrow::Status::Invalid("unsupported mode/case");
  if (mode == "generate" && case_name == "canonical") st = Generate(path, container);
  if (mode == "validate" && case_name == "canonical") st = Validate(path, container);
  if (mode == "generate" && case_name == "dict-delta") st = GenerateDictDelta(path, container);
  if (mode == "validate" && case_name == "dict-delta") st = ValidateDictDelta(path, container);
  if (mode == "generate" && case_name == "ree") st = GenerateRee(path, container);
  if (mode == "validate" && case_name == "ree") st = ValidateRee(path, container);
  if (mode == "generate" && case_name == "complex") st = GenerateComplex(path, container);
  if (mode == "validate" && case_name == "complex") st = ValidateComplex(path, container);
  if (mode == "generate" && case_name == "extension") st = GenerateExtension(path, container);
  if (mode == "validate" && case_name == "extension") st = ValidateExtension(path, container);
  if (mode == "generate" && case_name == "view") st = GenerateView(path, container);
  if (mode == "validate" && case_name == "view") st = ValidateView(path, container);
  if (!st.ok()) {
    std::cerr << st.ToString() << "\n";
    return 1;
  }
  return 0;
}
