#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/config.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

#if !defined(ARROW_VERSION_MAJOR) || (ARROW_VERSION_MAJOR < 18)
#error "tools/interop/cpp/interop.cpp requires Arrow C++ with Utf8View/BinaryView support"
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

  auto batch = arrow::RecordBatch::Make(schema, 3, {ids, names});
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
  auto first_batch = arrow::RecordBatch::Make(schema, 2, {first_encoded});

  arrow::StringBuilder second_strings;
  ARROW_RETURN_NOT_OK(second_strings.Append("green"));
  std::shared_ptr<arrow::Array> second_plain;
  ARROW_RETURN_NOT_OK(second_strings.Finish(&second_plain));
  ARROW_ASSIGN_OR_RAISE(auto second_encoded_datum, arrow::compute::CallFunction("dictionary_encode", {second_plain}));
  auto second_encoded = second_encoded_datum.make_array();
  auto second_batch = arrow::RecordBatch::Make(schema, 1, {second_encoded});

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

  auto batch = arrow::RecordBatch::Make(schema, 4, {sv, bv});
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

  auto batch = arrow::RecordBatch::Make(schema, 3, {values});
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
  auto keys = std::static_pointer_cast<arrow::Int32Array>(dict_arr->indices());
  auto values = std::static_pointer_cast<arrow::StringArray>(dict_arr->dictionary());
  if (keys->IsNull(row)) return arrow::Status::Invalid("dictionary key is null");
  const auto key = keys->Value(row);
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
  if (ree_type->run_end_type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("ree run_end_type must be int32");
  }
  if (ree_type->value_type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("ree value_type must be int32");
  }

  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 5) return arrow::Status::Invalid("invalid row count");

  auto ree_data = batch->column(0)->data();
  if (ree_data->child_data.size() != 2) {
    return arrow::Status::Invalid("invalid ree child count");
  }

  auto run_ends_any = arrow::MakeArray(ree_data->child_data[0]);
  auto values_any = arrow::MakeArray(ree_data->child_data[1]);
  auto run_ends = std::dynamic_pointer_cast<arrow::Int32Array>(run_ends_any);
  auto values = std::dynamic_pointer_cast<arrow::Int32Array>(values_any);
  if (!run_ends || !values) {
    return arrow::Status::Invalid("invalid ree child types");
  }
  if (run_ends->length() != 2 || values->length() != 2) {
    return arrow::Status::Invalid("invalid ree child lengths");
  }
  if (run_ends->Value(0) != 2 || run_ends->Value(1) != 5 || values->Value(0) != 100 ||
      values->Value(1) != 200) {
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
        << "usage: interop_cpp <generate|validate> <path.arrow> [canonical|dict-delta|ree|extension|view] [stream|file]\n";
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
              << "usage: interop_cpp <generate|validate> <path.arrow> [canonical|dict-delta|ree|extension|view] [stream|file]\n";
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
