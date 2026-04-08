#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

std::shared_ptr<arrow::Schema> CanonicalSchema() {
  return arrow::schema({
      arrow::field("id", arrow::int32(), false),
      arrow::field("name", arrow::utf8(), true),
  });
}

arrow::Status Generate(const std::string& path) {
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
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Close();
}

arrow::Status GenerateDictDelta(const std::string& path) {
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
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*first_batch));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*second_batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Close();
}

arrow::Status GenerateRee(const std::string& path) {
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
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Close();
}

arrow::Status Validate(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
  auto schema = reader->schema();
  if (schema->num_fields() != 2) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "id" || schema->field(0)->type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("invalid id field");
  }
  if (schema->field(1)->name() != "name" || schema->field(1)->type()->id() != arrow::Type::STRING) {
    return arrow::Status::Invalid("invalid name field");
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 3) return arrow::Status::Invalid("invalid row count");

  std::shared_ptr<arrow::RecordBatch> extra;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
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

arrow::Status ValidateDictDelta(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
  auto schema = reader->schema();
  if (schema->num_fields() != 1) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "color") return arrow::Status::Invalid("invalid color field");
  if (schema->field(0)->type()->id() != arrow::Type::DICTIONARY) {
    return arrow::Status::Invalid("color field must be dictionary type");
  }

  std::shared_ptr<arrow::RecordBatch> first;
  std::shared_ptr<arrow::RecordBatch> second;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&first));
  ARROW_RETURN_NOT_OK(reader->ReadNext(&second));
  if (!first || !second) return arrow::Status::Invalid("missing batches");
  if (first->num_rows() != 2 || second->num_rows() != 1) return arrow::Status::Invalid("invalid row counts");

  ARROW_ASSIGN_OR_RAISE(auto first0, DecodeDictionaryString(first->column(0), 0));
  ARROW_ASSIGN_OR_RAISE(auto first1, DecodeDictionaryString(first->column(0), 1));
  ARROW_ASSIGN_OR_RAISE(auto second0, DecodeDictionaryString(second->column(0), 0));
  if (first0 != "red" || first1 != "blue") return arrow::Status::Invalid("invalid first batch values");
  if (second0 != "green") return arrow::Status::Invalid("invalid second batch values");

  std::shared_ptr<arrow::RecordBatch> extra;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  if (extra) return arrow::Status::Invalid("unexpected extra batch");
  return arrow::Status::OK();
}

arrow::Status ValidateRee(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
  auto schema = reader->schema();
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

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 5) return arrow::Status::Invalid("invalid row count");

  const std::vector<int32_t> expected = {100, 100, 200, 200, 200};
  auto ree = batch->column(0);
  for (int64_t i = 0; i < static_cast<int64_t>(expected.size()); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, ree->GetScalar(i));
    auto int_scalar = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar);
    if (!int_scalar || !int_scalar->is_valid || int_scalar->value != expected[static_cast<size_t>(i)]) {
      return arrow::Status::Invalid("invalid ree values");
    }
  }

  std::shared_ptr<arrow::RecordBatch> extra;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  if (extra) return arrow::Status::Invalid("unexpected extra batch");
  return arrow::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3 && argc != 4) {
    std::cerr << "usage: interop_cpp <generate|validate> <path.arrow> [canonical|dict-delta|ree]\n";
    return 2;
  }
  const std::string mode = argv[1];
  const std::string path = argv[2];
  const std::string case_name = argc == 4 ? argv[3] : "canonical";

  arrow::Status st = arrow::Status::Invalid("unsupported mode/case");
  if (mode == "generate" && case_name == "canonical") st = Generate(path);
  if (mode == "validate" && case_name == "canonical") st = Validate(path);
  if (mode == "generate" && case_name == "dict-delta") st = GenerateDictDelta(path);
  if (mode == "validate" && case_name == "dict-delta") st = ValidateDictDelta(path);
  if (mode == "generate" && case_name == "ree") st = GenerateRee(path);
  if (mode == "validate" && case_name == "ree") st = ValidateRee(path);
  if (!st.ok()) {
    std::cerr << st.ToString() << "\n";
    return 1;
  }
  return 0;
}
