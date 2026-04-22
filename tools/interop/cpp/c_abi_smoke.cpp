#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "zarrow_c_api.h"

namespace {

struct ArrowSchemaGuard {
  ArrowSchema value{};
  ~ArrowSchemaGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
};

struct ArrowArrayGuard {
  ArrowArray value{};
  ~ArrowArrayGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
};

struct ArrowStreamGuard {
  ArrowArrayStream value{};
  ~ArrowStreamGuard() {
    if (value.release != nullptr) {
      value.release(&value);
    }
  }
};

std::shared_ptr<arrow::Schema> CanonicalSchema() {
  return arrow::schema({
      arrow::field("id", arrow::int32(), false),
      arrow::field("name", arrow::utf8(), true),
  });
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeCanonicalBatch() {
  auto schema = CanonicalSchema();
  auto ids = std::make_shared<arrow::Int32Array>(std::vector<int32_t>{1, 2, 3});
  auto names =
      std::make_shared<arrow::StringArray>(std::vector<std::optional<std::string>>{"alice", std::nullopt, "bob"});
  return arrow::RecordBatch::Make(schema, 3, {ids, names});
}

arrow::Status CheckCanonicalBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("canonical check: batch is null");
  }
  if (batch->num_columns() != 2 || batch->num_rows() != 3) {
    return arrow::Status::Invalid("canonical check: unexpected shape");
  }
  if (!batch->schema()->Equals(*CanonicalSchema(), true)) {
    return arrow::Status::Invalid("canonical check: schema mismatch");
  }

  auto ids = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
  auto names = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

  if (ids->Value(0) != 1 || ids->Value(1) != 2 || ids->Value(2) != 3) {
    return arrow::Status::Invalid("canonical check: id values mismatch");
  }
  if (names->GetString(0) != "alice" || names->IsValid(1) || names->GetString(2) != "bob") {
    return arrow::Status::Invalid("canonical check: name values mismatch");
  }
  return arrow::Status::OK();
}

arrow::Status ZarrowStatusToArrow(int rc, const std::string& context) {
  if (rc == ZARROW_C_STATUS_OK) {
    return arrow::Status::OK();
  }
  std::ostringstream ss;
  ss << context << " failed: rc=" << rc << " (" << zarrow_c_status_string(rc) << ")";
  return arrow::Status::Invalid(ss.str());
}

arrow::Status RunArraySchemaRoundtrip() {
  ARROW_ASSIGN_OR_RAISE(auto batch, MakeCanonicalBatch());

  ArrowSchemaGuard in_schema;
  ArrowArrayGuard in_array;
  ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(*batch, &in_array.value, &in_schema.value));

  zarrow_c_schema_handle* schema_handle = nullptr;
  zarrow_c_array_handle* array_handle = nullptr;
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_import_schema(&in_schema.value, &schema_handle),
      "zarrow_c_import_schema"));
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_import_array(schema_handle, &in_array.value, &array_handle),
      "zarrow_c_import_array"));

  ArrowSchemaGuard out_schema;
  ArrowArrayGuard out_array;
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_export_schema(schema_handle, &out_schema.value),
      "zarrow_c_export_schema"));
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_export_array(array_handle, &out_array.value),
      "zarrow_c_export_array"));

  zarrow_c_release_array(array_handle);
  zarrow_c_release_schema(schema_handle);

  ARROW_ASSIGN_OR_RAISE(auto imported_schema, arrow::ImportSchema(&out_schema.value));
  ARROW_ASSIGN_OR_RAISE(auto imported_batch, arrow::ImportRecordBatch(&out_array.value, imported_schema));
  return CheckCanonicalBatch(imported_batch);
}

arrow::Status RunStreamRoundtrip() {
  ARROW_ASSIGN_OR_RAISE(auto batch, MakeCanonicalBatch());

  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink.get(), batch->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());

  auto source = std::make_shared<arrow::io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(source));

  ArrowStreamGuard in_stream;
  ARROW_RETURN_NOT_OK(arrow::ExportRecordBatchReader(reader, &in_stream.value));

  zarrow_c_stream_handle* stream_handle = nullptr;
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_import_stream(&in_stream.value, &stream_handle),
      "zarrow_c_import_stream"));

  ArrowStreamGuard out_stream;
  ARROW_RETURN_NOT_OK(ZarrowStatusToArrow(
      zarrow_c_export_stream(stream_handle, &out_stream.value),
      "zarrow_c_export_stream"));
  zarrow_c_release_stream(stream_handle);

  ARROW_ASSIGN_OR_RAISE(auto imported_reader, arrow::ImportRecordBatchReader(&out_stream.value));
  ARROW_ASSIGN_OR_RAISE(auto first_batch, imported_reader->Next());
  ARROW_RETURN_NOT_OK(CheckCanonicalBatch(first_batch));
  ARROW_ASSIGN_OR_RAISE(auto eos, imported_reader->Next());
  if (eos != nullptr) {
    return arrow::Status::Invalid("stream roundtrip: expected EOS on second Next()");
  }
  return arrow::Status::OK();
}

}  // namespace

int main() {
  auto st = RunArraySchemaRoundtrip();
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  st = RunStreamRoundtrip();
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  std::cout << "c abi smoke ok" << std::endl;
  return 0;
}
