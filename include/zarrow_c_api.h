#ifndef ZARROW_C_API_H
#define ZARROW_C_API_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

typedef struct zarrow_c_schema_handle zarrow_c_schema_handle;
typedef struct zarrow_c_array_handle zarrow_c_array_handle;
typedef struct zarrow_c_stream_handle zarrow_c_stream_handle;

enum {
  ZARROW_C_STATUS_OK = 0,
  ZARROW_C_STATUS_INVALID_ARGUMENT = 1,
  ZARROW_C_STATUS_OUT_OF_MEMORY = 2,
  ZARROW_C_STATUS_RELEASED = 3,
  ZARROW_C_STATUS_INVALID_DATA = 4,
  ZARROW_C_STATUS_INTERNAL = 5,
};

uint32_t zarrow_c_abi_version(void);
const char* zarrow_c_status_string(int status);

int zarrow_c_import_schema(struct ArrowSchema* c_schema, zarrow_c_schema_handle** out_handle);
int zarrow_c_export_schema(const zarrow_c_schema_handle* handle, struct ArrowSchema* out_schema);
void zarrow_c_release_schema(zarrow_c_schema_handle* handle);

int zarrow_c_import_array(
    const zarrow_c_schema_handle* schema_handle,
    struct ArrowArray* c_array,
    zarrow_c_array_handle** out_handle);
int zarrow_c_export_array(const zarrow_c_array_handle* handle, struct ArrowArray* out_array);
void zarrow_c_release_array(zarrow_c_array_handle* handle);

int zarrow_c_import_stream(struct ArrowArrayStream* c_stream, zarrow_c_stream_handle** out_handle);
int zarrow_c_export_stream(const zarrow_c_stream_handle* handle, struct ArrowArrayStream* out_stream);
void zarrow_c_release_stream(zarrow_c_stream_handle* handle);

#ifdef __cplusplus
}
#endif

#endif
