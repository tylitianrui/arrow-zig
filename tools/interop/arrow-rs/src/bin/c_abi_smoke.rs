use std::error::Error;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{
    Array, ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};

#[repr(C)]
pub struct zarrow_c_schema_handle {
    _private: [u8; 0],
}

#[repr(C)]
pub struct zarrow_c_array_handle {
    _private: [u8; 0],
}

#[repr(C)]
pub struct zarrow_c_stream_handle {
    _private: [u8; 0],
}

#[link(name = "zarrow_c")]
unsafe extern "C" {
    fn zarrow_c_status_string(status: i32) -> *const c_char;

    fn zarrow_c_import_schema(c_schema: *mut FFI_ArrowSchema, out_handle: *mut *mut zarrow_c_schema_handle) -> i32;
    fn zarrow_c_export_schema(handle: *const zarrow_c_schema_handle, out_schema: *mut FFI_ArrowSchema) -> i32;
    fn zarrow_c_release_schema(handle: *mut zarrow_c_schema_handle);

    fn zarrow_c_import_array(
        schema_handle: *const zarrow_c_schema_handle,
        c_array: *mut FFI_ArrowArray,
        out_handle: *mut *mut zarrow_c_array_handle,
    ) -> i32;
    fn zarrow_c_export_array(handle: *const zarrow_c_array_handle, out_array: *mut FFI_ArrowArray) -> i32;
    fn zarrow_c_release_array(handle: *mut zarrow_c_array_handle);

    fn zarrow_c_import_stream(
        c_stream: *mut FFI_ArrowArrayStream,
        out_handle: *mut *mut zarrow_c_stream_handle,
    ) -> i32;
    fn zarrow_c_export_stream(handle: *const zarrow_c_stream_handle, out_stream: *mut FFI_ArrowArrayStream) -> i32;
    fn zarrow_c_release_stream(handle: *mut zarrow_c_stream_handle);
}

fn zstatus(rc: i32, context: &str) -> Result<(), Box<dyn Error>> {
    if rc == 0 {
        return Ok(());
    }
    let msg = unsafe {
        let raw = zarrow_c_status_string(rc);
        if raw.is_null() {
            "unknown".to_string()
        } else {
            CStr::from_ptr(raw).to_string_lossy().into_owned()
        }
    };
    Err(format!("{context} failed: rc={rc} ({msg})").into())
}

fn canonical_batch() -> Result<RecordBatch, Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")]));
    Ok(RecordBatch::try_new(schema, vec![ids, names])?)
}

fn check_canonical_batch(batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
    if batch.num_columns() != 2 || batch.num_rows() != 3 {
        return Err("canonical check: unexpected shape".into());
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("canonical check: id type mismatch")?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("canonical check: name type mismatch")?;

    if ids.value(0) != 1 || ids.value(1) != 2 || ids.value(2) != 3 {
        return Err("canonical check: id values mismatch".into());
    }
    if names.value(0) != "alice" || names.is_valid(1) || names.value(2) != "bob" {
        return Err("canonical check: name values mismatch".into());
    }
    Ok(())
}

fn array_schema_roundtrip() -> Result<(), Box<dyn Error>> {
    let batch = canonical_batch()?;
    let schema = batch.schema();

    let struct_arr = StructArray::new(schema.fields().clone(), batch.columns().to_vec(), None);
    let (mut ffi_array, mut ffi_schema) = to_ffi(&struct_arr.to_data())?;

    let mut schema_handle: *mut zarrow_c_schema_handle = std::ptr::null_mut();
    let mut array_handle: *mut zarrow_c_array_handle = std::ptr::null_mut();
    unsafe {
        zstatus(
            zarrow_c_import_schema(&mut ffi_schema as *mut _, &mut schema_handle as *mut _),
            "zarrow_c_import_schema",
        )?;
        zstatus(
            zarrow_c_import_array(schema_handle as *const _, &mut ffi_array as *mut _, &mut array_handle as *mut _),
            "zarrow_c_import_array",
        )?;
    }

    let mut out_schema = FFI_ArrowSchema::empty();
    let mut out_array = FFI_ArrowArray::empty();
    unsafe {
        zstatus(
            zarrow_c_export_schema(schema_handle as *const _, &mut out_schema as *mut _),
            "zarrow_c_export_schema",
        )?;
        zstatus(
            zarrow_c_export_array(array_handle as *const _, &mut out_array as *mut _),
            "zarrow_c_export_array",
        )?;
        zarrow_c_release_array(array_handle);
        zarrow_c_release_schema(schema_handle);
    }

    let out_data = unsafe { from_ffi(out_array, &out_schema)? };
    let out_arr = arrow_array::make_array(out_data);
    let out_struct = out_arr
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("roundtrip output is not struct array")?;
    let out_batch = RecordBatch::try_new(schema, out_struct.columns().to_vec())?;
    check_canonical_batch(&out_batch)
}

fn stream_roundtrip() -> Result<(), Box<dyn Error>> {
    let batch = canonical_batch()?;
    let schema = batch.schema();

    let input_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema));
    let mut input_stream = FFI_ArrowArrayStream::new(input_reader);

    let mut stream_handle: *mut zarrow_c_stream_handle = std::ptr::null_mut();
    unsafe {
        zstatus(
            zarrow_c_import_stream(&mut input_stream as *mut _, &mut stream_handle as *mut _),
            "zarrow_c_import_stream",
        )?;
    }

    let mut out_stream = FFI_ArrowArrayStream::empty();
    unsafe {
        zstatus(
            zarrow_c_export_stream(stream_handle as *const _, &mut out_stream as *mut _),
            "zarrow_c_export_stream",
        )?;
        zarrow_c_release_stream(stream_handle);
    }

    let mut reader = ArrowArrayStreamReader::try_new(out_stream)?;
    let first = reader.next().ok_or("stream roundtrip: missing first batch")??;
    check_canonical_batch(&first)?;
    if reader.next().is_some() {
        return Err("stream roundtrip: expected EOS on second next()".into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    array_schema_roundtrip()?;
    stream_roundtrip()?;
    println!("rust c abi smoke ok");
    Ok(())
}
