use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

fn canonical_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn generate(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = canonical_schema();
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names])?;

    let file = File::create(path)?;
    let mut writer = StreamWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

fn dict_delta_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "color",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]))
}

fn generate_dict_delta(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = dict_delta_schema();

    let mut first_builder = StringDictionaryBuilder::<Int32Type>::new();
    first_builder.append("red")?;
    first_builder.append("blue")?;
    let first_col: ArrayRef = Arc::new(first_builder.finish());
    let first_batch = RecordBatch::try_new(schema.clone(), vec![first_col])?;

    let bootstrap = StringArray::from(vec!["red", "blue"]);
    let mut second_builder = StringDictionaryBuilder::<Int32Type>::new_with_dictionary(1, &bootstrap)?;
    second_builder.append("green")?;
    let second_col: ArrayRef = Arc::new(second_builder.finish());
    let second_batch = RecordBatch::try_new(schema.clone(), vec![second_col])?;

    let file = File::create(path)?;
    let mut writer = StreamWriter::try_new(file, &schema)?;
    writer.write(&first_batch)?;
    writer.write(&second_batch)?;
    writer.finish()?;
    Ok(())
}

fn validate(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = StreamReader::try_new(file, None)?;
    let schema = reader.schema();
    if schema.fields().len() != 2 {
        return Err("invalid schema field count".into());
    }
    if schema.field(0).name() != "id" || schema.field(0).data_type() != &DataType::Int32 {
        return Err("invalid id field".into());
    }
    if schema.field(1).name() != "name" || schema.field(1).data_type() != &DataType::Utf8 {
        return Err("invalid name field".into());
    }

    let batch = reader.next().ok_or("missing batch")??;
    if batch.num_rows() != 3 {
        return Err("invalid row count".into());
    }
    if reader.next().is_some() {
        return Err("unexpected extra batch".into());
    }

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("id downcast failed")?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("name downcast failed")?;

    if ids.value(0) != 1 || ids.value(1) != 2 || ids.value(2) != 3 {
        return Err("invalid id values".into());
    }
    if names.value(0) != "alice" || !names.is_null(1) || names.value(2) != "bob" {
        return Err("invalid name values".into());
    }
    Ok(())
}

fn validate_dict_delta(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = StreamReader::try_new(file, None)?;
    let schema = reader.schema();
    if schema.fields().len() != 1 {
        return Err("invalid schema field count".into());
    }
    if schema.field(0).name() != "color" {
        return Err("invalid color field".into());
    }
    match schema.field(0).data_type() {
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => {}
        _ => return Err("color field must be dictionary utf8".into()),
    }

    let first = reader.next().ok_or("missing first batch")??;
    let second = reader.next().ok_or("missing second batch")??;
    if reader.next().is_some() {
        return Err("unexpected extra batch".into());
    }

    if first.num_rows() != 2 || second.num_rows() != 1 {
        return Err("invalid row counts".into());
    }

    let first_dict = first
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or("first color downcast failed")?;
    let second_dict = second
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or("second color downcast failed")?;

    let first_values = first_dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("first dictionary values downcast failed")?;
    let second_values = second_dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("second dictionary values downcast failed")?;

    let first_k0 = first_dict.key(0).ok_or("first key[0] null")?;
    let first_k1 = first_dict.key(1).ok_or("first key[1] null")?;
    let second_k0 = second_dict.key(0).ok_or("second key[0] null")?;

    if first_values.value(first_k0) != "red" || first_values.value(first_k1) != "blue" {
        return Err("invalid first batch values".into());
    }
    if second_values.value(second_k0) != "green" {
        return Err("invalid second batch values".into());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _exe = args.next();
    let mode = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = Path::new(&path);
    let case = args.next().unwrap_or_else(|| "canonical".to_string());

    match (mode.as_str(), case.as_str()) {
        ("generate", "canonical") => generate(path),
        ("validate", "canonical") => validate(path),
        ("generate", "dict-delta") => generate_dict_delta(path),
        ("validate", "dict-delta") => validate_dict_delta(path),
        _ => Err("usage: <generate|validate> <path.arrow> [canonical|dict-delta]".into()),
    }
}
