use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, Int32Array, RecordBatch, RunArray, StringArray};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContainerMode {
    Stream,
    File,
}

fn canonical_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn generate(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Writes one stream with:
    // - schema: id: int32 (non-null), name: utf8 (nullable)
    // - one record batch (3 rows)
    //   id=[1, 2, 3]
    //   name=["alice", null, "bob"]
    let schema = canonical_schema();
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names])?;

    let file = File::create(path)?;
    match container {
        ContainerMode::Stream => {
            let mut writer = StreamWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        ContainerMode::File => {
            let mut writer = FileWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
    }
    Ok(())
}

fn dict_delta_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "color",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]))
}

fn generate_dict_delta(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Writes one stream with dictionary-encoded column "color":
    // - schema: color: dictionary<int32, utf8>
    // - two record batches to exercise dictionary delta behavior
    //   batch1 decoded values=["red", "blue"]
    //   batch2 decoded values=["green"]
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
    match container {
        ContainerMode::Stream => {
            let mut writer = StreamWriter::try_new(file, &schema)?;
            writer.write(&first_batch)?;
            writer.write(&second_batch)?;
            writer.finish()?;
        }
        ContainerMode::File => {
            let mut writer = FileWriter::try_new(file, &schema)?;
            writer.write(&first_batch)?;
            writer.write(&second_batch)?;
            writer.finish()?;
        }
    }
    Ok(())
}

fn ree_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "ree",
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        ),
        true,
    )]))
}

fn extension_schema() -> Arc<Schema> {
    let mut md = HashMap::new();
    md.insert("ARROW:extension:name".to_string(), "com.example.int32_ext".to_string());
    md.insert("ARROW:extension:metadata".to_string(), "v1".to_string());
    md.insert("owner".to_string(), "interop".to_string());
    Arc::new(Schema::new(vec![Field::new("ext_i32", DataType::Int32, true).with_metadata(md)]))
}

fn generate_extension(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let schema = extension_schema();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(11)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values])?;

    let file = File::create(path)?;
    match container {
        ContainerMode::Stream => {
            let mut writer = StreamWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        ContainerMode::File => {
            let mut writer = FileWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
    }
    Ok(())
}

fn generate_ree(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Writes one stream with:
    // - schema: ree: run_end_encoded<int32, int32>
    // - one record batch (5 rows)
    //   run_ends=[2, 5], values=[100, 200]
    //   decoded logical values=[100, 100, 200, 200, 200]
    let schema = ree_schema();
    let run_ends = Int32Array::from(vec![2, 5]);
    let values = Int32Array::from(vec![100, 200]);
    let ree: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&run_ends, &values)?);
    let batch = RecordBatch::try_new(schema.clone(), vec![ree])?;

    let file = File::create(path)?;
    match container {
        ContainerMode::Stream => {
            let mut writer = StreamWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        ContainerMode::File => {
            let mut writer = FileWriter::try_new(file, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
    }
    Ok(())
}

fn validate(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Expected decoded content:
    // - one batch with 3 rows
    // - id=[1, 2, 3]
    // - name=["alice", null, "bob"]
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };
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

    let batch = reader.next_batch()?.ok_or("missing batch")?;
    if batch.num_rows() != 3 {
        return Err("invalid row count".into());
    }
    if reader.next_batch()?.is_some() {
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

fn validate_dict_delta(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Expected decoded content:
    // - two batches
    //   batch1 values=["red", "blue"]
    //   batch2 values=["green"]
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };
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

    let first = reader.next_batch()?.ok_or("missing first batch")?;
    let second = reader.next_batch()?.ok_or("missing second batch")?;
    if reader.next_batch()?.is_some() {
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

fn validate_ree(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    // Expected decoded content:
    // - one batch with 5 rows
    // - logical values=[100, 100, 200, 200, 200]
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };
    let schema = reader.schema();
    if schema.fields().len() != 1 {
        return Err("invalid schema field count".into());
    }
    if schema.field(0).name() != "ree" {
        return Err("invalid ree field".into());
    }
    match schema.field(0).data_type() {
        DataType::RunEndEncoded(run_ends, values)
            if run_ends.data_type() == &DataType::Int32 && values.data_type() == &DataType::Int32 => {}
        _ => return Err("ree field must be run_end_encoded<int32,int32>".into()),
    }

    let batch = reader.next_batch()?.ok_or("missing batch")?;
    if reader.next_batch()?.is_some() {
        return Err("unexpected extra batch".into());
    }
    if batch.num_rows() != 5 {
        return Err("invalid row count".into());
    }

    let ree = batch
        .column(0)
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .ok_or("ree downcast failed")?;
    let typed = ree.downcast::<Int32Array>().ok_or("ree values downcast failed")?;
    let actual: Vec<Option<i32>> = typed.into_iter().collect();
    let expected = vec![Some(100), Some(100), Some(200), Some(200), Some(200)];
    if actual != expected {
        return Err(format!("invalid ree values: {actual:?}").into());
    }

    Ok(())
}

fn validate_extension(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };
    let schema = reader.schema();
    if schema.fields().len() != 1 {
        return Err("invalid schema field count".into());
    }
    let field = schema.field(0);
    if field.name() != "ext_i32" || field.data_type() != &DataType::Int32 {
        return Err("invalid extension field".into());
    }
    let md = field.metadata();
    if md.get("ARROW:extension:name").map(|v| v.as_str()) != Some("com.example.int32_ext") {
        return Err("invalid extension name".into());
    }
    if md.get("ARROW:extension:metadata").map(|v| v.as_str()) != Some("v1") {
        return Err("invalid extension metadata".into());
    }
    if md.get("owner").map(|v| v.as_str()) != Some("interop") {
        return Err("invalid owner metadata".into());
    }

    let batch = reader.next_batch()?.ok_or("missing batch")?;
    if reader.next_batch()?.is_some() {
        return Err("unexpected extra batch".into());
    }
    if batch.num_rows() != 3 {
        return Err("invalid row count".into());
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("extension values downcast failed")?;
    if values.value(0) != 7 || !values.is_null(1) || values.value(2) != 11 {
        return Err("invalid extension values".into());
    }
    Ok(())
}

enum EitherReader {
    Stream(StreamReader<File>),
    File(FileReader<File>),
}

impl EitherReader {
    fn schema(&self) -> Arc<Schema> {
        match self {
            EitherReader::Stream(r) => r.schema(),
            EitherReader::File(r) => r.schema(),
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Box<dyn std::error::Error>> {
        match self {
            EitherReader::Stream(r) => Ok(r.next().transpose()?),
            EitherReader::File(r) => Ok(r.next().transpose()?),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _exe = args.next();
    let mode = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = Path::new(&path);
    let mut case = "canonical".to_string();
    let mut container = ContainerMode::Stream;
    if let Some(arg3) = args.next() {
        if arg3 == "stream" || arg3 == "file" {
            container = if arg3 == "file" { ContainerMode::File } else { ContainerMode::Stream };
        } else {
            case = arg3;
            if let Some(arg4) = args.next() {
                if arg4 == "stream" || arg4 == "file" {
                    container = if arg4 == "file" { ContainerMode::File } else { ContainerMode::Stream };
                } else {
                    return Err(
                        "usage: <generate|validate> <path.arrow> [canonical|dict-delta|ree|extension] [stream|file]"
                            .into(),
                    );
                }
            }
        }
    }
    if case == "dict-delta" && container == ContainerMode::File {
        return Err(
            "dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches"
                .into(),
        );
    }

    match (mode.as_str(), case.as_str()) {
        ("generate", "canonical") => generate(path, container),
        ("validate", "canonical") => validate(path, container),
        ("generate", "dict-delta") => generate_dict_delta(path, container),
        ("validate", "dict-delta") => validate_dict_delta(path, container),
        ("generate", "ree") => generate_ree(path, container),
        ("validate", "ree") => validate_ree(path, container),
        ("generate", "extension") => generate_extension(path, container),
        ("validate", "extension") => validate_extension(path, container),
        _ => Err("usage: <generate|validate> <path.arrow> [canonical|dict-delta|ree|extension] [stream|file]".into()),
    }
}
