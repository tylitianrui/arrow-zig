use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use arrow_array::builder::{BinaryViewBuilder, StringDictionaryBuilder, StringViewBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, Decimal128Array, DictionaryArray, Int32Array, ListArray,
    MapArray, RecordBatch, RunArray, StringArray, StringViewArray, StructArray, TimestampMillisecondArray,
    UnionArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};

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

fn view_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sv", DataType::Utf8View, true),
        Field::new("bv", DataType::BinaryView, true),
    ]))
}

fn complex_schema() -> Arc<Schema> {
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let struct_type = DataType::Struct(
        vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]
        .into(),
    );
    let map_entry_type = DataType::Struct(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into(),
    );
    let map_type = DataType::Map(Arc::new(Field::new("entries", map_entry_type, false)), false);
    let union_type = DataType::Union(
        UnionFields::new(
            vec![0_i8, 1_i8],
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("b", DataType::Boolean, true),
            ],
        ),
        UnionMode::Dense,
    );
    let dec_type = DataType::Decimal128(10, 2);
    let ts_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));

    Arc::new(Schema::new(vec![
        Field::new("list_i32", list_type, true),
        Field::new("struct_pair", struct_type, true),
        Field::new("map_i32_i32", map_type, true),
        Field::new("u_dense", union_type, true),
        Field::new("dec", dec_type, false),
        Field::new("ts", ts_type, false),
    ]))
}

fn generate_view(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let schema = view_schema();

    let mut sv_builder = StringViewBuilder::new();
    sv_builder.append_value("short");
    sv_builder.append_null();
    sv_builder.append_value("tiny");
    sv_builder.append_value("this string is longer than twelve");
    let sv: ArrayRef = Arc::new(sv_builder.finish());

    let mut bv_builder = BinaryViewBuilder::new();
    bv_builder.append_value(b"ab");
    bv_builder.append_value(b"this-binary-view-is-long");
    bv_builder.append_null();
    bv_builder.append_value(b"xy");
    let bv: ArrayRef = Arc::new(bv_builder.finish());

    let batch = RecordBatch::try_new(schema.clone(), vec![sv, bv])?;
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

fn generate_complex(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let schema = complex_schema();

    let list_col: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]));

    let struct_ids: ArrayRef = Arc::new(Int32Array::from(vec![10, 0, 30]));
    let struct_names: ArrayRef = Arc::new(StringArray::from(vec![Some("aa"), None, Some("cc")]));
    let struct_nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
    let struct_col: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]
        .into(),
        vec![struct_ids, struct_names],
        Some(struct_nulls),
    ));

    let map_keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let map_values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let map_entries = StructArray::from(vec![
        (Arc::new(Field::new("key", DataType::Int32, false)), map_keys),
        (Arc::new(Field::new("value", DataType::Int32, true)), map_values),
    ]);
    let map_offsets = OffsetBuffer::from_lengths([2_usize, 0, 1]);
    let map_nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
    let map_col: ArrayRef = Arc::new(MapArray::try_new(
        Arc::new(Field::new("entries", map_entries.data_type().clone(), false)),
        map_offsets,
        map_entries,
        Some(map_nulls),
        false,
    )?);

    let union_fields = UnionFields::new(
        vec![0_i8, 1_i8],
        vec![
            Field::new("i", DataType::Int32, true),
            Field::new("b", DataType::Boolean, true),
        ],
    );
    let union_type_ids = ScalarBuffer::from(vec![0_i8, 1_i8, 0_i8]);
    let union_offsets = ScalarBuffer::from(vec![0_i32, 0_i32, 1_i32]);
    let union_children = vec![
        Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
    ];
    let union_col: ArrayRef = Arc::new(UnionArray::try_new(
        union_fields,
        union_type_ids,
        Some(union_offsets),
        union_children,
    )?);

    let dec_col: ArrayRef =
        Arc::new(Decimal128Array::from(vec![12345_i128, -42_i128, 0_i128]).with_precision_and_scale(10, 2)?);
    let ts_col: ArrayRef = Arc::new(
        TimestampMillisecondArray::from(vec![1700000000000_i64, 1700000001000_i64, 1700000002000_i64])
            .with_timezone("UTC"),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![list_col, struct_col, map_col, union_col, dec_col, ts_col],
    )?;

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

fn validate_view(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };

    let schema = reader.schema();
    if schema.fields().len() != 2 {
        return Err("invalid schema field count".into());
    }
    if schema.field(0).name() != "sv" || schema.field(0).data_type() != &DataType::Utf8View {
        return Err("invalid sv field".into());
    }
    if schema.field(1).name() != "bv" || schema.field(1).data_type() != &DataType::BinaryView {
        return Err("invalid bv field".into());
    }

    let batch = reader.next_batch()?.ok_or("missing batch")?;
    if reader.next_batch()?.is_some() {
        return Err("unexpected extra batch".into());
    }
    if batch.num_rows() != 4 {
        return Err("invalid row count".into());
    }

    let sv = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("sv downcast failed")?;
    let bv = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or("bv downcast failed")?;

    if sv.value(0) != "short" || !sv.is_null(1) || sv.value(2) != "tiny" || sv.value(3) != "this string is longer than twelve" {
        return Err("invalid sv values".into());
    }
    if bv.value(0) != b"ab" || bv.value(1) != b"this-binary-view-is-long" || !bv.is_null(2) || bv.value(3) != b"xy" {
        return Err("invalid bv values".into());
    }

    Ok(())
}

fn validate_complex(path: &Path, container: ContainerMode) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = match container {
        ContainerMode::Stream => EitherReader::Stream(StreamReader::try_new(file, None)?),
        ContainerMode::File => EitherReader::File(FileReader::try_new(file, None)?),
    };
    let schema = reader.schema();
    if schema.fields().len() != 6 {
        return Err("invalid schema field count".into());
    }
    let expected_names = ["list_i32", "struct_pair", "map_i32_i32", "u_dense", "dec", "ts"];
    for (idx, name) in expected_names.iter().enumerate() {
        if schema.field(idx).name() != *name {
            return Err("invalid field names".into());
        }
    }
    if !matches!(schema.field(0).data_type(), DataType::List(_)) {
        return Err("list_i32 must be list type".into());
    }
    if !matches!(schema.field(1).data_type(), DataType::Struct(_)) {
        return Err("struct_pair must be struct type".into());
    }
    if !matches!(schema.field(2).data_type(), DataType::Map(_, _)) {
        return Err("map_i32_i32 must be map type".into());
    }
    if !matches!(schema.field(3).data_type(), DataType::Union(_, UnionMode::Dense)) {
        return Err("u_dense must be dense union type".into());
    }
    if schema.field(4).data_type() != &DataType::Decimal128(10, 2) {
        return Err("dec must be decimal128(10,2)".into());
    }
    if schema.field(5).data_type() != &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())) {
        return Err("ts must be timestamp(ms, UTC)".into());
    }

    let batch = reader.next_batch()?.ok_or("missing batch")?;
    if reader.next_batch()?.is_some() {
        return Err("unexpected extra batch".into());
    }
    if batch.num_rows() != 3 {
        return Err("invalid row count".into());
    }

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("list downcast failed")?;
    if list_col.is_null(0) || !list_col.is_null(1) || list_col.is_null(2) {
        return Err("invalid list nulls".into());
    }
    let l0 = list_col.value(0);
    let l2 = list_col.value(2);
    let l0v = l0.as_any().downcast_ref::<Int32Array>().ok_or("list[0] values downcast failed")?;
    let l2v = l2.as_any().downcast_ref::<Int32Array>().ok_or("list[2] values downcast failed")?;
    if l0v.len() != 2 || l0v.value(0) != 1 || l0v.value(1) != 2 || l2v.len() != 1 || l2v.value(0) != 3 {
        return Err("invalid list values".into());
    }

    let struct_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("struct downcast failed")?;
    if struct_col.is_null(0) || !struct_col.is_null(1) || struct_col.is_null(2) {
        return Err("invalid struct nulls".into());
    }
    let sid = struct_col
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("struct id downcast failed")?;
    let sname = struct_col
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("struct name downcast failed")?;
    if sid.value(0) != 10 || sid.value(2) != 30 || sname.value(0) != "aa" || sname.value(2) != "cc" {
        return Err("invalid struct values".into());
    }

    let map_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or("map downcast failed")?;
    if map_col.is_null(0) || !map_col.is_null(1) || map_col.is_null(2) {
        return Err("invalid map nulls".into());
    }
    let m0 = map_col.value(0);
    let m2 = map_col.value(2);
    let m0s = m0.as_any().downcast_ref::<StructArray>().ok_or("map[0] struct downcast failed")?;
    let m2s = m2.as_any().downcast_ref::<StructArray>().ok_or("map[2] struct downcast failed")?;
    let m0k = m0s.column(0).as_any().downcast_ref::<Int32Array>().ok_or("map[0] key downcast failed")?;
    let m0v = m0s.column(1).as_any().downcast_ref::<Int32Array>().ok_or("map[0] value downcast failed")?;
    let m2k = m2s.column(0).as_any().downcast_ref::<Int32Array>().ok_or("map[2] key downcast failed")?;
    let m2v = m2s.column(1).as_any().downcast_ref::<Int32Array>().ok_or("map[2] value downcast failed")?;
    if m0k.len() != 2
        || m0k.value(0) != 1
        || m0k.value(1) != 2
        || m0v.value(0) != 10
        || m0v.value(1) != 20
        || m2k.len() != 1
        || m2k.value(0) != 3
        || m2v.value(0) != 30
    {
        return Err("invalid map values".into());
    }

    let union_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or("union downcast failed")?;
    let t0 = union_col.type_id(0);
    let t1 = union_col.type_id(1);
    let t2 = union_col.type_id(2);
    if t0 != t2 || t0 == t1 {
        return Err("invalid union type ids".into());
    }
    if union_col.value_offset(0) != 0 || union_col.value_offset(1) != 0 || union_col.value_offset(2) != 1 {
        return Err("invalid union value offsets".into());
    }
    let u0 = union_col.value(0);
    let u1 = union_col.value(1);
    let u2 = union_col.value(2);
    let u0i = u0.as_any().downcast_ref::<Int32Array>().ok_or("union[0] downcast failed")?;
    let u1b = u1.as_any().downcast_ref::<BooleanArray>().ok_or("union[1] downcast failed")?;
    let u2i = u2.as_any().downcast_ref::<Int32Array>().ok_or("union[2] downcast failed")?;
    if u0i.value(0) != 100 || !u1b.value(0) || u2i.value(0) != 200 {
        return Err("invalid union values".into());
    }

    let dec = batch
        .column(4)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or("decimal downcast failed")?;
    if dec.value(0) != 12345 || dec.value(1) != -42 || dec.value(2) != 0 {
        return Err("invalid decimal values".into());
    }

    let ts = batch
        .column(5)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or("timestamp downcast failed")?;
    if ts.value(0) != 1700000000000 || ts.value(1) != 1700000001000 || ts.value(2) != 1700000002000 {
        return Err("invalid timestamp values".into());
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
                        "usage: <generate|validate> <path.arrow> [canonical|dict-delta|ree|complex|extension|view] [stream|file]"
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
        ("generate", "complex") => generate_complex(path, container),
        ("validate", "complex") => validate_complex(path, container),
        ("generate", "extension") => generate_extension(path, container),
        ("validate", "extension") => validate_extension(path, container),
        ("generate", "view") => generate_view(path, container),
        ("validate", "view") => validate_view(path, container),
        _ => Err("usage: <generate|validate> <path.arrow> [canonical|dict-delta|ree|complex|extension|view] [stream|file]".into()),
    }
}
