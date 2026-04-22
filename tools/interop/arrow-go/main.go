package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type containerMode string

type fixtureCase string

const (
	containerStream containerMode = "stream"
	containerFile   containerMode = "file"

	fixtureCanonical fixtureCase = "canonical"
	fixtureDictDelta fixtureCase = "dict-delta"
	fixtureREE       fixtureCase = "ree"
	fixtureREEInt16  fixtureCase = "ree-int16"
	fixtureREEInt64  fixtureCase = "ree-int64"
	fixtureComplex   fixtureCase = "complex"
	fixtureExtension fixtureCase = "extension"
	fixtureView      fixtureCase = "view"

	extensionTypeName     = "com.example.int32_ext"
	extensionTypeMetadata = "v1"
)

func usageError() error {
	return errors.New("usage: <generate|validate> <path.arrow> [canonical|dict-delta|ree|ree-int16|ree-int64|complex|extension|view] [stream|file]")
}

func parseContainer(raw string) (containerMode, error) {
	switch raw {
	case "", "stream":
		return containerStream, nil
	case "file":
		return containerFile, nil
	default:
		return "", usageError()
	}
}

func parseFixtureCase(raw string) (fixtureCase, error) {
	switch raw {
	case "", string(fixtureCanonical):
		return fixtureCanonical, nil
	case string(fixtureDictDelta):
		return fixtureDictDelta, nil
	case string(fixtureREE):
		return fixtureREE, nil
	case string(fixtureREEInt16):
		return fixtureREEInt16, nil
	case string(fixtureREEInt64):
		return fixtureREEInt64, nil
	case string(fixtureComplex):
		return fixtureComplex, nil
	case string(fixtureExtension):
		return fixtureExtension, nil
	case string(fixtureView):
		return fixtureView, nil
	default:
		return "", usageError()
	}
}

func validateCaseContainer(fixture fixtureCase, container containerMode) error {
	if fixture == fixtureDictDelta && container == containerFile {
		return errors.New("dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches")
	}
	return nil
}

func canonicalSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func dictDeltaSchema() *arrow.Schema {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	return arrow.NewSchema([]arrow.Field{{Name: "color", Type: dictType, Nullable: false}}, nil)
}

func reeSchema(runEndType arrow.DataType) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "ree", Type: arrow.RunEndEncodedOf(runEndType, arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
}

func complexSchema() *arrow.Schema {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	structType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	mapType := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	unionType := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		},
		[]arrow.UnionTypeCode{0, 1},
	)
	decType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}

	return arrow.NewSchema([]arrow.Field{
		{Name: "list_i32", Type: listType, Nullable: true},
		{Name: "struct_pair", Type: structType, Nullable: true},
		{Name: "map_i32_i32", Type: mapType, Nullable: true},
		{Name: "u_dense", Type: unionType, Nullable: true},
		{Name: "dec", Type: decType, Nullable: false},
		{Name: "ts", Type: tsType, Nullable: false},
	}, nil)
}

func extensionSchema(extType arrow.ExtensionType) *arrow.Schema {
	md := arrow.NewMetadata([]string{"owner"}, []string{"interop"})
	return arrow.NewSchema([]arrow.Field{{Name: "ext_i32", Type: extType, Nullable: true, Metadata: md}}, nil)
}

func viewSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "sv", Type: arrow.BinaryTypes.StringView, Nullable: true},
		{Name: "bv", Type: arrow.BinaryTypes.BinaryView, Nullable: true},
	}, nil)
}

func reeRunEndType(fixture fixtureCase) (arrow.DataType, error) {
	switch fixture {
	case fixtureREE:
		return arrow.PrimitiveTypes.Int32, nil
	case fixtureREEInt16:
		return arrow.PrimitiveTypes.Int16, nil
	case fixtureREEInt64:
		return arrow.PrimitiveTypes.Int64, nil
	default:
		return nil, fmt.Errorf("invalid ree fixture case: %s", fixture)
	}
}

type int32ExtType struct {
	arrow.ExtensionBase
}

func newInt32ExtType() *int32ExtType {
	return &int32ExtType{ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32}}
}

func (*int32ExtType) ArrayType() reflect.Type {
	return reflect.TypeOf(int32ExtArray{})
}

func (*int32ExtType) ExtensionName() string {
	return extensionTypeName
}

func (*int32ExtType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*int32ExtType)
	return ok
}

func (*int32ExtType) Serialize() string {
	return extensionTypeMetadata
}

func (*int32ExtType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !arrow.TypeEqual(storageType, arrow.PrimitiveTypes.Int32) {
		return nil, fmt.Errorf("unexpected extension storage type: got=%s want=int32", storageType)
	}
	if data != extensionTypeMetadata {
		return nil, fmt.Errorf("unexpected extension metadata: got=%q want=%q", data, extensionTypeMetadata)
	}
	return newInt32ExtType(), nil
}

type int32ExtArray struct {
	array.ExtensionArrayBase
}

func (a *int32ExtArray) ValueStr(i int) string {
	return a.Storage().ValueStr(i)
}

func ensureInt32ExtensionRegistered() (arrow.ExtensionType, func(), error) {
	if existing := arrow.GetExtensionType(extensionTypeName); existing != nil {
		return existing, func() {}, nil
	}

	ext := newInt32ExtType()
	if err := arrow.RegisterExtensionType(ext); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = arrow.UnregisterExtensionType(extensionTypeName)
	}
	return ext, cleanup, nil
}

func writeRecords(path string, container containerMode, schema *arrow.Schema, pool memory.Allocator, records []arrow.Record, extraOpts ...ipc.Option) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	switch container {
	case containerStream:
		opts := append([]ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(pool)}, extraOpts...)
		w := ipc.NewWriter(f, opts...)
		for _, rec := range records {
			if err := w.Write(rec); err != nil {
				_ = w.Close()
				return err
			}
		}
		return w.Close()

	case containerFile:
		opts := append([]ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(pool)}, extraOpts...)
		w, err := ipc.NewFileWriter(f, opts...)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if err := w.Write(rec); err != nil {
				_ = w.Close()
				return err
			}
		}
		return w.Close()

	default:
		return usageError()
	}
}

func releaseRecords(records []arrow.Record) {
	for _, rec := range records {
		rec.Release()
	}
}

func readRecords(path string, container containerMode, pool memory.Allocator) (*arrow.Schema, []arrow.Record, error) {
	switch container {
	case containerStream:
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		defer f.Close()

		r, err := ipc.NewReader(f, ipc.WithAllocator(pool))
		if err != nil {
			return nil, nil, err
		}
		defer r.Release()

		records := make([]arrow.Record, 0, 2)
		for r.Next() {
			rec := r.Record()
			rec.Retain()
			records = append(records, rec)
		}
		if err := r.Err(); err != nil {
			releaseRecords(records)
			return nil, nil, err
		}
		return r.Schema(), records, nil

	case containerFile:
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		defer f.Close()

		r, err := ipc.NewFileReader(f, ipc.WithAllocator(pool))
		if err != nil {
			return nil, nil, err
		}
		defer r.Close()

		records := make([]arrow.Record, 0, r.NumRecords())
		for i := 0; i < r.NumRecords(); i++ {
			rec, err := r.Record(i)
			if err != nil {
				releaseRecords(records)
				return nil, nil, err
			}
			rec.Retain()
			records = append(records, rec)
		}
		return r.Schema(), records, nil

	default:
		return nil, nil, usageError()
	}
}

func generateCanonical(path string, container containerMode, pool memory.Allocator) error {
	schema := canonicalSchema()

	idBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int32{1, 2, 3}, nil)
	ids := idBuilder.NewArray()
	defer ids.Release()

	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()
	nameBuilder.Append("alice")
	nameBuilder.AppendNull()
	nameBuilder.Append("bob")
	names := nameBuilder.NewArray()
	defer names.Release()

	record := array.NewRecord(schema, []arrow.Array{ids, names}, 3)
	defer record.Release()

	return writeRecords(path, container, schema, pool, []arrow.Record{record})
}

func generateDictDelta(path string, container containerMode, pool memory.Allocator) error {
	if container == containerFile {
		return errors.New("dict-delta fixture is stream-only: IPC file format disallows dictionary replacement across batches")
	}

	schema := dictDeltaSchema()
	dictType, ok := schema.Field(0).Type.(*arrow.DictionaryType)
	if !ok {
		return errors.New("dict-delta schema is not dictionary")
	}

	firstBuilder := array.NewDictionaryBuilder(pool, dictType)
	defer firstBuilder.Release()
	firstBinary, ok := firstBuilder.(*array.BinaryDictionaryBuilder)
	if !ok {
		return errors.New("dict-delta builder downcast failed")
	}
	if err := firstBinary.AppendString("red"); err != nil {
		return err
	}
	if err := firstBinary.AppendString("blue"); err != nil {
		return err
	}
	firstCol := firstBuilder.NewArray()
	defer firstCol.Release()
	firstRec := array.NewRecord(schema, []arrow.Array{firstCol}, 2)
	defer firstRec.Release()

	bootstrapBuilder := array.NewStringBuilder(pool)
	defer bootstrapBuilder.Release()
	bootstrapBuilder.Append("red")
	bootstrapBuilder.Append("blue")
	bootstrap := bootstrapBuilder.NewArray()
	defer bootstrap.Release()

	secondBuilder := array.NewDictionaryBuilderWithDict(pool, dictType, bootstrap)
	defer secondBuilder.Release()
	secondBinary, ok := secondBuilder.(*array.BinaryDictionaryBuilder)
	if !ok {
		return errors.New("dict-delta second builder downcast failed")
	}
	if err := secondBinary.AppendString("green"); err != nil {
		return err
	}
	secondCol := secondBuilder.NewArray()
	defer secondCol.Release()
	secondRec := array.NewRecord(schema, []arrow.Array{secondCol}, 1)
	defer secondRec.Release()

	return writeRecords(
		path,
		container,
		schema,
		pool,
		[]arrow.Record{firstRec, secondRec},
		ipc.WithDictionaryDeltas(true),
	)
}

func generateREE(path string, fixture fixtureCase, container containerMode, pool memory.Allocator) error {
	runEnds, err := reeRunEndType(fixture)
	if err != nil {
		return err
	}
	schema := reeSchema(runEnds)

	reeBuilder := array.NewRunEndEncodedBuilder(pool, runEnds, arrow.PrimitiveTypes.Int32)
	defer reeBuilder.Release()
	valueBuilder, ok := reeBuilder.ValueBuilder().(*array.Int32Builder)
	if !ok {
		return errors.New("ree value builder downcast failed")
	}

	reeBuilder.Append(2)
	valueBuilder.Append(100)
	reeBuilder.Append(3)
	valueBuilder.Append(200)

	reeCol := reeBuilder.NewArray()
	defer reeCol.Release()
	record := array.NewRecord(schema, []arrow.Array{reeCol}, 5)
	defer record.Release()

	return writeRecords(path, container, schema, pool, []arrow.Record{record})
}

func generateComplex(path string, container containerMode, pool memory.Allocator) error {
	schema := complexSchema()

	listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer listBuilder.Release()
	listValues := listBuilder.ValueBuilder().(*array.Int32Builder)
	listBuilder.Append(true)
	listValues.Append(1)
	listValues.Append(2)
	listBuilder.Append(false)
	listBuilder.Append(true)
	listValues.Append(3)
	listCol := listBuilder.NewArray()
	defer listCol.Release()

	structBuilder := array.NewStructBuilder(pool, schema.Field(1).Type.(*arrow.StructType))
	defer structBuilder.Release()
	structIDs := structBuilder.FieldBuilder(0).(*array.Int32Builder)
	structNames := structBuilder.FieldBuilder(1).(*array.StringBuilder)
	structBuilder.Append(true)
	structIDs.Append(10)
	structNames.Append("aa")
	structBuilder.Append(false)
	structBuilder.Append(true)
	structIDs.Append(30)
	structNames.Append("cc")
	structCol := structBuilder.NewArray()
	defer structCol.Release()

	mapBuilder := array.NewMapBuilder(pool, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, false)
	defer mapBuilder.Release()
	mapKeys := mapBuilder.KeyBuilder().(*array.Int32Builder)
	mapVals := mapBuilder.ItemBuilder().(*array.Int32Builder)
	mapBuilder.Append(true)
	mapKeys.Append(1)
	mapVals.Append(10)
	mapKeys.Append(2)
	mapVals.Append(20)
	mapBuilder.Append(false)
	mapBuilder.Append(true)
	mapKeys.Append(3)
	mapVals.Append(30)
	mapCol := mapBuilder.NewArray()
	defer mapCol.Release()

	unionBuilder := array.NewDenseUnionBuilder(pool, schema.Field(3).Type.(*arrow.DenseUnionType))
	defer unionBuilder.Release()
	unionInts := unionBuilder.Child(0).(*array.Int32Builder)
	unionBools := unionBuilder.Child(1).(*array.BooleanBuilder)
	unionBuilder.Append(0)
	unionInts.Append(100)
	unionBuilder.Append(1)
	unionBools.Append(true)
	unionBuilder.Append(0)
	unionInts.Append(200)
	unionCol := unionBuilder.NewArray()
	defer unionCol.Release()

	decBuilder := array.NewDecimal128Builder(pool, schema.Field(4).Type.(*arrow.Decimal128Type))
	defer decBuilder.Release()
	decBuilder.Append(decimal128.FromI64(12345))
	decBuilder.Append(decimal128.FromI64(-42))
	decBuilder.Append(decimal128.FromI64(0))
	decCol := decBuilder.NewArray()
	defer decCol.Release()

	tsBuilder := array.NewTimestampBuilder(pool, schema.Field(5).Type.(*arrow.TimestampType))
	defer tsBuilder.Release()
	tsBuilder.Append(arrow.Timestamp(1700000000000))
	tsBuilder.Append(arrow.Timestamp(1700000001000))
	tsBuilder.Append(arrow.Timestamp(1700000002000))
	tsCol := tsBuilder.NewArray()
	defer tsCol.Release()

	record := array.NewRecord(schema, []arrow.Array{listCol, structCol, mapCol, unionCol, decCol, tsCol}, 3)
	defer record.Release()

	return writeRecords(path, container, schema, pool, []arrow.Record{record})
}

func generateExtension(path string, container containerMode, pool memory.Allocator) error {
	extType, cleanup, err := ensureInt32ExtensionRegistered()
	if err != nil {
		return err
	}
	defer cleanup()

	schema := extensionSchema(extType)

	extBuilder := array.NewExtensionBuilder(pool, extType)
	defer extBuilder.Release()
	storage, ok := extBuilder.StorageBuilder().(*array.Int32Builder)
	if !ok {
		return errors.New("extension storage builder downcast failed")
	}
	storage.Append(7)
	storage.AppendNull()
	storage.Append(11)

	extCol := extBuilder.NewArray()
	defer extCol.Release()
	record := array.NewRecord(schema, []arrow.Array{extCol}, 3)
	defer record.Release()

	return writeRecords(path, container, schema, pool, []arrow.Record{record})
}

func generateView(path string, container containerMode, pool memory.Allocator) error {
	schema := viewSchema()

	svBuilder := array.NewStringViewBuilder(pool)
	defer svBuilder.Release()
	svBuilder.Append("short")
	svBuilder.AppendNull()
	svBuilder.Append("tiny")
	svBuilder.Append("this string is longer than twelve")
	svCol := svBuilder.NewArray()
	defer svCol.Release()

	bvBuilder := array.NewBinaryViewBuilder(pool)
	defer bvBuilder.Release()
	bvBuilder.Append([]byte("ab"))
	bvBuilder.Append([]byte("this-binary-view-is-long"))
	bvBuilder.AppendNull()
	bvBuilder.Append([]byte("xy"))
	bvCol := bvBuilder.NewArray()
	defer bvCol.Release()

	record := array.NewRecord(schema, []arrow.Array{svCol, bvCol}, 4)
	defer record.Release()

	return writeRecords(path, container, schema, pool, []arrow.Record{record})
}

func generate(path string, fixture fixtureCase, container containerMode) error {
	pool := memory.NewGoAllocator()

	switch fixture {
	case fixtureCanonical:
		return generateCanonical(path, container, pool)
	case fixtureDictDelta:
		return generateDictDelta(path, container, pool)
	case fixtureREE, fixtureREEInt16, fixtureREEInt64:
		return generateREE(path, fixture, container, pool)
	case fixtureComplex:
		return generateComplex(path, container, pool)
	case fixtureExtension:
		return generateExtension(path, container, pool)
	case fixtureView:
		return generateView(path, container, pool)
	default:
		return usageError()
	}
}

func validateCanonical(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 2 {
		return errors.New("invalid schema field count")
	}
	if fields[0].Name != "id" || fields[0].Type.ID() != arrow.INT32 {
		return errors.New("invalid id field")
	}
	if fields[1].Name != "name" || fields[1].Type.ID() != arrow.STRING {
		return errors.New("invalid name field")
	}
	if len(records) != 1 {
		return fmt.Errorf("expected exactly one batch, got=%d", len(records))
	}

	rec := records[0]
	if rec.NumRows() != 3 {
		return fmt.Errorf("invalid row count: got=%d want=3", rec.NumRows())
	}
	if rec.NumCols() != 2 {
		return fmt.Errorf("invalid column count: got=%d want=2", rec.NumCols())
	}

	ids, ok := rec.Column(0).(*array.Int32)
	if !ok {
		return errors.New("id column downcast failed")
	}
	names, ok := rec.Column(1).(*array.String)
	if !ok {
		return errors.New("name column downcast failed")
	}

	if ids.Value(0) != 1 || ids.Value(1) != 2 || ids.Value(2) != 3 {
		return errors.New("invalid id values")
	}
	if names.Value(0) != "alice" || !names.IsNull(1) || names.Value(2) != "bob" {
		return errors.New("invalid name values")
	}
	return nil
}

func validateDictDelta(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 1 {
		return errors.New("invalid schema field count")
	}
	if fields[0].Name != "color" || fields[0].Type.ID() != arrow.DICTIONARY {
		return errors.New("invalid color field")
	}
	dictType, ok := fields[0].Type.(*arrow.DictionaryType)
	if !ok {
		return errors.New("color field dictionary type assertion failed")
	}
	if dictType.ValueType.ID() != arrow.STRING {
		return errors.New("color dictionary must be utf8")
	}
	if len(records) != 2 {
		return fmt.Errorf("expected exactly two batches, got=%d", len(records))
	}
	if records[0].NumRows() != 2 || records[1].NumRows() != 1 {
		return errors.New("invalid dict-delta row counts")
	}

	firstDict, ok := records[0].Column(0).(*array.Dictionary)
	if !ok {
		return errors.New("first dict column downcast failed")
	}
	secondDict, ok := records[1].Column(0).(*array.Dictionary)
	if !ok {
		return errors.New("second dict column downcast failed")
	}

	firstValues, ok := firstDict.Dictionary().(*array.String)
	if !ok {
		return errors.New("first dictionary values downcast failed")
	}
	secondValues, ok := secondDict.Dictionary().(*array.String)
	if !ok {
		return errors.New("second dictionary values downcast failed")
	}

	if firstValues.Value(firstDict.GetValueIndex(0)) != "red" || firstValues.Value(firstDict.GetValueIndex(1)) != "blue" {
		return errors.New("invalid dict-delta first batch values")
	}
	if secondValues.Value(secondDict.GetValueIndex(0)) != "green" {
		return errors.New("invalid dict-delta second batch values")
	}
	return nil
}

func validateREE(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 1 {
		return errors.New("invalid schema field count")
	}
	if fields[0].Name != "ree" || fields[0].Type.ID() != arrow.RUN_END_ENCODED {
		return errors.New("invalid ree field")
	}
	reeType, ok := fields[0].Type.(*arrow.RunEndEncodedType)
	if !ok {
		return errors.New("ree type assertion failed")
	}
	switch reeType.RunEnds().ID() {
	case arrow.INT16, arrow.INT32, arrow.INT64:
	default:
		return errors.New("invalid ree run-end type")
	}
	if reeType.Encoded().ID() != arrow.INT32 {
		return errors.New("invalid ree value type")
	}
	if len(records) != 1 {
		return fmt.Errorf("expected exactly one batch, got=%d", len(records))
	}

	rec := records[0]
	if rec.NumRows() != 5 {
		return fmt.Errorf("invalid ree row count: got=%d want=5", rec.NumRows())
	}

	reeCol, ok := rec.Column(0).(*array.RunEndEncoded)
	if !ok {
		return errors.New("ree column downcast failed")
	}
	values, ok := reeCol.Values().(*array.Int32)
	if !ok {
		return errors.New("ree values downcast failed")
	}

	expected := []int32{100, 100, 200, 200, 200}
	for i, want := range expected {
		if got := values.Value(reeCol.GetPhysicalIndex(i)); got != want {
			return fmt.Errorf("invalid ree value at row %d: got=%d want=%d", i, got, want)
		}
	}
	return nil
}

func validateComplex(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 6 {
		return errors.New("invalid schema field count")
	}
	expectedNames := []string{"list_i32", "struct_pair", "map_i32_i32", "u_dense", "dec", "ts"}
	for i, name := range expectedNames {
		if fields[i].Name != name {
			return errors.New("invalid complex field names")
		}
	}
	if fields[0].Type.ID() != arrow.LIST ||
		fields[1].Type.ID() != arrow.STRUCT ||
		fields[2].Type.ID() != arrow.MAP ||
		fields[3].Type.ID() != arrow.DENSE_UNION ||
		fields[4].Type.ID() != arrow.DECIMAL128 ||
		fields[5].Type.ID() != arrow.TIMESTAMP {
		return errors.New("invalid complex field types")
	}
	decType, ok := fields[4].Type.(*arrow.Decimal128Type)
	if !ok || decType.Precision != 10 || decType.Scale != 2 {
		return errors.New("invalid decimal type")
	}
	tsType, ok := fields[5].Type.(*arrow.TimestampType)
	if !ok || tsType.Unit != arrow.Millisecond || tsType.TimeZone != "UTC" {
		return errors.New("invalid timestamp type")
	}

	if len(records) != 1 {
		return fmt.Errorf("expected exactly one batch, got=%d", len(records))
	}
	rec := records[0]
	if rec.NumRows() != 3 {
		return fmt.Errorf("invalid complex row count: got=%d want=3", rec.NumRows())
	}

	listCol, ok := rec.Column(0).(*array.List)
	if !ok {
		return errors.New("list column downcast failed")
	}
	if listCol.IsNull(0) || !listCol.IsNull(1) || listCol.IsNull(2) {
		return errors.New("invalid list nulls")
	}
	listOffsets := listCol.Offsets()
	if len(listOffsets) < 4 || listOffsets[0] != 0 || listOffsets[1] != 2 || listOffsets[2] != 2 || listOffsets[3] != 3 {
		return errors.New("invalid list offsets")
	}
	listValues, ok := listCol.ListValues().(*array.Int32)
	if !ok {
		return errors.New("list values downcast failed")
	}
	if listValues.Value(0) != 1 || listValues.Value(1) != 2 || listValues.Value(2) != 3 {
		return errors.New("invalid list values")
	}

	structCol, ok := rec.Column(1).(*array.Struct)
	if !ok {
		return errors.New("struct column downcast failed")
	}
	if structCol.IsNull(0) || !structCol.IsNull(1) || structCol.IsNull(2) {
		return errors.New("invalid struct nulls")
	}
	structIDs, ok := structCol.Field(0).(*array.Int32)
	if !ok {
		return errors.New("struct id downcast failed")
	}
	structNames, ok := structCol.Field(1).(*array.String)
	if !ok {
		return errors.New("struct name downcast failed")
	}
	if structIDs.Value(0) != 10 || structIDs.Value(2) != 30 || structNames.Value(0) != "aa" || structNames.Value(2) != "cc" {
		return errors.New("invalid struct values")
	}

	mapCol, ok := rec.Column(2).(*array.Map)
	if !ok {
		return errors.New("map column downcast failed")
	}
	if mapCol.IsNull(0) || !mapCol.IsNull(1) || mapCol.IsNull(2) {
		return errors.New("invalid map nulls")
	}
	mapOffsets := mapCol.Offsets()
	if len(mapOffsets) < 4 || mapOffsets[0] != 0 || mapOffsets[1] != 2 || mapOffsets[2] != 2 || mapOffsets[3] != 3 {
		return errors.New("invalid map offsets")
	}
	mapKeys, ok := mapCol.Keys().(*array.Int32)
	if !ok {
		return errors.New("map keys downcast failed")
	}
	mapItems, ok := mapCol.Items().(*array.Int32)
	if !ok {
		return errors.New("map items downcast failed")
	}
	if mapKeys.Value(0) != 1 || mapKeys.Value(1) != 2 || mapKeys.Value(2) != 3 {
		return errors.New("invalid map keys")
	}
	if mapItems.Value(0) != 10 || mapItems.Value(1) != 20 || mapItems.Value(2) != 30 {
		return errors.New("invalid map values")
	}

	unionCol, ok := rec.Column(3).(*array.DenseUnion)
	if !ok {
		return errors.New("union column downcast failed")
	}
	t0 := unionCol.TypeCode(0)
	t1 := unionCol.TypeCode(1)
	t2 := unionCol.TypeCode(2)
	if t0 != t2 || t0 == t1 {
		return errors.New("invalid union type ids")
	}
	if unionCol.ValueOffset(0) != 0 || unionCol.ValueOffset(1) != 0 || unionCol.ValueOffset(2) != 1 {
		return errors.New("invalid union offsets")
	}

	union0 := unionCol.Field(unionCol.ChildID(0))
	union1 := unionCol.Field(unionCol.ChildID(1))
	union2 := unionCol.Field(unionCol.ChildID(2))
	v0, ok := union0.(*array.Int32)
	if !ok {
		return errors.New("union value[0] downcast failed")
	}
	v1, ok := union1.(*array.Boolean)
	if !ok {
		return errors.New("union value[1] downcast failed")
	}
	v2, ok := union2.(*array.Int32)
	if !ok {
		return errors.New("union value[2] downcast failed")
	}
	if v0.Value(int(unionCol.ValueOffset(0))) != 100 || !v1.Value(int(unionCol.ValueOffset(1))) || v2.Value(int(unionCol.ValueOffset(2))) != 200 {
		return errors.New("invalid union values")
	}

	decCol, ok := rec.Column(4).(*array.Decimal128)
	if !ok {
		return errors.New("decimal column downcast failed")
	}
	if decCol.Value(0).Cmp(decimal128.FromI64(12345)) != 0 || decCol.Value(1).Cmp(decimal128.FromI64(-42)) != 0 || decCol.Value(2).Cmp(decimal128.FromI64(0)) != 0 {
		return errors.New("invalid decimal values")
	}

	tsCol, ok := rec.Column(5).(*array.Timestamp)
	if !ok {
		return errors.New("timestamp column downcast failed")
	}
	if tsCol.Value(0) != 1700000000000 || tsCol.Value(1) != 1700000001000 || tsCol.Value(2) != 1700000002000 {
		return errors.New("invalid timestamp values")
	}

	return nil
}

func validateExtension(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 1 {
		return errors.New("invalid schema field count")
	}
	if fields[0].Name != "ext_i32" || fields[0].Type.ID() != arrow.EXTENSION {
		return errors.New("invalid extension field")
	}
	extType, ok := fields[0].Type.(arrow.ExtensionType)
	if !ok {
		return errors.New("extension type assertion failed")
	}
	if extType.ExtensionName() != extensionTypeName {
		return errors.New("invalid extension name")
	}
	if extType.Serialize() != extensionTypeMetadata {
		return errors.New("invalid extension metadata")
	}
	if !arrow.TypeEqual(extType.StorageType(), arrow.PrimitiveTypes.Int32) {
		return errors.New("invalid extension storage type")
	}
	if fields[0].Metadata.Len() != 1 {
		return errors.New("invalid extension field metadata count")
	}
	if owner, ok := fields[0].Metadata.GetValue("owner"); !ok || owner != "interop" {
		return errors.New("invalid extension owner metadata")
	}
	if len(records) != 1 {
		return fmt.Errorf("expected exactly one batch, got=%d", len(records))
	}

	rec := records[0]
	if rec.NumRows() != 3 {
		return fmt.Errorf("invalid extension row count: got=%d want=3", rec.NumRows())
	}
	extArr, ok := rec.Column(0).(array.ExtensionArray)
	if !ok {
		return errors.New("extension array assertion failed")
	}
	storage, ok := extArr.Storage().(*array.Int32)
	if !ok {
		return errors.New("extension storage downcast failed")
	}
	if storage.Value(0) != 7 || !storage.IsNull(1) || storage.Value(2) != 11 {
		return errors.New("invalid extension values")
	}
	return nil
}

func validateView(schema *arrow.Schema, records []arrow.Record) error {
	fields := schema.Fields()
	if len(fields) != 2 {
		return errors.New("invalid schema field count")
	}
	if fields[0].Name != "sv" || fields[0].Type.ID() != arrow.STRING_VIEW {
		return errors.New("invalid sv field")
	}
	if fields[1].Name != "bv" || fields[1].Type.ID() != arrow.BINARY_VIEW {
		return errors.New("invalid bv field")
	}
	if len(records) != 1 {
		return fmt.Errorf("expected exactly one batch, got=%d", len(records))
	}

	rec := records[0]
	if rec.NumRows() != 4 {
		return fmt.Errorf("invalid view row count: got=%d want=4", rec.NumRows())
	}
	sv, ok := rec.Column(0).(*array.StringView)
	if !ok {
		return errors.New("sv downcast failed")
	}
	bv, ok := rec.Column(1).(*array.BinaryView)
	if !ok {
		return errors.New("bv downcast failed")
	}

	if sv.Value(0) != "short" || !sv.IsNull(1) || sv.Value(2) != "tiny" || sv.Value(3) != "this string is longer than twelve" {
		return errors.New("invalid sv values")
	}
	if !bytes.Equal(bv.Value(0), []byte("ab")) ||
		!bytes.Equal(bv.Value(1), []byte("this-binary-view-is-long")) ||
		!bv.IsNull(2) ||
		!bytes.Equal(bv.Value(3), []byte("xy")) {
		return errors.New("invalid bv values")
	}
	return nil
}

func validate(path string, fixture fixtureCase, container containerMode) error {
	pool := memory.NewGoAllocator()

	if fixture == fixtureExtension {
		_, cleanup, err := ensureInt32ExtensionRegistered()
		if err != nil {
			return err
		}
		defer cleanup()
	}

	schema, records, err := readRecords(path, container, pool)
	if err != nil {
		return err
	}
	defer releaseRecords(records)

	switch fixture {
	case fixtureCanonical:
		return validateCanonical(schema, records)
	case fixtureDictDelta:
		return validateDictDelta(schema, records)
	case fixtureREE, fixtureREEInt16, fixtureREEInt64:
		return validateREE(schema, records)
	case fixtureComplex:
		return validateComplex(schema, records)
	case fixtureExtension:
		return validateExtension(schema, records)
	case fixtureView:
		return validateView(schema, records)
	default:
		return usageError()
	}
}

func main() {
	args := os.Args[1:]
	if len(args) < 2 || len(args) > 4 {
		fmt.Fprintln(os.Stderr, usageError())
		os.Exit(2)
	}

	mode := args[0]
	path := args[1]
	fixtureRaw := string(fixtureCanonical)
	containerRaw := string(containerStream)

	if len(args) >= 3 {
		if args[2] == string(containerStream) || args[2] == string(containerFile) {
			containerRaw = args[2]
		} else {
			fixtureRaw = args[2]
			if len(args) == 4 {
				containerRaw = args[3]
			}
		}
	}

	fixture, err := parseFixtureCase(fixtureRaw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	container, err := parseContainer(containerRaw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := validateCaseContainer(fixture, container); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	switch mode {
	case "generate":
		err = generate(path, fixture, container)
	case "validate":
		err = validate(path, fixture, container)
	default:
		err = usageError()
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
