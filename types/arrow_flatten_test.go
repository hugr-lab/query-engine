package types

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNeedsFlatten_Simple(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	if NeedsFlatten(schema) {
		t.Error("expected NeedsFlatten=false for simple schema")
	}
}

func TestNeedsFlatten_WithStruct(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "addr", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
		)},
	}, nil)

	if !NeedsFlatten(schema) {
		t.Error("expected NeedsFlatten=true for schema with struct")
	}
}

func TestNeedsFlatten_WithList(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}, nil)

	if !NeedsFlatten(schema) {
		t.Error("expected NeedsFlatten=true for schema with list")
	}
}

func TestFlattenRecord_NoComplexTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.NumCols() != 2 || out.NumRows() != 2 {
		t.Errorf("expected 2 cols 2 rows, got %d cols %d rows", out.NumCols(), out.NumRows())
	}
}

func TestFlattenRecord_StructFirstLevel(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "addr", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "zip", Type: arrow.PrimitiveTypes.Int32},
		)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	sb := bldr.Field(1).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("NYC")
	sb.FieldBuilder(1).(*array.Int32Builder).Append(10001)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("LA")
	sb.FieldBuilder(1).(*array.Int32Builder).Append(90001)

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.NumCols() != 3 {
		t.Fatalf("expected 3 cols, got %d", out.NumCols())
	}

	expectedNames := []string{"id", "addr.city", "addr.zip"}
	for i, name := range expectedNames {
		if out.Schema().Field(i).Name != name {
			t.Errorf("field %d: expected %q, got %q", i, name, out.Schema().Field(i).Name)
		}
	}

	cityCol := out.Column(1).(*array.String)
	if cityCol.Value(0) != "NYC" || cityCol.Value(1) != "LA" {
		t.Errorf("unexpected city values: %v, %v", cityCol.Value(0), cityCol.Value(1))
	}
}

func TestFlattenRecord_NestedStruct(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.StructOf(
			arrow.Field{Name: "inner", Type: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64},
			)},
		)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	outer := bldr.Field(0).(*array.StructBuilder)
	outer.Append(true)
	inner := outer.FieldBuilder(0).(*array.StructBuilder)
	inner.Append(true)
	inner.FieldBuilder(0).(*array.Int64Builder).Append(42)

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.NumCols() != 1 {
		t.Fatalf("expected 1 col, got %d", out.NumCols())
	}
	if out.Schema().Field(0).Name != "data.inner.value" {
		t.Errorf("expected 'data.inner.value', got %q", out.Schema().Field(0).Name)
	}

	val := out.Column(0).(*array.Int64)
	if val.Value(0) != 42 {
		t.Errorf("expected 42, got %d", val.Value(0))
	}
}

func TestFlattenRecord_StructWithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	sb := bldr.Field(0).(*array.StructBuilder)
	sb.AppendNull()
	sb.FieldBuilder(0).(*array.Int64Builder).Append(0)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(99)

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	col := out.Column(0)
	if !col.IsNull(0) {
		t.Error("row 0 should be null (parent struct is null)")
	}
	if col.IsNull(1) {
		t.Error("row 1 should not be null")
	}
}

func TestFlattenRecord_ListToJSON(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	lb := bldr.Field(0).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.StringBuilder)

	lb.Append(true)
	vb.Append("a")
	vb.Append("b")

	lb.Append(true)
	vb.Append("c")

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.Schema().Field(0).Type.ID() != arrow.STRING {
		t.Fatalf("expected STRING type, got %s", out.Schema().Field(0).Type)
	}

	col := out.Column(0).(*array.String)

	var row0 []string
	if err := json.Unmarshal([]byte(col.Value(0)), &row0); err != nil {
		t.Fatal(err)
	}
	if len(row0) != 2 || row0[0] != "a" || row0[1] != "b" {
		t.Errorf("row 0: expected [a b], got %v", row0)
	}

	var row1 []string
	if err := json.Unmarshal([]byte(col.Value(1)), &row1); err != nil {
		t.Fatal(err)
	}
	if len(row1) != 1 || row1[0] != "c" {
		t.Errorf("row 1: expected [c], got %v", row1)
	}
}

func TestFlattenRecord_MapToJSON(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "props", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	mb := bldr.Field(0).(*array.MapBuilder)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.Int64Builder)

	mb.Append(true)
	kb.Append("x")
	ib.Append(1)
	kb.Append("y")
	ib.Append(2)

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	col := out.Column(0).(*array.String)
	// Arrow's GetOneForMarshal serializes Map as array of {key, value} structs
	var entries []map[string]any
	if err := json.Unmarshal([]byte(col.Value(0)), &entries); err != nil {
		t.Fatalf("failed to parse map JSON %q: %v", col.Value(0), err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestFlattenRecord_NullList(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	lb := bldr.Field(0).(*array.ListBuilder)
	lb.AppendNull()
	lb.Append(true)
	lb.ValueBuilder().(*array.StringBuilder).Append("x")

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	col := out.Column(0).(*array.String)
	if !col.IsNull(0) {
		t.Error("row 0 should be null")
	}
	if col.IsNull(1) {
		t.Error("row 1 should not be null")
	}
}

func TestFlattenRecord_StructWithList(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.StructOf(
			arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	sb := bldr.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
	lb := sb.FieldBuilder(1).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.StringBuilder).Append("a")
	lb.ValueBuilder().(*array.StringBuilder).Append("b")

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.NumCols() != 2 {
		t.Fatalf("expected 2 cols, got %d", out.NumCols())
	}

	expectedNames := []string{"data.id", "data.tags"}
	for i, name := range expectedNames {
		if out.Schema().Field(i).Name != name {
			t.Errorf("field %d: expected %q, got %q", i, name, out.Schema().Field(i).Name)
		}
	}

	tagsCol := out.Column(1).(*array.String)
	var tags []string
	if err := json.Unmarshal([]byte(tagsCol.Value(0)), &tags); err != nil {
		t.Fatal(err)
	}
	if len(tags) != 2 || tags[0] != "a" {
		t.Errorf("expected [a b], got %v", tags)
	}
}

func TestFlattenRecord_NoMemoryLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	geomMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name"},
		[]string{"ogc.wkb"},
	)

	tests := []struct {
		name   string
		schema *arrow.Schema
		build  func(*array.RecordBuilder)
	}{
		{
			name: "no_complex_types",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			build: func(b *array.RecordBuilder) {
				b.Field(0).(*array.Int64Builder).Append(1)
				b.Field(1).(*array.StringBuilder).Append("a")
			},
		},
		{
			name: "struct_flat",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "s", Type: arrow.StructOf(
					arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "y", Type: arrow.BinaryTypes.String},
				)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				sb := b.Field(0).(*array.StructBuilder)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.Int64Builder).Append(42)
				sb.FieldBuilder(1).(*array.StringBuilder).Append("v")
			},
		},
		{
			name: "nested_struct",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.StructOf(
						arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Float64},
					)},
				)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				outer := b.Field(0).(*array.StructBuilder)
				outer.Append(true)
				inner := outer.FieldBuilder(0).(*array.StructBuilder)
				inner.Append(true)
				inner.FieldBuilder(0).(*array.Float64Builder).Append(3.14)
			},
		},
		{
			name: "struct_with_null",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "s", Type: arrow.StructOf(
					arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				sb := b.Field(0).(*array.StructBuilder)
				sb.AppendNull()
				sb.FieldBuilder(0).(*array.Int64Builder).Append(0)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
			},
		},
		{
			name: "list_to_json",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				lb := b.Field(0).(*array.ListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.StringBuilder).Append("a")
				lb.ValueBuilder().(*array.StringBuilder).Append("b")
			},
		},
		{
			name: "struct_with_geometry_metadata",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "place", Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
					arrow.Field{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMeta},
				)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				sb := b.Field(0).(*array.StructBuilder)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.StringBuilder).Append("park")
				sb.FieldBuilder(1).(*array.BinaryBuilder).Append([]byte{0x01, 0x02})
			},
		},
		{
			name: "struct_with_list_inside",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "data", Type: arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
				)},
			}, nil),
			build: func(b *array.RecordBuilder) {
				sb := b.Field(0).(*array.StructBuilder)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
				lb := sb.FieldBuilder(1).(*array.ListBuilder)
				lb.Append(true)
				lb.ValueBuilder().(*array.Int32Builder).Append(10)
				lb.ValueBuilder().(*array.Int32Builder).Append(20)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bldr := array.NewRecordBuilder(mem, tt.schema)
			tt.build(bldr)
			rec := bldr.NewRecord()
			bldr.Release()

			out := FlattenRecord(rec, mem)
			rec.Release()

			// Access data to ensure it's valid
			for i := range out.NumCols() {
				col := out.Column(int(i))
				for j := range col.Len() {
					_ = col.IsNull(j)
				}
			}

			out.Release()
		})
	}
}

func TestFlattenRecord_NestedGeometryMetadata(t *testing.T) {
	mem := memory.NewGoAllocator()

	geomMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"ogc.wkb", `{"srid":4326}`},
	)
	schemaMeta := arrow.NewMetadata(
		[]string{"X-Hugr-Geometry", "X-Hugr-Geometry-Fields"},
		[]string{"true", `{"location":{"format":"WKB","srid":4326}}`},
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "place", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "location", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMeta},
		)},
	}, &schemaMeta)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).Append(1)
	sb := bldr.Field(1).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("park")
	sb.FieldBuilder(1).(*array.BinaryBuilder).Append([]byte{0x01, 0x02, 0x03})

	rec := bldr.NewRecord()
	defer rec.Release()

	out := FlattenRecord(rec, mem)
	defer out.Release()

	if out.NumCols() != 3 {
		t.Fatalf("expected 3 cols, got %d", out.NumCols())
	}

	// Check field names
	expectedNames := []string{"id", "place.name", "place.location"}
	for i, name := range expectedNames {
		if out.Schema().Field(i).Name != name {
			t.Errorf("field %d: expected %q, got %q", i, name, out.Schema().Field(i).Name)
		}
	}

	// Check geometry field metadata preserved
	geomField := out.Schema().Field(2)
	if !geomField.HasMetadata() {
		t.Fatal("place.location field should have metadata")
	}
	idx := geomField.Metadata.FindKey("ARROW:extension:name")
	if idx < 0 {
		t.Fatal("place.location missing ARROW:extension:name metadata")
	}
	if got := geomField.Metadata.Values()[idx]; got != "ogc.wkb" {
		t.Errorf("expected extension name 'ogc.wkb', got %q", got)
	}
	idx = geomField.Metadata.FindKey("ARROW:extension:metadata")
	if idx < 0 {
		t.Fatal("place.location missing ARROW:extension:metadata")
	}
	if got := geomField.Metadata.Values()[idx]; got != `{"srid":4326}` {
		t.Errorf("expected extension metadata '{\"srid\":4326}', got %q", got)
	}

	// Check geometry field type preserved (Binary)
	if geomField.Type.ID() != arrow.BINARY {
		t.Errorf("expected BINARY type for geometry, got %s", geomField.Type)
	}

	// Check schema-level metadata preserved
	if !out.Schema().HasMetadata() {
		t.Fatal("schema should have metadata")
	}
	idx = out.Schema().Metadata().FindKey("X-Hugr-Geometry")
	if idx < 0 {
		t.Fatal("schema missing X-Hugr-Geometry metadata")
	}
	if got := out.Schema().Metadata().Values()[idx]; got != "true" {
		t.Errorf("expected X-Hugr-Geometry='true', got %q", got)
	}

	// Check data integrity
	binCol := out.Column(2).(*array.Binary)
	if got := binCol.Value(0); len(got) != 3 || got[0] != 0x01 {
		t.Errorf("unexpected binary data: %v", got)
	}
}

func TestFlattenSchema_NestedGeometryMetadata(t *testing.T) {
	geomMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"ogc.wkb", `{"srid":4326}`},
	)
	schemaMeta := arrow.NewMetadata(
		[]string{"X-Hugr-Geometry"},
		[]string{"true"},
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "geo", Type: arrow.StructOf(
			arrow.Field{Name: "point", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMeta},
		)},
	}, &schemaMeta)

	flat := FlattenSchema(schema)

	if len(flat.Fields()) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(flat.Fields()))
	}

	// Check flattened field name
	if flat.Field(1).Name != "geo.point" {
		t.Errorf("expected 'geo.point', got %q", flat.Field(1).Name)
	}

	// Check metadata preserved on flattened field
	f := flat.Field(1)
	if !f.HasMetadata() {
		t.Fatal("geo.point should have metadata")
	}
	idx := f.Metadata.FindKey("ARROW:extension:name")
	if idx < 0 {
		t.Fatal("geo.point missing ARROW:extension:name")
	}
	if got := f.Metadata.Values()[idx]; got != "ogc.wkb" {
		t.Errorf("expected 'ogc.wkb', got %q", got)
	}

	// Check schema metadata preserved
	if !flat.HasMetadata() {
		t.Fatal("flattened schema should have metadata")
	}
	idx = flat.Metadata().FindKey("X-Hugr-Geometry")
	if idx < 0 {
		t.Fatal("schema missing X-Hugr-Geometry")
	}
}

func TestFlattenSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.StructOf(
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64},
			)},
			arrow.Field{Name: "c", Type: arrow.BinaryTypes.String},
		)},
		{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
	}, nil)

	flat := FlattenSchema(schema)

	expected := []struct {
		name string
		dt   arrow.DataType
	}{
		{"id", arrow.PrimitiveTypes.Int64},
		{"s.a.b", arrow.PrimitiveTypes.Float64},
		{"s.c", arrow.BinaryTypes.String},
		{"tags", arrow.BinaryTypes.String},
	}

	if len(flat.Fields()) != len(expected) {
		t.Fatalf("expected %d fields, got %d", len(expected), len(flat.Fields()))
	}

	for i, e := range expected {
		f := flat.Field(i)
		if f.Name != e.name {
			t.Errorf("field %d name: expected %q, got %q", i, e.name, f.Name)
		}
		if f.Type.ID() != e.dt.ID() {
			t.Errorf("field %d type: expected %s, got %s", i, e.dt, f.Type)
		}
	}
}
