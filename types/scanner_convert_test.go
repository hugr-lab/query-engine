package types

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestConvert_Primitives covers the conversion matrix rows for primitives,
// strings, binary, and null handling.
func TestConvert_Primitives(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
		b.Field(1).(*array.Int8Builder).AppendValues([]int8{1, 0}, []bool{true, false})
		b.Field(2).(*array.Int32Builder).AppendValues([]int32{42, 0}, nil)
		b.Field(3).(*array.Int64Builder).AppendValues([]int64{9_000_000_000, 0}, nil)
		b.Field(4).(*array.Uint64Builder).AppendValues([]uint64{18_000_000_000_000_000_000, 0}, nil)
		b.Field(5).(*array.Float32Builder).AppendValues([]float32{1.5, 0}, nil)
		b.Field(6).(*array.Float64Builder).AppendValues([]float64{2.5, 0}, nil)
		b.Field(7).(*array.StringBuilder).AppendValues([]string{"hello", ""}, nil)
		b.Field(8).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bytes"), nil}, []bool{true, false})
	})
	defer tbl.Release()

	type row struct {
		B    bool    `json:"b"`
		I8   int8    `json:"i8"`
		I32  int32   `json:"i32"`
		I64  int64   `json:"i64"`
		U64  uint64  `json:"u64"`
		F32  float32 `json:"f32"`
		F64  float64 `json:"f64"`
		S    string  `json:"s"`
		Bin  []byte  `json:"bin"`
	}

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %d", len(got))
	}

	r0 := got[0]
	if !r0.B || r0.I8 != 1 || r0.I32 != 42 || r0.I64 != 9_000_000_000 ||
		r0.U64 != 18_000_000_000_000_000_000 || r0.F32 != 1.5 || r0.F64 != 2.5 ||
		r0.S != "hello" || string(r0.Bin) != "bytes" {
		t.Fatalf("row 0 mismatch: %+v", r0)
	}

	r1 := got[1]
	// Null int8 → zero value (non-pointer field).
	if r1.I8 != 0 || r1.S != "" || r1.Bin != nil {
		t.Fatalf("row 1 (nulls) mismatch: %+v (bin=%v)", r1, r1.Bin)
	}
}

// TestConvert_NullsIntoPointers checks pointer fields become nil on null.
func TestConvert_NullsIntoPointers(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 0}, []bool{true, false})
	})
	defer tbl.Release()

	type row struct {
		N *int32 `json:"n"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if got[0].N == nil || *got[0].N != 7 {
		t.Fatalf("row 0 pointer: want *7, got %v", got[0].N)
	}
	if got[1].N != nil {
		t.Fatalf("row 1 pointer: want nil, got %v", *got[1].N)
	}
}

// TestConvert_MapDest verifies map[string]any destination.
func TestConvert_MapDest(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.BinaryTypes.String},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(11)
		b.Field(1).(*array.StringBuilder).Append("xx")
	})
	defer tbl.Release()

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	m := map[string]any{}
	if err := rows.Scan(&m); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if m["a"].(int32) != 11 || m["b"].(string) != "xx" {
		t.Fatalf("map mismatch: %+v", m)
	}
}

// TestConvert_SliceDest verifies []any destination in column order.
func TestConvert_SliceDest(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.BinaryTypes.String},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(11)
		b.Field(1).(*array.StringBuilder).Append("xx")
	})
	defer tbl.Release()

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var s []any
	if err := rows.Scan(&s); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(s) != 2 || s[0].(int32) != 11 || s[1].(string) != "xx" {
		t.Fatalf("slice mismatch: %+v", s)
	}
}

// TestConvert_AnyDest verifies *any fills map[string]any.
func TestConvert_AnyDest(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Float64Builder).Append(3.14)
	})
	defer tbl.Release()

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var out any
	if err := rows.Scan(&out); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("want map[string]any, got %T", out)
	}
	if m["x"].(float64) != 3.14 {
		t.Fatalf("value: %v", m["x"])
	}
}

// TestTimestampAware covers Arrow Timestamp(Unit, TimeZone) → time.Time
// with the column timezone preserved (FR-005, SC-002).
func TestTimestampAware(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		t.Skipf("timezone data unavailable: %v", err)
	}
	want := time.Date(2026, 4, 1, 10, 0, 0, 0, loc)
	// Arrow Timestamp is stored as int64 since epoch in the column's unit.
	ms := want.UnixMilli()

	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Europe/Moscow"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: tsType},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ms))
	})
	defer tbl.Release()

	type row struct {
		Ts time.Time `json:"ts"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !r.Ts.Equal(want) {
		t.Fatalf("moment mismatch: want %v, got %v", want, r.Ts)
	}
	if r.Ts.Location().String() != "Europe/Moscow" {
		t.Fatalf("location: want Europe/Moscow, got %s", r.Ts.Location())
	}
}

// TestTimestampNaive covers naive Timestamp → time.Time (defaults to UTC).
func TestTimestampNaive(t *testing.T) {
	want := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	us := want.UnixMicro()
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: tsType}}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(us))
	})
	defer tbl.Release()

	type row struct {
		Ts time.Time `json:"ts"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !r.Ts.Equal(want) {
		t.Fatalf("want %v, got %v", want, r.Ts)
	}
	if r.Ts.Location() != time.UTC {
		t.Fatalf("naive default location: want UTC, got %s", r.Ts.Location())
	}
}

// TestDateTimeDest covers naive Timestamp → types.DateTime.
func TestDateTimeDest(t *testing.T) {
	want := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	ms := want.UnixMilli()
	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: ""}
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: tsType}}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ms))
	})
	defer tbl.Release()

	type row struct {
		Ts DateTime `json:"ts"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	got := time.Time(r.Ts)
	if !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestDateAndTime covers Date32, Date64, Time32/64, Duration conversions.
func TestDateAndTime(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "d32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "d64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "t32", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "t64", Type: arrow.FixedWidthTypes.Time64us},
	}, nil)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	day := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		// Date32: days since epoch.
		days := arrow.Date32(day.Sub(epoch).Hours() / 24)
		b.Field(0).(*array.Date32Builder).Append(days)
		// Date64: ms since epoch (midnight).
		b.Field(1).(*array.Date64Builder).Append(arrow.Date64(day.UnixMilli()))
		// Time32(ms): 3600_000 ms = 1h.
		b.Field(2).(*array.Time32Builder).Append(arrow.Time32(3_600_000))
		// Time64(us): 3600_000_000 us = 1h.
		b.Field(3).(*array.Time64Builder).Append(arrow.Time64(3_600_000_000))
	})
	defer tbl.Release()

	type row struct {
		D32 time.Time     `json:"d32"`
		D64 time.Time     `json:"d64"`
		T32 time.Duration `json:"t32"`
		T64 time.Duration `json:"t64"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !r.D32.Equal(day) {
		t.Fatalf("Date32: want %v, got %v", day, r.D32)
	}
	if !r.D64.Equal(day) {
		t.Fatalf("Date64: want %v, got %v", day, r.D64)
	}
	if r.T32 != time.Hour {
		t.Fatalf("Time32: want 1h, got %v", r.T32)
	}
	if r.T64 != time.Hour {
		t.Fatalf("Time64: want 1h, got %v", r.T64)
	}
}

// TestTimestampUnknownTZ: bogus timezone name falls back to UTC without error.
func TestTimestampUnknownTZ(t *testing.T) {
	want := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	ms := want.UnixMilli()
	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Invalid/NotAZone"}
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: tsType}}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ms))
	})
	defer tbl.Release()

	type row struct {
		Ts time.Time `json:"ts"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan (unknown TZ should fall back to UTC): %v", err)
	}
	if !r.Ts.Equal(want) {
		t.Fatalf("moment mismatch: want %v, got %v", want, r.Ts)
	}
}

// TestConvert_EmbeddedStruct verifies Go anonymous embedding (flatten) works
// both at the top-level destination AND inside a nested Arrow struct field
// (the list-element / reference case that powers real nested GraphQL shapes).
func TestConvert_EmbeddedStruct(t *testing.T) {
	// Arrow schema:
	//   id     int64
	//   name   string
	//   nested struct<id:int64, label:string>
	//   xs     list<struct<id:int64, value:int32>>
	nestedType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
	)
	listElemType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32},
	)
	listType := arrow.ListOf(listElemType)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "nested", Type: nestedType},
		{Name: "xs", Type: listType},
	}, nil)

	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int64Builder).Append(42)
		b.Field(1).(*array.StringBuilder).Append("alpha")
		nb := b.Field(2).(*array.StructBuilder)
		nb.Append(true)
		nb.FieldBuilder(0).(*array.Int64Builder).Append(99)
		nb.FieldBuilder(1).(*array.StringBuilder).Append("nested-label")
		lb := b.Field(3).(*array.ListBuilder)
		lb.Append(true)
		esb := lb.ValueBuilder().(*array.StructBuilder)
		for i, v := range []int32{10, 20} {
			esb.Append(true)
			esb.FieldBuilder(0).(*array.Int64Builder).Append(int64(i + 1))
			esb.FieldBuilder(1).(*array.Int32Builder).Append(v)
		}
	})
	defer tbl.Release()

	// Base struct embedded both at top level and inside the list element.
	type Base struct {
		ID int64 `json:"id"`
	}
	type Nested struct {
		Base         // embedded — inherit "id" from here
		Label string `json:"label"`
	}
	type ListItem struct {
		Base          // embedded inside a list-element struct
		Value int32  `json:"value"`
	}
	type Row struct {
		Base                    // top-level embedded
		Name   string    `json:"name"`
		Nested Nested    `json:"nested"`
		Xs     []ListItem `json:"xs"`
	}

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r Row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if r.ID != 42 {
		t.Fatalf("top-level embedded ID: want 42, got %d", r.ID)
	}
	if r.Name != "alpha" {
		t.Fatalf("Name: %q", r.Name)
	}
	if r.Nested.ID != 99 || r.Nested.Label != "nested-label" {
		t.Fatalf("nested embedded: %+v", r.Nested)
	}
	if len(r.Xs) != 2 {
		t.Fatalf("xs len: %d", len(r.Xs))
	}
	if r.Xs[0].ID != 1 || r.Xs[0].Value != 10 {
		t.Fatalf("xs[0] embedded: %+v", r.Xs[0])
	}
	if r.Xs[1].ID != 2 || r.Xs[1].Value != 20 {
		t.Fatalf("xs[1] embedded: %+v", r.Xs[1])
	}
}

// TestConvert_ListAndStruct covers nested list and struct scan.
func TestConvert_ListAndStruct(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	structType := arrow.StructOf(
		arrow.Field{Name: "k", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int32},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "xs", Type: listType},
		{Name: "obj", Type: structType},
	}, nil)

	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		lb := b.Field(0).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)

		sb := b.Field(1).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.StringBuilder).Append("key")
		sb.FieldBuilder(1).(*array.Int32Builder).Append(99)
	})
	defer tbl.Release()

	type inner struct {
		K string `json:"k"`
		V int32  `json:"v"`
	}
	type row struct {
		Xs  []int32 `json:"xs"`
		Obj inner   `json:"obj"`
	}

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(r.Xs) != 3 || r.Xs[0] != 1 || r.Xs[2] != 3 {
		t.Fatalf("list: %+v", r.Xs)
	}
	if r.Obj.K != "key" || r.Obj.V != 99 {
		t.Fatalf("struct: %+v", r.Obj)
	}
}
