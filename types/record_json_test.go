package types

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// recordFromFields builds a one-row RecordBatch from the supplied fields +
// value appenders, suitable for direct RecordToJSON exercising.
func recordFromFields(t *testing.T, fields []arrow.Field, appendVals func(b *array.RecordBuilder)) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(func() { b.Release() })
	appendVals(b)
	return b.NewRecordBatch()
}

// recordToJSONString is a convenience wrapper returning the stringified
// output of RecordToJSON on a single record batch.
func recordToJSONString(t *testing.T, rec arrow.RecordBatch, asArray bool, geomInfo map[string]GeometryInfo) string {
	t.Helper()
	var buf bytes.Buffer
	if err := RecordToJSON(rec, asArray, &buf, geomInfo); err != nil {
		t.Fatalf("RecordToJSON: %v", err)
	}
	return buf.String()
}

func TestRecordToJSON_Primitives(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(42)
		b.Field(1).(*array.Float64Builder).Append(3.14)
		b.Field(2).(*array.StringBuilder).Append("hi")
		b.Field(3).(*array.BooleanBuilder).Append(true)
	})
	defer rec.Release()

	got := recordToJSONString(t, rec, false, nil)
	want := `{"i":42,"f":3.14,"s":"hi","b":true}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRecordToJSON_TimestampRFC3339Nano(t *testing.T) {
	// Format = Go's time.RFC3339Nano: trailing zeros trimmed, UTC → "Z".
	// DuckDB side emits the same shape via rtrim + CASE in
	// scalar_timestamp.go:timestampTZSQL.
	ts := time.Date(2024, 3, 15, 12, 30, 45, 123456789, time.UTC)
	cases := []struct {
		name     string
		unit     arrow.TimeUnit
		tz       string
		val      int64
		expected string
	}{
		{"second-utc", arrow.Second, "UTC", ts.Unix(), `"2024-03-15T12:30:45Z"`},
		{"milli-utc", arrow.Millisecond, "UTC", ts.UnixMilli(), `"2024-03-15T12:30:45.123Z"`},
		{"micro-utc", arrow.Microsecond, "UTC", ts.UnixMicro(), `"2024-03-15T12:30:45.123456Z"`},
		{"nano-utc", arrow.Nanosecond, "UTC", ts.UnixNano(), `"2024-03-15T12:30:45.123456789Z"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := recordFromFields(t, []arrow.Field{
				{Name: "t", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: tc.tz}},
			}, func(b *array.RecordBuilder) {
				b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(tc.val))
			})
			defer rec.Release()
			got := recordToJSONString(t, rec, false, nil)
			want := `{"t":` + tc.expected + `}`
			if got != want {
				t.Fatalf("got %s want %s", got, want)
			}
		})
	}
}

func TestRecordToJSON_TimestampNaive(t *testing.T) {
	// TimestampType with empty TimeZone = naive (DateTime in the GraphQL
	// schema). Rendered as-if-UTC so the output is strict RFC 3339 Nano
	// and parseable by Go's time.Time.UnmarshalJSON.
	rec := recordFromFields(t, []arrow.Field{
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Date(2024, 3, 15, 12, 30, 45, 123456000, time.UTC).UnixMicro()))
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	want := `{"t":"2024-03-15T12:30:45.123456Z"}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRecordToJSON_TimestampWithTimezone(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/New_York"}},
	}, func(b *array.RecordBuilder) {
		// 2024-03-15T12:00:00 UTC = 2024-03-15T08:00:00-04:00 (DST).
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC).Unix()))
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	if !strings.Contains(got, `2024-03-15T08:00:00`) {
		t.Fatalf("expected TZ-adjusted timestamp, got %s", got)
	}
	if !strings.Contains(got, `-04:00`) && !strings.Contains(got, `-05:00`) {
		t.Fatalf("expected offset with colon, got %s", got)
	}
}

func TestRecordToJSON_Date(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
	}, func(b *array.RecordBuilder) {
		// 2024-03-15 = epoch day 19797.
		b.Field(0).(*array.Date32Builder).Append(arrow.Date32(19797))
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	if got != `{"d":"2024-03-15"}` {
		t.Fatalf("got %s", got)
	}
}

func TestRecordToJSON_Decimal128(t *testing.T) {
	dt := &arrow.Decimal128Type{Precision: 12, Scale: 3}
	rec := recordFromFields(t, []arrow.Field{
		{Name: "d", Type: dt},
	}, func(b *array.RecordBuilder) {
		// 12345 / 10^3 = 12.345
		b.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(12345))
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	if got != `{"d":12.345}` {
		t.Fatalf("got %s", got)
	}
	// Negative.
	rec2 := recordFromFields(t, []arrow.Field{
		{Name: "d", Type: dt},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(-5))
	})
	defer rec2.Release()
	got = recordToJSONString(t, rec2, false, nil)
	if got != `{"d":-0.005}` {
		t.Fatalf("neg: got %s", got)
	}
}

func TestRecordToJSON_GeoJSONStringViaMetadata(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "geom", Type: arrow.BinaryTypes.String},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(1)
		b.Field(1).(*array.StringBuilder).Append(`{"type":"Point","coordinates":[1,2]}`)
	})
	defer rec.Release()

	geomInfo := map[string]GeometryInfo{
		"geom": {Format: "GeoJSONString"},
	}
	got := recordToJSONString(t, rec, false, geomInfo)
	// geom must be embedded as an OBJECT, not a quoted string.
	want := `{"id":1,"geom":{"type":"Point","coordinates":[1,2]}}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRecordToJSON_GeoJSONStringInsideStruct(t *testing.T) {
	// Struct column with a nested geom field tagged as GeoJSONString.
	inner := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "geom", Type: arrow.BinaryTypes.String},
	)
	rec := recordFromFields(t, []arrow.Field{
		{Name: "top", Type: inner},
	}, func(b *array.RecordBuilder) {
		sb := b.Field(0).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(5)
		sb.FieldBuilder(1).(*array.StringBuilder).Append(`{"type":"Point","coordinates":[3,4]}`)
	})
	defer rec.Release()

	geomInfo := map[string]GeometryInfo{"top.geom": {Format: "GeoJSONString"}}
	got := recordToJSONString(t, rec, false, geomInfo)
	want := `{"top":{"id":5,"geom":{"type":"Point","coordinates":[3,4]}}}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRecordToJSON_NullValues(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).AppendNull()
		b.Field(1).(*array.StringBuilder).AppendNull()
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	if got != `{"i":null,"s":null}` {
		t.Fatalf("got %s", got)
	}
}

func TestRecordToJSON_AsArraySingleColumn(t *testing.T) {
	rec := recordFromFields(t, []arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int32},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, true, nil)
	// Single-column asArray flattens to bare values, comma-separated (no
	// outer brackets — MarshalJSON owns those).
	if got != `1,2,3` {
		t.Fatalf("got %s", got)
	}
}

func TestRecordToJSON_NestedList(t *testing.T) {
	// list<int32> — each row emits a JSON array.
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	rec := recordFromFields(t, []arrow.Field{
		{Name: "nums", Type: listType},
	}, func(b *array.RecordBuilder) {
		lb := b.Field(0).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	if got != `{"nums":[10,20,30]}` {
		t.Fatalf("got %s", got)
	}
}

func TestRecordToJSON_ObjectRowRoundTripsAsValidJSON(t *testing.T) {
	// The output must parse as valid JSON by stdlib — not just look right.
	rec := recordFromFields(t, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(7)
		b.Field(1).(*array.StringBuilder).Append("row")
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()))
	})
	defer rec.Release()
	got := recordToJSONString(t, rec, false, nil)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("output not valid JSON: %v\nraw: %s", err, got)
	}
}
