package types

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
)

// tableWithRow builds a one-row ArrowTable for scan tests.
func tableWithRow(t *testing.T) *ArrowTableChunked {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(func() { b.Release() })
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20}, nil)
	rec := b.NewRecordBatch()
	tbl := NewArrowTable()
	t.Cleanup(func() { tbl.Release() })
	tbl.Append(rec)
	rec.Release()
	return tbl
}

func TestScan_NilResponse(t *testing.T) {
	if _, err := Scan[int](nil, "x"); !errors.Is(err, ErrNoData) {
		t.Fatalf("want ErrNoData, got %v", err)
	}
}

func TestScan_SliceDispatchesToScanTable(t *testing.T) {
	r := &Response{Data: map[string]any{"rows": tableWithRow(t)}}
	type row struct {
		N int32 `json:"n"`
	}
	got, err := Scan[[]row](r, "rows")
	if err != nil {
		t.Fatalf("Scan[[]row]: %v", err)
	}
	if len(got) != 2 || got[0].N != 10 || got[1].N != 20 {
		t.Fatalf("got %+v", got)
	}
}

func TestScan_StructDispatchesToScanObject(t *testing.T) {
	v := JsonValue(`{"name":"alice","age":30}`)
	r := &Response{Data: map[string]any{"u": &v}}
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	got, err := Scan[user](r, "u")
	if err != nil {
		t.Fatalf("Scan[user]: %v", err)
	}
	if got.Name != "alice" || got.Age != 30 {
		t.Fatalf("got %+v", got)
	}
}

func TestScan_StructWithGeometry(t *testing.T) {
	v := JsonValue(`{"id":1,"geom":{"type":"Point","coordinates":[3,4]}}`)
	r := &Response{Data: map[string]any{"p": &v}}
	type place struct {
		ID   int          `json:"id"`
		Geom orb.Geometry `json:"geom"`
	}
	got, err := Scan[place](r, "p")
	if err != nil {
		t.Fatalf("Scan[place]: %v", err)
	}
	p, ok := got.Geom.(orb.Point)
	if !ok || p[0] != 3 || p[1] != 4 {
		t.Fatalf("got %+v", got)
	}
}

func TestScan_SliceOnObjectPath_ReturnsError(t *testing.T) {
	v := JsonValue(`{"name":"alice"}`)
	r := &Response{Data: map[string]any{"u": &v}}
	// The caller expects a list but the path holds an object —
	// ScanTable must reject this with ErrWrongDataPath.
	_, err := Scan[[]any](r, "u")
	if !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("want ErrWrongDataPath, got %v", err)
	}
}

func TestScan_ObjectOnTablePath_ReturnsError(t *testing.T) {
	r := &Response{Data: map[string]any{"rows": tableWithRow(t)}}
	// The caller expects an object but the path holds a table —
	// ScanObject must reject with ErrWrongDataPath.
	_, err := Scan[struct{}](r, "rows")
	if !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("want ErrWrongDataPath, got %v", err)
	}
}
