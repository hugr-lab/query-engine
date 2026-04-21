package types

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
)

// Parity check: ScanData should decode orb.Geometry / time.Time
// identically to ScanTable / ScanObject — server (engine.Query) and
// client (IPC) results produce byte-identical Response.Data shape so
// this covers both.

func TestScanData_Object_WithGeometry(t *testing.T) {
	jv := JsonValue(`{"id":1,"geom":{"type":"Point","coordinates":[10,20]}}`)
	r := &Response{Data: map[string]any{"obj": &jv}}

	var dest struct {
		ID   int          `json:"id"`
		Geom orb.Geometry `json:"geom"`
	}
	if err := r.ScanData("obj", &dest); err != nil {
		t.Fatalf("ScanData: %v", err)
	}
	if dest.ID != 1 {
		t.Errorf("ID=%d, want 1", dest.ID)
	}
	p, ok := dest.Geom.(orb.Point)
	if !ok || p[0] != 10 || p[1] != 20 {
		t.Errorf("Geom=%+v, want orb.Point{10,20}", dest.Geom)
	}
}

func TestScanData_Table_WithGeometry(t *testing.T) {
	// Build an ArrowTable with one row containing a geometry column
	// tagged as GeoJSONString via GeometryInfo — the path the planner
	// takes for list queries from PG.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "geom", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{
		`{"type":"Point","coordinates":[1,2]}`,
		`{"type":"Point","coordinates":[3,4]}`,
	}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	tbl := NewArrowTable()
	defer tbl.Release()
	tbl.Append(rec)
	tbl.SetGeometryInfo(map[string]GeometryInfo{
		"geom": {Format: "GeoJSONString"},
	})

	r := &Response{Data: map[string]any{"rows": tbl}}

	type row struct {
		ID   int32        `json:"id"`
		Geom orb.Geometry `json:"geom"`
	}
	var dest []row
	if err := r.ScanData("rows", &dest); err != nil {
		t.Fatalf("ScanData: %v", err)
	}
	if len(dest) != 2 {
		t.Fatalf("len=%d, want 2", len(dest))
	}
	p0, ok := dest[0].Geom.(orb.Point)
	if !ok || p0[0] != 1 || p0[1] != 2 {
		t.Errorf("rows[0].Geom=%+v", dest[0].Geom)
	}
	p1, ok := dest[1].Geom.(orb.Point)
	if !ok || p1[0] != 3 || p1[1] != 4 {
		t.Errorf("rows[1].Geom=%+v", dest[1].Geom)
	}
}

func TestScanData_Table_WithTimestamp(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).Append(1)
	wanted := time.Date(2024, 3, 15, 12, 30, 45, 123456000, time.UTC)
	b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(wanted.UnixMicro()))
	rec := b.NewRecordBatch()
	defer rec.Release()

	tbl := NewArrowTable()
	defer tbl.Release()
	tbl.Append(rec)

	r := &Response{Data: map[string]any{"rows": tbl}}

	type row struct {
		ID int32     `json:"id"`
		TS time.Time `json:"ts"`
	}
	var dest []row
	if err := r.ScanData("rows", &dest); err != nil {
		t.Fatalf("ScanData: %v", err)
	}
	if len(dest) != 1 {
		t.Fatalf("len=%d, want 1", len(dest))
	}
	if !dest[0].TS.Equal(wanted) {
		t.Errorf("TS=%s, want %s", dest[0].TS, wanted)
	}
}

func TestScanData_NestedNamespace(t *testing.T) {
	// ScanData navigates dotted paths through nested map namespaces;
	// verify the traversal still works after the refactor.
	jv := JsonValue(`{"version":"1.2.3"}`)
	r := &Response{Data: map[string]any{
		"core": map[string]any{
			"info": &jv,
		},
	}}
	var dest struct {
		Version string `json:"version"`
	}
	if err := r.ScanData("core.info", &dest); err != nil {
		t.Fatalf("ScanData: %v", err)
	}
	if dest.Version != "1.2.3" {
		t.Errorf("Version=%q", dest.Version)
	}
}

func TestScanData_MissingPath_ReturnsErrWrongDataPath(t *testing.T) {
	r := &Response{Data: map[string]any{"a": map[string]any{"b": "value"}}}
	var dest any
	err := r.ScanData("a.c", &dest)
	if !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("err=%v, want ErrWrongDataPath", err)
	}
}

func TestScanData_NilLeaf_ReturnsErrNoData(t *testing.T) {
	// Legacy semantics: explicit-null leaves return ErrNoData (vs
	// ErrWrongDataPath for missing-in-map).
	r := &Response{Data: map[string]any{"x": nil}}
	var dest any
	err := r.ScanData("x", &dest)
	if !errors.Is(err, ErrNoData) {
		t.Fatalf("err=%v, want ErrNoData", err)
	}
}

func TestScanData_NilResponse_ReturnsErrNoData(t *testing.T) {
	r := &Response{}
	var dest any
	err := r.ScanData("anything", &dest)
	if !errors.Is(err, ErrNoData) {
		t.Fatalf("err=%v, want ErrNoData", err)
	}
}

func TestScanData_PlainStruct_FastPath(t *testing.T) {
	// Plain structs (no geometry) take the scanObject fast path ≡ stdlib
	// json.Unmarshal; behaviour must be identical to pre-refactor.
	jv := JsonValue(`{"name":"alice","age":30}`)
	r := &Response{Data: map[string]any{"u": &jv}}

	var dest struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := r.ScanData("u", &dest); err != nil {
		t.Fatalf("ScanData: %v", err)
	}
	if dest.Name != "alice" || dest.Age != 30 {
		t.Errorf("dest=%+v", dest)
	}
}
