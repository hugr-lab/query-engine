package types

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
)

// TestScanParity_ObjectAndTable demonstrates the unified-scan promise:
// the same Go struct — with orb.Geometry and time.Time fields — scans
// identically whether the response leaf is an object (*JsonValue from
// the server scalar path or IPC object part) or a table (native
// ArrowTable from the server list path or IPC Arrow part).
//
// This is a Go-level parity proof; full embedded-engine-vs-IPC-client
// deep-equal verification runs via the E2E suite under integration-test/
// (209/0/0) where the same query is served to both consumers.
func TestScanParity_ObjectAndTable(t *testing.T) {
	type Row struct {
		ID    int32        `json:"id"`
		Name  string       `json:"name"`
		Geom  orb.Geometry `json:"geom"`
		TS    time.Time    `json:"ts"`
	}

	// Case 1: object path — *JsonValue leaf.
	jsonBody := `{"id":1,"name":"alice","geom":{"type":"Point","coordinates":[10,20]},"ts":"2024-03-15T12:00:00Z"}`
	jv := JsonValue(jsonBody)
	respObj := &Response{Data: map[string]any{"u": &jv}}

	obj, err := Scan[Row](respObj, "u")
	if err != nil {
		t.Fatalf("object scan: %v", err)
	}

	// Case 2: table path — native Arrow with equivalent data as a single row.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		// Geometry as native extension — decoded by scanner's registry.
		// For a fabricated test we use the same decoded orb.Geometry
		// via RegisterGeometryDecoder route; simplest is to encode as
		// WKB binary with geoarrow.wkb extension, but arrow-go needs
		// the extension type registered. Use plain utf8 with
		// GeoJSON text + GeometryInfo tagging — exercises the
		// GeoJSONString metadata path on the scanner.
		{Name: "geom", Type: arrow.BinaryTypes.String},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append("alice")
	b.Field(2).(*array.StringBuilder).Append(`{"type":"Point","coordinates":[10,20]}`)
	b.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC).UnixMicro()))
	rec := b.NewRecordBatch()
	defer rec.Release()

	tbl := NewArrowTable()
	defer tbl.Release()
	tbl.Append(rec)
	// Tag the nested utf8 geom column via GeometryInfo — same metadata
	// the planner attaches on list queries from the engine side and
	// the IPC client rehydrates from the X-Hugr-Geometry-Fields header.
	tbl.SetGeometryInfo(map[string]GeometryInfo{
		"geom": {Format: "GeoJSONString"},
	})
	respTbl := &Response{Data: map[string]any{"rows": tbl}}

	rows, err := Scan[[]Row](respTbl, "rows")
	if err != nil {
		t.Fatalf("table scan: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	tabRow := rows[0]

	// Parity: every field equal between object and table paths.
	if obj.ID != tabRow.ID {
		t.Errorf("ID mismatch: obj=%d tab=%d", obj.ID, tabRow.ID)
	}
	if obj.Name != tabRow.Name {
		t.Errorf("Name mismatch: obj=%q tab=%q", obj.Name, tabRow.Name)
	}
	if !reflect.DeepEqual(obj.Geom, tabRow.Geom) {
		t.Errorf("Geom mismatch: obj=%v (%T) tab=%v (%T)", obj.Geom, obj.Geom, tabRow.Geom, tabRow.Geom)
	}
	// Instant equality (timezone-aware) — the object path sees RFC3339,
	// the table path sees native Arrow timestamp with microsecond unit.
	if !obj.TS.Equal(tabRow.TS) {
		t.Errorf("TS mismatch: obj=%s tab=%s", obj.TS, tabRow.TS)
	}
}
