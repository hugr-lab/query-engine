package types

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// Helpers ----------------------------------------------------------------

// responseWithObject wraps a *JsonValue leaf in a Response tree so
// ScanObject can navigate to it at path "x".
func responseWithObject(raw string) *Response {
	v := JsonValue(raw)
	return &Response{Data: map[string]any{"x": &v}}
}

// Cases ------------------------------------------------------------------

func TestScanObject_PlainStruct_FastPath(t *testing.T) {
	// Plain structs without geometry → fast path (json.Unmarshal). This
	// test exists to guard against accidental slow-path regression for
	// the most common case.
	r := responseWithObject(`{"id":42,"name":"ok"}`)
	var dest struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.ID != 42 || dest.Name != "ok" {
		t.Fatalf("dest=%+v", dest)
	}
	// Verify the plan says "no custom decode needed".
	plan := planFor(reflect.TypeOf(dest))
	if plan.needsCustomDecode {
		t.Fatalf("expected fast path for plain struct, got custom-decode plan")
	}
}

func TestScanObject_OrbGeometry_Interface(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"Point","coordinates":[1,2]}}`)
	var dest struct {
		Geom orb.Geometry `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	p, ok := dest.Geom.(orb.Point)
	if !ok {
		t.Fatalf("got %T, want orb.Point", dest.Geom)
	}
	if p[0] != 1 || p[1] != 2 {
		t.Fatalf("coords=%v", p)
	}
}

func TestScanObject_OrbPoint_Concrete(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"Point","coordinates":[10,20]}}`)
	var dest struct {
		Geom orb.Point `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.Geom[0] != 10 || dest.Geom[1] != 20 {
		t.Fatalf("coords=%v", dest.Geom)
	}
}

func TestScanObject_OrbLineString(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"LineString","coordinates":[[1,2],[3,4]]}}`)
	var dest struct {
		Geom orb.LineString `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if len(dest.Geom) != 2 {
		t.Fatalf("len=%d", len(dest.Geom))
	}
}

func TestScanObject_OrbPolygon(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}}`)
	var dest struct {
		Geom orb.Polygon `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if len(dest.Geom) != 1 || len(dest.Geom[0]) != 5 {
		t.Fatalf("polygon=%v", dest.Geom)
	}
}

func TestScanObject_GeoJSONGeometry(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"Point","coordinates":[5,6]}}`)
	var dest struct {
		Geom geojson.Geometry `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	p, ok := dest.Geom.Coordinates.(orb.Point)
	if !ok {
		t.Fatalf("got %T, want orb.Point", dest.Geom.Coordinates)
	}
	if p[0] != 5 || p[1] != 6 {
		t.Fatalf("coords=%v", p)
	}
}

func TestScanObject_GeometryFromQuotedString(t *testing.T) {
	// Engine wraps nested struct fields as JSON strings containing GeoJSON
	// text; DuckDB's STRUCT→JSON cast re-parses them, but if a caller
	// reaches into a raw JsonValue manually they might still see the
	// quoted form. The decoder accepts both.
	r := responseWithObject(`{"geom":"{\"type\":\"Point\",\"coordinates\":[7,8]}"}`)
	var dest struct {
		Geom orb.Geometry `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	p, ok := dest.Geom.(orb.Point)
	if !ok || p[0] != 7 || p[1] != 8 {
		t.Fatalf("dest=%+v", dest)
	}
}

func TestScanObject_NullGeometry(t *testing.T) {
	r := responseWithObject(`{"geom":null}`)
	var dest struct {
		Geom orb.Geometry `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.Geom != nil {
		t.Fatalf("want nil, got %v", dest.Geom)
	}
}

func TestScanObject_NestedStructGeometry(t *testing.T) {
	r := responseWithObject(`{
		"id": 1,
		"inner": { "geom": { "type":"Point","coordinates":[3,4] }, "label":"x" }
	}`)
	type inner struct {
		Geom  orb.Geometry `json:"geom"`
		Label string       `json:"label"`
	}
	var dest struct {
		ID    int   `json:"id"`
		Inner inner `json:"inner"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.ID != 1 || dest.Inner.Label != "x" {
		t.Fatalf("dest=%+v", dest)
	}
	p, ok := dest.Inner.Geom.(orb.Point)
	if !ok || p[0] != 3 || p[1] != 4 {
		t.Fatalf("inner.geom=%+v", dest.Inner.Geom)
	}
}

func TestScanObject_SliceOfStructWithGeometry(t *testing.T) {
	r := responseWithObject(`[
		{"id":1,"geom":{"type":"Point","coordinates":[1,2]}},
		{"id":2,"geom":null}
	]`)
	type row struct {
		ID   int          `json:"id"`
		Geom orb.Geometry `json:"geom"`
	}
	var dest []row
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if len(dest) != 2 {
		t.Fatalf("len=%d", len(dest))
	}
	if _, ok := dest[0].Geom.(orb.Point); !ok {
		t.Fatalf("row0.geom=%T", dest[0].Geom)
	}
	if dest[1].Geom != nil {
		t.Fatalf("row1.geom=%v, want nil", dest[1].Geom)
	}
}

func TestScanObject_MapOfStructWithGeometry(t *testing.T) {
	r := responseWithObject(`{
		"a":{"geom":{"type":"Point","coordinates":[1,1]}},
		"b":{"geom":{"type":"Point","coordinates":[2,2]}}
	}`)
	type row struct {
		Geom orb.Geometry `json:"geom"`
	}
	var dest map[string]row
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if len(dest) != 2 {
		t.Fatalf("len=%d", len(dest))
	}
	pa, ok := dest["a"].Geom.(orb.Point)
	if !ok || pa[0] != 1 {
		t.Fatalf("a.geom=%+v", dest["a"].Geom)
	}
}

func TestScanObject_PointerToGeometry(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"Point","coordinates":[9,9]}}`)
	var dest struct {
		Geom *orb.Point `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.Geom == nil {
		t.Fatalf("want non-nil")
	}
	if dest.Geom[0] != 9 || dest.Geom[1] != 9 {
		t.Fatalf("geom=%v", *dest.Geom)
	}
}

func TestScanObject_PointerNull(t *testing.T) {
	r := responseWithObject(`{"geom":null}`)
	var dest struct {
		Geom *orb.Point `json:"geom"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.Geom != nil {
		t.Fatalf("want nil")
	}
}

func TestScanObject_TimeTime(t *testing.T) {
	// time.Time doesn't need the slow path — stdlib handles RFC3339.
	// But it must survive a plan that contains geometry elsewhere.
	r := responseWithObject(`{"geom":{"type":"Point","coordinates":[1,2]},"ts":"2024-03-15T12:00:00Z"}`)
	var dest struct {
		Geom orb.Geometry `json:"geom"`
		TS   time.Time    `json:"ts"`
	}
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.TS.IsZero() {
		t.Fatalf("ts not populated")
	}
	if _, ok := dest.Geom.(orb.Point); !ok {
		t.Fatalf("geom=%T", dest.Geom)
	}
}

func TestScanObject_EmbeddedStruct(t *testing.T) {
	type base struct {
		ID int `json:"id"`
	}
	type row struct {
		base
		Geom orb.Geometry `json:"geom"`
	}
	r := responseWithObject(`{"id":7,"geom":{"type":"Point","coordinates":[1,1]}}`)
	var dest row
	if err := r.ScanObject("x", &dest); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if dest.ID != 7 {
		t.Fatalf("id=%d", dest.ID)
	}
	if _, ok := dest.Geom.(orb.Point); !ok {
		t.Fatalf("geom=%T", dest.Geom)
	}
}

func TestScanObject_TablePathRejected(t *testing.T) {
	r := &Response{Data: map[string]any{"t": NewArrowTable()}}
	var dest struct{}
	err := r.ScanObject("t", &dest)
	if !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("want ErrWrongDataPath, got %v", err)
	}
}

func TestScanObject_MissingPath(t *testing.T) {
	r := &Response{Data: map[string]any{}}
	var dest struct{}
	err := r.ScanObject("missing", &dest)
	if !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("want ErrWrongDataPath, got %v", err)
	}
}

func TestScanObject_InvalidGeometryBytes(t *testing.T) {
	r := responseWithObject(`{"geom":{"type":"NotAType","coordinates":[]}}`)
	var dest struct {
		Geom orb.Geometry `json:"geom"`
	}
	err := r.ScanObject("x", &dest)
	if !errors.Is(err, ErrGeometryDecode) {
		t.Fatalf("want ErrGeometryDecode wrap, got %v", err)
	}
}
