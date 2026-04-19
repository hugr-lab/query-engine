package types

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/geojson"
)

// wkbFieldMeta returns Arrow field metadata tagged with the given extension name.
func extNameMeta(name string) arrow.Metadata {
	return arrow.MetadataFrom(map[string]string{arrowExtNameKey: name})
}

// newGeomWKBTable builds a table with one Binary column carrying WKB of the
// given geometries. The field is tagged via metadata as geoarrow.wkb — we
// don't need to register a full extension type in the Arrow registry because
// our detection layer falls back to field metadata.
func newGeomWKBTable(t *testing.T, geoms []orb.Geometry) *ArrowTableChunked {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta("geoarrow.wkb")},
	}, nil)

	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	bb := b.Field(0).(*array.BinaryBuilder)
	for _, g := range geoms {
		if g == nil {
			bb.AppendNull()
			continue
		}
		bytes, err := wkb.Marshal(g)
		if err != nil {
			t.Fatalf("wkb.Marshal: %v", err)
		}
		bb.Append(bytes)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	tbl := NewArrowTable()
	tbl.Append(rec)
	return tbl
}

func TestGeometry_WKB_Scan(t *testing.T) {
	pt := orb.Point{10, 20}
	ls := orb.LineString{{1, 2}, {3, 4}, {5, 6}}
	tbl := newGeomWKBTable(t, []orb.Geometry{pt, ls, nil})
	defer tbl.Release()

	type row struct {
		Geom orb.Geometry `json:"geom"`
	}
	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()

	var got []orb.Geometry
	for rows.Next() {
		var r row
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r.Geom)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	if !orb.Equal(got[0], pt) {
		t.Fatalf("row 0: want %v, got %v", pt, got[0])
	}
	if !orb.Equal(got[1], ls) {
		t.Fatalf("row 1: want %v, got %v", ls, got[1])
	}
	if got[2] != nil {
		t.Fatalf("row 2 (null): want nil, got %v", got[2])
	}
}

func TestGeometry_WKB_ConcreteSubtype(t *testing.T) {
	pt := orb.Point{1.1, 2.2}
	tbl := newGeomWKBTable(t, []orb.Geometry{pt})
	defer tbl.Release()

	type row struct {
		Geom orb.Point `json:"geom"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan into orb.Point: %v", err)
	}
	if r.Geom != pt {
		t.Fatalf("want %v, got %v", pt, r.Geom)
	}
}

func TestGeometry_WKT_Scan(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "g", Type: arrow.BinaryTypes.String, Metadata: extNameMeta("geoarrow.wkt")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append("POINT (5 6)")
	})
	defer tbl.Release()

	type row struct {
		G orb.Geometry `json:"g"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := orb.Point{5, 6}
	if !orb.Equal(r.G, want) {
		t.Fatalf("want %v, got %v", want, r.G)
	}
}

func TestGeometry_GeoJSON_Binary(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "g", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta("hugr.geojson")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.BinaryBuilder).Append([]byte(`{"type":"Point","coordinates":[1,2]}`))
	})
	defer tbl.Release()

	type row struct {
		G orb.Geometry `json:"g"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !orb.Equal(r.G, orb.Point{1, 2}) {
		t.Fatalf("got %v", r.G)
	}
}

func TestGeometry_GeoJSON_String(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "g", Type: arrow.BinaryTypes.String, Metadata: extNameMeta("hugr.geojson")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append(`{"type":"LineString","coordinates":[[0,0],[1,1]]}`)
	})
	defer tbl.Release()

	type row struct {
		G orb.Geometry `json:"g"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := orb.LineString{{0, 0}, {1, 1}}
	if !orb.Equal(r.G, want) {
		t.Fatalf("want %v, got %v", want, r.G)
	}
}

func TestGeometry_Native_Point(t *testing.T) {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "p", Type: pointType, Metadata: extNameMeta("geoarrow.point")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		sb := b.Field(0).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Float64Builder).Append(7)
		sb.FieldBuilder(1).(*array.Float64Builder).Append(8)
	})
	defer tbl.Release()

	type row struct {
		P orb.Geometry `json:"p"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !orb.Equal(r.P, orb.Point{7, 8}) {
		t.Fatalf("got %v", r.P)
	}
}

func TestGeometry_Native_LineString(t *testing.T) {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	lineType := arrow.ListOf(pointType)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ls", Type: lineType, Metadata: extNameMeta("geoarrow.linestring")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		lb := b.Field(0).(*array.ListBuilder)
		lb.Append(true)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		pts := []orb.Point{{0, 0}, {1, 1}, {2, 2}}
		for _, p := range pts {
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Float64Builder).Append(p[0])
			sb.FieldBuilder(1).(*array.Float64Builder).Append(p[1])
		}
	})
	defer tbl.Release()

	type row struct {
		LS orb.Geometry `json:"ls"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := orb.LineString{{0, 0}, {1, 1}, {2, 2}}
	if !orb.Equal(r.LS, want) {
		t.Fatalf("got %v", r.LS)
	}
}

func TestGeometry_Native_Polygon(t *testing.T) {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	ringsType := arrow.ListOf(arrow.ListOf(pointType))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "poly", Type: ringsType, Metadata: extNameMeta("geoarrow.polygon")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		outer := b.Field(0).(*array.ListBuilder)
		outer.Append(true)
		ringBuilder := outer.ValueBuilder().(*array.ListBuilder)
		// Two rings: exterior + 0 holes → here for simplicity put one ring
		ringBuilder.Append(true)
		sb := ringBuilder.ValueBuilder().(*array.StructBuilder)
		ring := []orb.Point{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}
		for _, p := range ring {
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Float64Builder).Append(p[0])
			sb.FieldBuilder(1).(*array.Float64Builder).Append(p[1])
		}
	})
	defer tbl.Release()

	type row struct {
		P orb.Geometry `json:"poly"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	poly, ok := r.P.(orb.Polygon)
	if !ok || len(poly) != 1 || len(poly[0]) != 5 {
		t.Fatalf("want polygon with 1 ring × 5 points, got %#v", r.P)
	}
	if poly[0][0] != (orb.Point{0, 0}) || poly[0][2] != (orb.Point{1, 1}) {
		t.Fatalf("unexpected polygon points: %v", poly[0])
	}
}

// TestGeometry_NestedStruct — the headline case: a struct with a nested struct
// field, the outer column carries WKB, the inner column carries GeoJSONString.
// This mirrors the shape `{ table { geom ref { geom } } }` emitted by the engine
// where top-level geometry ships as WKB and nested-reference geometry ships as
// hugr.geojson. Validates decoder dispatch at both levels.
func TestGeometry_NestedStruct(t *testing.T) {
	topPt := orb.Point{100, 200}
	topWKB, err := wkb.Marshal(topPt)
	if err != nil {
		t.Fatalf("wkb.Marshal: %v", err)
	}
	refStructType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		arrow.Field{
			Name:     "geom",
			Type:     arrow.BinaryTypes.String,
			Metadata: extNameMeta("hugr.geojson"),
		},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta("geoarrow.wkb")},
		{Name: "ref", Type: refStructType},
	}, nil)

	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append("alpha")
		b.Field(1).(*array.BinaryBuilder).Append(topWKB)
		sb := b.Field(2).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.StringBuilder).Append("beta")
		sb.FieldBuilder(1).(*array.StringBuilder).Append(`{"type":"Point","coordinates":[300,400]}`)
	})
	defer tbl.Release()

	type ref struct {
		Name string       `json:"name"`
		Geom orb.Geometry `json:"geom"`
	}
	type row struct {
		Name string       `json:"name"`
		Geom orb.Geometry `json:"geom"`
		Ref  ref          `json:"ref"`
	}

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan nested: %v", err)
	}
	if r.Name != "alpha" {
		t.Fatalf("top name: %q", r.Name)
	}
	if !orb.Equal(r.Geom, topPt) {
		t.Fatalf("top geom: want %v, got %v", topPt, r.Geom)
	}
	if r.Ref.Name != "beta" {
		t.Fatalf("ref name: %q", r.Ref.Name)
	}
	if !orb.Equal(r.Ref.Geom, orb.Point{300, 400}) {
		t.Fatalf("ref geom: want Point(300,400), got %v", r.Ref.Geom)
	}
}

// TestGeometry_RawPassthrough — FR-012: when the destination is any / []byte /
// string / map[string]any the scanner must NOT decode geometry.
func TestGeometry_RawPassthrough(t *testing.T) {
	pt := orb.Point{9, 8}
	tbl := newGeomWKBTable(t, []orb.Geometry{pt})
	defer tbl.Release()

	// 1. []byte destination — raw WKB bytes.
	rows, _ := tbl.Rows()
	rows.Next()
	var asBytes struct {
		Geom []byte `json:"geom"`
	}
	if err := rows.Scan(&asBytes); err != nil {
		t.Fatalf("Scan bytes: %v", err)
	}
	if len(asBytes.Geom) == 0 {
		t.Fatal("bytes empty — expected raw WKB")
	}
	roundTrip, err := wkb.Unmarshal(asBytes.Geom)
	if err != nil {
		t.Fatalf("roundtrip WKB: %v", err)
	}
	if !orb.Equal(roundTrip, pt) {
		t.Fatalf("bytes dest did not contain WKB of point")
	}
	rows.Close()

	// 2. map[string]any destination — bytes as []byte value.
	rows2, _ := tbl.Rows()
	defer rows2.Close()
	rows2.Next()
	m := map[string]any{}
	if err := rows2.Scan(&m); err != nil {
		t.Fatalf("Scan map: %v", err)
	}
	b, ok := m["geom"].([]byte)
	if !ok || len(b) == 0 {
		t.Fatalf("map[geom] should be []byte, got %T = %v", m["geom"], m["geom"])
	}
	// 3. any destination — same story via *any.
	rows3, _ := tbl.Rows()
	defer rows3.Close()
	rows3.Next()
	var anyDst any
	if err := rows3.Scan(&anyDst); err != nil {
		t.Fatalf("Scan *any: %v", err)
	}
	asMap, ok := anyDst.(map[string]any)
	if !ok {
		t.Fatalf("any dest should be map[string]any, got %T", anyDst)
	}
	if _, ok := asMap["geom"].([]byte); !ok {
		t.Fatalf("*any geom not []byte: %T", asMap["geom"])
	}
}

// TestGeometry_BadBytes — FR-023: error wrapping with column+row and
// errors.Is(err, ErrGeometryDecode).
func TestGeometry_BadBytes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta("geoarrow.wkb")},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.BinaryBuilder).Append([]byte{0xFF, 0x00, 0xFF}) // not valid WKB
	})
	defer tbl.Release()

	type row struct {
		Geom orb.Geometry `json:"geom"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	err := rows.Scan(&r)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrGeometryDecode) {
		t.Fatalf("want errors.Is(err, ErrGeometryDecode), got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `scan column "geom"`) || !strings.Contains(msg, "row 0") {
		t.Fatalf("error message missing column+row: %s", msg)
	}
}

// TestGeometry_GeojsonGeometryDest verifies that geojson.Geometry (the wrapper
// struct from paulmach/orb/geojson) is supported as a destination type so
// callers can use the same struct definition for both ScanTable (Arrow) and
// ScanObject (JSON). Covers WKB, untagged GeoJSON string, and pointer fields.
func TestGeometry_GeojsonGeometryDest(t *testing.T) {
	pt := orb.Point{10, 20}
	wkbBytes, err := wkb.Marshal(pt)
	if err != nil {
		t.Fatalf("wkb.Marshal: %v", err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "wkb_geom", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta("geoarrow.wkb")},
		{Name: "json_geom", Type: arrow.BinaryTypes.String}, // untagged JSON
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.BinaryBuilder).Append(wkbBytes)
		b.Field(1).(*array.StringBuilder).Append(`{"type":"LineString","coordinates":[[0,0],[1,1]]}`)
	})
	defer tbl.Release()

	// Value-kind destination.
	type rowVal struct {
		WKB  geojson.Geometry `json:"wkb_geom"`
		JSON geojson.Geometry `json:"json_geom"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var rv rowVal
	if err := rows.Scan(&rv); err != nil {
		t.Fatalf("Scan value: %v", err)
	}
	if g := rv.WKB.Geometry(); !orb.Equal(g, pt) {
		t.Fatalf("wkb → geojson.Geometry: want %v, got %v", pt, g)
	}
	wantLS := orb.LineString{{0, 0}, {1, 1}}
	if g := rv.JSON.Geometry(); !orb.Equal(g, wantLS) {
		t.Fatalf("json string → geojson.Geometry: want %v, got %v", wantLS, g)
	}

	// Pointer-kind destination.
	type rowPtr struct {
		WKB  *geojson.Geometry `json:"wkb_geom"`
		JSON *geojson.Geometry `json:"json_geom"`
	}
	rows2, _ := tbl.Rows()
	defer rows2.Close()
	rows2.Next()
	var rp rowPtr
	if err := rows2.Scan(&rp); err != nil {
		t.Fatalf("Scan pointer: %v", err)
	}
	if rp.WKB == nil || !orb.Equal(rp.WKB.Geometry(), pt) {
		t.Fatalf("*geojson.Geometry wkb: %v", rp.WKB)
	}
	if rp.JSON == nil || !orb.Equal(rp.JSON.Geometry(), wantLS) {
		t.Fatalf("*geojson.Geometry json: %v", rp.JSON)
	}
}

// TestGeometry_UntaggedStringAuto verifies that a String column carrying
// geometry WITHOUT any ARROW:extension:name metadata still decodes into
// orb.Geometry. The engine's ipc-query addGeometryFieldMeta walker misses
// some nested paths (observed on tf.digital_twin.roads.parts[].geom).
// The scanner's heuristic peeks at the first non-space byte: '{' → GeoJSON,
// otherwise → WKT.
func TestGeometry_UntaggedStringAuto(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    orb.Geometry
	}{
		{"geojson point", `{"type":"Point","coordinates":[1,2]}`, orb.Point{1, 2}},
		{"geojson line", `{"type":"LineString","coordinates":[[0,0],[1,1],[2,2]]}`,
			orb.LineString{{0, 0}, {1, 1}, {2, 2}}},
		{"wkt point", `POINT (5 6)`, orb.Point{5, 6}},
		{"wkt line with leading space", `   LINESTRING (0 0, 1 1)`, orb.LineString{{0, 0}, {1, 1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "g", Type: arrow.BinaryTypes.String}, // no metadata!
			}, nil)
			tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
				b.Field(0).(*array.StringBuilder).Append(tc.payload)
			})
			defer tbl.Release()

			type row struct {
				G orb.Geometry `json:"g"`
			}
			rows, _ := tbl.Rows()
			defer rows.Close()
			rows.Next()
			var r row
			if err := rows.Scan(&r); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !orb.Equal(r.G, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, r.G)
			}
		})
	}
}

// TestGeometry_UntaggedStringInsideList covers the real-world shape that
// triggered the heuristic fallback: a list-of-struct column where one struct
// field is an untagged string carrying GeoJSON.
func TestGeometry_UntaggedStringInsideList(t *testing.T) {
	partType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "geom", Type: arrow.BinaryTypes.String}, // untagged
	)
	listType := arrow.ListOf(partType)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "parts", Type: listType},
	}, nil)

	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		lb := b.Field(0).(*array.ListBuilder)
		lb.Append(true)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
		sb.FieldBuilder(1).(*array.StringBuilder).Append(`{"type":"Point","coordinates":[10,20]}`)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int64Builder).Append(2)
		sb.FieldBuilder(1).(*array.StringBuilder).Append(`{"type":"LineString","coordinates":[[0,0],[5,5]]}`)
	})
	defer tbl.Release()

	type Part struct {
		ID   int64        `json:"id"`
		Geom orb.Geometry `json:"geom"`
	}
	type Row struct {
		Parts []Part `json:"parts"`
	}

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r Row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(r.Parts) != 2 {
		t.Fatalf("want 2 parts, got %d", len(r.Parts))
	}
	if !orb.Equal(r.Parts[0].Geom, orb.Point{10, 20}) {
		t.Fatalf("part[0].geom: %v", r.Parts[0].Geom)
	}
	want := orb.LineString{{0, 0}, {5, 5}}
	if !orb.Equal(r.Parts[1].Geom, want) {
		t.Fatalf("part[1].geom: %v", r.Parts[1].Geom)
	}
}

// TestGeometry_RegisterCustom — FR-013 / SC-006: custom decoder hook.
func TestGeometry_RegisterCustom(t *testing.T) {
	const myExt = "test.fake-geo"
	called := false
	RegisterGeometryDecoder(myExt, func(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
		called = true
		return orb.Point{111, 222}, nil
	})
	defer RegisterGeometryDecoder(myExt, nil)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "g", Type: arrow.BinaryTypes.Binary, Metadata: extNameMeta(myExt)},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.BinaryBuilder).Append([]byte("ignored"))
	})
	defer tbl.Release()

	type row struct {
		G orb.Geometry `json:"g"`
	}
	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()
	var r row
	if err := rows.Scan(&r); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !called {
		t.Fatal("custom decoder not invoked")
	}
	if !orb.Equal(r.G, orb.Point{111, 222}) {
		t.Fatalf("got %v", r.G)
	}
}
