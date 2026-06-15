package types

import (
	"encoding/json"
	"testing"

	"github.com/paulmach/orb"
)

// TestParseGeometryValue_CoordinateNumericForms guards parseGeoJSONPoint
// against the input forms a coordinate may legitimately arrive as:
//   - float64 (default for encoding/json into any)
//   - int / int64 (what gqlparser emits for GraphQL integer literals)
//   - json.Number (when json.Decoder.UseNumber() is in effect)
//
// All three must yield the same orb.Point.
func TestParseGeometryValue_CoordinateNumericForms(t *testing.T) {
	cases := []struct {
		name   string
		coords []any
	}{
		{name: "float64 (json.Unmarshal default)", coords: []any{5.0, 47.0}},
		{name: "int (GraphQL integer literal in some parsers)", coords: []any{5, 47}},
		{name: "int64 (gqlparser integer literal)", coords: []any{int64(5), int64(47)}},
		{name: "int32", coords: []any{int32(5), int32(47)}},
		{name: "float32", coords: []any{float32(5), float32(47)}},
		{name: "json.Number (UseNumber decoder)", coords: []any{json.Number("5"), json.Number("47")}},
		{name: "mixed int and float", coords: []any{5, 47.5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := ParseGeometryValue(map[string]any{
				"type":        "Point",
				"coordinates": tc.coords,
			})
			if err != nil {
				t.Fatalf("ParseGeometryValue: %v", err)
			}
			p, ok := g.(orb.Point)
			if !ok {
				t.Fatalf("expected orb.Point, got %T", g)
			}
			// Use the float-cast equivalents of inputs to compute expected.
			ex, _ := coordToFloat64(tc.coords[0])
			ey, _ := coordToFloat64(tc.coords[1])
			if p[0] != ex || p[1] != ey {
				t.Fatalf("point: got (%v,%v), want (%v,%v)", p[0], p[1], ex, ey)
			}
		})
	}
}

// TestParseGeometryValue_PolygonWithIntCoords mirrors the integration-test
// shape that surfaced this gap: GeoJSON Polygon with integer literals at
// every coordinate, as produced by `coordinates: [[[5,47],...]]` in
// GraphQL.
func TestParseGeometryValue_PolygonWithIntCoords(t *testing.T) {
	g, err := ParseGeometryValue(map[string]any{
		"type": "Polygon",
		"coordinates": []any{
			[]any{
				[]any{5, 47},
				[]any{15, 47},
				[]any{15, 55},
				[]any{5, 55},
				[]any{5, 47},
			},
		},
	})
	if err != nil {
		t.Fatalf("ParseGeometryValue: %v", err)
	}
	poly, ok := g.(orb.Polygon)
	if !ok {
		t.Fatalf("expected orb.Polygon, got %T", g)
	}
	if len(poly) != 1 || len(poly[0]) != 5 {
		t.Fatalf("unexpected ring shape: %v", poly)
	}
	if poly[0][0] != (orb.Point{5, 47}) || poly[0][2] != (orb.Point{15, 55}) {
		t.Fatalf("polygon coords wrong: %v", poly)
	}
}

func TestParseGeometryValue_NonNumberCoordRejected(t *testing.T) {
	_, err := ParseGeometryValue(map[string]any{
		"type":        "Point",
		"coordinates": []any{"5", "47"}, // strings, not numbers
	})
	if err == nil {
		t.Fatal("expected error for string coordinates, got nil")
	}
}
