package engines

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/paulmach/orb"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestArrowIngestJSONStagingExpr(t *testing.T) {
	tests := []struct {
		name string
		typ  arrow.DataType
		want string
	}{
		{name: "string", typ: arrow.BinaryTypes.String, want: "CAST(payload AS JSON)"},
		{name: "large string", typ: arrow.BinaryTypes.LargeString, want: "CAST(payload AS JSON)"},
		{name: "string view", typ: arrow.BinaryTypes.StringView, want: "CAST(payload AS JSON)"},
		{name: "binary", typ: arrow.BinaryTypes.Binary, want: "CAST(decode(payload) AS JSON)"},
		{name: "large binary", typ: arrow.BinaryTypes.LargeBinary, want: "CAST(decode(payload) AS JSON)"},
		{name: "binary view", typ: arrow.BinaryTypes.BinaryView, want: "CAST(decode(payload) AS JSON)"},
		{name: "struct", typ: arrow.StructOf(), want: "to_json(payload)"},
		{name: "list", typ: arrow.ListOf(arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "large list", typ: arrow.LargeListOf(arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "fixed size list", typ: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "list view", typ: arrow.ListViewOf(arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "large list view", typ: arrow.LargeListViewOf(arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "map", typ: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), want: "to_json(payload)"},
		{name: "scalar", typ: arrow.PrimitiveTypes.Int64, want: "to_json(payload)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := arrowIngestJSONStagingExpr(arrow.Field{Name: "payload", Type: tt.typ}, "payload")
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestArrowIngestStagingBuildsNativeGeoArrowSelectExpr(t *testing.T) {
	field := geometryTestField("")
	staging := NewArrowIngestStagingBuilder()

	tests := []struct {
		ext  string
		want string
	}{
		{"geoarrow.point", "ST_Point(struct_extract(geom, 'x'), struct_extract(geom, 'y'))"},
		{"geoarrow.linestring", "ST_MakeLine(list_transform(geom"},
		{"geoarrow.polygon", "ST_MakePolygon(ST_MakeLine(list_transform(geom[1]"},
		{"geoarrow.multipoint", "ST_Multi(ST_Collect(list_transform(geom"},
		{"geoarrow.multilinestring", "ST_Multi(ST_Collect(list_transform(geom"},
		{"geoarrow.multipolygon", "ST_Multi(ST_Collect(list_transform(geom"},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			got, err := staging.SelectExpr(field, arrow.Field{
				Name:     "geom",
				Type:     geoArrowTestType(tt.ext),
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": tt.ext}),
			}, "geom")
			if err != nil {
				t.Fatal(err)
			}
			if got == "geom" {
				t.Fatalf("expected explicit conversion, got raw column")
			}
			if !strings.Contains(got, tt.want) ||
				strings.Contains(got, "ST_GeomFromText(") ||
				strings.Contains(got, "ST_AsText(") {
				t.Fatalf("unexpected conversion for %s: %s", tt.ext, got)
			}
		})
	}
}

func TestArrowIngestStagingBuildsDirectGeometrySelectExpr(t *testing.T) {
	field := geometryTestField("")
	staging := NewArrowIngestStagingBuilder()

	tests := []struct {
		name string
		typ  arrow.DataType
		ext  string
		want string
	}{
		{
			name: "trusted geoarrow wkb is already materialized as geometry",
			typ:  arrow.BinaryTypes.Binary,
			ext:  "geoarrow.wkb",
			want: "geom",
		},
		{
			name: "trusted geoarrow wkt parses directly from text",
			typ:  arrow.BinaryTypes.String,
			ext:  "geoarrow.wkt",
			want: "ST_GeomFromText(geom, true)",
		},
		{
			name: "trusted geoarrow geojson parses directly from json",
			typ:  arrow.BinaryTypes.String,
			ext:  "geoarrow.geojson",
			want: "ST_GeomFromGeoJSON(geom)",
		},
		{
			name: "trusted hugr geojson parses directly from json",
			typ:  arrow.BinaryTypes.String,
			ext:  "hugr.geojson",
			want: "ST_GeomFromGeoJSON(geom)",
		},
		{
			name: "trusted plain geojson parses directly from json",
			typ:  arrow.BinaryTypes.String,
			ext:  "geojson",
			want: "ST_GeomFromGeoJSON(geom)",
		},
		{
			name: "unannotated binary parses directly as wkb",
			typ:  arrow.BinaryTypes.Binary,
			want: "ST_GeomFromWKB(geom)",
		},
		{
			name: "unannotated string chooses geojson or wkt without text roundtrip",
			typ:  arrow.BinaryTypes.String,
			want: "CASE WHEN starts_with(trim(geom), '{') THEN ST_GeomFromGeoJSON(geom) ELSE ST_GeomFromText(geom, true) END",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := arrow.Metadata{}
			if tt.ext != "" {
				meta = arrow.MetadataFrom(map[string]string{"ARROW:extension:name": tt.ext})
			}
			got, err := staging.SelectExpr(field, arrow.Field{
				Name:     "geom",
				Type:     tt.typ,
				Metadata: meta,
			}, "geom")
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("expected %s, got %s", tt.want, got)
			}
			if strings.Contains(got, "ST_AsText") {
				t.Fatalf("expected direct geometry expression without ST_AsText, got %s", got)
			}
		})
	}
}

func TestArrowIngestRejectsNativeGeoArrowUnionLayouts(t *testing.T) {
	field := geometryTestField("")
	staging := NewArrowIngestStagingBuilder()
	for _, ext := range []string{"geoarrow.geometry", "geoarrow.geometrycollection"} {
		t.Run(ext, func(t *testing.T) {
			_, err := staging.SelectExpr(field, arrow.Field{
				Name:     "geom",
				Type:     arrow.StructOf(),
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": ext}),
			}, "geom")
			if err == nil {
				t.Fatalf("expected %s to be rejected", ext)
			}
		})
	}
}

func TestArrowIngestRejectsUnsupportedGeometryExtensionMetadata(t *testing.T) {
	field := geometryTestField("")
	staging := NewArrowIngestStagingBuilder()
	for _, tt := range []struct {
		name string
		typ  arrow.DataType
		ext  string
	}{
		{
			name: "string-like column does not fall back to WKT when metadata is unsupported",
			typ:  arrow.BinaryTypes.String,
			ext:  "geoarrow.curve",
		},
		{
			name: "binary-like column does not fall back to WKB when metadata is unsupported",
			typ:  arrow.BinaryTypes.Binary,
			ext:  "hugr.unknown_geometry",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := staging.SelectExpr(field, arrow.Field{
				Name:     "geom",
				Type:     tt.typ,
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": tt.ext}),
			}, "geom")
			if err == nil {
				t.Fatalf("expected unsupported extension %q to be rejected", tt.ext)
			}
			if !strings.Contains(err.Error(), "unsupported GeoArrow extension") {
				t.Fatalf("unexpected error for %q: %v", tt.ext, err)
			}
		})
	}
}

func TestArrowIngestStagingLiteralExpr(t *testing.T) {
	staging := NewArrowIngestStagingBuilder()

	jsonSQL, err := staging.LiteralExpr(nil, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(jsonSQL, "JSONB") || !strings.Contains(jsonSQL, "::JSON") {
		t.Fatalf("expected DuckDB JSON literal, got %s", jsonSQL)
	}

	geomSQL, err := staging.LiteralExpr(geometryTestField("4326"), orb.Point{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(geomSQL, "ST_GeomFromWKB(from_hex('0101000000") ||
		strings.Contains(geomSQL, "'SRID=4326;'") ||
		strings.Contains(geomSQL, "ST_GeomFromText") ||
		strings.Contains(geomSQL, "POINT") {
		t.Fatalf("expected canonical WKB geometry literal, got %s", geomSQL)
	}
}

func geometryTestField(srid string) *ast.Field {
	def := &ast.FieldDefinition{
		Name: "geom",
		Type: ast.NamedType(base.GeometryTypeName, nil),
	}
	if srid != "" {
		def.Directives = ast.DirectiveList{
			&ast.Directive{
				Name: base.FieldGeometryInfoDirectiveName,
				Arguments: ast.ArgumentList{
					&ast.Argument{Name: base.ArgSRID, Value: &ast.Value{Raw: srid}},
				},
			},
		}
	}
	return &ast.Field{
		Name:       "geom",
		Alias:      "geom",
		Definition: def,
	}
}

func geoArrowTestType(ext string) arrow.DataType {
	point := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	switch ext {
	case "geoarrow.point":
		return point
	case "geoarrow.linestring", "geoarrow.multipoint":
		return arrow.ListOf(point)
	case "geoarrow.polygon", "geoarrow.multilinestring":
		return arrow.ListOf(arrow.ListOf(point))
	case "geoarrow.multipolygon":
		return arrow.ListOf(arrow.ListOf(arrow.ListOf(point)))
	default:
		return point
	}
}
