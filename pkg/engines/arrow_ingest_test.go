package engines

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/paulmach/orb"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestDuckDBArrowIngestBuildsNativeGeoArrowSelectExpr(t *testing.T) {
	field := geometryTestField("")

	tests := []struct {
		ext  string
		want string
	}{
		{"geoarrow.point", "POINT"},
		{"geoarrow.linestring", "LINESTRING"},
		{"geoarrow.polygon", "POLYGON"},
		{"geoarrow.multipoint", "MULTIPOINT"},
		{"geoarrow.multilinestring", "MULTILINESTRING"},
		{"geoarrow.multipolygon", "MULTIPOLYGON"},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			got, err := duckDBArrowIngestSelectExpr(field, arrow.Field{
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
			if !strings.Contains(got, "ST_GeomFromText(") || !strings.Contains(got, tt.want) {
				t.Fatalf("unexpected conversion for %s: %s", tt.ext, got)
			}
		})
	}
}

func TestDuckDBArrowIngestBuildsGeoJSONAliasSelectExpr(t *testing.T) {
	field := geometryTestField("")

	for _, ext := range []string{"geoarrow.geojson", "hugr.geojson", "geojson"} {
		t.Run(ext, func(t *testing.T) {
			got, err := duckDBArrowIngestSelectExpr(field, arrow.Field{
				Name:     "geom_geojson",
				Type:     arrow.BinaryTypes.String,
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": ext}),
			}, "geom_geojson")
			if err != nil {
				t.Fatal(err)
			}
			want := "ST_GeomFromText(ST_AsText(ST_GeomFromGeoJSON(geom_geojson)), true)"
			if got != want {
				t.Fatalf("expected %s, got %s", want, got)
			}
		})
	}
}

func TestPostgresArrowIngestBuildsNativeGeoArrowEWKTSelectExpr(t *testing.T) {
	field := geometryTestField("4326")

	tests := []struct {
		ext  string
		want string
	}{
		{"geoarrow.point", "POINT"},
		{"geoarrow.linestring", "LINESTRING"},
		{"geoarrow.polygon", "POLYGON"},
		{"geoarrow.multipoint", "MULTIPOINT"},
		{"geoarrow.multilinestring", "MULTILINESTRING"},
		{"geoarrow.multipolygon", "MULTIPOLYGON"},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			got, err := postgresArrowIngestSelectExpr(field, arrow.Field{
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
			if !strings.Contains(got, "'SRID=4326;' || ") || !strings.Contains(got, tt.want) {
				t.Fatalf("unexpected conversion for %s: %s", tt.ext, got)
			}
		})
	}
}

func TestPostgresArrowIngestBuildsGeoJSONAliasEWKTSelectExpr(t *testing.T) {
	field := geometryTestField("4326")

	for _, ext := range []string{"geoarrow.geojson", "hugr.geojson", "geojson"} {
		t.Run(ext, func(t *testing.T) {
			got, err := postgresArrowIngestSelectExpr(field, arrow.Field{
				Name:     "geom_geojson",
				Type:     arrow.BinaryTypes.String,
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": ext}),
			}, "geom_geojson")
			if err != nil {
				t.Fatal(err)
			}
			want := "'SRID=4326;' || ST_AsText(ST_GeomFromGeoJSON(geom_geojson))"
			if got != want {
				t.Fatalf("expected %s, got %s", want, got)
			}
		})
	}
}

func TestArrowIngestRejectsNativeGeoArrowUnionLayouts(t *testing.T) {
	field := geometryTestField("")
	for _, ext := range []string{"geoarrow.geometry", "geoarrow.geometrycollection"} {
		t.Run(ext, func(t *testing.T) {
			_, err := duckDBArrowIngestSelectExpr(field, arrow.Field{
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
			_, err := duckDBArrowIngestSelectExpr(field, arrow.Field{
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

func TestPostgresArrowIngestLiteralExprUsesDuckDBStagingLiterals(t *testing.T) {
	engine := &Postgres{}

	jsonSQL, err := engine.ArrowIngestLiteralExpr(nil, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(jsonSQL, "JSONB") || !strings.Contains(jsonSQL, "::JSON") {
		t.Fatalf("expected DuckDB JSON literal, got %s", jsonSQL)
	}

	geomSQL, err := engine.ArrowIngestLiteralExpr(geometryTestField("4326"), orb.Point{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(geomSQL, "'SRID=4326;' || ") || !strings.Contains(geomSQL, "POINT") {
		t.Fatalf("expected Postgres EWKT literal, got %s", geomSQL)
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
