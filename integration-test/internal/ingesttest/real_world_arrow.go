//go:build duckdb_arrow

package ingesttest

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func RealWorldSingleStreamColumns() []string {
	columns := []string{"name", "value", "is_active"}
	return append(columns, CountedGeometryColumns()...)
}

func RealWorldGeometrySingleStreamSchema(t testing.TB, geometryArrowFields []arrow.Field) *arrow.Schema {
	t.Helper()

	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}

	geometryFields := make(map[string]arrow.Field)
	for _, field := range geometryArrowFields {
		field.Nullable = true
		geometryFields[field.Name] = field
	}
	for _, name := range CountedGeometryColumns() {
		field, ok := geometryFields[name]
		require.Truef(t, ok, "geometry field %q must exist", name)
		fields = append(fields, field)
	}
	return arrow.NewSchema(fields, nil)
}

func MakeRealWorldGeometrySingleStreamRecord(t testing.TB, rowPrefix string, features []RealWorldFeature, geometryArrowFields []arrow.Field) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()

	schema := RealWorldGeometrySingleStreamSchema(t, geometryArrowFields)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	geometryKindRows := make(map[string]int)
	for _, feature := range features {
		geometryKind := feature.Properties.GeometryKind
		row := geometryKindRows[geometryKind]
		geometryKindRows[geometryKind]++
		appendRealWorldGeometrySingleStreamRow(t, b, rowPrefix, row, feature)
	}

	return b.NewRecordBatch(), schema
}

func appendRealWorldGeometrySingleStreamRow(t testing.TB, b *array.RecordBuilder, rowPrefix string, row int, feature RealWorldFeature) {
	t.Helper()

	RecordFieldBuilder(t, b, "name").(*array.StringBuilder).Append(RowName(rowPrefix, row, feature))
	RecordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(float64(row))
	RecordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(row%2 == 0)

	appenders := realWorldGeometryColumnAppenders(t, b, feature)
	for _, col := range CountedGeometryColumns() {
		if appendValue, ok := appenders[col]; ok {
			appendValue()
			continue
		}
		RecordFieldBuilder(t, b, col).AppendNull()
	}
}

func realWorldGeometryColumnAppenders(t testing.TB, b *array.RecordBuilder, feature RealWorldFeature) map[string]func() {
	t.Helper()

	switch feature.Properties.GeometryKind {
	case "point":
		point := PointFromGeometry(t, feature.Geometry)
		pointWKB := WKBPoint(t, point)
		return map[string]func(){
			"geom_point_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_point_native")
				appendNativePoint(t, builder, nativeGeometryBuilderLayout(t, builder, 0), point)
			},
			"geom_point_wkb": func() { RecordFieldBuilder(t, b, "geom_point_wkb").(*array.BinaryBuilder).Append(pointWKB) },
			"geom_point_hexwkb": func() {
				RecordFieldBuilder(t, b, "geom_point_hexwkb").(*array.StringBuilder).Append(strings.ToUpper(hex.EncodeToString(pointWKB)))
			},
		}
	case "line":
		line := LineStringFromGeometry(t, feature.Geometry)
		wkbLine := WKBLineString(t, line)
		return map[string]func(){
			"geom_line_wkt": func() {
				RecordFieldBuilder(t, b, "geom_line_wkt").(*array.StringBuilder).Append(RealWorldLineWKT(line))
			},
			"geom_line_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_line_native")
				appendNativePointList(t, builder, nativeGeometryBuilderLayout(t, builder, 1), line)
			},
			"geom_line_wkb": func() { RecordFieldBuilder(t, b, "geom_line_wkb").(*array.BinaryBuilder).Append(wkbLine) },
		}
	case "polygon":
		polygon := PolygonFromGeometry(t, feature.Geometry)
		geoJSON := RealWorldPolygonGeoJSON(polygon)
		wkbPolygon := WKBPolygon(t, polygon)
		return map[string]func(){
			"geom_polygon_geojson":       func() { RecordFieldBuilder(t, b, "geom_polygon_geojson").(*array.StringBuilder).Append(geoJSON) },
			"geom_polygon_hugr_geojson":  func() { RecordFieldBuilder(t, b, "geom_polygon_hugr_geojson").(*array.StringBuilder).Append(geoJSON) },
			"geom_polygon_plain_geojson": func() { RecordFieldBuilder(t, b, "geom_polygon_plain_geojson").(*array.StringBuilder).Append(geoJSON) },
			"geom_polygon_geojson_struct": func() {
				AppendGeoJSONPolygonStructFromRings(t, RecordFieldBuilder(t, b, "geom_polygon_geojson_struct"), polygon)
			},
			"geom_polygon_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_polygon_native")
				appendNativePointListList(t, builder, nativeGeometryBuilderLayout(t, builder, 2), polygon)
			},
			"geom_polygon_wkb": func() { RecordFieldBuilder(t, b, "geom_polygon_wkb").(*array.BinaryBuilder).Append(wkbPolygon) },
		}
	case "multipoint":
		points := MultiPointFromGeometry(t, feature.Geometry)
		wkbMultiPoint := WKBMultiPoint(t, points)
		return map[string]func(){
			"geom_multipoint_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_multipoint_native")
				appendNativePointList(t, builder, nativeGeometryBuilderLayout(t, builder, 1), points)
			},
			"geom_multipoint_wkb": func() { RecordFieldBuilder(t, b, "geom_multipoint_wkb").(*array.BinaryBuilder).Append(wkbMultiPoint) },
		}
	case "multiline":
		lines := MultiLineStringFromGeometry(t, feature.Geometry)
		wkbMultiLine := WKBMultiLineString(t, lines)
		return map[string]func(){
			"geom_multiline_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_multiline_native")
				appendNativePointListList(t, builder, nativeGeometryBuilderLayout(t, builder, 2), lines)
			},
			"geom_multiline_wkb": func() { RecordFieldBuilder(t, b, "geom_multiline_wkb").(*array.BinaryBuilder).Append(wkbMultiLine) },
		}
	case "multipolygon":
		polygons := MultiPolygonFromGeometry(t, feature.Geometry)
		wkbMultiPolygon := WKBMultiPolygon(t, polygons)
		return map[string]func(){
			"geom_multipolygon_native": func() {
				builder := RecordFieldBuilder(t, b, "geom_multipolygon_native")
				appendNativePointListListList(t, builder, nativeGeometryBuilderLayout(t, builder, 3), polygons)
			},
			"geom_multipolygon_wkb": func() {
				RecordFieldBuilder(t, b, "geom_multipolygon_wkb").(*array.BinaryBuilder).Append(wkbMultiPolygon)
			},
		}
	default:
		t.Fatalf("unsupported real-world geometry kind %q", feature.Properties.GeometryKind)
		return nil
	}
}
