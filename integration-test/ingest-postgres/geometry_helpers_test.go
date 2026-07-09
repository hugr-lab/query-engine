//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/integration-test/internal/ingesttest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type geometryTypesRow struct {
	name        string
	value       float64
	active      bool
	point       xyPoint
	shapeOrigin xyPoint
}

func makeGeometryTypesRecord(t *testing.T, rows []geometryTypesRow) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()

	schema := geometryTypesSchema()
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for _, row := range rows {
		appendGeometryTypesRow(t, b, row)
	}

	return b.NewRecordBatch(), schema
}

func geometryTypesSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}
	fields = append(fields, geometryArrowFields()...)
	return arrow.NewSchema(fields, nil)
}

func geometryArrowFields() []arrow.Field {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	lineType := arrow.ListOf(pointType)
	polygonType := arrow.ListOf(lineType)
	fields := make([]arrow.Field, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		field := arrow.Field{
			Name:     col.name,
			Type:     col.arrowType,
			Nullable: false,
		}
		if col.arrowExtension != "" {
			field.Metadata = arrow.MetadataFrom(map[string]string{"ARROW:extension:name": col.arrowExtension})
		}
		fields = append(fields, field)
	}
	return fields
}

type geometryValueColumn struct {
	name           string
	arrowType      arrow.DataType
	arrowExtension string
	expectedWKT    func(point, x, y string) string
	expectedSRID   int
}

func geometryValueColumns(pointType, lineType, polygonType arrow.DataType) []geometryValueColumn {
	geoJSONStructType := arrow.StructOf(
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "coordinates", Type: arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Float64))), Nullable: false},
	)
	line := func(_ string, x string, y string) string {
		return fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1))
	}
	polygon := func(_ string, x string, y string) string { return polygonWKT(x, y) }
	point := func(point string, _ string, _ string) string { return point }
	multiPoint := func(_ string, x string, y string) string {
		return fmt.Sprintf("MULTIPOINT(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), y)
	}
	multiLine := func(_ string, x string, y string) string {
		return fmt.Sprintf("MULTILINESTRING((%s %s,%s %s),(%s %s,%s %s))", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 2), addCoord(x, 3), addCoord(y, 3))
	}
	multiPolygon := func(_ string, x string, y string) string { return multiPolygonWKT(x, y) }

	return []geometryValueColumn{
		{name: "geom_point_native", arrowType: pointType, arrowExtension: "geoarrow.point", expectedWKT: point},
		{name: "geom_point_native_4326", arrowType: pointType, arrowExtension: "geoarrow.point", expectedWKT: point, expectedSRID: 4326},
		{name: "geom_line_wkt", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.wkt", expectedWKT: line},
		{name: "geom_line_wkt_4326", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.wkt", expectedWKT: line, expectedSRID: 4326},
		{name: "geom_polygon_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.geojson", expectedWKT: polygon},
		{name: "geom_polygon_hugr_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "hugr.geojson", expectedWKT: polygon},
		{name: "geom_polygon_plain_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "geojson", expectedWKT: polygon},
		{name: "geom_polygon_geojson_struct", arrowType: geoJSONStructType, expectedWKT: polygon},
		{name: "geom_point_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: point},
		{name: "geom_point_hexwkb", arrowType: arrow.BinaryTypes.String, arrowExtension: "hugr.hexwkb", expectedWKT: point},
		{name: "geom_line_native", arrowType: lineType, arrowExtension: "geoarrow.linestring", expectedWKT: line},
		{name: "geom_line_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: line},
		{name: "geom_polygon_native", arrowType: polygonType, arrowExtension: "geoarrow.polygon", expectedWKT: polygon},
		{name: "geom_polygon_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: polygon},
		{name: "geom_multipoint_native", arrowType: lineType, arrowExtension: "geoarrow.multipoint", expectedWKT: multiPoint},
		{name: "geom_multipoint_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: multiPoint},
		{name: "geom_multiline_native", arrowType: polygonType, arrowExtension: "geoarrow.multilinestring", expectedWKT: multiLine},
		{name: "geom_multiline_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: multiLine},
		{name: "geom_multipolygon_native", arrowType: arrow.ListOf(polygonType), arrowExtension: "geoarrow.multipolygon", expectedWKT: multiPolygon},
		{name: "geom_multipolygon_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: multiPolygon},
	}
}

func geometryTypesColumns() []string {
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := []string{"name", "value", "is_active"}
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		columns = append(columns, col.name)
	}
	return columns
}

func geometryExpected(point, x, y string) []string {
	pointType, lineType, polygonType := geometryArrowTypes()
	values := make([]string, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		values = append(values, col.expectedWKT(point, x, y))
	}
	return values
}

func geometrySRIDExpected() []int {
	pointType, lineType, polygonType := geometryArrowTypes()
	srids := make([]int, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		srids = append(srids, col.expectedSRID)
	}
	return srids
}

func geometryArrowTypes() (pointType, lineType, polygonType arrow.DataType) {
	pointType = arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	lineType = arrow.ListOf(pointType)
	polygonType = arrow.ListOf(lineType)
	return pointType, lineType, polygonType
}

func geometrySelectList(withSRID bool) string {
	pointType, lineType, polygonType := geometryArrowTypes()
	exprs := make([]string, 0, len(geometryValueColumns(pointType, lineType, polygonType))*2)
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		exprs = append(exprs, "ST_AsText("+col.name+")")
		if withSRID {
			exprs = append(exprs, "ST_SRID("+col.name+")")
		}
	}
	return strings.Join(exprs, ",\n")
}

type sqlScanner interface {
	Scan(dest ...any) error
}

func scanGeometryValuesWithSRID(t *testing.T, scanner sqlScanner) ([]string, []int) {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	values := make([]string, len(columns))
	srids := make([]int, len(columns))
	scanArgs := make([]any, 0, len(columns)*2)
	for i := range columns {
		scanArgs = append(scanArgs, &values[i], &srids[i])
	}
	require.NoError(t, scanner.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return values, srids
}

func scanNamedGeometryValuesWithSRID(t *testing.T, rows *sql.Rows) (string, []string, []int) {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	var name string
	values := make([]string, len(columns))
	srids := make([]int, len(columns))
	scanArgs := []any{&name}
	for i := range columns {
		scanArgs = append(scanArgs, &values[i], &srids[i])
	}
	require.NoError(t, rows.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return name, values, srids
}

func polygonWKT(x, y string) string {
	return fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s),(%s %s,%s %s,%s %s,%s %s,%s %s))",
		x, y,
		x, addCoord(y, 4),
		addCoord(x, 4), addCoord(y, 4),
		addCoord(x, 4), y,
		x, y,
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 1),
	)
}

func multiPolygonWKT(x, y string) string {
	return fmt.Sprintf("MULTIPOLYGON(((%s %s,%s %s,%s %s,%s %s,%s %s),(%s %s,%s %s,%s %s,%s %s,%s %s)),((%s %s,%s %s,%s %s,%s %s,%s %s)))",
		x, y,
		x, addCoord(y, 4),
		addCoord(x, 4), addCoord(y, 4),
		addCoord(x, 4), y,
		x, y,
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 10), addCoord(y, 10),
		addCoord(x, 10), addCoord(y, 12),
		addCoord(x, 12), addCoord(y, 12),
		addCoord(x, 12), addCoord(y, 10),
		addCoord(x, 10), addCoord(y, 10),
	)
}

func assertGeometryReadThroughHugr(t *testing.T, service *hugr.Service, dsName, filter string, expected []map[string]any) {
	t.Helper()

	query := fmt.Sprintf(`{
		%s {
			events(%s, order_by: [{field: "name", direction: ASC}]) {
				name
				geom_point_native
				geom_point_native_4326
				geom_line_wkt
				geom_line_wkt_4326
				geom_polygon_geojson
				geom_polygon_hugr_geojson
				geom_polygon_plain_geojson
				geom_polygon_geojson_struct
				geom_point_wkb
				geom_point_hexwkb
				geom_line_native
				geom_line_wkb
				geom_polygon_native
				geom_polygon_wkb
				geom_multipoint_native
				geom_multipoint_wkb
				geom_multiline_native
				geom_multiline_wkb
				geom_multipolygon_native
				geom_multipolygon_wkb
			}
		}
	}`, dsName, filter)

	res, err := service.Query(context.Background(), query, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoErrorf(t, res.Err(), "graphql error for query: %s", query)

	body, err := json.Marshal(res)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	data, ok := payload["data"].(map[string]any)
	require.True(t, ok, "response data must be an object: %s", string(body))
	root, ok := data[dsName].(map[string]any)
	require.True(t, ok, "response data.%s must be an object: %s", dsName, string(body))
	rawRows, ok := root["events"].([]any)
	require.True(t, ok, "response data.%s.events must be an array: %s", dsName, string(body))

	got := make([]map[string]any, 0, len(rawRows))
	for _, raw := range rawRows {
		row, ok := raw.(map[string]any)
		require.True(t, ok, "event row must be an object: %#v", raw)
		got = append(got, row)
	}
	assert.Equal(t, expected, got)
}

func geometryReadExpected(name string, point xyPoint, x, y float64) map[string]any {
	return map[string]any{
		"name":                        name,
		"geom_point_native":           geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_point_native_4326":      geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_line_wkt":               geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_line_wkt_4326":          geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_polygon_geojson":        geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_polygon_hugr_geojson":   geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_polygon_plain_geojson":  geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_polygon_geojson_struct": geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_point_wkb":              geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_point_hexwkb":           geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_line_native":            geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_line_wkb":               geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_polygon_native":         geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_polygon_wkb":            geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_multipoint_native":      geoJSONGeometry("MultiPoint", pointCoordinates(multiPoints(x, y))),
		"geom_multipoint_wkb":         geoJSONGeometry("MultiPoint", pointCoordinates(multiPoints(x, y))),
		"geom_multiline_native":       geoJSONGeometry("MultiLineString", nestedPointCoordinates(multiLines(x, y))),
		"geom_multiline_wkb":          geoJSONGeometry("MultiLineString", nestedPointCoordinates(multiLines(x, y))),
		"geom_multipolygon_native":    geoJSONGeometry("MultiPolygon", deepPointCoordinates(multiPolygons(x, y))),
		"geom_multipolygon_wkb":       geoJSONGeometry("MultiPolygon", deepPointCoordinates(multiPolygons(x, y))),
	}
}

func geometryBatchRow(namePrefix string, row int) (string, xyPoint) {
	return fmt.Sprintf("%s-%06d", namePrefix, row), xyPoint{
		X: float64(row % 100),
		Y: float64(row / 1000),
	}
}

func appendGeometryTypesRow(t *testing.T, b *array.RecordBuilder, row geometryTypesRow) {
	t.Helper()
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append(row.name)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(row.value)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(row.active)
	appendGeometryValueFields(t, b, row)
}

func appendGeometryValueFields(t *testing.T, b *array.RecordBuilder, row geometryTypesRow) {
	t.Helper()
	x, y := row.shapeOrigin.X, row.shapeOrigin.Y

	appendPoint(recordFieldBuilder(t, b, "geom_point_native").(*array.StructBuilder), row.point)
	appendPoint(recordFieldBuilder(t, b, "geom_point_native_4326").(*array.StructBuilder), row.point)
	recordFieldBuilder(t, b, "geom_line_wkt").(*array.StringBuilder).Append(lineWKT(x, y))
	recordFieldBuilder(t, b, "geom_line_wkt_4326").(*array.StringBuilder).Append(lineWKT(x, y))
	recordFieldBuilder(t, b, "geom_polygon_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	recordFieldBuilder(t, b, "geom_polygon_hugr_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	recordFieldBuilder(t, b, "geom_polygon_plain_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	appendGeoJSONPolygonStruct(t, recordFieldBuilder(t, b, "geom_polygon_geojson_struct"), x, y)

	pointWKB := wkbPoint(t, row.point)
	recordFieldBuilder(t, b, "geom_point_wkb").(*array.BinaryBuilder).Append(pointWKB)
	recordFieldBuilder(t, b, "geom_point_hexwkb").(*array.StringBuilder).Append(strings.ToUpper(hex.EncodeToString(pointWKB)))
	appendPointList(recordFieldBuilder(t, b, "geom_line_native").(*array.ListBuilder), linePoints(x, y))
	recordFieldBuilder(t, b, "geom_line_wkb").(*array.BinaryBuilder).Append(wkbLineString(t, linePoints(x, y)))
	appendPointListList(recordFieldBuilder(t, b, "geom_polygon_native").(*array.ListBuilder), polygonRings(x, y))
	recordFieldBuilder(t, b, "geom_polygon_wkb").(*array.BinaryBuilder).Append(wkbPolygon(t, polygonRings(x, y)))
	appendPointList(recordFieldBuilder(t, b, "geom_multipoint_native").(*array.ListBuilder), multiPoints(x, y))
	recordFieldBuilder(t, b, "geom_multipoint_wkb").(*array.BinaryBuilder).Append(wkbMultiPoint(t, multiPoints(x, y)))
	appendPointListList(recordFieldBuilder(t, b, "geom_multiline_native").(*array.ListBuilder), multiLines(x, y))
	recordFieldBuilder(t, b, "geom_multiline_wkb").(*array.BinaryBuilder).Append(wkbMultiLineString(t, multiLines(x, y)))
	appendPointListListList(recordFieldBuilder(t, b, "geom_multipolygon_native").(*array.ListBuilder), multiPolygons(x, y))
	recordFieldBuilder(t, b, "geom_multipolygon_wkb").(*array.BinaryBuilder).Append(wkbMultiPolygon(t, multiPolygons(x, y)))
}

type xyPoint = ingesttest.Point

var (
	addCoord                            = ingesttest.AddCoord
	appendGeoJSONPolygonStruct          = ingesttest.AppendGeoJSONPolygonStruct
	appendGeoJSONPolygonStructFromRings = ingesttest.AppendGeoJSONPolygonStructFromRings
	appendPoint                         = ingesttest.AppendPoint
	appendPointList                     = ingesttest.AppendPointList
	appendPointListList                 = ingesttest.AppendPointListList
	appendPointListListList             = ingesttest.AppendPointListListList
	compactWKT                          = ingesttest.CompactWKT
	coord                               = ingesttest.Coord
	deepPointCoordinates                = ingesttest.DeepPointCoordinates
	geoJSONGeometry                     = ingesttest.GeoJSONGeometry
	linePoints                          = ingesttest.LinePoints
	lineWKT                             = ingesttest.LineWKT
	multiLines                          = ingesttest.MultiLines
	multiPoints                         = ingesttest.MultiPoints
	multiPolygons                       = ingesttest.MultiPolygons
	nestedPointCoordinates              = ingesttest.NestedPointCoordinates
	pointCoordinate                     = ingesttest.PointCoordinate
	pointCoordinates                    = ingesttest.PointCoordinates
	pointWKT                            = ingesttest.PointWKT
	polygonGeoJSON                      = ingesttest.PolygonGeoJSON
	polygonRings                        = ingesttest.PolygonRings
	realWorldLineWKT                    = ingesttest.RealWorldLineWKT
	realWorldMultiLineWKT               = ingesttest.RealWorldMultiLineWKT
	realWorldMultiPointWKT              = ingesttest.RealWorldMultiPointWKT
	realWorldMultiPolygonWKT            = ingesttest.RealWorldMultiPolygonWKT
	realWorldPointWKT                   = ingesttest.RealWorldPointWKT
	realWorldPolygonGeoJSON             = ingesttest.RealWorldPolygonGeoJSON
	realWorldPolygonWKT                 = ingesttest.RealWorldPolygonWKT
	wkbLineString                       = ingesttest.WKBLineString
	wkbMultiLineString                  = ingesttest.WKBMultiLineString
	wkbMultiPoint                       = ingesttest.WKBMultiPoint
	wkbMultiPolygon                     = ingesttest.WKBMultiPolygon
	wkbPoint                            = ingesttest.WKBPoint
	wkbPolygon                          = ingesttest.WKBPolygon
)
