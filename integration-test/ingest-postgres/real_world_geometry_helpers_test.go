//go:build duckdb_arrow

package ingest_postgres_test

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/integration-test/internal/ingesttest"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type naturalEarthFeature = ingesttest.RealWorldFeature
type naturalEarthGeometry = ingesttest.RealWorldGeometry
type naturalEarthExpectedCounts = ingesttest.RealWorldExpectedCounts
type naturalEarthSample = ingesttest.RealWorldSample

var (
	firstNaturalEarthFeatureByGeometryKind     = ingesttest.FirstFeatureByGeometryKind
	firstNaturalEarthFeatureWithGeometryKind   = ingesttest.FirstFeatureWithGeometryKind
	loadNaturalEarthGeometryFeatures           = ingesttest.LoadNaturalEarthGeometryFeatures
	loadOSMGeometryFeatures                    = ingesttest.LoadOSMGeometryFeatures
	naturalEarthColumnsForGeometryKind         = ingesttest.ColumnsForGeometryKind
	naturalEarthCountedGeometryColumns         = ingesttest.CountedGeometryColumns
	naturalEarthExpectedColumnCounts           = ingesttest.ExpectedColumnCounts
	naturalEarthExpectedDBGeometryTypeCounts   = ingesttest.ExpectedDBGeometryTypeCounts
	naturalEarthExpectedGeometryByColumn       = ingesttest.ExpectedGeometryByColumn
	naturalEarthExpectedFeatureCount           = ingesttest.ExpectedFeatureCount
	naturalEarthFeaturesByGeometryKind         = ingesttest.FeaturesByGeometryKind
	naturalEarthGeometryColumnsForGeometryKind = ingesttest.GeometryColumnsForGeometryKind
	naturalEarthGeometryExpectedCounts         = ingesttest.NaturalEarthGeometryExpectedCounts
	naturalEarthGeometryValuesExpectedCounts   = ingesttest.NaturalEarthGeometryValuesExpectedCounts
	naturalEarthLineStringFromGeometry         = ingesttest.LineStringFromGeometry
	naturalEarthMultiLineStringFromGeometry    = ingesttest.MultiLineStringFromGeometry
	naturalEarthMultiPointFromGeometry         = ingesttest.MultiPointFromGeometry
	naturalEarthMultiPolygonFromGeometry       = ingesttest.MultiPolygonFromGeometry
	naturalEarthPointFromGeometry              = ingesttest.PointFromGeometry
	naturalEarthPolygonFromGeometry            = ingesttest.PolygonFromGeometry
	naturalEarthGeometryKindOrder              = ingesttest.GeometryKindOrder
	osmGeometryExpectedCounts                  = ingesttest.OSMGeometryExpectedCounts
	osmGeometryValuesExpectedCounts            = ingesttest.OSMGeometryValuesExpectedCounts
	realWorldRowName                           = ingesttest.RowName
)

func naturalEarthRowName(row int, feature naturalEarthFeature) string {
	return realWorldRowName("natural-earth", row, feature)
}

func makeRealWorldGeoJSONStructNullRecord(t *testing.T, namePrefix string, polygon [][]xyPoint) (arrow.RecordBatch, string) {
	t.Helper()

	geoJSONStructType := arrow.StructOf(
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "coordinates", Type: arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Float64))), Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "geom_geojson_struct", Type: geoJSONStructType, Nullable: true},
	}, nil)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	appendGeoJSONStructRow(t, b, namePrefix+"-valid", 1, true)
	appendGeoJSONPolygonStructFromRings(t, recordFieldBuilder(t, b, "geom_geojson_struct"), polygon)

	appendGeoJSONStructRow(t, b, namePrefix+"-null", 2, true)
	recordFieldBuilder(t, b, "geom_geojson_struct").(*array.StructBuilder).AppendNull()

	return b.NewRecordBatch(), realWorldPolygonWKT(polygon)
}

func appendGeoJSONStructRow(t *testing.T, b *array.RecordBuilder, name string, value float64, active bool) {
	t.Helper()

	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append(name)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(value)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(active)
}

func assertGeoJSONStructNullRows(t *testing.T, db *sql.DB, namePrefix, expectedWKT string) {
	t.Helper()

	rows, err := db.Query(`
		SELECT name, geom_geojson_struct IS NULL, COALESCE(ST_AsText(geom_geojson_struct), '')
		FROM events
		WHERE name LIKE $1
		ORDER BY name
	`, namePrefix+"-%")
	require.NoError(t, err)
	defer rows.Close()

	got := make(map[string]struct {
		isNull bool
		wkt    string
	})
	for rows.Next() {
		var name string
		var isNull bool
		var wkt string
		require.NoError(t, rows.Scan(&name, &isNull, &wkt))
		got[name] = struct {
			isNull bool
			wkt    string
		}{isNull: isNull, wkt: compactWKT(wkt)}
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 2)
	assert.Equal(t, struct {
		isNull bool
		wkt    string
	}{isNull: true}, got[namePrefix+"-null"])
	assert.Equal(t, struct {
		isNull bool
		wkt    string
	}{wkt: compactWKT(expectedWKT)}, got[namePrefix+"-valid"])
}

func ingestNaturalEarthGeometryFeatures(t *testing.T, env *ingestEnv, features []naturalEarthFeature) int {
	t.Helper()

	return ingestRealWorldGeometryFeatures(t, env, "natural-earth", features)
}

func ingestRealWorldGeometryFeatures(t *testing.T, env *ingestEnv, rowPrefix string, features []naturalEarthFeature) int {
	t.Helper()

	featuresByGeometryKind := naturalEarthFeaturesByGeometryKind(features)
	inserted := 0
	for _, geometryKind := range naturalEarthGeometryKindOrder() {
		geometryKindFeatures := featuresByGeometryKind[geometryKind]
		if len(geometryKindFeatures) == 0 {
			continue
		}
		rec, schema := makeNaturalEarthGeometryTypesRecord(t, rowPrefix, geometryKind, geometryKindFeatures)

		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		require.NoError(t, w.Write(rec))
		require.NoError(t, w.Close())
		rec.Release()

		resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dsName+".events",
			"application/vnd.apache.arrow.stream", &buf)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

		var out hugrclient.IngestResult
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, int64(len(geometryKindFeatures)), out.Inserted)
		assert.ElementsMatch(t, naturalEarthColumnsForGeometryKind(geometryKind), out.Columns)
		inserted += len(geometryKindFeatures)
	}
	return inserted
}

func naturalEarthGeometryTypesSchema(t *testing.T, geometryKind string) *arrow.Schema {
	t.Helper()

	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}

	geometryFields := make(map[string]arrow.Field)
	for _, field := range geometryArrowFields() {
		geometryFields[field.Name] = field
	}
	for _, name := range naturalEarthGeometryColumnsForGeometryKind(geometryKind) {
		field, ok := geometryFields[name]
		require.Truef(t, ok, "geometry field %q must exist", name)
		fields = append(fields, field)
	}
	return arrow.NewSchema(fields, nil)
}

func makeNaturalEarthGeometryTypesRecord(t *testing.T, rowPrefix, geometryKind string, features []naturalEarthFeature) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()

	schema := naturalEarthGeometryTypesSchema(t, geometryKind)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i, feature := range features {
		appendNaturalEarthGeometryTypesRow(t, b, rowPrefix, i, feature)
	}

	return b.NewRecordBatch(), schema
}

func appendNaturalEarthGeometryTypesRow(t *testing.T, b *array.RecordBuilder, rowPrefix string, row int, feature naturalEarthFeature) {
	t.Helper()

	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append(realWorldRowName(rowPrefix, row, feature))
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(float64(row))
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(row%2 == 0)

	switch feature.Properties.GeometryKind {
	case "point":
		appendNaturalEarthPointFields(t, b, naturalEarthPointFromGeometry(t, feature.Geometry))
	case "line":
		appendNaturalEarthLineFields(t, b, naturalEarthLineStringFromGeometry(t, feature.Geometry))
	case "polygon":
		appendNaturalEarthPolygonFields(t, b, naturalEarthPolygonFromGeometry(t, feature.Geometry))
	case "multipoint":
		appendNaturalEarthMultiPointFields(t, b, naturalEarthMultiPointFromGeometry(t, feature.Geometry))
	case "multiline":
		appendNaturalEarthMultiLineFields(t, b, naturalEarthMultiLineStringFromGeometry(t, feature.Geometry))
	case "multipolygon":
		appendNaturalEarthMultiPolygonFields(t, b, naturalEarthMultiPolygonFromGeometry(t, feature.Geometry))
	default:
		t.Fatalf("unsupported Natural Earth geometry kind %q", feature.Properties.GeometryKind)
	}
}

func appendNaturalEarthPointFields(t *testing.T, b *array.RecordBuilder, point xyPoint) {
	t.Helper()

	wkbPoint, err := wkb.Marshal(orb.Point{point.X, point.Y})
	require.NoError(t, err)

	appendPoint(recordFieldBuilder(t, b, "geom").(*array.StructBuilder), point)
	recordFieldBuilder(t, b, "geom_wkb").(*array.BinaryBuilder).Append(wkbPoint)
	recordFieldBuilder(t, b, "geom_hexwkb").(*array.StringBuilder).Append(strings.ToUpper(hex.EncodeToString(wkbPoint)))
}

func appendNaturalEarthLineFields(t *testing.T, b *array.RecordBuilder, line []xyPoint) {
	t.Helper()

	recordFieldBuilder(t, b, "geom_wkt").(*array.StringBuilder).Append(realWorldLineWKT(line))
	appendPointList(recordFieldBuilder(t, b, "geom_line").(*array.ListBuilder), line)
}

func appendNaturalEarthPolygonFields(t *testing.T, b *array.RecordBuilder, polygon [][]xyPoint) {
	t.Helper()

	geoJSON := realWorldPolygonGeoJSON(polygon)
	recordFieldBuilder(t, b, "geom_geojson").(*array.StringBuilder).Append(geoJSON)
	recordFieldBuilder(t, b, "geom_hugr_geojson").(*array.StringBuilder).Append(geoJSON)
	recordFieldBuilder(t, b, "geom_plain_geojson").(*array.StringBuilder).Append(geoJSON)
	appendGeoJSONPolygonStructFromRings(t, recordFieldBuilder(t, b, "geom_geojson_struct"), polygon)
	appendPointListList(recordFieldBuilder(t, b, "geom_polygon_native").(*array.ListBuilder), polygon)
}

func appendNaturalEarthMultiPointFields(t *testing.T, b *array.RecordBuilder, points []xyPoint) {
	t.Helper()

	appendPointList(recordFieldBuilder(t, b, "geom_multipoint").(*array.ListBuilder), points)
}

func appendNaturalEarthMultiLineFields(t *testing.T, b *array.RecordBuilder, lines [][]xyPoint) {
	t.Helper()

	appendPointListList(recordFieldBuilder(t, b, "geom_multiline").(*array.ListBuilder), lines)
}

func appendNaturalEarthMultiPolygonFields(t *testing.T, b *array.RecordBuilder, polygons [][][]xyPoint) {
	t.Helper()

	appendPointListListList(recordFieldBuilder(t, b, "geom_multipolygon").(*array.ListBuilder), polygons)
}

func assertNaturalEarthGeometryCounts(t *testing.T, db *sql.DB, geometryKindCounts map[string]int) {
	t.Helper()

	assertRealWorldGeometryCounts(t, db, "natural-earth", geometryKindCounts)
}

func assertRealWorldGeometryCounts(t *testing.T, db *sql.DB, rowPrefix string, geometryKindCounts map[string]int) {
	t.Helper()

	expected := naturalEarthExpectedColumnCounts(geometryKindCounts)
	columns := naturalEarthCountedGeometryColumns()
	selects := []string{"COUNT(*)"}
	for _, col := range columns {
		selects = append(selects, "COUNT("+col+")")
	}

	row := db.QueryRow(fmt.Sprintf(`
		SELECT %s
		FROM events
		WHERE name LIKE '%s-%%'
	`, strings.Join(selects, ", "), rowPrefix))

	total := 0
	values := make([]int, len(columns))
	args := []any{&total}
	for i := range values {
		args = append(args, &values[i])
	}
	require.NoError(t, row.Scan(args...))
	assert.Equal(t, naturalEarthExpectedFeatureCount(geometryKindCounts), total)
	for i, col := range columns {
		assert.Equalf(t, expected[col], values[i], "non-null count for %s", col)
	}
}

func assertNaturalEarthDBGeometryTypeCounts(t *testing.T, db *sql.DB, geometryKindCounts map[string]int) {
	t.Helper()

	assertRealWorldDBGeometryTypeCounts(t, db, "natural-earth", geometryKindCounts)
}

func assertRealWorldDBGeometryTypeCounts(t *testing.T, db *sql.DB, rowPrefix string, geometryKindCounts map[string]int) {
	t.Helper()

	expected := naturalEarthExpectedDBGeometryTypeCounts(geometryKindCounts)
	selects := make([]string, 0, len(naturalEarthCountedGeometryColumns()))
	for _, col := range naturalEarthCountedGeometryColumns() {
		selects = append(selects, fmt.Sprintf("SELECT GeometryType(%s) AS geometry_type FROM events WHERE name LIKE '%s-%%' AND %s IS NOT NULL", col, rowPrefix, col))
	}
	rows, err := db.Query(fmt.Sprintf(`
		SELECT geometry_type, COUNT(*)
		FROM (%s) geometries
		GROUP BY geometry_type
	`, strings.Join(selects, "\nUNION ALL\n")))
	require.NoError(t, err)
	defer rows.Close()

	got := make(map[string]int)
	for rows.Next() {
		var typ string
		var count int
		require.NoError(t, rows.Scan(&typ, &count))
		got[typ] = count
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, expected, got)
}

func assertNaturalEarthGeometrySamples(t *testing.T, db *sql.DB, features []naturalEarthFeature) {
	t.Helper()

	assertRealWorldGeometrySamples(t, db, "natural-earth", features)
}

func assertRealWorldGeometrySamples(t *testing.T, db *sql.DB, rowPrefix string, features []naturalEarthFeature) {
	t.Helper()

	samples := firstNaturalEarthFeatureByGeometryKind(t, features)

	for _, geometryKind := range naturalEarthGeometryKindOrder() {
		assertRealWorldFeatureEquals(t, db, rowPrefix, samples[geometryKind])
	}
}

func assertNaturalEarthGeometryValues(t *testing.T, db *sql.DB, features []naturalEarthFeature) {
	t.Helper()

	assertRealWorldGeometryValues(t, db, "natural-earth", features)
}

func assertRealWorldGeometryValues(t *testing.T, db *sql.DB, rowPrefix string, features []naturalEarthFeature) {
	t.Helper()

	geometryKindRows := make(map[string]int)
	for _, feature := range features {
		geometryKind := feature.Properties.GeometryKind
		row := geometryKindRows[geometryKind]
		geometryKindRows[geometryKind]++
		assertRealWorldFeatureEquals(t, db, rowPrefix, naturalEarthSample{Row: row, Feature: feature})
	}
}

func assertNaturalEarthFeatureEquals(t *testing.T, db *sql.DB, sample naturalEarthSample) {
	t.Helper()

	assertRealWorldFeatureEquals(t, db, "natural-earth", sample)
}

func assertRealWorldFeatureEquals(t *testing.T, db *sql.DB, rowPrefix string, sample naturalEarthSample) {
	t.Helper()

	assertNaturalEarthColumnValuesEqual(t, db, rowPrefix, sample, naturalEarthExpectedGeometryByColumn(t, sample.Feature))
}

func assertNaturalEarthColumnValuesEqual(t *testing.T, db *sql.DB, rowPrefix string, sample naturalEarthSample, expectedByColumn map[string]string) {
	t.Helper()

	columns := make([]string, 0, len(expectedByColumn))
	for col := range expectedByColumn {
		columns = append(columns, col)
	}

	selects := make([]string, 0, len(columns))
	args := make([]any, 0, len(columns)+1)
	for _, col := range columns {
		args = append(args, expectedByColumn[col])
		selects = append(selects, fmt.Sprintf("ST_Equals(%s, ST_GeomFromText($%d))", col, len(args)))
	}
	args = append(args, realWorldRowName(rowPrefix, sample.Row, sample.Feature))
	whereParam := len(args)

	row := db.QueryRow(fmt.Sprintf(`
		SELECT %s
		FROM events
		WHERE name = $%d
	`, strings.Join(selects, ", "), whereParam), args...)

	matches := make([]bool, len(columns))
	scanArgs := make([]any, 0, len(columns))
	for i := range matches {
		scanArgs = append(scanArgs, &matches[i])
	}
	require.NoError(t, row.Scan(scanArgs...))
	for i, col := range columns {
		assert.Truef(t, matches[i], "sample %s should match fixture geometry for %s", realWorldRowName(rowPrefix, sample.Row, sample.Feature), col)
	}
}
