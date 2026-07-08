//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngest_HTTP_NaturalEarthGeometry10kRecordsFixedSizeList_DuckDB
// validates the committed real-world fixture before ingesting it as one Arrow
// stream whose native GeoArrow coordinates use FixedSizeList<Float64>[2].
//
// Human fixture check with jq:
//
//	jq '.features | length' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson
//	jq '.features[].geometry.type' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson | sort | uniq -c
//
// This bulk test verifies that all 10k rows were inserted and that non-null
// target column counts match fixture geometry kind counts. It also checks one real
// geometry per kind with ST_Equals; row-by-row geometry value checks live in
// TestIngest_HTTP_NaturalEarthGeometryFixedSizeList_DuckDB below.
func TestIngest_HTTP_NaturalEarthGeometry10kRecordsFixedSizeList_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry.geojson",
		naturalEarthGeometryExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFixedSizeListSingleStream(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, geometryKindCounts)
	assertNaturalEarthGeometrySamples(t, ro, features)
}

func TestIngest_HTTP_NaturalEarthGeometry10kRecordsStruct_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry.geojson",
		naturalEarthGeometryExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometrySingleStream(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, geometryKindCounts)
	assertNaturalEarthGeometrySamples(t, ro, features)
}

func TestIngest_HTTP_NaturalEarthGeometryFixedSizeList_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFixedSizeListSingleStream(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, geometryKindCounts)
	assertNaturalEarthGeometryValues(t, ro, features)
}

func TestIngest_HTTP_NaturalEarthGeometryStruct_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometrySingleStream(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, geometryKindCounts)
	assertNaturalEarthGeometryValues(t, ro, features)
}

// TestIngest_HTTP_OSMGeometry10kRecords_DuckDB validates the committed OSM fixture
// before ingesting it.
//
// Human fixture check with jq:
//
//	jq '.features | length' integration-test/ingest/testdata/real-world/osm/osm_geometry.geojson
//	jq '.features[].geometry.type' integration-test/ingest/testdata/real-world/osm/osm_geometry.geojson | sort | uniq -c
//
// The fixture is a committed OpenStreetMap/Overpass extract; MultiPoint,
// MultiLineString and MultiPolygon rows are derived from real OSM nodes/ways
// so the ingest path still covers every geometry representation we support.
func TestIngest_HTTP_OSMGeometry10kRecords_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadOSMGeometryFeatures(t,
		"osm_geometry.geojson",
		osmGeometryExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertRealWorldGeometryCounts(t, ro, "osm", geometryKindCounts)
	assertRealWorldDBGeometryTypeCounts(t, ro, "osm", geometryKindCounts)
	assertRealWorldGeometrySamples(t, ro, "osm", features)
}

func TestIngest_HTTP_OSMGeometry_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertRealWorldGeometryCounts(t, ro, "osm", geometryKindCounts)
	assertRealWorldDBGeometryTypeCounts(t, ro, "osm", geometryKindCounts)
	assertRealWorldGeometryValues(t, ro, "osm", features)
}

func TestIngest_HTTP_NaturalEarthGeoJSONStructNull_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	feature := firstRealWorldFeatureWithGeometryKind(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "natural-earth-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_polygon_geojson_struct"}, res.Columns)

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)
	assertGeoJSONStructNullRows(t, ro, "natural-earth-geojson-struct-null", expectedWKT)
}

func TestIngest_HTTP_OSMGeoJSONStructNull_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	feature := firstRealWorldFeatureWithGeometryKind(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "osm-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_polygon_geojson_struct"}, res.Columns)

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)
	assertGeoJSONStructNullRows(t, ro, "osm-geojson-struct-null", expectedWKT)
}
