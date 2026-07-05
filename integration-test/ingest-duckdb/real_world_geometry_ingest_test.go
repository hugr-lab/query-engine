//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest_HTTP_NaturalEarthGeometryTypes_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry.geojson",
		naturalEarthGeometryExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, roleCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, roleCounts)
	assertNaturalEarthGeometrySamples(t, ro, features)
}

func TestIngest_HTTP_NaturalEarthGeometryValues_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertNaturalEarthGeometryCounts(t, ro, roleCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, ro, roleCounts)
	assertNaturalEarthGeometryValues(t, ro, features)
}

// TestIngest_HTTP_OSMGeometryTypes_DuckDB validates the committed OSM fixture
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
func TestIngest_HTTP_OSMGeometryTypes_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadOSMGeometryFeatures(t,
		"osm_geometry.geojson",
		osmGeometryExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertRealWorldGeometryCounts(t, ro, "osm", roleCounts)
	assertRealWorldDBGeometryTypeCounts(t, ro, "osm", roleCounts)
	assertRealWorldGeometrySamples(t, ro, "osm", features)
}

func TestIngest_HTTP_OSMGeometryValues_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	assertRealWorldGeometryCounts(t, ro, "osm", roleCounts)
	assertRealWorldDBGeometryTypeCounts(t, ro, "osm", roleCounts)
	assertRealWorldGeometryValues(t, ro, "osm", features)
}

func TestIngest_HTTP_NaturalEarthGeoJSONStructNull_DuckDB(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	feature := firstRealWorldFeatureWithRole(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "natural-earth-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_geojson_struct"}, res.Columns)

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
	feature := firstRealWorldFeatureWithRole(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "osm-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_geojson_struct"}, res.Columns)

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)
	assertGeoJSONStructNullRows(t, ro, "osm-geojson-struct-null", expectedWKT)
}
