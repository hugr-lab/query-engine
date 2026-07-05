//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest_HTTP_NaturalEarthGeometryTypes_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry.geojson",
		naturalEarthGeometryExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	assertNaturalEarthGeometryCounts(t, env.pgConn, roleCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, env.pgConn, roleCounts)
	assertNaturalEarthGeometrySamples(t, env.pgConn, features)
}

func TestIngest_HTTP_NaturalEarthGeometryValues_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	assertNaturalEarthGeometryCounts(t, env.pgConn, roleCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, env.pgConn, roleCounts)
	assertNaturalEarthGeometryValues(t, env.pgConn, features)
}

// TestIngest_HTTP_OSMGeometryTypes_Postgres validates the committed OSM fixture
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
func TestIngest_HTTP_OSMGeometryTypes_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadOSMGeometryFeatures(t,
		"osm_geometry.geojson",
		osmGeometryExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	assertRealWorldGeometryCounts(t, env.pgConn, "osm", roleCounts)
	assertRealWorldDBGeometryTypeCounts(t, env.pgConn, "osm", roleCounts)
	assertRealWorldGeometrySamples(t, env.pgConn, "osm", features)
}

func TestIngest_HTTP_OSMGeometryValues_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, roleCounts := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	assertRealWorldGeometryCounts(t, env.pgConn, "osm", roleCounts)
	assertRealWorldDBGeometryTypeCounts(t, env.pgConn, "osm", roleCounts)
	assertRealWorldGeometryValues(t, env.pgConn, "osm", features)
}

func TestIngest_HTTP_NaturalEarthGeoJSONStructNull_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	feature := firstNaturalEarthFeatureWithRole(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "natural-earth-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dsName+".events", rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_geojson_struct"}, res.Columns)

	assertGeoJSONStructNullRows(t, env.pgConn, "natural-earth-geojson-struct-null", expectedWKT)
}

func TestIngest_HTTP_OSMGeoJSONStructNull_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	feature := firstNaturalEarthFeatureWithRole(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "osm-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dsName+".events", rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_geojson_struct"}, res.Columns)

	assertGeoJSONStructNullRows(t, env.pgConn, "osm-geojson-struct-null", expectedWKT)
}
