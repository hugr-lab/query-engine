//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngest_HTTP_NaturalEarthGeometry10kRecords_Postgres validates the committed
// real-world fixture before ingesting it.
//
// Human fixture check with jq:
//
//	jq '.features | length' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson
//	jq '.features[].geometry.type' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson | sort | uniq -c
//
// This bulk test verifies that all 10k rows were inserted and that non-null
// target column counts match fixture geometry kind counts. It also checks one real
// geometry per kind with ST_Equals; row-by-row geometry value checks live in
// TestIngest_HTTP_NaturalEarthGeometryValues_Postgres below.
func TestIngest_HTTP_NaturalEarthGeometry10kRecords_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry.geojson",
		naturalEarthGeometryExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	assertNaturalEarthGeometryCounts(t, env.pgConn, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, env.pgConn, geometryKindCounts)
	assertNaturalEarthGeometrySamples(t, env.pgConn, features)
}

func TestIngest_HTTP_NaturalEarthGeometry_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	inserted := ingestNaturalEarthGeometryFeatures(t, env, features)
	assert.Equal(t, len(features), inserted)

	assertNaturalEarthGeometryCounts(t, env.pgConn, geometryKindCounts)
	assertNaturalEarthDBGeometryTypeCounts(t, env.pgConn, geometryKindCounts)
	assertNaturalEarthGeometryValues(t, env.pgConn, features)
}

// TestIngest_HTTP_OSMGeometry10kRecords_Postgres validates the committed OSM fixture
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
func TestIngest_HTTP_OSMGeometry10kRecords_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadOSMGeometryFeatures(t,
		"osm_geometry.geojson",
		osmGeometryExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	assertRealWorldGeometryCounts(t, env.pgConn, "osm", geometryKindCounts)
	assertRealWorldDBGeometryTypeCounts(t, env.pgConn, "osm", geometryKindCounts)
	assertRealWorldGeometrySamples(t, env.pgConn, "osm", features)
}

func TestIngest_HTTP_OSMGeometry_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, geometryKindCounts := loadOSMGeometryFeatures(t,
		"osm_geometry_values.geojson",
		osmGeometryValuesExpectedCounts,
	)
	inserted := ingestRealWorldGeometryFeatures(t, env, "osm", features)
	assert.Equal(t, len(features), inserted)

	assertRealWorldGeometryCounts(t, env.pgConn, "osm", geometryKindCounts)
	assertRealWorldDBGeometryTypeCounts(t, env.pgConn, "osm", geometryKindCounts)
	assertRealWorldGeometryValues(t, env.pgConn, "osm", features)
}

func TestIngest_HTTP_NaturalEarthGeoJSONStructNull_Postgres(t *testing.T) {
	env := setupEnv(t)
	features, _ := loadNaturalEarthGeometryFeatures(t,
		"natural_earth_geometry_values.geojson",
		naturalEarthGeometryValuesExpectedCounts,
	)
	feature := firstNaturalEarthFeatureWithGeometryKind(t, features, "polygon")
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
	feature := firstNaturalEarthFeatureWithGeometryKind(t, features, "polygon")
	polygon := naturalEarthPolygonFromGeometry(t, feature.Geometry)
	rec, expectedWKT := makeRealWorldGeoJSONStructNullRecord(t, "osm-geojson-struct-null", polygon)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dsName+".events", rec)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "geom_geojson_struct"}, res.Columns)

	assertGeoJSONStructNullRows(t, env.pgConn, "osm-geojson-struct-null", expectedWKT)
}
