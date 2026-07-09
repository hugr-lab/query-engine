//go:build duckdb_arrow

package ingest_postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngest_Postgres_GeometryEdgeCases verifies that the native
// DuckDB GEOMETRY -> PostGIS bridge faithfully carries geometries that the
// existing suite never exercised: SQL NULL, 3D (Z) coordinates, EMPTY
// geometries and a mixed GEOMETRYCOLLECTION. The target column is a bare
// `geometry` (no typmod) so PostGIS accepts any type/dimension and the
// assertions reflect exactly what crossed the bridge — not what a typmod
// coerced. Geometry is sent as geoarrow.wkt so DuckDB staging normalises it to
// a canonical GEOMETRY via ST_GeomFromText before the bridge writes it out.
func TestIngest_Postgres_GeometryEdgeCases(t *testing.T) {
	env := setupEnv(t)

	_, err := env.pgConn.ExecContext(context.Background(),
		"TRUNCATE TABLE geom_edge RESTART IDENTITY")
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{
			Name:     "geom",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"}),
		},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	names := recordFieldBuilder(t, b, "name").(*array.StringBuilder)
	geoms := recordFieldBuilder(t, b, "geom").(*array.StringBuilder)

	names.Append("a_null")
	geoms.AppendNull()
	names.Append("b_point_z")
	geoms.Append("POINT Z (1 2 3)")
	names.Append("c_empty_point")
	geoms.Append("POINT EMPTY")
	names.Append("d_geomcollection")
	geoms.Append("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))")

	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), "pg_ingest.geom_edge", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(4), res.Inserted)

	type edgeRow struct {
		isNull  bool
		gtype   string
		zmflag  int
		isEmpty bool
		numGeom int
	}
	rows, err := env.pgConn.Query(`
		SELECT name,
			geom IS NULL,
			COALESCE(GeometryType(geom), ''),
			COALESCE(ST_Zmflag(geom), -1),
			COALESCE(ST_IsEmpty(geom), false),
			COALESCE(ST_NumGeometries(geom), 0)
		FROM geom_edge ORDER BY name`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]edgeRow{}
	for rows.Next() {
		var name string
		var r edgeRow
		require.NoError(t, rows.Scan(&name, &r.isNull, &r.gtype, &r.zmflag, &r.isEmpty, &r.numGeom))
		got[name] = r
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 4)

	// NULL geometry must round-trip as SQL NULL.
	assert.True(t, got["a_null"].isNull, "NULL geometry must stay NULL through the native bridge")

	// 3D point: the Z dimension must survive DuckDB GEOMETRY -> PostGIS.
	assert.False(t, got["b_point_z"].isNull)
	assert.Equal(t, "POINT", got["b_point_z"].gtype)
	assert.Equal(t, 2, got["b_point_z"].zmflag, "ST_Zmflag 2 == XYZ (Z present, no M)")

	// EMPTY geometry must remain an empty geometry of the right type.
	assert.Equal(t, "POINT", got["c_empty_point"].gtype)
	assert.True(t, got["c_empty_point"].isEmpty, "POINT EMPTY must survive as empty")

	// Mixed GeometryCollection must keep its member count.
	assert.Equal(t, "GEOMETRYCOLLECTION", got["d_geomcollection"].gtype)
	assert.Equal(t, 2, got["d_geomcollection"].numGeom)

	// Exact coordinates for the 3D point.
	var x, y, z float64
	require.NoError(t, env.pgConn.QueryRow(
		"SELECT ST_X(geom), ST_Y(geom), ST_Z(geom) FROM geom_edge WHERE name = 'b_point_z'",
	).Scan(&x, &y, &z))
	assert.Equal(t, [3]float64{1, 2, 3}, [3]float64{x, y, z})
}

func TestIngest_HTTP_GeometryPoints(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []geometryTypesRow{
		{name: "geo-a", value: 1, active: true, point: xyPoint{X: 30.5, Y: 50.25}, shapeOrigin: xyPoint{X: 0, Y: 0}},
		{name: "geo-b", value: 2, active: true, point: xyPoint{X: -73.935242, Y: 40.730610}, shapeOrigin: xyPoint{X: 1, Y: 1}},
	})
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

	var out hugrclient.IngestResult
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(2), out.Inserted)
	assert.ElementsMatch(t, geometryTypesColumns(), out.Columns)

	rows, err := env.pgConn.Query(fmt.Sprintf(`
			SELECT name,
				%s
		FROM events
		WHERE name LIKE 'geo-%%'
		ORDER BY name
	`, geometrySelectList(true)))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	gotSRID := map[string][]int{}
	for rows.Next() {
		name, values, srids := scanNamedGeometryValuesWithSRID(t, rows)
		got[name] = values
		gotSRID[name] = srids
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
	assert.Equal(t, map[string][]int{
		"geo-a": geometrySRIDExpected(),
		"geo-b": geometrySRIDExpected(),
	}, gotSRID)
}
