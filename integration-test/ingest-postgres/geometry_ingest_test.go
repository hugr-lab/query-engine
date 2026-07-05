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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestIngest_HTTP_GeometryTypes(t *testing.T) {
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

func TestIngest_HTTP_RealWorldGeometryTypes(t *testing.T) {
	env := setupEnv(t)
	geom := moscowOSMGeometrySet()

	rec, schema := makeRealWorldGeometryTypesRecord(t, "osm-moscow-kremlin", geom)
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
	assert.Equal(t, int64(1), out.Inserted)
	assert.ElementsMatch(t, geometryTypesColumns(), out.Columns)

	values, srids := scanGeometryValuesWithSRID(t, env.pgConn.QueryRow(fmt.Sprintf(`
			SELECT %s
		FROM events
		WHERE name = 'osm-moscow-kremlin'
	`, geometrySelectList(true))))
	assert.Equal(t, realWorldGeometryExpected(geom), values)
	assert.Equal(t, geometrySRIDExpected(), srids)
}

// TestIngest_HTTP_NaturalEarthGeometryTypes_Postgres validates the committed
// real-world fixture before ingesting it.
//
// Human fixture check with jq:
//
//	jq '.features | length' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson
//	jq '.features[].geometry.type' integration-test/ingest/testdata/real-world/natural-earth/natural_earth_geometry.geojson | sort | uniq -c
//
// This bulk test verifies that all 10k rows were inserted and that non-null
// target column counts match fixture role counts. It also checks one real
// geometry per role with ST_Equals; row-by-row geometry value checks live in
// TestIngest_HTTP_NaturalEarthGeometryValues_Postgres below.

func TestIngest_HTTP_GeometryTypes_ReadThroughHugr(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []geometryTypesRow{
		{name: "geo-read-a", value: 1, active: true, point: xyPoint{X: 30.5, Y: 50.25}, shapeOrigin: xyPoint{X: 0, Y: 0}},
		{name: "geo-read-b", value: 2, active: true, point: xyPoint{X: -73.935242, Y: 40.730610}, shapeOrigin: xyPoint{X: 1, Y: 1}},
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

	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { like: "geo-read-%" } }`, []map[string]any{
		geometryReadExpected("geo-read-a", xyPoint{X: 30.5, Y: 50.25}, 0, 0),
		geometryReadExpected("geo-read-b", xyPoint{X: -73.935242, Y: 40.730610}, 1, 1),
	})
}

func TestIngest_HTTP_GeometryTypes_Bulk50k(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "pg-geo-bulk"
	)
	schema := geometryTypesSchema()
	pool := memory.NewGoAllocator()

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		w := ipc.NewWriter(pw, ipc.WithSchema(schema))
		var streamErr error
		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			rec := buildGeometryTypesBatch(t, pool, schema, batchIdx, rowsPerBatch, namePrefix)
			if err := w.Write(rec); err != nil {
				streamErr = fmt.Errorf("write geometry batch %d: %w", batchIdx, err)
				rec.Release()
				break
			}
			rec.Release()
		}
		if err := w.Close(); err != nil && streamErr == nil {
			streamErr = fmt.Errorf("close arrow writer: %w", err)
		}
		_ = pw.CloseWithError(streamErr)
		writeErr <- streamErr
	}()

	start := time.Now()
	resp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", pr)
	werr := <-writeErr
	require.NoError(t, werr, "writer goroutine failed")
	require.NoError(t, postErr)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

	var out hugrclient.IngestResult
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(totalRows), out.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE name LIKE 'pg-geo-bulk-%'").Scan(&count))
	assert.Equal(t, totalRows, count)

	values, srids := scanGeometryValuesWithSRID(t, env.pgConn.QueryRow(fmt.Sprintf(`
			SELECT %s
		FROM events
		WHERE name = 'pg-geo-bulk-049999'
		`, geometrySelectList(true))))
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)
	assert.Equal(t, geometrySRIDExpected(), srids)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { eq: "pg-geo-bulk-049999" } }`, []map[string]any{
		geometryReadExpected("pg-geo-bulk-049999", xyPoint{X: 99, Y: 49}, 99, 49),
	})

	elapsed := time.Since(start)
	t.Logf("geometry bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// lazyEventsReader is an array.RecordReader that generates events-table
// RecordBatches on demand. This is the shape of a real-world Arrow producer
// (parquet scanner, CDC tap, kafka batcher) — the whole stream is never
// materialised in memory beyond the batch currently being consumed.
