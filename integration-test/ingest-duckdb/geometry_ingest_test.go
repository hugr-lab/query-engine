//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest_HTTP_GeometryTypes_DuckDB(t *testing.T) {
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

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

	var out hugrclient.IngestResult
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(2), out.Inserted)
	assert.ElementsMatch(t, geometryTypesColumns(), out.Columns)

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)

	rows, err := ro.Query(fmt.Sprintf(`
			SELECT name,
				%s
		FROM events
		WHERE name LIKE 'geo-%%'
		ORDER BY name
	`, geometrySelectList()))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	for rows.Next() {
		name, values := scanNamedGeometryValues(t, rows)
		got[name] = values
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
}

// TestIngest_HTTP_NaturalEarthGeometryTypes_DuckDB validates the committed
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
// TestIngest_HTTP_NaturalEarthGeometryValues_DuckDB below.

func TestIngest_HTTP_GeometryTypes_ReadThroughHugr_DuckDB(t *testing.T) {
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

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
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

func TestIngest_HTTP_GeometryTypes_Bulk50k_DuckDB(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-geo-bulk"
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
	resp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
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

	ro := env.openRO(t)
	defer ro.Close()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events WHERE name LIKE 'dk-geo-bulk-%'").Scan(&count))
	assert.Equal(t, totalRows, count)

	values := scanGeometryValues(t, ro.QueryRow(fmt.Sprintf(`
		SELECT %s
		FROM events
		WHERE name = 'dk-geo-bulk-049999'
	`, geometrySelectList())))
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { eq: "dk-geo-bulk-049999" } }`, []map[string]any{
		geometryReadExpected("dk-geo-bulk-049999", xyPoint{X: 99, Y: 49}, 99, 49),
	})

	elapsed := time.Since(start)
	t.Logf("geometry bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// --- helpers --------------------------------------------------------------
