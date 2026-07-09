//go:build duckdb_arrow

package ingest_postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestIngest_Postgres_RoundTrip(t *testing.T) {
	env := setupEnv(t)

	now := arrow.Timestamp(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC).UnixMicro())
	rec := makeEventsRecord(t,
		[]string{"alpha", "beta", "gamma"},
		[]float64{1.5, 2.5, 3.5},
		[]bool{true, false, true},
		[]string{`{"k":"v"}`, "", `{"x":1}`},
		[]arrow.Timestamp{now, now, now},
	)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "pg_ingest.events", res.DataObject)
	assert.Equal(t, int64(3), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "payload", "created_at"}, res.Columns)

	// Verify by reading directly from postgres.
	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 3, count)

	rows, err := env.pgConn.Query("SELECT name, value, is_active, payload IS NOT NULL FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()
	var (
		gotNames   []string
		gotValues  []float64
		gotActive  []bool
		gotHasJSON []bool
	)
	for rows.Next() {
		var n string
		var v float64
		var a, j bool
		require.NoError(t, rows.Scan(&n, &v, &a, &j))
		gotNames = append(gotNames, n)
		gotValues = append(gotValues, v)
		gotActive = append(gotActive, a)
		gotHasJSON = append(gotHasJSON, j)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, gotNames)
	assert.Equal(t, []float64{1.5, 2.5, 3.5}, gotValues)
	assert.Equal(t, []bool{true, false, true}, gotActive)
	assert.Equal(t, []bool{true, false, true}, gotHasJSON) // beta has NULL payload
}

func TestIngest_Postgres_TwoBatches(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	mk := func(names []string) arrow.RecordBatch {
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		fields := eventsRecordBuildersFor(b)
		fields.names.AppendValues(names, nil)
		vals := make([]float64, len(names))
		for i := range vals {
			vals[i] = float64(i)
		}
		fields.values.AppendValues(vals, nil)
		active := make([]bool, len(names))
		for i := range active {
			active[i] = true
		}
		fields.active.AppendValues(active, nil)
		fields.payloads.AppendNulls(len(names))
		ts := make([]arrow.Timestamp, len(names))
		for i := range ts {
			ts[i] = arrow.Timestamp(time.Now().UTC().UnixMicro())
		}
		fields.createdAt.AppendValues(ts, nil)
		return b.NewRecordBatch()
	}
	rec1 := mk([]string{"a", "b"})
	defer rec1.Release()
	rec2 := mk([]string{"c", "d", "e"})
	defer rec2.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec1, rec2})
	require.NoError(t, err)
	defer rr.Release()

	res, err := env.client.Ingest(context.Background(), "pg_ingest.events", rr)
	require.NoError(t, err)
	assert.Equal(t, int64(5), res.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 5, count)
}

// TestIngest_Postgres_Bulk exercises the typed Go client at real-world scale:
// 50 batches × 1000 rows streamed through array.RecordReader, never
// materialised in memory beyond the current batch. Mirrors the wire-level
// bulk path in TestIngest_HTTP_Direct, but goes through hugrclient.Client.
func TestIngest_Postgres_Bulk(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
	)

	reader := newLazyEventsReader(
		memory.NewGoAllocator(),
		numBatches, rowsPerBatch,
		time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC),
	)
	defer reader.Release()

	start := time.Now()
	res, err := env.client.Ingest(context.Background(), "pg_ingest.events", reader)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(totalRows), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "payload", "created_at"}, res.Columns)

	// Time the COUNT(*) immediately after the POST returns. If the server
	// were lying / writing asynchronously, this query would either be slow
	// (waiting for in-flight writes to land) or return a partial value.
	countStart := time.Now()
	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all rows must be visible the moment POST returns")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

	// Spot-check the first five rows for per-column fidelity through the
	// client → server → DuckDB → postgres-extension → Postgres pipeline.
	rows, err := env.pgConn.Query(`SELECT name, value, is_active, payload IS NULL FROM events ORDER BY value LIMIT 5`)
	require.NoError(t, err)
	defer rows.Close()
	var (
		sampleNames       []string
		sampleValues      []float64
		sampleActive      []bool
		samplePayloadNull []bool
	)
	for rows.Next() {
		var n string
		var v float64
		var a, pn bool
		require.NoError(t, rows.Scan(&n, &v, &a, &pn))
		sampleNames = append(sampleNames, n)
		sampleValues = append(sampleValues, v)
		sampleActive = append(sampleActive, a)
		samplePayloadNull = append(samplePayloadNull, pn)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"evt-000000", "evt-000001", "evt-000002", "evt-000003", "evt-000004"}, sampleNames)
	assert.Equal(t, []float64{0, 0.5, 1.0, 1.5, 2.0}, sampleValues)
	assert.Equal(t, []bool{true, false, true, false, true}, sampleActive)
	// row%5 == 0 ⇒ payload IS NULL; in the first five rows that's just row 0.
	assert.Equal(t, []bool{true, false, false, false, false}, samplePayloadNull)

	var activeCount int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE is_active").Scan(&activeCount))
	assert.Equal(t, totalRows/2, activeCount)

	t.Logf("bulk ingest via Go client: %d rows in %d batches in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_Postgres_Stream covers Client.IngestStream — the low-level API
// that takes a raw Arrow IPC stream as io.Reader. We serialise a buffer
// ourselves and verify it lands in Postgres.
func TestIngest_Postgres_Stream(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues([]string{"s1", "s2"}, nil)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).AppendValues([]float64{10, 20}, nil)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	res, err := env.client.IngestStream(context.Background(), "pg_ingest.events", &buf)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 2, count)
}

// TestIngest_Postgres_Stream_Empty checks that IngestStream rejects nil body
// without sending anything to the server.
func TestIngest_Postgres_Stream_Empty(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestStream(context.Background(), "pg_ingest.events", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "body is nil")

	_, err = env.client.IngestStream(context.Background(), "", bytes.NewReader([]byte{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data_object")
}

func TestIngest_Postgres_UnknownColumn(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "not_a_column", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	recordFieldBuilder(t, b, "not_a_column").(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	_, err := env.client.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_column")

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 0, count, "no rows should have been inserted on validation failure")
}

func TestIngest_Postgres_UnknownDataObject(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	recordFieldBuilder(t, b, "x").(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	_, err := env.client.IngestRecord(context.Background(), "pg_ingest.does_not_exist", rec)
	require.Error(t, err)
}

// TestIngest_HTTP_Direct exercises low-level HTTP behaviour that the typed
// client smoothes over: bad Content-Type, missing data_object, wrong method.
// It writes a small Arrow stream to validate request parsing of /ipc/ingest.
func TestIngest_HTTP_Direct(t *testing.T) {
	env := setupEnv(t)

	// Missing data_object.
	resp, err := http.Post(env.server.URL+"/ipc/ingest", "application/vnd.apache.arrow.stream", bytes.NewReader(nil))
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body=%s", string(b))

	// Wrong method.
	req, _ := http.NewRequest(http.MethodGet, env.server.URL+"/ipc/ingest?data_object=pg_ingest.events", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	// Wrong content type.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"text/plain", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)

	// Body is not a valid Arrow stream.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", bytes.NewReader([]byte("not arrow")))
	require.NoError(t, err)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body=%s", string(b))

	// Happy-path direct POST returning JSON.
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bld := array.NewRecordBuilder(pool, schema)
	recordFieldBuilder(t, bld, "name").(*array.StringBuilder).AppendValues([]string{"direct"}, nil)
	recordFieldBuilder(t, bld, "value").(*array.Float64Builder).AppendValues([]float64{42}, nil)
	recordFieldBuilder(t, bld, "is_active").(*array.BooleanBuilder).AppendValues([]bool{true}, nil)
	rec := bld.NewRecordBatch()
	bld.Release()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out hugrclient.IngestResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	resp.Body.Close()
	assert.Equal(t, int64(1), out.Inserted)
}
