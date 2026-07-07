//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestIngest_DuckDB_RoundTrip(t *testing.T) {
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

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, env.dataObject, res.DataObject)
	assert.Equal(t, int64(3), res.Inserted)
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "payload", "created_at"}, res.Columns)

	// Verify via a fresh READ_ONLY verifier connection (independent of hugr's
	// session). Open a new handle per verification to guarantee a post-write
	// snapshot — see openRO doc.
	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 3, count)

	ro2 := env.openRO(t)
	defer ro2.Close()
	rows, err := ro2.Query("SELECT name, value, is_active, payload IS NOT NULL FROM events ORDER BY name")
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
	assert.Equal(t, []bool{true, false, true}, gotHasJSON)
}

func TestIngest_DuckDB_UnknownColumn(t *testing.T) {
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

	_, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_column")

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 0, count, "no rows should have been inserted on validation failure")
}

func TestIngest_DuckDB_UnknownDataObject(t *testing.T) {
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

	_, err := env.client.IngestRecord(context.Background(), env.dsName+".does_not_exist", rec)
	require.Error(t, err)
}

func TestIngest_DuckDB_TwoBatches(t *testing.T) {
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

	res, err := env.client.Ingest(context.Background(), env.dataObject, rr)
	require.NoError(t, err)
	assert.Equal(t, int64(5), res.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 5, count)
}

// TestIngest_DuckDB_Bulk — 50k rows via the typed Go client + NewLazyReader
// (lazy generation, never materialised), with post-POST COUNT(*) timing
// check against a fresh READ_ONLY verifier.
func TestIngest_DuckDB_Bulk(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-bulk"
	)

	pool := memory.NewGoAllocator()
	schema := eventsArrowSchema()
	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	batchIdx := 0
	reader := hugrclient.NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		if batchIdx >= numBatches {
			return nil, nil
		}
		rec := buildEventsBatch(pool, schema, batchIdx, rowsPerBatch, namePrefix, base)
		batchIdx++
		return rec, nil
	})
	defer reader.Release()

	start := time.Now()
	res, err := env.client.Ingest(context.Background(), env.dataObject, reader)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, int64(totalRows), res.Inserted)

	// post-POST COUNT(*) through a fresh READ_ONLY connection — synchronicity.
	ro := env.openRO(t)
	defer ro.Close()
	countStart := time.Now()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all rows must be visible the moment POST returns")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

	// Spot-check first 5 rows by content.
	ro2 := env.openRO(t)
	defer ro2.Close()
	rows, err := ro2.Query(`SELECT name, value, is_active, payload IS NULL FROM events ORDER BY value LIMIT 5`)
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
	assert.Equal(t, []string{
		namePrefix + "-000000", namePrefix + "-000001", namePrefix + "-000002",
		namePrefix + "-000003", namePrefix + "-000004",
	}, sampleNames)
	assert.Equal(t, []float64{0, 0.5, 1.0, 1.5, 2.0}, sampleValues)
	assert.Equal(t, []bool{true, false, true, false, true}, sampleActive)
	// row%5 == 0 ⇒ payload IS NULL; only row 0 in the first five.
	assert.Equal(t, []bool{true, false, false, false, false}, samplePayloadNull)

	ro3 := env.openRO(t)
	defer ro3.Close()
	var activeCount int
	require.NoError(t, ro3.QueryRow("SELECT COUNT(*) FROM events WHERE is_active").Scan(&activeCount))
	assert.Equal(t, totalRows/2, activeCount)

	t.Logf("bulk ingest via Go client: %d rows in %d batches in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_DuckDB_Stream — IngestStream happy path with a pre-serialised
// Arrow buffer.
func TestIngest_DuckDB_Stream(t *testing.T) {
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

	res, err := env.client.IngestStream(context.Background(), env.dataObject, &buf)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 2, count)
}

func TestIngest_DuckDB_Stream_Empty(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestStream(context.Background(), env.dataObject, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "body is nil")

	_, err = env.client.IngestStream(context.Background(), "", bytes.NewReader([]byte{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data_object")
}

// TestIngest_DuckDB_LazyReader streams 50k rows through the typed client using
// a lazy Arrow reader instead of materialising all batches up front.
func TestIngest_DuckDB_LazyReader(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-lz"
	)
	pool := memory.NewGoAllocator()
	schema := eventsArrowSchema()
	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	batchIdx := 0
	reader := hugrclient.NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		if batchIdx >= numBatches {
			return nil, nil
		}
		rec := buildEventsBatch(pool, schema, batchIdx, rowsPerBatch, namePrefix, base)
		batchIdx++
		return rec, nil
	})
	defer reader.Release()

	start := time.Now()
	res, err := env.client.Ingest(context.Background(), env.dataObject, reader)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, int64(totalRows), res.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, totalRows, count)

	t.Logf("lazy-reader bulk ingest: %d rows in %d batches in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_LazyReader_Termination_DuckDB — engine-agnostic unit-style test
// for NewLazyReader's termination semantics. Doesn't need the server, but
// mirrors the PG suite for full symmetry.
func TestIngest_LazyReader_Termination_DuckDB(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	mk := func(v int32) arrow.RecordBatch {
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		recordFieldBuilder(t, b, "x").(*array.Int32Builder).Append(v)
		return b.NewRecordBatch()
	}

	// gen returns batches then nil → clean end-of-stream.
	{
		i := 0
		r := hugrclient.NewLazyReader(schema, func() (arrow.RecordBatch, error) {
			if i >= 3 {
				return nil, nil
			}
			i++
			return mk(int32(i)), nil
		})
		defer r.Release()
		seen := 0
		for r.Next() {
			seen++
		}
		require.NoError(t, r.Err())
		assert.Equal(t, 3, seen)
		assert.False(t, r.Next(), "Next after end-of-stream stays false")
	}

	// gen returns an error → Err() exposes it, stream terminates.
	{
		errBoom := errors.New("boom")
		i := 0
		r := hugrclient.NewLazyReader(schema, func() (arrow.RecordBatch, error) {
			if i == 2 {
				return nil, errBoom
			}
			i++
			return mk(int32(i)), nil
		})
		defer r.Release()
		seen := 0
		for r.Next() {
			seen++
		}
		assert.Equal(t, 2, seen, "yielded batches before failing call")
		require.Error(t, r.Err())
		assert.ErrorIs(t, r.Err(), errBoom)
	}
}

// TestIngest_HTTP_Direct_DuckDB exercises low-level HTTP behaviour against
// /ipc/ingest (bad Content-Type, missing data_object, wrong method, invalid
// body) plus a real-world bulk path streamed through io.Pipe straight into
// the request body. Mirrors TestIngest_HTTP_Direct from the PG suite.
func TestIngest_HTTP_Direct_DuckDB(t *testing.T) {
	env := setupEnv(t)

	// Missing data_object.
	resp, err := http.Post(env.server.URL+"/ipc/ingest", "application/vnd.apache.arrow.stream", bytes.NewReader(nil))
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body=%s", string(b))

	// Wrong method.
	req, _ := http.NewRequest(http.MethodGet, env.server.URL+"/ipc/ingest?data_object="+env.dataObject, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	// Wrong content type.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"text/plain", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)

	// Body is not a valid Arrow stream.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"application/vnd.apache.arrow.stream", bytes.NewReader([]byte("not arrow")))
	require.NoError(t, err)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body=%s", string(b))

	// Happy path — single small record.
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

	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out hugrclient.IngestResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	resp.Body.Close()
	assert.Equal(t, int64(1), out.Inserted)

	// --- Real-world bulk via io.Pipe streamed into the request body.
	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-direct"
	)
	bulkSchema := eventsArrowSchema()

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		w := ipc.NewWriter(pw, ipc.WithSchema(bulkSchema))
		base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)
		var streamErr error
		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			batchRec := buildEventsBatch(pool, bulkSchema, batchIdx, rowsPerBatch, namePrefix, base)
			if werr := w.Write(batchRec); werr != nil {
				streamErr = fmt.Errorf("write batch %d: %w", batchIdx, werr)
				batchRec.Release()
				break
			}
			batchRec.Release()
		}
		if cerr := w.Close(); cerr != nil && streamErr == nil {
			streamErr = fmt.Errorf("close arrow writer: %w", cerr)
		}
		_ = pw.CloseWithError(streamErr)
		writeErr <- streamErr
	}()

	start := time.Now()
	bulkResp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"application/vnd.apache.arrow.stream", pr)
	werr := <-writeErr
	require.NoError(t, werr, "writer goroutine failed")
	require.NoError(t, postErr)
	require.Equal(t, http.StatusOK, bulkResp.StatusCode)
	var bulkResult hugrclient.IngestResult
	require.NoError(t, json.NewDecoder(bulkResp.Body).Decode(&bulkResult))
	bulkResp.Body.Close()
	elapsed := time.Since(start)
	assert.Equal(t, int64(totalRows), bulkResult.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	countStart := time.Now()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events WHERE name LIKE 'dk-direct-%'").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all dk-direct rows visible immediately after POST")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

	t.Logf("bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}
