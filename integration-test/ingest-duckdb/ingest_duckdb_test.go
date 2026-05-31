//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// ingestEnv mirrors the Postgres counterpart but everything runs in-process
// against a local .duckdb file in t.TempDir():
//   - the .duckdb file is created and seeded (CREATE SEQUENCE + CREATE TABLE)
//     via a direct sql.Open("duckdb", path) connection;
//   - that connection is then CLOSED — only one process can hold the write
//     lock, and we hand the file over to hugr next;
//   - hugr is started in-process, registers the file as a "duckdb" data
//     source named "duck_ingest" and ATTACHes it;
//   - the verifier sql.DB is re-opened in READ_ONLY mode (DuckDB allows
//     concurrent read-only connections while another writer holds the file),
//     giving the test an independent view of the data — analogous to the
//     pgx-based pgConn in the Postgres test suite.
type ingestEnv struct {
	service *hugr.Service
	server  *httptest.Server
	client  *hugrclient.Client
	dbPath  string
}

// openRO returns a fresh READ_ONLY sql.DB handle to the events database.
// DuckDB RO connections opened in the same process as a writer DO NOT
// transparently refresh snapshot across pooled connections, so we open a
// fresh handle per verification — this gives us a guaranteed post-write
// snapshot at the moment of the assertion. Callers should `defer Close()`.
func (e *ingestEnv) openRO(t *testing.T) *sql.DB {
	t.Helper()
	conn, err := sql.Open("duckdb", e.dbPath+"?access_mode=read_only")
	require.NoError(t, err)
	require.NoError(t, conn.PingContext(context.Background()))
	return conn
}

func setupEnv(t *testing.T) *ingestEnv {
	t.Helper()
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// 1. Seed schema with a private writer; close before hugr opens it.
	seed, err := sql.Open("duckdb", dbPath)
	require.NoError(t, err)
	_, err = seed.ExecContext(ctx, `
		CREATE SEQUENCE events_id_seq;
		CREATE TABLE events (
			id BIGINT PRIMARY KEY DEFAULT nextval('events_id_seq'),
			name VARCHAR NOT NULL,
			value DOUBLE NOT NULL,
			is_active BOOLEAN NOT NULL DEFAULT true,
			payload JSON,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	require.NoError(t, err)
	require.NoError(t, seed.Close())

	// 2. Schema path for the localFS catalog.
	schemaDir, err := filepath.Abs(filepath.Join("testdata", "schemas", "duck_ingest"))
	require.NoError(t, err)
	require.DirExists(t, schemaDir)

	// 3. Start hugr in-process.
	service, err := hugr.New(hugr.Config{
		Debug:  true,
		DB:     db.Config{},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{
					Allowed: true,
					Role:    "admin",
				}),
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, service.Init(ctx))

	// 4. Register & load the duckdb data source pointed at the file.
	mustQuery(t, ctx, service, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "duck_ingest",
			"type":      "duckdb",
			"prefix":    "duck_ingest",
			"as_module": true,
			"path":      dbPath,
			"catalogs": []map[string]any{{
				"name": "duck_ingest",
				"type": "localFS",
				"path": schemaDir,
			}},
		},
	})
	mustQuery(t, ctx, service, `mutation { function { core { load_data_source(name: "duck_ingest") { success message } } } }`, nil)

	srv := httptest.NewServer(service)

	c := hugrclient.NewClient(srv.URL + "/ipc")

	env := &ingestEnv{
		service: service,
		server:  srv,
		client:  c,
		dbPath:  dbPath,
	}
	t.Cleanup(func() {
		srv.Close()
		service.Close()
	})
	return env
}

func mustQuery(t *testing.T, ctx context.Context, s *hugr.Service, q string, vars map[string]any) {
	t.Helper()
	res, err := s.Query(ctx, q, vars)
	require.NoError(t, err)
	if res.Err() != nil {
		require.NoErrorf(t, res.Err(), "graphql error for query: %s", q)
	}
	res.Close()
}

func makeEventsRecord(t *testing.T, names []string, values []float64, active []bool, payload []string, created []arrow.Timestamp) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues(names, nil)
	b.Field(1).(*array.Float64Builder).AppendValues(values, nil)
	b.Field(2).(*array.BooleanBuilder).AppendValues(active, nil)
	pBuilder := b.Field(3).(*array.StringBuilder)
	for _, p := range payload {
		if p == "" {
			pBuilder.AppendNull()
		} else {
			pBuilder.Append(p)
		}
	}
	tsBuilder := b.Field(4).(*array.TimestampBuilder)
	tsBuilder.AppendValues(created, nil)
	return b.NewRecord()
}

// --- Core tests -----------------------------------------------------------

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

	res, err := env.client.IngestRecord(context.Background(), "duck_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "duck_ingest.events", res.DataObject)
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
		gotNames    []string
		gotValues   []float64
		gotActive   []bool
		gotHasJSON  []bool
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
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	_, err := env.client.IngestRecord(context.Background(), "duck_ingest.events", rec)
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
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	_, err := env.client.IngestRecord(context.Background(), "duck_ingest.does_not_exist", rec)
	require.Error(t, err)
}

func TestIngest_DuckDB_MultipleBatches(t *testing.T) {
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
		b.Field(0).(*array.StringBuilder).AppendValues(names, nil)
		vals := make([]float64, len(names))
		for i := range vals {
			vals[i] = float64(i)
		}
		b.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
		active := make([]bool, len(names))
		for i := range active {
			active[i] = true
		}
		b.Field(2).(*array.BooleanBuilder).AppendValues(active, nil)
		b.Field(3).(*array.StringBuilder).AppendNulls(len(names))
		ts := make([]arrow.Timestamp, len(names))
		for i := range ts {
			ts[i] = arrow.Timestamp(time.Now().UTC().UnixMicro())
		}
		b.Field(4).(*array.TimestampBuilder).AppendValues(ts, nil)
		return b.NewRecord()
	}
	rec1 := mk([]string{"a", "b"})
	defer rec1.Release()
	rec2 := mk([]string{"c", "d", "e"})
	defer rec2.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec1, rec2})
	require.NoError(t, err)
	defer rr.Release()

	res, err := env.client.Ingest(context.Background(), "duck_ingest.events", rr)
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
	res, err := env.client.Ingest(context.Background(), "duck_ingest.events", reader)
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
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"s1", "s2"}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{10, 20}, nil)
	b.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	rec := b.NewRecord()
	b.Release()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	res, err := env.client.IngestStream(context.Background(), "duck_ingest.events", &buf)
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
	_, err := env.client.IngestStream(context.Background(), "duck_ingest.events", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "body is nil")

	_, err = env.client.IngestStream(context.Background(), "", bytes.NewReader([]byte{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data_object")
}

// TestIngest_DuckDB_ArrowIPCFile_StreamFormat — 50k×1000 stream-format file
// → IngestArrowIPCFile → byte-forwarded to /ipc/ingest.
func TestIngest_DuckDB_ArrowIPCFile_StreamFormat(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-fs"
	)
	path := filepath.Join(t.TempDir(), "events_stream.arrows")
	writeEventsArrowFile(t, path, namePrefix, arrowStreamFormat, numBatches, rowsPerBatch)
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.NotEqual(t, "ARROW1", string(head[:6]), "stream format must not start with ARROW1 magic")

	start := time.Now()
	res, err := env.client.IngestArrowIPCFile(context.Background(), "duck_ingest.events", path)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, int64(totalRows), res.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, totalRows, count)

	t.Logf("arrow ipc stream file ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_DuckDB_ArrowIPCFile_FileFormat — 50k×1000 file-format (.arrow,
// ARROW1 magic + footer) → IngestArrowIPCFile detects magic, re-streams via
// ipc.FileReader.
func TestIngest_DuckDB_ArrowIPCFile_FileFormat(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "dk-ff"
	)
	path := filepath.Join(t.TempDir(), "events_file.arrow")
	writeEventsArrowFile(t, path, namePrefix, arrowFileFmt, numBatches, rowsPerBatch)
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.Equal(t, "ARROW1", string(head[:6]), "file format must start with ARROW1 magic")

	start := time.Now()
	res, err := env.client.IngestArrowIPCFile(context.Background(), "duck_ingest.events", path)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, int64(totalRows), res.Inserted)

	ro := env.openRO(t)
	defer ro.Close()
	var count int
	require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, totalRows, count)

	t.Logf("arrow ipc file-format ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

func TestIngest_DuckDB_ArrowIPCFile_NotFound(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestArrowIPCFile(context.Background(), "duck_ingest.events",
		filepath.Join(t.TempDir(), "does-not-exist.arrows"))
	require.Error(t, err)
}

// TestIngest_DuckDB_LazyReader — alias: bulk ingest via NewLazyReader, but
// keeping symmetry with PG suite name. Same scenario as Bulk above but with
// a distinct prefix so the suite can run all tests against a single setup
// without collisions if combined.
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
	res, err := env.client.Ingest(context.Background(), "duck_ingest.events", reader)
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
		b.Field(0).(*array.Int32Builder).Append(v)
		return b.NewRecord()
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
	req, _ := http.NewRequest(http.MethodGet, env.server.URL+"/ipc/ingest?data_object=duck_ingest.events", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	// Wrong content type.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=duck_ingest.events",
		"text/plain", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)

	// Body is not a valid Arrow stream.
	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=duck_ingest.events",
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
	bld.Field(0).(*array.StringBuilder).AppendValues([]string{"direct"}, nil)
	bld.Field(1).(*array.Float64Builder).AppendValues([]float64{42}, nil)
	bld.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true}, nil)
	rec := bld.NewRecord()
	bld.Release()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=duck_ingest.events",
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
	bulkResp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object=duck_ingest.events",
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

// --- helpers --------------------------------------------------------------

type arrowFileFormat int

const (
	arrowStreamFormat arrowFileFormat = iota
	arrowFileFmt
)

func eventsArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
}

// buildEventsBatch produces one RecordBatch of `rowsPerBatch` rows for the
// events schema. Row payload pattern matches the PG bulk fixtures so the
// spot-check assertions are reusable.
func buildEventsBatch(pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string, base time.Time) arrow.RecordBatch {
	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()
	names := rb.Field(0).(*array.StringBuilder)
	values := rb.Field(1).(*array.Float64Builder)
	active := rb.Field(2).(*array.BooleanBuilder)
	payloads := rb.Field(3).(*array.StringBuilder)
	ts := rb.Field(4).(*array.TimestampBuilder)
	for i := 0; i < rowsPerBatch; i++ {
		row := batchIdx*rowsPerBatch + i
		names.Append(fmt.Sprintf("%s-%06d", namePrefix, row))
		values.Append(float64(row) * 0.5)
		active.Append(row%2 == 0)
		if row%5 == 0 {
			payloads.AppendNull()
		} else {
			payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		ts.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	return rb.NewRecord()
}

// writeEventsArrowFile writes an Arrow IPC file (stream or file format) at
// path with `numBatches * rowsPerBatch` rows for the events schema.
func writeEventsArrowFile(t *testing.T, path, namePrefix string, format arrowFileFormat, numBatches, rowsPerBatch int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := eventsArrowSchema()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	type writer interface {
		Write(arrow.RecordBatch) error
		Close() error
	}
	var w writer
	switch format {
	case arrowStreamFormat:
		w = ipc.NewWriter(f, ipc.WithSchema(schema))
	case arrowFileFmt:
		fw, ferr := ipc.NewFileWriter(f, ipc.WithSchema(schema))
		require.NoError(t, ferr)
		w = fw
	default:
		t.Fatalf("unknown arrow file format: %d", format)
	}

	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		rec := buildEventsBatch(pool, schema, batchIdx, rowsPerBatch, namePrefix, base)
		require.NoError(t, w.Write(rec))
		rec.Release()
	}
	require.NoError(t, w.Close())
}

// Silence "imported and not used" if a refactor leaves a quoted ref around.
var _ atomic.Int64
