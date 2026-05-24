//go:build duckdb_arrow

package ingest_test

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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

const (
	envPostgresDSN = "INGEST_POSTGRES_DSN"
	envSchemasPath = "HUGR_INGEST_SCHEMAS_PATH"
)

// ingestEnv encapsulates a hugr service + an HTTP test server in front of it
// plus a direct sql.DB handle to the underlying postgres for verification.
type ingestEnv struct {
	service *hugr.Service
	server  *httptest.Server
	pgConn  *sql.DB
	client  *hugrclient.Client
	dsName  string
}

func setupEnv(t *testing.T) *ingestEnv {
	t.Helper()
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("%s not set — run integration-test/ingest/run.sh to spin up a postgres container", envPostgresDSN)
	}
	schemasPath := os.Getenv(envSchemasPath)
	if schemasPath == "" {
		// fall back to repo-relative path
		schemasPath = filepath.Join("testdata", "schemas")
	}
	abs, err := filepath.Abs(schemasPath)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(abs, "pg_ingest"))

	ctx := context.Background()

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

	// Register & load the postgres data source pointed at the test database.
	mustQuery(t, ctx, service, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "pg_ingest",
			"type":      "postgres",
			"prefix":    "pg_ingest",
			"as_module": true,
			"path":      dsn,
			"catalogs": []map[string]any{{
				"name": "pg_ingest",
				"type": "localFS",
				"path": filepath.Join(abs, "pg_ingest"),
			}},
		},
	})
	mustQuery(t, ctx, service, `mutation { function { core { load_data_source(name: "pg_ingest") { success message } } } }`, nil)

	srv := httptest.NewServer(service)

	pgConn, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, pgConn.PingContext(ctx))

	// Truncate before each suite to guarantee determinism.
	_, err = pgConn.ExecContext(ctx, "TRUNCATE TABLE events RESTART IDENTITY")
	require.NoError(t, err)

	c := hugrclient.NewClient(srv.URL + "/ipc")

	env := &ingestEnv{
		service: service,
		server:  srv,
		pgConn:  pgConn,
		client:  c,
		dsName:  "pg_ingest",
	}
	t.Cleanup(func() {
		srv.Close()
		_ = pgConn.Close()
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

// makeEventsRecord builds a single Arrow RecordBatch with the columns of the
// pg_ingest.events table (excluding id, which is autogen).
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

// --- Tests ----------------------------------------------------------------

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
	assert.Equal(t, []bool{true, false, true}, gotHasJSON) // beta has NULL payload
}

func TestIngest_Postgres_MultipleBatches(t *testing.T) {
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

// TestIngest_Postgres_ArrowIPCFile_StreamFormat writes an Arrow IPC stream
// file to disk and ingests it via IngestArrowIPCFile. The file has no
// ARROW1 magic, so the client should byte-forward it to /ipc/ingest.
func TestIngest_Postgres_ArrowIPCFile_StreamFormat(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	mk := func(names []string, vals []float64) arrow.RecordBatch {
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).AppendValues(names, nil)
		b.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
		act := make([]bool, len(names))
		for i := range act {
			act[i] = true
		}
		b.Field(2).(*array.BooleanBuilder).AppendValues(act, nil)
		return b.NewRecord()
	}
	rec1 := mk([]string{"f1", "f2"}, []float64{1, 2})
	defer rec1.Release()
	rec2 := mk([]string{"f3"}, []float64{3})
	defer rec2.Release()

	dir := t.TempDir()
	path := filepath.Join(dir, "events_stream.arrows")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := ipc.NewWriter(f, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec1))
	require.NoError(t, w.Write(rec2))
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())

	res, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events", path)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(3), res.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 3, count)

	// Verify both batches landed (ordered by name).
	rows, err := env.pgConn.Query("SELECT name, value FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()
	var gotNames []string
	var gotVals []float64
	for rows.Next() {
		var n string
		var v float64
		require.NoError(t, rows.Scan(&n, &v))
		gotNames = append(gotNames, n)
		gotVals = append(gotVals, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"f1", "f2", "f3"}, gotNames)
	assert.Equal(t, []float64{1, 2, 3}, gotVals)
}

// TestIngest_Postgres_ArrowIPCFile_FileFormat writes an Arrow IPC *file*
// format (.arrow with ARROW1 magic) to disk and ingests it via
// IngestArrowIPCFile. The client should detect the magic, use FileReader,
// and re-emit as a stream to the server.
func TestIngest_Postgres_ArrowIPCFile_FileFormat(t *testing.T) {
	env := setupEnv(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"file1", "file2", "file3"}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{100, 200, 300}, nil)
	b.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, true, false}, nil)
	rec := b.NewRecord()
	b.Release()
	defer rec.Release()

	dir := t.TempDir()
	path := filepath.Join(dir, "events_file.arrow")
	f, err := os.Create(path)
	require.NoError(t, err)
	fw, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
	require.NoError(t, err)
	require.NoError(t, fw.Write(rec))
	require.NoError(t, fw.Close())
	require.NoError(t, f.Close())

	// Sanity-check that we actually wrote the file format (ARROW1 prefix).
	prefix, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(prefix), 6)
	assert.Equal(t, "ARROW1", string(prefix[:6]), "test setup must produce file format with ARROW1 magic")

	res, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events", path)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(3), res.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, 3, count)

	rows, err := env.pgConn.Query("SELECT name, value FROM events ORDER BY value")
	require.NoError(t, err)
	defer rows.Close()
	var gotNames []string
	for rows.Next() {
		var n string
		var v float64
		require.NoError(t, rows.Scan(&n, &v))
		gotNames = append(gotNames, n)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"file1", "file2", "file3"}, gotNames)
}

// TestIngest_Postgres_ArrowIPCFile_NotFound checks that a missing file
// surfaces a clean error without touching the server.
func TestIngest_Postgres_ArrowIPCFile_NotFound(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events",
		filepath.Join(t.TempDir(), "does-not-exist.arrows"))
	require.Error(t, err)
}

// TestIngest_Postgres_LazyReader exercises NewLazyReader at scale: 50×1000
// rows generated on demand by a closure, no boilerplate RecordReader
// implementation. Mirrors TestIngest_Postgres_Bulk to prove the helper is
// equivalent.
func TestIngest_Postgres_LazyReader(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
	)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	batchIdx := 0
	reader := hugrclient.NewLazyReader(schema, func() (arrow.RecordBatch, error) {
		if batchIdx >= numBatches {
			return nil, nil
		}
		rb := array.NewRecordBuilder(pool, schema)
		defer rb.Release()
		names := rb.Field(0).(*array.StringBuilder)
		values := rb.Field(1).(*array.Float64Builder)
		active := rb.Field(2).(*array.BooleanBuilder)
		payloads := rb.Field(3).(*array.StringBuilder)
		ts := rb.Field(4).(*array.TimestampBuilder)
		for i := 0; i < rowsPerBatch; i++ {
			row := batchIdx*rowsPerBatch + i
			names.Append(fmt.Sprintf("lz-%06d", row))
			values.Append(float64(row) * 0.5)
			active.Append(row%2 == 0)
			if row%5 == 0 {
				payloads.AppendNull()
			} else {
				payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
			}
			ts.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
		}
		rec := rb.NewRecord()
		batchIdx++
		return rec, nil
	})
	defer reader.Release()

	start := time.Now()
	res, err := env.client.Ingest(context.Background(), "pg_ingest.events", reader)
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, int64(totalRows), res.Inserted)

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	assert.Equal(t, totalRows, count)

	t.Logf("lazy-reader bulk ingest: %d rows in %d batches in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_LazyReader_Termination is a unit-style test for NewLazyReader's
// termination semantics (no server / postgres needed): (nil, nil) ends the
// stream; (_, err) surfaces via Err().
func TestIngest_LazyReader_Termination(t *testing.T) {
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

	// Case 1: gen returns batches then nil — clean end-of-stream.
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

	// Case 2: gen returns an error — surfaces via Err, terminates stream.
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
		assert.Equal(t, 2, seen, "should yield batches before the failing call")
		require.Error(t, r.Err())
		assert.ErrorIs(t, r.Err(), errBoom)
	}
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
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
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
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
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

	resp, err = http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out hugrclient.IngestResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	resp.Body.Close()
	assert.Equal(t, int64(1), out.Inserted)

	// --- Real-world bulk path -------------------------------------------------
	// A producer (ETL/CDC/telemetry) streams many RecordBatches in a single
	// Arrow IPC stream over one HTTP POST. The whole payload is never
	// materialised in memory client-side — we pipe the writer goroutine
	// straight into the request body. This is where /ipc/ingest pays off vs.
	// GraphQL `insert_events(data: ...)` mutations.
	_, err = env.pgConn.ExecContext(context.Background(), "TRUNCATE TABLE events RESTART IDENTITY")
	require.NoError(t, err)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
	)

	bulkSchema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		w := ipc.NewWriter(pw, ipc.WithSchema(bulkSchema))
		base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)
		var streamErr error
		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			rb := array.NewRecordBuilder(pool, bulkSchema)
			names := rb.Field(0).(*array.StringBuilder)
			values := rb.Field(1).(*array.Float64Builder)
			active := rb.Field(2).(*array.BooleanBuilder)
			payloads := rb.Field(3).(*array.StringBuilder)
			ts := rb.Field(4).(*array.TimestampBuilder)
			for i := 0; i < rowsPerBatch; i++ {
				row := batchIdx*rowsPerBatch + i
				names.Append(fmt.Sprintf("evt-%06d", row))
				values.Append(float64(row) * 0.5)
				active.Append(row%2 == 0)
				if row%5 == 0 {
					payloads.AppendNull()
				} else {
					payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
				}
				ts.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
			}
			batchRec := rb.NewRecord()
			rb.Release()
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
	bulkResp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
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
	assert.ElementsMatch(t, []string{"name", "value", "is_active", "payload", "created_at"}, bulkResult.Columns)

	// Time the COUNT(*) right after the POST returns to prove the writes are
	// synchronous: if the server reported "inserted" before the data was
	// actually committed to Postgres, COUNT(*) would either lag or be partial.
	countStart := time.Now()
	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all rows must be visible the moment POST returns")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

	// Spot-check a sample to confirm per-row fidelity end-to-end.
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

	// Cross-check the active-row count to ensure the boolean column survived
	// without bit-packing artefacts across batch boundaries.
	var activeCount int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE is_active").Scan(&activeCount))
	assert.Equal(t, totalRows/2, activeCount)

	t.Logf("bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// lazyEventsReader is an array.RecordReader that generates events-table
// RecordBatches on demand. This is the shape of a real-world Arrow producer
// (parquet scanner, CDC tap, kafka batcher) — the whole stream is never
// materialised in memory beyond the batch currently being consumed.
type lazyEventsReader struct {
	pool         memory.Allocator
	schema       *arrow.Schema
	numBatches   int
	rowsPerBatch int
	base         time.Time

	batchIdx int
	current  arrow.RecordBatch
	err      error
	refCount atomic.Int64
}

func newLazyEventsReader(pool memory.Allocator, numBatches, rowsPerBatch int, base time.Time) *lazyEventsReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	r := &lazyEventsReader{
		pool:         pool,
		schema:       schema,
		numBatches:   numBatches,
		rowsPerBatch: rowsPerBatch,
		base:         base,
	}
	r.refCount.Add(1)
	return r
}

func (r *lazyEventsReader) Schema() *arrow.Schema { return r.schema }
func (r *lazyEventsReader) Err() error            { return r.err }

func (r *lazyEventsReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.batchIdx >= r.numBatches {
		return false
	}
	rb := array.NewRecordBuilder(r.pool, r.schema)
	defer rb.Release()
	names := rb.Field(0).(*array.StringBuilder)
	values := rb.Field(1).(*array.Float64Builder)
	active := rb.Field(2).(*array.BooleanBuilder)
	payloads := rb.Field(3).(*array.StringBuilder)
	ts := rb.Field(4).(*array.TimestampBuilder)
	for i := 0; i < r.rowsPerBatch; i++ {
		row := r.batchIdx*r.rowsPerBatch + i
		names.Append(fmt.Sprintf("evt-%06d", row))
		values.Append(float64(row) * 0.5)
		active.Append(row%2 == 0)
		if row%5 == 0 {
			payloads.AppendNull()
		} else {
			payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		ts.Append(arrow.Timestamp(r.base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	r.current = rb.NewRecord()
	r.batchIdx++
	return true
}

func (r *lazyEventsReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *lazyEventsReader) Record() arrow.RecordBatch      { return r.current }

func (r *lazyEventsReader) Retain() { r.refCount.Add(1) }
func (r *lazyEventsReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
	}
}
