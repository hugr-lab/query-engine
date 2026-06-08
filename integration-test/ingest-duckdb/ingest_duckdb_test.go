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
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// ingestEnv is per-test state on top of a shared hugr.Service (initialised
// once in TestMain). Each test owns a unique .duckdb file and a unique data
// source name, so tests don't share table state. Cleanup unloads the source
// to DETACH the file before t.TempDir() removes it.
type ingestEnv struct {
	service    *hugr.Service
	server     *httptest.Server
	client     *hugrclient.Client
	dbPath     string
	dsName     string // unique data source / catalog prefix, e.g. "duck_ingest_3"
	dataObject string // dsName + ".events"
}

// Shared service initialised once for the whole package — see TestMain.
// hugr.New + service.Init costs ~17s; doing it once cuts the package
// wall-clock from 13×17s ≈ 3.5min down to one-off ~17s + ~ms/test.
var (
	sharedService *hugr.Service
	sharedServer  *httptest.Server
	sharedClient  *hugrclient.Client
	dsCounter     atomic.Int64
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		Debug:  false, // shared service runs many tests — keep logs quiet
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
	if err != nil {
		log.Fatalf("hugr.New: %v", err)
	}
	if err := service.Init(ctx); err != nil {
		log.Fatalf("service.Init: %v", err)
	}
	sharedService = service
	sharedServer = httptest.NewServer(service)
	sharedClient = hugrclient.NewClient(sharedServer.URL + "/ipc")

	code := m.Run()

	sharedServer.Close()
	_ = service.Close()
	os.Exit(code)
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

	n := dsCounter.Add(1)
	dsName := fmt.Sprintf("duck_ingest_%d", n)
	dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%d.duckdb", n))

	// 1. Seed schema with a private writer; close before hugr opens it.
	seed, err := sql.Open("duckdb", dbPath)
	require.NoError(t, err)
	_, err = seed.ExecContext(ctx, `
		INSTALL spatial; LOAD spatial;
		CREATE SEQUENCE events_id_seq;
		CREATE TABLE events (
			id BIGINT PRIMARY KEY DEFAULT nextval('events_id_seq'),
			name VARCHAR NOT NULL,
			value DOUBLE NOT NULL,
			is_active BOOLEAN NOT NULL DEFAULT true,
			payload JSON,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			geom GEOMETRY,
			geom_wkt GEOMETRY,
			geom_geojson GEOMETRY,
			geom_wkb GEOMETRY,
			geom_line GEOMETRY,
			geom_polygon_native GEOMETRY,
			geom_multipoint GEOMETRY,
			geom_multiline GEOMETRY,
			geom_multipolygon GEOMETRY
		);
	`)
	require.NoError(t, err)
	require.NoError(t, seed.Close())

	// 2. Schema path for the localFS catalog.
	schemaDir, err := filepath.Abs(filepath.Join("testdata", "schemas", "duck_ingest"))
	require.NoError(t, err)
	require.DirExists(t, schemaDir)

	// 3. Register & load this test's unique data source on the SHARED service.
	mustQuery(t, ctx, sharedService, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      dsName,
			"type":      "duckdb",
			"prefix":    dsName,
			"as_module": true,
			"path":      dbPath,
			"catalogs": []map[string]any{{
				"name": dsName,
				"type": "localFS",
				"path": schemaDir,
			}},
		},
	})
	mustQuery(t, ctx, sharedService, `mutation($name: String!) {
		function { core { load_data_source(name: $name) { success message } } }
	}`, map[string]any{"name": dsName})

	env := &ingestEnv{
		service:    sharedService,
		server:     sharedServer,
		client:     sharedClient,
		dbPath:     dbPath,
		dsName:     dsName,
		dataObject: dsName + ".events",
	}

	// Unload on test completion so DETACH releases the .duckdb file before
	// t.TempDir() removes it. Best-effort: ignore errors (next test uses a
	// different name + file, so a leak is harmless within a single run).
	t.Cleanup(func() {
		res, err := sharedService.Query(ctx, `mutation($name: String!, $hard: Boolean) {
			function { core { unload_data_source(name: $name, hard: $hard) { success message } } }
		}`, map[string]any{"name": dsName, "hard": true})
		if err == nil {
			res.Close()
		}
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
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
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
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	_, err := env.client.IngestRecord(context.Background(), env.dsName+".does_not_exist", rec)
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
	res, err := env.client.IngestArrowIPCFile(context.Background(), env.dataObject, path)
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
	res, err := env.client.IngestArrowIPCFile(context.Background(), env.dataObject, path)
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
	_, err := env.client.IngestArrowIPCFile(context.Background(), env.dataObject,
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

func TestIngest_HTTP_GeometryTypes_DuckDB(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []string{"geo-a", "geo-b"}, [][2]float64{{30.5, 50.25}, {-73.935242, 40.730610}})
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

	rows, err := ro.Query(`
		SELECT name,
			ST_AsText(geom), ST_AsText(geom_wkt), ST_AsText(geom_geojson), ST_AsText(geom_wkb),
			ST_AsText(geom_line), ST_AsText(geom_polygon_native), ST_AsText(geom_multipoint),
			ST_AsText(geom_multiline), ST_AsText(geom_multipolygon)
		FROM events
		WHERE name LIKE 'geo-%'
		ORDER BY name
	`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	for rows.Next() {
		var name string
		values := make([]string, 9)
		scanArgs := []any{&name}
		for i := range values {
			scanArgs = append(scanArgs, &values[i])
		}
		require.NoError(t, rows.Scan(scanArgs...))
		for i := range values {
			values[i] = compactWKT(values[i])
		}
		got[name] = values
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
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
			rec := buildGeometryTypesBatch(pool, schema, batchIdx, rowsPerBatch, namePrefix)
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

	values := make([]string, 9)
	require.NoError(t, ro.QueryRow(`
		SELECT ST_AsText(geom), ST_AsText(geom_wkt), ST_AsText(geom_geojson), ST_AsText(geom_wkb),
			ST_AsText(geom_line), ST_AsText(geom_polygon_native), ST_AsText(geom_multipoint),
			ST_AsText(geom_multiline), ST_AsText(geom_multipolygon)
		FROM events
		WHERE name = 'dk-geo-bulk-049999'
	`).Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7], &values[8]))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)

	elapsed := time.Since(start)
	t.Logf("geometry bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
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

func makeGeometryTypesRecord(t *testing.T, names []string, points [][2]float64) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()
	require.Len(t, points, len(names))

	schema := geometryTypesSchema()
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i, name := range names {
		appendGeometryTypesRow(b, name, float64(i+1), true, points[i], float64(i), float64(i))
	}

	return b.NewRecordBatch(), schema
}

func geometryTypesSchema() *arrow.Schema {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	lineType := arrow.ListOf(pointType)
	polygonType := arrow.ListOf(lineType)
	return arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "geom", Type: pointType, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.point"})},
		{Name: "geom_wkt", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"})},
		{Name: "geom_geojson", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.geojson"})},
		{Name: "geom_wkb", Type: arrow.BinaryTypes.Binary, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkb"})},
		{Name: "geom_line", Type: lineType, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.linestring"})},
		{Name: "geom_polygon_native", Type: polygonType, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.polygon"})},
		{Name: "geom_multipoint", Type: lineType, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.multipoint"})},
		{Name: "geom_multiline", Type: polygonType, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.multilinestring"})},
		{Name: "geom_multipolygon", Type: arrow.ListOf(polygonType), Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.multipolygon"})},
	}, nil)
}

func geometryTypesColumns() []string {
	return []string{
		"name", "value", "is_active",
		"geom", "geom_wkt", "geom_geojson", "geom_wkb",
		"geom_line", "geom_polygon_native", "geom_multipoint",
		"geom_multiline", "geom_multipolygon",
	}
}

func geometryExpected(point, x, y string) []string {
	return []string{
		point,
		fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1)),
		fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))", x, y, x, addCoord(y, 1), addCoord(x, 1), addCoord(y, 1), addCoord(x, 1), y, x, y),
		point,
		fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1)),
		fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))", x, y, x, addCoord(y, 1), addCoord(x, 1), addCoord(y, 1), addCoord(x, 1), y, x, y),
		fmt.Sprintf("MULTIPOINT(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), y),
		fmt.Sprintf("MULTILINESTRING((%s %s,%s %s),(%s %s,%s %s))", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 2), addCoord(x, 3), addCoord(y, 3)),
		fmt.Sprintf("MULTIPOLYGON(((%s %s,%s %s,%s %s,%s %s,%s %s)))", x, y, x, addCoord(y, 1), addCoord(x, 1), addCoord(y, 1), addCoord(x, 1), y, x, y),
	}
}

func addCoord(v string, delta float64) string {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(err)
	}
	return coord(f + delta)
}

func compactWKT(s string) string {
	s = strings.ReplaceAll(s, ", ", ",")
	s = strings.ReplaceAll(s, " (", "(")
	if strings.HasPrefix(s, "MULTIPOINT((") && strings.HasSuffix(s, "))") {
		inner := strings.TrimSuffix(strings.TrimPrefix(s, "MULTIPOINT(("), "))")
		s = "MULTIPOINT(" + strings.ReplaceAll(inner, "),(", ",") + ")"
	}
	return s
}

func buildGeometryTypesBatch(pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string) arrow.RecordBatch {
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i := 0; i < rowsPerBatch; i++ {
		row := batchIdx*rowsPerBatch + i
		x := float64(row % 100)
		y := float64(row / 1000)
		appendGeometryTypesRow(b, fmt.Sprintf("%s-%06d", namePrefix, row), float64(row)*0.5, row%2 == 0, [2]float64{x, y}, x, y)
	}
	return b.NewRecordBatch()
}

func appendGeometryTypesRow(b *array.RecordBuilder, name string, value float64, active bool, point [2]float64, shapeX, shapeY float64) {
	b.Field(0).(*array.StringBuilder).Append(name)
	b.Field(1).(*array.Float64Builder).Append(value)
	b.Field(2).(*array.BooleanBuilder).Append(active)

	sb := b.Field(3).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Float64Builder).Append(point[0])
	sb.FieldBuilder(1).(*array.Float64Builder).Append(point[1])

	b.Field(4).(*array.StringBuilder).Append(lineWKT(shapeX, shapeY))
	b.Field(5).(*array.StringBuilder).Append(polygonGeoJSON(shapeX, shapeY))
	wkbPoint, _ := wkb.Marshal(orb.Point{point[0], point[1]})
	b.Field(6).(*array.BinaryBuilder).Append(wkbPoint)
	appendPointList(b.Field(7).(*array.ListBuilder), linePoints(shapeX, shapeY))
	appendPointListList(b.Field(8).(*array.ListBuilder), polygonRings(shapeX, shapeY))
	appendPointList(b.Field(9).(*array.ListBuilder), multiPoints(shapeX, shapeY))
	appendPointListList(b.Field(10).(*array.ListBuilder), multiLines(shapeX, shapeY))
	appendPointListListList(b.Field(11).(*array.ListBuilder), multiPolygons(shapeX, shapeY))
}

type xyPoint [2]float64

func appendPoint(sb *array.StructBuilder, point xyPoint) {
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Float64Builder).Append(point[0])
	sb.FieldBuilder(1).(*array.Float64Builder).Append(point[1])
}

func appendPointList(lb *array.ListBuilder, points []xyPoint) {
	lb.Append(true)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for _, point := range points {
		appendPoint(sb, point)
	}
}

func appendPointListList(lb *array.ListBuilder, lines [][]xyPoint) {
	lb.Append(true)
	inner := lb.ValueBuilder().(*array.ListBuilder)
	for _, points := range lines {
		appendPointList(inner, points)
	}
}

func appendPointListListList(lb *array.ListBuilder, polygons [][][]xyPoint) {
	lb.Append(true)
	inner := lb.ValueBuilder().(*array.ListBuilder)
	for _, rings := range polygons {
		appendPointListList(inner, rings)
	}
}

func linePoints(x, y float64) []xyPoint {
	return []xyPoint{{x, y}, {x + 1, y + 1}, {x + 2, y + 1}}
}

func polygonRings(x, y float64) [][]xyPoint {
	return [][]xyPoint{{{x, y}, {x, y + 1}, {x + 1, y + 1}, {x + 1, y}, {x, y}}}
}

func multiPoints(x, y float64) []xyPoint {
	return []xyPoint{{x, y}, {x + 1, y + 1}, {x + 2, y}}
}

func multiLines(x, y float64) [][]xyPoint {
	return [][]xyPoint{
		{{x, y}, {x + 1, y + 1}},
		{{x + 2, y + 2}, {x + 3, y + 3}},
	}
}

func multiPolygons(x, y float64) [][][]xyPoint {
	return [][][]xyPoint{polygonRings(x, y)}
}

func lineWKT(x, y float64) string {
	return fmt.Sprintf("LINESTRING (%s %s, %s %s, %s %s)",
		coord(x), coord(y),
		coord(x+1), coord(y+1),
		coord(x+2), coord(y+1))
}

func polygonGeoJSON(x, y float64) string {
	return fmt.Sprintf(`{"type":"Polygon","coordinates":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}`,
		coord(x), coord(y),
		coord(x), coord(y+1),
		coord(x+1), coord(y+1),
		coord(x+1), coord(y),
		coord(x), coord(y))
}

func coord(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
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
