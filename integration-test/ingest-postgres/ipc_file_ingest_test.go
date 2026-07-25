//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type arrowFileFormat int

const (
	arrowStreamFormat arrowFileFormat = iota
	arrowFileFmt
)

type arrowIPCRecordWriter interface {
	Write(arrow.RecordBatch) error
	Close() error
}

func newArrowIPCRecordWriter(t *testing.T, f *os.File, schema *arrow.Schema, format arrowFileFormat) arrowIPCRecordWriter {
	t.Helper()

	switch format {
	case arrowStreamFormat:
		return ipc.NewWriter(f, ipc.WithSchema(schema))
	case arrowFileFmt:
		w, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
		require.NoError(t, err)
		return w
	default:
		t.Fatalf("unknown arrow file format: %d", format)
		return nil
	}
}

func writeArrowIPCFile(t *testing.T, path string, schema *arrow.Schema, format arrowFileFormat, numBatches int, buildBatch func(batchIdx int) arrow.RecordBatch) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	w := newArrowIPCRecordWriter(t, f, schema, format)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		rec := buildBatch(batchIdx)
		require.NoError(t, w.Write(rec))
		rec.Release()
	}
	require.NoError(t, w.Close())
}

// writeEventsArrowFile produces an Arrow IPC file at path in the given
// format with numBatches × rowsPerBatch synthetic events rows. namePrefix is
// embedded in the `name` column so different tests can write to the same
// table without colliding on uniqueness assertions.
func writeEventsArrowFile(t *testing.T, path, namePrefix string, format arrowFileFormat, numBatches, rowsPerBatch int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := eventsArrowFileSchema()
	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	writeArrowIPCFile(t, path, schema, format, numBatches, func(batchIdx int) arrow.RecordBatch {
		rb := array.NewRecordBuilder(pool, schema)
		fields := eventsRecordBuildersFor(rb)
		for i := 0; i < rowsPerBatch; i++ {
			row := batchIdx*rowsPerBatch + i
			name, point := geometryBatchRow(namePrefix, row)
			fields.names.Append(name)
			fields.values.Append(float64(row) * 0.5)
			fields.active.Append(row%2 == 0)
			if row%5 == 0 {
				fields.payloads.AppendNull()
			} else {
				fields.payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
			}
			fields.createdAt.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
			appendGeometryValueFields(t, rb, geometryTypesRow{point: point, shapeOrigin: point})
		}
		rec := rb.NewRecordBatch()
		rb.Release()
		return rec
	})
}

// TestIngest_Postgres_ArrowIPCFile_StreamFormat builds a 50×1000-row Arrow
// IPC *stream* file on disk and ingests it via IngestArrowIPCFile. The
// client should detect "no ARROW1 magic" and byte-forward the file body
// straight into /ipc/ingest — the bulk path with zero re-serialisation.

func TestIngest_Postgres_ArrowIPCFile_StreamFormat(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "stream"
	)

	path := filepath.Join(t.TempDir(), "events_stream.arrows")
	writeEventsArrowFile(t, path, namePrefix, arrowStreamFormat, numBatches, rowsPerBatch)

	// Sanity-check that the file is actually stream format (no ARROW1).
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.NotEqual(t, "ARROW1", string(head[:6]), "test setup must produce stream format (no ARROW1 magic)")

	start := time.Now()
	res, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events", path)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(totalRows), res.Inserted)

	// Synchronicity check: COUNT(*) must see all rows the moment POST returns.
	countStart := time.Now()
	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all rows must be visible immediately")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

	// Spot-check the first 5 rows by content (rows produced by namePrefix-N).
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
	assert.Equal(t, []string{namePrefix + "-000000", namePrefix + "-000001", namePrefix + "-000002", namePrefix + "-000003", namePrefix + "-000004"}, sampleNames)
	assert.Equal(t, []float64{0, 0.5, 1.0, 1.5, 2.0}, sampleValues)
	assert.Equal(t, []bool{true, false, true, false, true}, sampleActive)
	assert.Equal(t, []bool{true, false, false, false, false}, samplePayloadNull)

	// Active-row count guards against bit-packing artefacts across batches.
	var activeCount int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE is_active").Scan(&activeCount))
	assert.Equal(t, totalRows/2, activeCount)
	assertArrowIPCFileGeometry(t, env, namePrefix, totalRows)

	t.Logf("arrow ipc stream file ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

// TestIngest_Postgres_ArrowIPCFile_FileFormat builds a 50×1000-row Arrow IPC
// *file* format file (ARROW1 magic + random-access footer) on disk and
// ingests it via IngestArrowIPCFile. The client should detect the magic,
// open the file with ipc.FileReader, and re-emit as a stream to the server.
func TestIngest_Postgres_ArrowIPCFile_FileFormat(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "file"
	)

	path := filepath.Join(t.TempDir(), "events_file.arrow")
	writeEventsArrowFile(t, path, namePrefix, arrowFileFmt, numBatches, rowsPerBatch)

	// Sanity-check that we actually wrote the file format (ARROW1 prefix).
	head, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(head), 6)
	assert.Equal(t, "ARROW1", string(head[:6]), "test setup must produce file format with ARROW1 magic")

	start := time.Now()
	res, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events", path)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(totalRows), res.Inserted)

	// Synchronicity check.
	countStart := time.Now()
	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
	countElapsed := time.Since(countStart)
	assert.Equal(t, totalRows, count, "all rows must be visible immediately")
	t.Logf("post-POST COUNT(*) visibility: %d rows in %s — no async lag", count, countElapsed)

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
	assert.Equal(t, []string{namePrefix + "-000000", namePrefix + "-000001", namePrefix + "-000002", namePrefix + "-000003", namePrefix + "-000004"}, sampleNames)
	assert.Equal(t, []float64{0, 0.5, 1.0, 1.5, 2.0}, sampleValues)
	assert.Equal(t, []bool{true, false, true, false, true}, sampleActive)
	assert.Equal(t, []bool{true, false, false, false, false}, samplePayloadNull)

	var activeCount int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE is_active").Scan(&activeCount))
	assert.Equal(t, totalRows/2, activeCount)
	assertArrowIPCFileGeometry(t, env, namePrefix, totalRows)

	t.Logf("arrow ipc file-format ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

func assertArrowIPCFileGeometry(t *testing.T, env *ingestEnv, namePrefix string, totalRows int) {
	t.Helper()
	lastName, lastPoint := geometryBatchRow(namePrefix, totalRows-1)
	values, srids := scanGeometryValuesWithSRID(t, env.pgConn.QueryRow(fmt.Sprintf(`
			SELECT %s
		FROM events
		WHERE name = $1
		`, geometrySelectList(true)), lastName))
	assert.Equal(t, geometryExpected(pointWKT(lastPoint), coord(lastPoint.X), coord(lastPoint.Y)), values)
	assert.Equal(t, geometrySRIDExpected(), srids)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, fmt.Sprintf(`filter: { name: { eq: "%s" } }`, lastName), []map[string]any{
		geometryReadExpected(lastName, lastPoint, lastPoint.X, lastPoint.Y),
	})
}

// TestIngest_Postgres_ArrowIPCFile_NotFound checks that a missing file
// surfaces a clean error without touching the server.
func TestIngest_Postgres_ArrowIPCFile_NotFound(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestArrowIPCFile(context.Background(), "pg_ingest.events",
		filepath.Join(t.TempDir(), "does-not-exist.arrows"))
	require.Error(t, err)
}
