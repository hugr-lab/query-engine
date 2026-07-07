//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"context"
	"database/sql"
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

// TestIngest_DuckDB_ArrowIPCFile_StreamFormat writes a 50k-row Arrow IPC
// stream file and verifies IngestArrowIPCFile forwards it to /ipc/ingest.
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
	assertArrowIPCFileGeometry(t, env, ro, namePrefix, totalRows)

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
	assertArrowIPCFileGeometry(t, env, ro, namePrefix, totalRows)

	t.Logf("arrow ipc file-format ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
}

func assertArrowIPCFileGeometry(t *testing.T, env *ingestEnv, ro *sql.DB, namePrefix string, totalRows int) {
	t.Helper()
	_, err := ro.Exec("LOAD spatial")
	require.NoError(t, err)

	lastName, lastPoint := geometryBatchRow(namePrefix, totalRows-1)
	values := scanGeometryValues(t, ro.QueryRow(fmt.Sprintf(`
		SELECT %s
		FROM events
		WHERE name = ?
	`, geometrySelectList()), lastName))
	assert.Equal(t, geometryExpected(pointWKT(lastPoint), coord(lastPoint.X), coord(lastPoint.Y)), values)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, fmt.Sprintf(`filter: { name: { eq: "%s" } }`, lastName), []map[string]any{
		geometryReadExpected(lastName, lastPoint, lastPoint.X, lastPoint.Y),
	})
}

func TestIngest_DuckDB_ArrowIPCFile_NotFound(t *testing.T) {
	env := setupEnv(t)
	_, err := env.client.IngestArrowIPCFile(context.Background(), env.dataObject,
		filepath.Join(t.TempDir(), "does-not-exist.arrows"))
	require.Error(t, err)
}

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

// writeEventsArrowFile writes an Arrow IPC file (stream or file format) at
// path with `numBatches * rowsPerBatch` rows for the events schema.
func writeEventsArrowFile(t *testing.T, path, namePrefix string, format arrowFileFormat, numBatches, rowsPerBatch int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := eventsArrowFileSchema()
	base := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	writeArrowIPCFile(t, path, schema, format, numBatches, func(batchIdx int) arrow.RecordBatch {
		rb := array.NewRecordBuilder(pool, schema)
		defer rb.Release()
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
		return rb.NewRecordBatch()
	})
}
