//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		fields := eventsRecordBuildersFor(rb)
		for i := 0; i < rowsPerBatch; i++ {
			row := batchIdx*rowsPerBatch + i
			fields.names.Append(fmt.Sprintf("lz-%06d", row))
			fields.values.Append(float64(row) * 0.5)
			fields.active.Append(row%2 == 0)
			if row%5 == 0 {
				fields.payloads.AppendNull()
			} else {
				fields.payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
			}
			fields.createdAt.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
		}
		rec := rb.NewRecordBatch()
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
		recordFieldBuilder(t, b, "x").(*array.Int32Builder).Append(v)
		return b.NewRecordBatch()
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
	fields := eventsRecordBuildersFor(rb)
	for i := 0; i < r.rowsPerBatch; i++ {
		row := r.batchIdx*r.rowsPerBatch + i
		fields.names.Append(fmt.Sprintf("evt-%06d", row))
		fields.values.Append(float64(row) * 0.5)
		fields.active.Append(row%2 == 0)
		if row%5 == 0 {
			fields.payloads.AppendNull()
		} else {
			fields.payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		fields.createdAt.Append(arrow.Timestamp(r.base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	r.current = rb.NewRecordBatch()
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
