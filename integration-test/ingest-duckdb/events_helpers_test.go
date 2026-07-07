//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/query-engine/integration-test/internal/ingesttest"
)

func makeEventsRecord(t *testing.T, names []string, values []float64, active []bool, payload []string, created []arrow.Timestamp) arrow.RecordBatch {
	t.Helper()
	return ingesttest.MakeEventsRecord(t, names, values, active, payload, created)
}

func recordFieldBuilder(t *testing.T, b *array.RecordBuilder, name string) array.Builder {
	t.Helper()
	return ingesttest.RecordFieldBuilder(t, b, name)
}

func mustRecordFieldBuilder(b *array.RecordBuilder, name string) array.Builder {
	return ingesttest.MustRecordFieldBuilder(b, name)
}

type eventsRecordBuilders struct {
	names     *array.StringBuilder
	values    *array.Float64Builder
	active    *array.BooleanBuilder
	payloads  *array.StringBuilder
	createdAt *array.TimestampBuilder
}

func eventsRecordBuildersFor(b *array.RecordBuilder) eventsRecordBuilders {
	fields := ingesttest.EventsRecordBuildersFor(b)
	return eventsRecordBuilders{
		names:     fields.Names,
		values:    fields.Values,
		active:    fields.Active,
		payloads:  fields.Payloads,
		createdAt: fields.CreatedAt,
	}
}

func eventsArrowSchema() *arrow.Schema {
	return ingesttest.EventsArrowSchema()
}

func eventsArrowFileSchema() *arrow.Schema {
	return ingesttest.EventsArrowFileSchema(geometryArrowFields())
}

func buildEventsBatch(pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string, base time.Time) arrow.RecordBatch {
	return ingesttest.BuildEventsBatch(pool, schema, batchIdx, rowsPerBatch, namePrefix, base)
}
