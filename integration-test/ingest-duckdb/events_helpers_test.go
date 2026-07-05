//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

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
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues(names, nil)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).AppendValues(values, nil)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).AppendValues(active, nil)
	pBuilder := recordFieldBuilder(t, b, "payload").(*array.StringBuilder)
	for _, p := range payload {
		if p == "" {
			pBuilder.AppendNull()
		} else {
			pBuilder.Append(p)
		}
	}
	tsBuilder := recordFieldBuilder(t, b, "created_at").(*array.TimestampBuilder)
	tsBuilder.AppendValues(created, nil)
	return b.NewRecordBatch()
}

func recordFieldBuilder(t *testing.T, b *array.RecordBuilder, name string) array.Builder {
	t.Helper()
	indices := b.Schema().FieldIndices(name)
	require.Len(t, indices, 1, "arrow field %q must exist exactly once", name)
	return b.Field(indices[0])
}

func mustRecordFieldBuilder(b *array.RecordBuilder, name string) array.Builder {
	indices := b.Schema().FieldIndices(name)
	if len(indices) != 1 {
		panic(fmt.Sprintf("arrow field %q must exist exactly once", name))
	}
	return b.Field(indices[0])
}

type eventsRecordBuilders struct {
	names     *array.StringBuilder
	values    *array.Float64Builder
	active    *array.BooleanBuilder
	payloads  *array.StringBuilder
	createdAt *array.TimestampBuilder
}

func eventsRecordBuildersFor(b *array.RecordBuilder) eventsRecordBuilders {
	return eventsRecordBuilders{
		names:     mustRecordFieldBuilder(b, "name").(*array.StringBuilder),
		values:    mustRecordFieldBuilder(b, "value").(*array.Float64Builder),
		active:    mustRecordFieldBuilder(b, "is_active").(*array.BooleanBuilder),
		payloads:  mustRecordFieldBuilder(b, "payload").(*array.StringBuilder),
		createdAt: mustRecordFieldBuilder(b, "created_at").(*array.TimestampBuilder),
	}
}

func eventsArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
}

func eventsArrowFileSchema() *arrow.Schema {
	fields := append([]arrow.Field{}, eventsArrowSchema().Fields()...)
	fields = append(fields, geometryArrowFields()...)
	return arrow.NewSchema(fields, nil)
}

func buildEventsBatch(pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string, base time.Time) arrow.RecordBatch {
	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()
	fields := eventsRecordBuildersFor(rb)
	for i := 0; i < rowsPerBatch; i++ {
		row := batchIdx*rowsPerBatch + i
		fields.names.Append(fmt.Sprintf("%s-%06d", namePrefix, row))
		fields.values.Append(float64(row) * 0.5)
		fields.active.Append(row%2 == 0)
		if row%5 == 0 {
			fields.payloads.AppendNull()
		} else {
			fields.payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		fields.createdAt.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	return rb.NewRecordBatch()
}
