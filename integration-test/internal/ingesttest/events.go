//go:build duckdb_arrow

package ingesttest

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func MakeEventsRecord(t testing.TB, names []string, values []float64, active []bool, payload []string, created []arrow.Timestamp) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := EventsArrowSchema()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	RecordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues(names, nil)
	RecordFieldBuilder(t, b, "value").(*array.Float64Builder).AppendValues(values, nil)
	RecordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).AppendValues(active, nil)
	pBuilder := RecordFieldBuilder(t, b, "payload").(*array.StringBuilder)
	for _, p := range payload {
		if p == "" {
			pBuilder.AppendNull()
		} else {
			pBuilder.Append(p)
		}
	}
	RecordFieldBuilder(t, b, "created_at").(*array.TimestampBuilder).AppendValues(created, nil)
	return b.NewRecordBatch()
}

func RecordFieldBuilder(t testing.TB, b *array.RecordBuilder, name string) array.Builder {
	t.Helper()
	indices := b.Schema().FieldIndices(name)
	require.Len(t, indices, 1, "arrow field %q must exist exactly once", name)
	return b.Field(indices[0])
}

func MustRecordFieldBuilder(b *array.RecordBuilder, name string) array.Builder {
	indices := b.Schema().FieldIndices(name)
	if len(indices) != 1 {
		panic(fmt.Sprintf("arrow field %q must exist exactly once", name))
	}
	return b.Field(indices[0])
}

type EventsRecordBuilders struct {
	Names     *array.StringBuilder
	Values    *array.Float64Builder
	Active    *array.BooleanBuilder
	Payloads  *array.StringBuilder
	CreatedAt *array.TimestampBuilder
}

func EventsRecordBuildersFor(b *array.RecordBuilder) EventsRecordBuilders {
	return EventsRecordBuilders{
		Names:     MustRecordFieldBuilder(b, "name").(*array.StringBuilder),
		Values:    MustRecordFieldBuilder(b, "value").(*array.Float64Builder),
		Active:    MustRecordFieldBuilder(b, "is_active").(*array.BooleanBuilder),
		Payloads:  MustRecordFieldBuilder(b, "payload").(*array.StringBuilder),
		CreatedAt: MustRecordFieldBuilder(b, "created_at").(*array.TimestampBuilder),
	}
}

func EventsArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
}

func EventsArrowFileSchema(geometryFields []arrow.Field) *arrow.Schema {
	fields := append([]arrow.Field{}, EventsArrowSchema().Fields()...)
	fields = append(fields, geometryFields...)
	return arrow.NewSchema(fields, nil)
}

func BuildEventsBatch(pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string, base time.Time) arrow.RecordBatch {
	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()
	fields := EventsRecordBuildersFor(rb)
	for i := 0; i < rowsPerBatch; i++ {
		row := batchIdx*rowsPerBatch + i
		fields.Names.Append(fmt.Sprintf("%s-%06d", namePrefix, row))
		fields.Values.Append(float64(row) * 0.5)
		fields.Active.Append(row%2 == 0)
		if row%5 == 0 {
			fields.Payloads.AppendNull()
		} else {
			fields.Payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		fields.CreatedAt.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	return rb.NewRecordBatch()
}
