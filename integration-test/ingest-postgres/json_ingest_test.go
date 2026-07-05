//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	hugr "github.com/hugr-lab/query-engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeMalformedJSONRecord(t *testing.T, binary bool) arrow.RecordBatch {
	t.Helper()
	payloadType := arrow.DataType(arrow.BinaryTypes.String)
	payloadName := "payload"
	if binary {
		payloadType = arrow.BinaryTypes.Binary
		payloadName = "payload_binary"
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: payloadName, Type: payloadType, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append("malformed-json")
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(1)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(true)
	payloadBuilder := recordFieldBuilder(t, b, payloadName)
	if binary {
		payloadBuilder.(*array.BinaryBuilder).Append([]byte(`{"unterminated":`))
	} else {
		payloadBuilder.(*array.StringBuilder).Append(`{"unterminated":`)
	}
	return b.NewRecordBatch()
}

type jsonPhysicalTypeSpec struct {
	name           string
	dataType       arrow.DataType
	arrowExtension string
	expected       any
	appendValue    func(*testing.T, array.Builder)
}

const (
	jsonStructKindField = iota
	jsonStructCountField
)

func jsonPhysicalTypeSpecs(t *testing.T) []jsonPhysicalTypeSpec {
	t.Helper()
	structType := arrow.StructOf(
		arrow.Field{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	)
	arrowJSONType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	require.NoError(t, err)

	return []jsonPhysicalTypeSpec{
		{name: "payload", dataType: arrow.BinaryTypes.String, expected: map[string]any{"kind": "string"}, appendValue: appendJSONText(`{"kind":"string"}`)},
		{name: "payload_large_string", dataType: arrow.BinaryTypes.LargeString, expected: map[string]any{"kind": "large_string"}, appendValue: appendJSONText(`{"kind":"large_string"}`)},
		{name: "payload_string_view", dataType: arrow.BinaryTypes.StringView, expected: map[string]any{"kind": "string_view"}, appendValue: appendJSONText(`{"kind":"string_view"}`)},
		{name: "payload_binary", dataType: arrow.BinaryTypes.Binary, expected: map[string]any{"kind": "binary"}, appendValue: appendJSONText(`{"kind":"binary"}`)},
		{name: "payload_large_binary", dataType: arrow.BinaryTypes.LargeBinary, expected: map[string]any{"kind": "large_binary"}, appendValue: appendJSONText(`{"kind":"large_binary"}`)},
		{name: "payload_binary_view", dataType: arrow.BinaryTypes.BinaryView, expected: map[string]any{"kind": "binary_view"}, appendValue: appendJSONText(`{"kind":"binary_view"}`)},
		{name: "payload_struct", dataType: structType, expected: map[string]any{"kind": "struct", "count": float64(14)}, appendValue: appendJSONStruct("struct", 14)},
		{name: "payload_list", dataType: arrow.ListOf(arrow.PrimitiveTypes.Int64), expected: []any{float64(1), float64(2)}, appendValue: appendInt64JSONList(1, 2)},
		{name: "payload_large_list", dataType: arrow.LargeListOf(arrow.PrimitiveTypes.Int64), expected: []any{float64(3), float64(4)}, appendValue: appendInt64JSONList(3, 4)},
		{name: "payload_fixed_size_list", dataType: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64), expected: []any{float64(5), float64(6)}, appendValue: appendInt64JSONList(5, 6)},
		{name: "payload_list_view", dataType: arrow.ListViewOf(arrow.PrimitiveTypes.Int64), expected: []any{float64(7), float64(8)}, appendValue: appendInt64JSONList(7, 8)},
		{name: "payload_large_list_view", dataType: arrow.LargeListViewOf(arrow.PrimitiveTypes.Int64), expected: []any{float64(9), float64(10)}, appendValue: appendInt64JSONList(9, 10)},
		{name: "payload_map", dataType: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), expected: map[string]any{"a": float64(11), "b": float64(12)}, appendValue: appendInt64JSONMap([]string{"a", "b"}, []int64{11, 12})},
		{name: "payload_scalar", dataType: arrow.PrimitiveTypes.Int64, expected: "13", appendValue: appendInt64JSONScalar(13)},
		{name: "payload_arrow_json", dataType: arrowJSONType, expected: map[string]any{"kind": "arrow_json"}, appendValue: appendArrowJSONText(`{"kind":"arrow_json"}`)},
	}
}

func jsonPhysicalTypeColumns(t *testing.T) []string {
	t.Helper()
	specs := jsonPhysicalTypeSpecs(t)
	columns := make([]string, 0, len(specs))
	for _, spec := range specs {
		columns = append(columns, spec.name)
	}
	return columns
}

func makeJSONPhysicalTypesRecord(t *testing.T) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	specs := jsonPhysicalTypeSpecs(t)
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}
	for _, spec := range specs {
		field := arrow.Field{Name: spec.name, Type: spec.dataType, Nullable: false}
		if spec.arrowExtension != "" {
			field.Metadata = arrow.MetadataFrom(map[string]string{"ARROW:extension:name": spec.arrowExtension})
		}
		fields = append(fields, field)
	}
	schema := arrow.NewSchema(fields, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append("json-physical-types")
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(1)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(true)
	for _, spec := range specs {
		spec.appendValue(t, recordFieldBuilder(t, b, spec.name))
	}
	return b.NewRecordBatch()
}

func appendJSONText(value string) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		switch b := builder.(type) {
		case *array.StringBuilder:
			b.Append(value)
		case *array.LargeStringBuilder:
			b.Append(value)
		case *array.StringViewBuilder:
			b.Append(value)
		case *array.BinaryBuilder:
			b.Append([]byte(value))
		case *array.BinaryViewBuilder:
			b.Append([]byte(value))
		default:
			require.Failf(t, "unsupported JSON text builder", "got %T", builder)
		}
	}
}

func appendJSONStruct(kind string, count int64) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		structBuilder, ok := builder.(*array.StructBuilder)
		require.Truef(t, ok, "got %T, want *array.StructBuilder", builder)
		structBuilder.Append(true)
		structBuilder.FieldBuilder(jsonStructKindField).(*array.StringBuilder).Append(kind)
		structBuilder.FieldBuilder(jsonStructCountField).(*array.Int64Builder).Append(count)
	}
}

func appendInt64JSONList(values ...int64) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		switch b := builder.(type) {
		case *array.ListBuilder:
			b.Append(true)
			b.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
		case *array.LargeListBuilder:
			b.Append(true)
			b.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
		case *array.FixedSizeListBuilder:
			b.Append(true)
			b.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
		case *array.ListViewBuilder:
			b.AppendWithSize(true, len(values))
			b.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
		case *array.LargeListViewBuilder:
			b.AppendWithSize(true, len(values))
			b.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
		default:
			require.Failf(t, "unsupported JSON list builder", "got %T", builder)
		}
	}
}

func appendInt64JSONMap(keys []string, values []int64) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		mapBuilder, ok := builder.(*array.MapBuilder)
		require.Truef(t, ok, "got %T, want *array.MapBuilder", builder)
		mapBuilder.Append(true)
		mapBuilder.KeyBuilder().(*array.StringBuilder).AppendValues(keys, nil)
		mapBuilder.ItemBuilder().(*array.Int64Builder).AppendValues(values, nil)
	}
}

func appendInt64JSONScalar(value int64) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		intBuilder, ok := builder.(*array.Int64Builder)
		require.Truef(t, ok, "got %T, want *array.Int64Builder", builder)
		intBuilder.Append(value)
	}
}

func appendArrowJSONText(value string) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		extensionBuilder, ok := builder.(*array.ExtensionBuilder)
		require.Truef(t, ok, "got %T, want *array.ExtensionBuilder", builder)
		extensionBuilder.StorageBuilder().(*array.StringBuilder).Append(value)
	}
}

func jsonPhysicalTypesExpected(t *testing.T) map[string]any {
	t.Helper()
	expected := map[string]any{"name": "json-physical-types"}
	for _, spec := range jsonPhysicalTypeSpecs(t) {
		expected[spec.name] = spec.expected
	}
	return expected
}

func assertJSONPhysicalTypesReadThroughHugr(t *testing.T, service *hugr.Service, dsName string) {
	t.Helper()
	query := fmt.Sprintf(`{
		%s {
			events(filter: {name: {eq: "json-physical-types"}}) {
				name
				%s
			}
		}
	}`, dsName, strings.Join(jsonPhysicalTypeColumns(t), "\n"))
	res, err := service.Query(context.Background(), query, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoErrorf(t, res.Err(), "graphql error for query: %s", query)

	body, err := json.Marshal(res)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	data := payload["data"].(map[string]any)
	root := data[dsName].(map[string]any)
	rows := root["events"].([]any)
	require.Len(t, rows, 1, "response: %s", string(body))
	assert.Equal(t, jsonPhysicalTypesExpected(t), rows[0])
}

// --- Tests ----------------------------------------------------------------

func TestIngest_Postgres_JSONPhysicalTypes(t *testing.T) {
	env := setupEnv(t)
	rec := makeJSONPhysicalTypesRecord(t)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Inserted)
	expectedColumns := append([]string{"name", "value", "is_active"}, jsonPhysicalTypeColumns(t)...)
	assert.ElementsMatch(t, expectedColumns, res.Columns)
	assertJSONPhysicalTypesReadThroughHugr(t, env.service, env.dsName)
}

func TestIngest_Postgres_RejectsMalformedJSON(t *testing.T) {
	for _, tt := range []struct {
		name   string
		binary bool
	}{
		{name: "string"},
		{name: "binary", binary: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := setupEnv(t)
			rec := makeMalformedJSONRecord(t, tt.binary)
			defer rec.Release()

			_, err := env.client.IngestRecord(context.Background(), "pg_ingest.events", rec)
			require.Error(t, err)

			var count int
			require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
			assert.Zero(t, count, "a failed JSON cast must roll back the entire ingest")
		})
	}
}
