//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
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
	"github.com/apache/arrow-go/v18/arrow/extensions"
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

const ingestTestAPIKey = "ingest-test-api-key"

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
				auth.NewApiKey("ingest-test", auth.ApiKeyConfig{
					Key:         ingestTestAPIKey,
					DefaultRole: "admin",
				}),
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
			owner_id BIGINT,
			payload JSON,
			payload_large_string JSON,
			payload_string_view JSON,
			payload_binary JSON,
			payload_large_binary JSON,
			payload_binary_view JSON,
			payload_struct JSON,
			payload_list JSON,
			payload_large_list JSON,
			payload_fixed_size_list JSON,
			payload_list_view JSON,
			payload_large_list_view JSON,
			payload_map JSON,
			payload_scalar JSON,
			payload_arrow_json JSON,
			payload_geo_point JSON,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			geom GEOMETRY,
			geom_wkt GEOMETRY,
				geom_geojson GEOMETRY,
				geom_hugr_geojson GEOMETRY,
				geom_plain_geojson GEOMETRY,
				geom_geojson_struct GEOMETRY,
				geom_geojson_arrow_json GEOMETRY,
				geom_wkb GEOMETRY,
				geom_hexwkb GEOMETRY,
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

func registerIngestPermissionRole(t *testing.T, service *hugr.Service, role, mutationModule string) {
	t.Helper()
	registerIngestPermissionRoleData(t, service, role, mutationModule, map[string]any{
		"owner_id": "[$auth.user_id_int]",
	})
}

func registerIngestPermissionRoleData(t *testing.T, service *hugr.Service, role, mutationModule string, data map[string]any) {
	t.Helper()
	ctx := context.Background()
	mustQuery(t, ctx, service, `mutation($role: core_roles_mut_input_data!, $allowAll: core_role_permissions_mut_input_data!, $inject: core_role_permissions_mut_input_data!) {
		core {
			insert_roles(data: $role) { name }
			allow_all: insert_role_permissions(data: $allowAll) { role type_name field_name }
			inject_owner: insert_role_permissions(data: $inject) { role type_name field_name }
		}
	}`, map[string]any{
		"role": map[string]any{
			"name":        role,
			"description": "IPC ingest permission data integration test role",
		},
		"allowAll": map[string]any{
			"role":       role,
			"type_name":  "*",
			"field_name": "*",
		},
		"inject": map[string]any{
			"role":       role,
			"type_name":  mutationModule,
			"field_name": "insert_events",
			"data":       data,
		},
	})
}

func moduleMutationName(module string) string {
	return "_module_" + strings.ReplaceAll(module, ".", "_") + "_mutation"
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
	geoPointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
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
		{name: "payload_geo_point", dataType: geoPointType, arrowExtension: "geoarrow.point", expected: geoJSONGeometry("Point", pointCoordinate(xyPoint{x: 30.5, y: 50.25})), appendValue: appendGeoArrowJSONPoint(xyPoint{x: 30.5, y: 50.25})},
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

func appendGeoArrowJSONPoint(point xyPoint) func(*testing.T, array.Builder) {
	return func(t *testing.T, builder array.Builder) {
		t.Helper()
		structBuilder, ok := builder.(*array.StructBuilder)
		require.Truef(t, ok, "got %T, want *array.StructBuilder", builder)
		appendPoint(structBuilder, point)
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

func TestIngest_DuckDB_JSONPhysicalTypes(t *testing.T) {
	env := setupEnv(t)
	rec := makeJSONPhysicalTypesRecord(t)
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Inserted)
	expectedColumns := append([]string{"name", "value", "is_active"}, jsonPhysicalTypeColumns(t)...)
	assert.ElementsMatch(t, expectedColumns, res.Columns)
	assertJSONPhysicalTypesReadThroughHugr(t, env.service, env.dsName)
}

func TestIngest_DuckDB_RejectsMalformedJSON(t *testing.T) {
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

			_, err := env.client.IngestRecord(context.Background(), env.dataObject, rec)
			require.Error(t, err)

			ro := env.openRO(t)
			defer ro.Close()
			var count int
			require.NoError(t, ro.QueryRow("SELECT COUNT(*) FROM events").Scan(&count))
			assert.Zero(t, count, "a failed JSON cast must roll back the entire ingest")
		})
	}
}

func TestIngest_DuckDB_PermissionData(t *testing.T) {
	env := setupEnv(t)

	const ownerID = 4242
	role := "ingest_perm_" + env.dsName
	registerIngestPermissionRole(t, env.service, role, moduleMutationName(env.dsName))

	now := arrow.Timestamp(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC).UnixMicro())
	rec := makeEventsRecord(t,
		[]string{"perm-alpha", "perm-beta"},
		[]float64{11.5, 12.5},
		[]bool{true, true},
		[]string{"", ""},
		[]arrow.Timestamp{now, now},
	)
	defer rec.Release()

	permClient := hugrclient.NewClient(env.server.URL+"/ipc",
		hugrclient.WithApiKey(ingestTestAPIKey),
		hugrclient.WithUserRole(role),
		hugrclient.WithUserInfo(strconv.Itoa(ownerID), "permission-user"),
	)
	res, err := permClient.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "owner_id", "owner_id must be injected by permissions, not sent in Arrow")

	ro := env.openRO(t)
	defer ro.Close()
	rows, err := ro.Query("SELECT name, owner_id FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]int64{}
	for rows.Next() {
		var (
			name    string
			ownerID int64
		)
		require.NoError(t, rows.Scan(&name, &ownerID))
		got[name] = ownerID
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]int64{
		"perm-alpha": ownerID,
		"perm-beta":  ownerID,
	}, got)
}

func TestIngest_DuckDB_PermissionDataGeometry(t *testing.T) {
	env := setupEnv(t)

	role := "ingest_perm_geom_" + env.dsName
	registerIngestPermissionRoleData(t, env.service, role, moduleMutationName(env.dsName), map[string]any{
		"geom": "POINT (7.25 8.5)",
	})

	now := arrow.Timestamp(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC).UnixMicro())
	rec := makeEventsRecord(t,
		[]string{"perm-geom-alpha", "perm-geom-beta"},
		[]float64{21.5, 22.5},
		[]bool{true, true},
		[]string{"", ""},
		[]arrow.Timestamp{now, now},
	)
	defer rec.Release()

	permClient := hugrclient.NewClient(env.server.URL+"/ipc",
		hugrclient.WithApiKey(ingestTestAPIKey),
		hugrclient.WithUserRole(role),
		hugrclient.WithUserInfo("7", "permission-geometry-user"),
	)
	res, err := permClient.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "geom", "geom must be injected by permissions, not sent in Arrow")

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)

	rows, err := ro.Query("SELECT name, ST_AsText(geom) FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]string{}
	for rows.Next() {
		var name, geom string
		require.NoError(t, rows.Scan(&name, &geom))
		got[name] = compactWKT(geom)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]string{
		"perm-geom-alpha": "POINT(7.25 8.5)",
		"perm-geom-beta":  "POINT(7.25 8.5)",
	}, got)
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
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	recordFieldBuilder(t, b, "not_a_column").(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
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
	recordFieldBuilder(t, b, "x").(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
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
		fields := eventsRecordBuildersFor(b)
		fields.names.AppendValues(names, nil)
		vals := make([]float64, len(names))
		for i := range vals {
			vals[i] = float64(i)
		}
		fields.values.AppendValues(vals, nil)
		active := make([]bool, len(names))
		for i := range active {
			active[i] = true
		}
		fields.active.AppendValues(active, nil)
		fields.payloads.AppendNulls(len(names))
		ts := make([]arrow.Timestamp, len(names))
		for i := range ts {
			ts[i] = arrow.Timestamp(time.Now().UTC().UnixMicro())
		}
		fields.createdAt.AppendValues(ts, nil)
		return b.NewRecordBatch()
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
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).AppendValues([]string{"s1", "s2"}, nil)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).AppendValues([]float64{10, 20}, nil)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	rec := b.NewRecordBatch()
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
	assert.Equal(t, geometryExpected(pointWKT(lastPoint), coord(lastPoint.x), coord(lastPoint.y)), values)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, fmt.Sprintf(`filter: { name: { eq: "%s" } }`, lastName), []map[string]any{
		geometryReadExpected(lastName, lastPoint, lastPoint.x, lastPoint.y),
	})
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
		recordFieldBuilder(t, b, "x").(*array.Int32Builder).Append(v)
		return b.NewRecordBatch()
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
	recordFieldBuilder(t, bld, "name").(*array.StringBuilder).AppendValues([]string{"direct"}, nil)
	recordFieldBuilder(t, bld, "value").(*array.Float64Builder).AppendValues([]float64{42}, nil)
	recordFieldBuilder(t, bld, "is_active").(*array.BooleanBuilder).AppendValues([]bool{true}, nil)
	rec := bld.NewRecordBatch()
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

	rec, schema := makeGeometryTypesRecord(t, []geometryTypesRow{
		{name: "geo-a", value: 1, active: true, point: xyPoint{x: 30.5, y: 50.25}, shapeOrigin: xyPoint{x: 0, y: 0}},
		{name: "geo-b", value: 2, active: true, point: xyPoint{x: -73.935242, y: 40.730610}, shapeOrigin: xyPoint{x: 1, y: 1}},
	})
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

	rows, err := ro.Query(fmt.Sprintf(`
			SELECT name,
				%s
		FROM events
		WHERE name LIKE 'geo-%%'
		ORDER BY name
	`, geometrySelectList()))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	for rows.Next() {
		name, values := scanNamedGeometryValues(t, rows)
		got[name] = values
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
}

func TestIngest_HTTP_GeometryTypes_ReadThroughHugr_DuckDB(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []geometryTypesRow{
		{name: "geo-read-a", value: 1, active: true, point: xyPoint{x: 30.5, y: 50.25}, shapeOrigin: xyPoint{x: 0, y: 0}},
		{name: "geo-read-b", value: 2, active: true, point: xyPoint{x: -73.935242, y: 40.730610}, shapeOrigin: xyPoint{x: 1, y: 1}},
	})
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

	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { like: "geo-read-%" } }`, []map[string]any{
		geometryReadExpected("geo-read-a", xyPoint{x: 30.5, y: 50.25}, 0, 0),
		geometryReadExpected("geo-read-b", xyPoint{x: -73.935242, y: 40.730610}, 1, 1),
	})
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
			rec := buildGeometryTypesBatch(t, pool, schema, batchIdx, rowsPerBatch, namePrefix)
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

	values := scanGeometryValues(t, ro.QueryRow(fmt.Sprintf(`
		SELECT %s
		FROM events
		WHERE name = 'dk-geo-bulk-049999'
	`, geometrySelectList())))
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { eq: "dk-geo-bulk-049999" } }`, []map[string]any{
		geometryReadExpected("dk-geo-bulk-049999", xyPoint{x: 99, y: 49}, 99, 49),
	})

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

func eventsArrowFileSchema() *arrow.Schema {
	fields := append([]arrow.Field{}, eventsArrowSchema().Fields()...)
	fields = append(fields, geometryArrowFields()...)
	return arrow.NewSchema(fields, nil)
}

type geometryTypesRow struct {
	name        string
	value       float64
	active      bool
	point       xyPoint
	shapeOrigin xyPoint
}

func makeGeometryTypesRecord(t *testing.T, rows []geometryTypesRow) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()

	schema := geometryTypesSchema()
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for _, row := range rows {
		appendGeometryTypesRow(t, b, row)
	}

	return b.NewRecordBatch(), schema
}

func geometryTypesSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}
	fields = append(fields, geometryArrowFields()...)
	return arrow.NewSchema(fields, nil)
}

func geometryArrowFields() []arrow.Field {
	pointType := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	lineType := arrow.ListOf(pointType)
	polygonType := arrow.ListOf(lineType)
	fields := make([]arrow.Field, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		field := arrow.Field{
			Name:     col.name,
			Type:     col.arrowType,
			Nullable: false,
		}
		if col.arrowExtension != "" {
			field.Metadata = arrow.MetadataFrom(map[string]string{"ARROW:extension:name": col.arrowExtension})
		}
		fields = append(fields, field)
	}
	return fields
}

type geometryValueColumn struct {
	name           string
	arrowType      arrow.DataType
	arrowExtension string
	expectedWKT    func(point, x, y string) string
}

func geometryValueColumns(pointType, lineType, polygonType arrow.DataType) []geometryValueColumn {
	geoJSONStructType := arrow.StructOf(
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "coordinates", Type: arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Float64))), Nullable: false},
	)
	line := func(_ string, x string, y string) string {
		return fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1))
	}
	polygon := func(_ string, x string, y string) string { return polygonWKT(x, y) }
	point := func(point string, _ string, _ string) string { return point }
	multiPoint := func(_ string, x string, y string) string {
		return fmt.Sprintf("MULTIPOINT(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), y)
	}
	multiLine := func(_ string, x string, y string) string {
		return fmt.Sprintf("MULTILINESTRING((%s %s,%s %s),(%s %s,%s %s))", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 2), addCoord(x, 3), addCoord(y, 3))
	}
	multiPolygon := func(_ string, x string, y string) string { return multiPolygonWKT(x, y) }

	return []geometryValueColumn{
		{name: "geom", arrowType: pointType, arrowExtension: "geoarrow.point", expectedWKT: point},
		{name: "geom_wkt", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.wkt", expectedWKT: line},
		{name: "geom_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.geojson", expectedWKT: polygon},
		{name: "geom_hugr_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "hugr.geojson", expectedWKT: polygon},
		{name: "geom_plain_geojson", arrowType: arrow.BinaryTypes.String, arrowExtension: "geojson", expectedWKT: polygon},
		{name: "geom_geojson_struct", arrowType: geoJSONStructType, expectedWKT: polygon},
		{name: "geom_geojson_arrow_json", arrowType: mustArrowJSONType(), arrowExtension: "arrow.json", expectedWKT: polygon},
		{name: "geom_wkb", arrowType: arrow.BinaryTypes.Binary, arrowExtension: "geoarrow.wkb", expectedWKT: point},
		{name: "geom_hexwkb", arrowType: arrow.BinaryTypes.String, arrowExtension: "hugr.hexwkb", expectedWKT: point},
		{name: "geom_line", arrowType: lineType, arrowExtension: "geoarrow.linestring", expectedWKT: line},
		{name: "geom_polygon_native", arrowType: polygonType, arrowExtension: "geoarrow.polygon", expectedWKT: polygon},
		{name: "geom_multipoint", arrowType: lineType, arrowExtension: "geoarrow.multipoint", expectedWKT: multiPoint},
		{name: "geom_multiline", arrowType: polygonType, arrowExtension: "geoarrow.multilinestring", expectedWKT: multiLine},
		{name: "geom_multipolygon", arrowType: arrow.ListOf(polygonType), arrowExtension: "geoarrow.multipolygon", expectedWKT: multiPolygon},
	}
}

func mustArrowJSONType() arrow.DataType {
	typ, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		panic(err)
	}
	return typ
}

func geometryTypesColumns() []string {
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := []string{"name", "value", "is_active"}
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		columns = append(columns, col.name)
	}
	return columns
}

func geometryExpected(point, x, y string) []string {
	pointType, lineType, polygonType := geometryArrowTypes()
	values := make([]string, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		values = append(values, col.expectedWKT(point, x, y))
	}
	return values
}

func geometryArrowTypes() (pointType, lineType, polygonType arrow.DataType) {
	pointType = arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	lineType = arrow.ListOf(pointType)
	polygonType = arrow.ListOf(lineType)
	return pointType, lineType, polygonType
}

func geometrySelectList() string {
	pointType, lineType, polygonType := geometryArrowTypes()
	exprs := make([]string, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		exprs = append(exprs, "ST_AsText("+col.name+")")
	}
	return strings.Join(exprs, ",\n")
}

type sqlScanner interface {
	Scan(dest ...any) error
}

func scanGeometryValues(t *testing.T, scanner sqlScanner) []string {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	values := make([]string, len(columns))
	scanArgs := make([]any, 0, len(columns))
	for i := range columns {
		scanArgs = append(scanArgs, &values[i])
	}
	require.NoError(t, scanner.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return values
}

func scanNamedGeometryValues(t *testing.T, rows *sql.Rows) (string, []string) {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	var name string
	values := make([]string, len(columns))
	scanArgs := []any{&name}
	for i := range columns {
		scanArgs = append(scanArgs, &values[i])
	}
	require.NoError(t, rows.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return name, values
}

func polygonWKT(x, y string) string {
	return fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s),(%s %s,%s %s,%s %s,%s %s,%s %s))",
		x, y,
		x, addCoord(y, 4),
		addCoord(x, 4), addCoord(y, 4),
		addCoord(x, 4), y,
		x, y,
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 1),
	)
}

func multiPolygonWKT(x, y string) string {
	return fmt.Sprintf("MULTIPOLYGON(((%s %s,%s %s,%s %s,%s %s,%s %s),(%s %s,%s %s,%s %s,%s %s,%s %s)),((%s %s,%s %s,%s %s,%s %s,%s %s)))",
		x, y,
		x, addCoord(y, 4),
		addCoord(x, 4), addCoord(y, 4),
		addCoord(x, 4), y,
		x, y,
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 1),
		addCoord(x, 2), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 2),
		addCoord(x, 1), addCoord(y, 1),
		addCoord(x, 10), addCoord(y, 10),
		addCoord(x, 10), addCoord(y, 12),
		addCoord(x, 12), addCoord(y, 12),
		addCoord(x, 12), addCoord(y, 10),
		addCoord(x, 10), addCoord(y, 10),
	)
}

func assertGeometryReadThroughHugr(t *testing.T, service *hugr.Service, dsName, filter string, expected []map[string]any) {
	t.Helper()

	query := fmt.Sprintf(`{
		%s {
			events(%s, order_by: [{field: "name", direction: ASC}]) {
				name
				geom
					geom_wkt
					geom_geojson
					geom_hugr_geojson
					geom_plain_geojson
					geom_geojson_struct
					geom_geojson_arrow_json
					geom_wkb
					geom_hexwkb
				geom_line
				geom_polygon_native
				geom_multipoint
				geom_multiline
				geom_multipolygon
			}
		}
	}`, dsName, filter)

	res, err := service.Query(context.Background(), query, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoErrorf(t, res.Err(), "graphql error for query: %s", query)

	body, err := json.Marshal(res)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	data, ok := payload["data"].(map[string]any)
	require.True(t, ok, "response data must be an object: %s", string(body))
	root, ok := data[dsName].(map[string]any)
	require.True(t, ok, "response data.%s must be an object: %s", dsName, string(body))
	rawRows, ok := root["events"].([]any)
	require.True(t, ok, "response data.%s.events must be an array: %s", dsName, string(body))

	got := make([]map[string]any, 0, len(rawRows))
	for _, raw := range rawRows {
		row, ok := raw.(map[string]any)
		require.True(t, ok, "event row must be an object: %#v", raw)
		got = append(got, row)
	}
	assert.Equal(t, expected, got)
}

func geometryReadExpected(name string, point xyPoint, x, y float64) map[string]any {
	return map[string]any{
		"name":                    name,
		"geom":                    geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_wkt":                geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_geojson":            geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_hugr_geojson":       geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_plain_geojson":      geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_geojson_struct":     geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_geojson_arrow_json": geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_wkb":                geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_hexwkb":             geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_line":               geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_polygon_native":     geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_multipoint":         geoJSONGeometry("MultiPoint", pointCoordinates(multiPoints(x, y))),
		"geom_multiline":          geoJSONGeometry("MultiLineString", nestedPointCoordinates(multiLines(x, y))),
		"geom_multipolygon":       geoJSONGeometry("MultiPolygon", deepPointCoordinates(multiPolygons(x, y))),
	}
}

func geoJSONGeometry(typ string, coordinates any) map[string]any {
	return map[string]any{
		"type":        typ,
		"coordinates": coordinates,
	}
}

func pointCoordinate(point xyPoint) []any {
	return []any{point.x, point.y}
}

func pointCoordinates(points []xyPoint) []any {
	coords := make([]any, 0, len(points))
	for _, point := range points {
		coords = append(coords, pointCoordinate(point))
	}
	return coords
}

func nestedPointCoordinates(lines [][]xyPoint) []any {
	coords := make([]any, 0, len(lines))
	for _, line := range lines {
		coords = append(coords, pointCoordinates(line))
	}
	return coords
}

func deepPointCoordinates(polygons [][][]xyPoint) []any {
	coords := make([]any, 0, len(polygons))
	for _, polygon := range polygons {
		coords = append(coords, nestedPointCoordinates(polygon))
	}
	return coords
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

func buildGeometryTypesBatch(t *testing.T, pool memory.Allocator, schema *arrow.Schema, batchIdx, rowsPerBatch int, namePrefix string) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i := 0; i < rowsPerBatch; i++ {
		row := batchIdx*rowsPerBatch + i
		name, point := geometryBatchRow(namePrefix, row)
		appendGeometryTypesRow(t, b, geometryTypesRow{
			name:        name,
			value:       float64(row) * 0.5,
			active:      row%2 == 0,
			point:       point,
			shapeOrigin: point,
		})
	}
	return b.NewRecordBatch()
}

func geometryBatchRow(namePrefix string, row int) (string, xyPoint) {
	return fmt.Sprintf("%s-%06d", namePrefix, row), xyPoint{
		x: float64(row % 100),
		y: float64(row / 1000),
	}
}

func appendGeometryTypesRow(t *testing.T, b *array.RecordBuilder, row geometryTypesRow) {
	t.Helper()
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append(row.name)
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(row.value)
	recordFieldBuilder(t, b, "is_active").(*array.BooleanBuilder).Append(row.active)
	appendGeometryValueFields(t, b, row)
}

func appendGeometryValueFields(t *testing.T, b *array.RecordBuilder, row geometryTypesRow) {
	t.Helper()
	x, y := row.shapeOrigin.x, row.shapeOrigin.y

	appendPoint(recordFieldBuilder(t, b, "geom").(*array.StructBuilder), row.point)
	recordFieldBuilder(t, b, "geom_wkt").(*array.StringBuilder).Append(lineWKT(x, y))
	recordFieldBuilder(t, b, "geom_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	recordFieldBuilder(t, b, "geom_hugr_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	recordFieldBuilder(t, b, "geom_plain_geojson").(*array.StringBuilder).Append(polygonGeoJSON(x, y))
	appendGeoJSONPolygonStruct(t, recordFieldBuilder(t, b, "geom_geojson_struct"), x, y)
	recordFieldBuilder(t, b, "geom_geojson_arrow_json").(*array.ExtensionBuilder).StorageBuilder().(*array.StringBuilder).Append(polygonGeoJSON(x, y))

	wkbPoint, err := wkb.Marshal(orb.Point{row.point.x, row.point.y})
	require.NoError(t, err)
	recordFieldBuilder(t, b, "geom_wkb").(*array.BinaryBuilder).Append(wkbPoint)
	recordFieldBuilder(t, b, "geom_hexwkb").(*array.StringBuilder).Append(strings.ToUpper(hex.EncodeToString(wkbPoint)))
	appendPointList(recordFieldBuilder(t, b, "geom_line").(*array.ListBuilder), linePoints(x, y))
	appendPointListList(recordFieldBuilder(t, b, "geom_polygon_native").(*array.ListBuilder), polygonRings(x, y))
	appendPointList(recordFieldBuilder(t, b, "geom_multipoint").(*array.ListBuilder), multiPoints(x, y))
	appendPointListList(recordFieldBuilder(t, b, "geom_multiline").(*array.ListBuilder), multiLines(x, y))
	appendPointListListList(recordFieldBuilder(t, b, "geom_multipolygon").(*array.ListBuilder), multiPolygons(x, y))
}

type xyPoint struct {
	x float64
	y float64
}

const (
	geoArrowPointXField = iota
	geoArrowPointYField
)

const (
	geoJSONGeometryTypeField = iota
	geoJSONGeometryCoordinatesField
)

func appendPoint(sb *array.StructBuilder, point xyPoint) {
	sb.Append(true)
	sb.FieldBuilder(geoArrowPointXField).(*array.Float64Builder).Append(point.x)
	sb.FieldBuilder(geoArrowPointYField).(*array.Float64Builder).Append(point.y)
}

func appendGeoJSONPolygonStruct(t *testing.T, builder array.Builder, x, y float64) {
	t.Helper()
	sb, ok := builder.(*array.StructBuilder)
	require.Truef(t, ok, "got %T, want *array.StructBuilder", builder)

	sb.Append(true)
	sb.FieldBuilder(geoJSONGeometryTypeField).(*array.StringBuilder).Append("Polygon")
	appendGeoJSONPolygonCoordinates(sb.FieldBuilder(geoJSONGeometryCoordinatesField).(*array.ListBuilder), polygonRings(x, y))
}

func appendGeoJSONPolygonCoordinates(lb *array.ListBuilder, rings [][]xyPoint) {
	lb.Append(true)
	ringBuilder := lb.ValueBuilder().(*array.ListBuilder)
	for _, ring := range rings {
		ringBuilder.Append(true)
		pointBuilder := ringBuilder.ValueBuilder().(*array.ListBuilder)
		for _, point := range ring {
			pointBuilder.Append(true)
			pointBuilder.ValueBuilder().(*array.Float64Builder).AppendValues([]float64{point.x, point.y}, nil)
		}
	}
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
	return []xyPoint{{x: x, y: y}, {x: x + 1, y: y + 1}, {x: x + 2, y: y + 1}}
}

func polygonRings(x, y float64) [][]xyPoint {
	return [][]xyPoint{
		{{x: x, y: y}, {x: x, y: y + 4}, {x: x + 4, y: y + 4}, {x: x + 4, y: y}, {x: x, y: y}},
		{{x: x + 1, y: y + 1}, {x: x + 2, y: y + 1}, {x: x + 2, y: y + 2}, {x: x + 1, y: y + 2}, {x: x + 1, y: y + 1}},
	}
}

func multiPoints(x, y float64) []xyPoint {
	return []xyPoint{{x: x, y: y}, {x: x + 1, y: y + 1}, {x: x + 2, y: y}}
}

func multiLines(x, y float64) [][]xyPoint {
	return [][]xyPoint{
		{{x: x, y: y}, {x: x + 1, y: y + 1}},
		{{x: x + 2, y: y + 2}, {x: x + 3, y: y + 3}},
	}
}

func multiPolygons(x, y float64) [][][]xyPoint {
	return [][][]xyPoint{
		polygonRings(x, y),
		{{{x: x + 10, y: y + 10}, {x: x + 10, y: y + 12}, {x: x + 12, y: y + 12}, {x: x + 12, y: y + 10}, {x: x + 10, y: y + 10}}},
	}
}

func lineWKT(x, y float64) string {
	return fmt.Sprintf("LINESTRING (%s %s, %s %s, %s %s)",
		coord(x), coord(y),
		coord(x+1), coord(y+1),
		coord(x+2), coord(y+1))
}

func pointWKT(point xyPoint) string {
	return fmt.Sprintf("POINT(%s %s)", coord(point.x), coord(point.y))
}

func polygonGeoJSON(x, y float64) string {
	return fmt.Sprintf(`{"type":"Polygon","coordinates":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]],[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}`,
		coord(x), coord(y),
		coord(x), coord(y+4),
		coord(x+4), coord(y+4),
		coord(x+4), coord(y),
		coord(x), coord(y),
		coord(x+1), coord(y+1),
		coord(x+2), coord(y+1),
		coord(x+2), coord(y+2),
		coord(x+1), coord(y+2),
		coord(x+1), coord(y+1))
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

// Silence "imported and not used" if a refactor leaves a quoted ref around.
var _ atomic.Int64
