//go:build duckdb_arrow

package ingest_postgres_test

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
	_ "github.com/jackc/pgx/v5/stdlib"
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

const (
	envPostgresDSN   = "INGEST_POSTGRES_DSN"
	envSchemasPath   = "HUGR_INGEST_SCHEMAS_PATH"
	ingestTestAPIKey = "ingest-test-api-key"
)

// ingestEnv is per-test view on top of a shared hugr.Service (initialised
// once in TestMain). hugr.New + service.Init costs ~17s; doing it once cuts
// the package wall-clock from N×17s down to a one-off ~17s + ~ms/test.
type ingestEnv struct {
	service *hugr.Service
	server  *httptest.Server
	pgConn  *sql.DB
	client  *hugrclient.Client
	dsName  string
}

// Shared state — set up in TestMain when the postgres DSN env var is present.
// Tests Skip when sharedService is nil (DSN not configured).
var (
	sharedService *hugr.Service
	sharedServer  *httptest.Server
	sharedPgConn  *sql.DB
	sharedClient  *hugrclient.Client
)

func TestMain(m *testing.M) {
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		// No DSN configured — let tests Skip individually with a friendly
		// message. Don't fail the package.
		os.Exit(m.Run())
	}

	schemasPath := os.Getenv(envSchemasPath)
	if schemasPath == "" {
		schemasPath = filepath.Join("testdata", "schemas")
	}
	abs, err := filepath.Abs(schemasPath)
	if err != nil {
		log.Fatalf("resolve schemas path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(abs, "pg_ingest")); err != nil {
		log.Fatalf("schemas/pg_ingest dir not found at %s: %v", abs, err)
	}

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

	// Register & load the postgres data source pointed at the test database.
	regRes, err := service.Query(ctx, `mutation($data: core_data_sources_mut_input_data!) {
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
	if err != nil {
		log.Fatalf("register pg_ingest: %v", err)
	}
	if regRes.Err() != nil {
		log.Fatalf("register pg_ingest graphql error: %v", regRes.Err())
	}
	regRes.Close()

	loadRes, err := service.Query(ctx, `mutation { function { core { load_data_source(name: "pg_ingest") { success message } } } }`, nil)
	if err != nil {
		log.Fatalf("load pg_ingest: %v", err)
	}
	if loadRes.Err() != nil {
		log.Fatalf("load pg_ingest graphql error: %v", loadRes.Err())
	}
	loadRes.Close()

	srv := httptest.NewServer(service)

	pgConn, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open pg verifier conn: %v", err)
	}
	if err := pgConn.PingContext(ctx); err != nil {
		log.Fatalf("ping pg verifier conn: %v", err)
	}

	sharedService = service
	sharedServer = srv
	sharedPgConn = pgConn
	sharedClient = hugrclient.NewClient(srv.URL + "/ipc")

	code := m.Run()

	_ = pgConn.Close()
	srv.Close()
	_ = service.Close()
	os.Exit(code)
}

func setupEnv(t *testing.T) *ingestEnv {
	t.Helper()
	if sharedService == nil {
		t.Skipf("%s not set — run integration-test/ingest-postgres/run.sh to spin up a postgres container", envPostgresDSN)
	}

	// Truncate before each test to guarantee determinism.
	_, err := sharedPgConn.ExecContext(context.Background(), "TRUNCATE TABLE events, binary_events RESTART IDENTITY")
	require.NoError(t, err)

	return &ingestEnv{
		service: sharedService,
		server:  sharedServer,
		pgConn:  sharedPgConn,
		client:  sharedClient,
		dsName:  "pg_ingest",
	}
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
	assert.Equal(t, []bool{true, false, true}, gotHasJSON) // beta has NULL payload
}

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

func TestIngest_Postgres_UsesBinaryCopyWithoutTextOnlyTypes(t *testing.T) {
	env := setupEnv(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{
			Name:     "geom",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"}),
		},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	recordFieldBuilder(t, b, "name").(*array.StringBuilder).Append("binary-copy")
	recordFieldBuilder(t, b, "value").(*array.Float64Builder).Append(42)
	recordFieldBuilder(t, b, "geom").(*array.StringBuilder).Append("POINT (7.25 8.5)")
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), "pg_ingest.binary_events", rec)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Inserted)

	var name, geom string
	require.NoError(t, env.pgConn.QueryRow(
		"SELECT name, ST_AsText(geom) FROM binary_events",
	).Scan(&name, &geom))
	assert.Equal(t, "binary-copy", name)
	assert.Equal(t, "POINT(7.25 8.5)", compactWKT(geom))

	const copyPrefix = `COPY "public"."binary_events"`
	var serverLog string
	require.Eventually(t, func() bool {
		err := env.pgConn.QueryRow("SELECT pg_read_file(pg_current_logfile())").Scan(&serverLog)
		return err == nil && strings.Contains(serverLog, copyPrefix) &&
			strings.Contains(serverLog[strings.LastIndex(serverLog, copyPrefix):], "FORMAT BINARY")
	}, 5*time.Second, 100*time.Millisecond, "postgres log did not contain binary COPY for binary_events")
}

// TestIngest_Postgres_GeometryEdgeCases verifies that the native
// DuckDB GEOMETRY -> PostGIS bridge faithfully carries geometries that the
// existing suite never exercised: SQL NULL, 3D (Z) coordinates, EMPTY
// geometries and a mixed GEOMETRYCOLLECTION. The target column is a bare
// `geometry` (no typmod) so PostGIS accepts any type/dimension and the
// assertions reflect exactly what crossed the bridge — not what a typmod
// coerced. Geometry is sent as geoarrow.wkt so DuckDB staging normalises it to
// a canonical GEOMETRY via ST_GeomFromText before the bridge writes it out.
func TestIngest_Postgres_GeometryEdgeCases(t *testing.T) {
	env := setupEnv(t)

	_, err := env.pgConn.ExecContext(context.Background(),
		"TRUNCATE TABLE geom_edge RESTART IDENTITY")
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{
			Name:     "geom",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"}),
		},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	names := recordFieldBuilder(t, b, "name").(*array.StringBuilder)
	geoms := recordFieldBuilder(t, b, "geom").(*array.StringBuilder)

	names.Append("a_null")
	geoms.AppendNull()
	names.Append("b_point_z")
	geoms.Append("POINT Z (1 2 3)")
	names.Append("c_empty_point")
	geoms.Append("POINT EMPTY")
	names.Append("d_geomcollection")
	geoms.Append("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))")

	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	res, err := env.client.IngestRecord(context.Background(), "pg_ingest.geom_edge", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(4), res.Inserted)

	type edgeRow struct {
		isNull  bool
		gtype   string
		zmflag  int
		isEmpty bool
		numGeom int
	}
	rows, err := env.pgConn.Query(`
		SELECT name,
			geom IS NULL,
			COALESCE(GeometryType(geom), ''),
			COALESCE(ST_Zmflag(geom), -1),
			COALESCE(ST_IsEmpty(geom), false),
			COALESCE(ST_NumGeometries(geom), 0)
		FROM geom_edge ORDER BY name`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]edgeRow{}
	for rows.Next() {
		var name string
		var r edgeRow
		require.NoError(t, rows.Scan(&name, &r.isNull, &r.gtype, &r.zmflag, &r.isEmpty, &r.numGeom))
		got[name] = r
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 4)

	// NULL geometry must round-trip as SQL NULL.
	assert.True(t, got["a_null"].isNull, "NULL geometry must stay NULL through the native bridge")

	// 3D point: the Z dimension must survive DuckDB GEOMETRY -> PostGIS.
	assert.False(t, got["b_point_z"].isNull)
	assert.Equal(t, "POINT", got["b_point_z"].gtype)
	assert.Equal(t, 2, got["b_point_z"].zmflag, "ST_Zmflag 2 == XYZ (Z present, no M)")

	// EMPTY geometry must remain an empty geometry of the right type.
	assert.Equal(t, "POINT", got["c_empty_point"].gtype)
	assert.True(t, got["c_empty_point"].isEmpty, "POINT EMPTY must survive as empty")

	// Mixed GeometryCollection must keep its member count.
	assert.Equal(t, "GEOMETRYCOLLECTION", got["d_geomcollection"].gtype)
	assert.Equal(t, 2, got["d_geomcollection"].numGeom)

	// Exact coordinates for the 3D point.
	var x, y, z float64
	require.NoError(t, env.pgConn.QueryRow(
		"SELECT ST_X(geom), ST_Y(geom), ST_Z(geom) FROM geom_edge WHERE name = 'b_point_z'",
	).Scan(&x, &y, &z))
	assert.Equal(t, [3]float64{1, 2, 3}, [3]float64{x, y, z})
}

func TestIngest_Postgres_PermissionData(t *testing.T) {
	env := setupEnv(t)

	const ownerID = 4343
	role := "ingest_perm_pg"
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
	res, err := permClient.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "owner_id", "owner_id must be injected by permissions, not sent in Arrow")

	rows, err := env.pgConn.Query("SELECT name, owner_id FROM events ORDER BY name")
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

func TestIngest_Postgres_PermissionDataGeometry(t *testing.T) {
	env := setupEnv(t)

	role := "ingest_perm_geom_pg"
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
	res, err := permClient.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "geom", "geom must be injected by permissions, not sent in Arrow")

	rows, err := env.pgConn.Query("SELECT name, ST_AsText(geom), ST_SRID(geom) FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]string{}
	gotSRID := map[string]int{}
	for rows.Next() {
		var name, geom string
		var srid int
		require.NoError(t, rows.Scan(&name, &geom, &srid))
		got[name] = compactWKT(geom)
		gotSRID[name] = srid
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]string{
		"perm-geom-alpha": "POINT(7.25 8.5)",
		"perm-geom-beta":  "POINT(7.25 8.5)",
	}, got)
	assert.Equal(t, map[string]int{
		"perm-geom-alpha": 0,
		"perm-geom-beta":  0,
	}, gotSRID)
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

// arrowFileFormat picks between Arrow IPC stream (no magic) and Arrow IPC
// file (ARROW1 prefix) for the writeEventsArrowFile helper.
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
	expectedSRID   int
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
		{name: "geom_4326", arrowType: pointType, arrowExtension: "geoarrow.point", expectedWKT: point, expectedSRID: 4326},
		{name: "geom_wkt", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.wkt", expectedWKT: line},
		{name: "geom_wkt_4326", arrowType: arrow.BinaryTypes.String, arrowExtension: "geoarrow.wkt", expectedWKT: line, expectedSRID: 4326},
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

func geometrySRIDExpected() []int {
	pointType, lineType, polygonType := geometryArrowTypes()
	srids := make([]int, 0, len(geometryValueColumns(pointType, lineType, polygonType)))
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		srids = append(srids, col.expectedSRID)
	}
	return srids
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

func geometrySelectList(withSRID bool) string {
	pointType, lineType, polygonType := geometryArrowTypes()
	exprs := make([]string, 0, len(geometryValueColumns(pointType, lineType, polygonType))*2)
	for _, col := range geometryValueColumns(pointType, lineType, polygonType) {
		exprs = append(exprs, "ST_AsText("+col.name+")")
		if withSRID {
			exprs = append(exprs, "ST_SRID("+col.name+")")
		}
	}
	return strings.Join(exprs, ",\n")
}

type sqlScanner interface {
	Scan(dest ...any) error
}

func scanGeometryValuesWithSRID(t *testing.T, scanner sqlScanner) ([]string, []int) {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	values := make([]string, len(columns))
	srids := make([]int, len(columns))
	scanArgs := make([]any, 0, len(columns)*2)
	for i := range columns {
		scanArgs = append(scanArgs, &values[i], &srids[i])
	}
	require.NoError(t, scanner.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return values, srids
}

func scanNamedGeometryValuesWithSRID(t *testing.T, rows *sql.Rows) (string, []string, []int) {
	t.Helper()
	pointType, lineType, polygonType := geometryArrowTypes()
	columns := geometryValueColumns(pointType, lineType, polygonType)
	var name string
	values := make([]string, len(columns))
	srids := make([]int, len(columns))
	scanArgs := []any{&name}
	for i := range columns {
		scanArgs = append(scanArgs, &values[i], &srids[i])
	}
	require.NoError(t, rows.Scan(scanArgs...))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	return name, values, srids
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
				geom_4326
				geom_wkt
				geom_wkt_4326
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
		"geom_4326":               geoJSONGeometry("Point", pointCoordinate(point)),
		"geom_wkt":                geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_wkt_4326":           geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
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
	appendPoint(recordFieldBuilder(t, b, "geom_4326").(*array.StructBuilder), row.point)
	recordFieldBuilder(t, b, "geom_wkt").(*array.StringBuilder).Append(lineWKT(x, y))
	recordFieldBuilder(t, b, "geom_wkt_4326").(*array.StringBuilder).Append(lineWKT(x, y))
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
	assert.Equal(t, geometryExpected(pointWKT(lastPoint), coord(lastPoint.x), coord(lastPoint.y)), values)
	assert.Equal(t, geometrySRIDExpected(), srids)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, fmt.Sprintf(`filter: { name: { eq: "%s" } }`, lastName), []map[string]any{
		geometryReadExpected(lastName, lastPoint, lastPoint.x, lastPoint.y),
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

func TestIngest_Postgres_UnknownColumn(t *testing.T) {
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
	recordFieldBuilder(t, b, "x").(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecordBatch()
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
			fields := eventsRecordBuildersFor(rb)
			for i := 0; i < rowsPerBatch; i++ {
				row := batchIdx*rowsPerBatch + i
				fields.names.Append(fmt.Sprintf("evt-%06d", row))
				fields.values.Append(float64(row) * 0.5)
				fields.active.Append(row%2 == 0)
				if row%5 == 0 {
					fields.payloads.AppendNull()
				} else {
					fields.payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
				}
				fields.createdAt.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
			}
			batchRec := rb.NewRecordBatch()
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

func TestIngest_HTTP_GeometryTypes(t *testing.T) {
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

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

	var out hugrclient.IngestResult
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(2), out.Inserted)
	assert.ElementsMatch(t, geometryTypesColumns(), out.Columns)

	rows, err := env.pgConn.Query(fmt.Sprintf(`
			SELECT name,
				%s
		FROM events
		WHERE name LIKE 'geo-%%'
		ORDER BY name
	`, geometrySelectList(true)))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	gotSRID := map[string][]int{}
	for rows.Next() {
		name, values, srids := scanNamedGeometryValuesWithSRID(t, rows)
		got[name] = values
		gotSRID[name] = srids
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
	assert.Equal(t, map[string][]int{
		"geo-a": geometrySRIDExpected(),
		"geo-b": geometrySRIDExpected(),
	}, gotSRID)
}

func TestIngest_HTTP_GeometryTypes_ReadThroughHugr(t *testing.T) {
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

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
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

func TestIngest_HTTP_GeometryTypes_Bulk50k(t *testing.T) {
	env := setupEnv(t)

	const (
		numBatches   = 50
		rowsPerBatch = 1000
		totalRows    = numBatches * rowsPerBatch
		namePrefix   = "pg-geo-bulk"
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
	resp, postErr := http.Post(env.server.URL+"/ipc/ingest?data_object=pg_ingest.events",
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

	var count int
	require.NoError(t, env.pgConn.QueryRow("SELECT COUNT(*) FROM events WHERE name LIKE 'pg-geo-bulk-%'").Scan(&count))
	assert.Equal(t, totalRows, count)

	values, srids := scanGeometryValuesWithSRID(t, env.pgConn.QueryRow(fmt.Sprintf(`
			SELECT %s
		FROM events
		WHERE name = 'pg-geo-bulk-049999'
		`, geometrySelectList(true))))
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)
	assert.Equal(t, geometrySRIDExpected(), srids)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { eq: "pg-geo-bulk-049999" } }`, []map[string]any{
		geometryReadExpected("pg-geo-bulk-049999", xyPoint{x: 99, y: 49}, 99, 49),
	})

	elapsed := time.Since(start)
	t.Logf("geometry bulk ingest: %d rows in %d batches via one /ipc/ingest POST in %s (%.0f rows/s)",
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
