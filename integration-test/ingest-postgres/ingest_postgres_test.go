//go:build duckdb_arrow

package ingest_postgres_test

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

	// Truncate before each test to guarantee determinism (single shared table).
	_, err := sharedPgConn.ExecContext(context.Background(), "TRUNCATE TABLE events RESTART IDENTITY")
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
		"perm-geom-alpha": 4326,
		"perm-geom-beta":  4326,
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
		{Name: "geom_hugr_geojson", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "hugr.geojson"})},
		{Name: "geom_plain_geojson", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geojson"})},
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
		"geom", "geom_wkt", "geom_geojson",
		"geom_hugr_geojson", "geom_plain_geojson", "geom_wkb",
		"geom_line", "geom_polygon_native", "geom_multipoint",
		"geom_multiline", "geom_multipolygon",
	}
}

func geometryExpected(point, x, y string) []string {
	return []string{
		point,
		fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1)),
		polygonWKT(x, y),
		polygonWKT(x, y),
		polygonWKT(x, y),
		point,
		fmt.Sprintf("LINESTRING(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 1)),
		polygonWKT(x, y),
		fmt.Sprintf("MULTIPOINT(%s %s,%s %s,%s %s)", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), y),
		fmt.Sprintf("MULTILINESTRING((%s %s,%s %s),(%s %s,%s %s))", x, y, addCoord(x, 1), addCoord(y, 1), addCoord(x, 2), addCoord(y, 2), addCoord(x, 3), addCoord(y, 3)),
		multiPolygonWKT(x, y),
	}
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
				geom_wkb
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

func geometryReadExpected(name string, point [2]float64, x, y float64) map[string]any {
	return map[string]any{
		"name":                name,
		"geom":                geoJSONGeometry("Point", pointCoordinate(xyPoint{point[0], point[1]})),
		"geom_wkt":            geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_geojson":        geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_hugr_geojson":   geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_plain_geojson":  geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_wkb":            geoJSONGeometry("Point", pointCoordinate(xyPoint{point[0], point[1]})),
		"geom_line":           geoJSONGeometry("LineString", pointCoordinates(linePoints(x, y))),
		"geom_polygon_native": geoJSONGeometry("Polygon", nestedPointCoordinates(polygonRings(x, y))),
		"geom_multipoint":     geoJSONGeometry("MultiPoint", pointCoordinates(multiPoints(x, y))),
		"geom_multiline":      geoJSONGeometry("MultiLineString", nestedPointCoordinates(multiLines(x, y))),
		"geom_multipolygon":   geoJSONGeometry("MultiPolygon", deepPointCoordinates(multiPolygons(x, y))),
	}
}

func geoJSONGeometry(typ string, coordinates any) map[string]any {
	return map[string]any{
		"type":        typ,
		"coordinates": coordinates,
	}
}

func pointCoordinate(point xyPoint) []any {
	return []any{point[0], point[1]}
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
	b.Field(6).(*array.StringBuilder).Append(polygonGeoJSON(shapeX, shapeY))
	b.Field(7).(*array.StringBuilder).Append(polygonGeoJSON(shapeX, shapeY))
	wkbPoint, _ := wkb.Marshal(orb.Point{point[0], point[1]})
	b.Field(8).(*array.BinaryBuilder).Append(wkbPoint)
	appendPointList(b.Field(9).(*array.ListBuilder), linePoints(shapeX, shapeY))
	appendPointListList(b.Field(10).(*array.ListBuilder), polygonRings(shapeX, shapeY))
	appendPointList(b.Field(11).(*array.ListBuilder), multiPoints(shapeX, shapeY))
	appendPointListList(b.Field(12).(*array.ListBuilder), multiLines(shapeX, shapeY))
	appendPointListListList(b.Field(13).(*array.ListBuilder), multiPolygons(shapeX, shapeY))
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
	return [][]xyPoint{
		{{x, y}, {x, y + 4}, {x + 4, y + 4}, {x + 4, y}, {x, y}},
		{{x + 1, y + 1}, {x + 2, y + 1}, {x + 2, y + 2}, {x + 1, y + 2}, {x + 1, y + 1}},
	}
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
	return [][][]xyPoint{
		polygonRings(x, y),
		{{{x + 10, y + 10}, {x + 10, y + 12}, {x + 12, y + 12}, {x + 12, y + 10}, {x + 10, y + 10}}},
	}
}

func lineWKT(x, y float64) string {
	return fmt.Sprintf("LINESTRING (%s %s, %s %s, %s %s)",
		coord(x), coord(y),
		coord(x+1), coord(y+1),
		coord(x+2), coord(y+1))
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

// writeEventsArrowFile produces an Arrow IPC file at path in the given
// format with numBatches × rowsPerBatch synthetic events rows. namePrefix is
// embedded in the `name` column so different tests can write to the same
// table without colliding on uniqueness assertions.
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
		rb := array.NewRecordBuilder(pool, schema)
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
		rec := rb.NewRecord()
		rb.Release()
		require.NoError(t, w.Write(rec))
		rec.Release()
	}
	require.NoError(t, w.Close())
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

	t.Logf("arrow ipc file-format ingest: %d rows from %d-batch file in %s (%.0f rows/s)",
		totalRows, numBatches, elapsed, float64(totalRows)/elapsed.Seconds())
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
		names := rb.Field(0).(*array.StringBuilder)
		values := rb.Field(1).(*array.Float64Builder)
		active := rb.Field(2).(*array.BooleanBuilder)
		payloads := rb.Field(3).(*array.StringBuilder)
		ts := rb.Field(4).(*array.TimestampBuilder)
		for i := 0; i < rowsPerBatch; i++ {
			row := batchIdx*rowsPerBatch + i
			names.Append(fmt.Sprintf("lz-%06d", row))
			values.Append(float64(row) * 0.5)
			active.Append(row%2 == 0)
			if row%5 == 0 {
				payloads.AppendNull()
			} else {
				payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
			}
			ts.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
		}
		rec := rb.NewRecord()
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
		b.Field(0).(*array.Int32Builder).Append(v)
		return b.NewRecord()
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
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"x"}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
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
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
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
			names := rb.Field(0).(*array.StringBuilder)
			values := rb.Field(1).(*array.Float64Builder)
			active := rb.Field(2).(*array.BooleanBuilder)
			payloads := rb.Field(3).(*array.StringBuilder)
			ts := rb.Field(4).(*array.TimestampBuilder)
			for i := 0; i < rowsPerBatch; i++ {
				row := batchIdx*rowsPerBatch + i
				names.Append(fmt.Sprintf("evt-%06d", row))
				values.Append(float64(row) * 0.5)
				active.Append(row%2 == 0)
				if row%5 == 0 {
					payloads.AppendNull()
				} else {
					payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
				}
				ts.Append(arrow.Timestamp(base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
			}
			batchRec := rb.NewRecord()
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

	rec, schema := makeGeometryTypesRecord(t, []string{"geo-a", "geo-b"}, [][2]float64{{30.5, 50.25}, {-73.935242, 40.730610}})
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

	rows, err := env.pgConn.Query(`
		SELECT name,
			ST_AsText(geom), ST_SRID(geom),
			ST_AsText(geom_wkt), ST_SRID(geom_wkt),
			ST_AsText(geom_geojson), ST_SRID(geom_geojson),
			ST_AsText(geom_hugr_geojson), ST_SRID(geom_hugr_geojson),
			ST_AsText(geom_plain_geojson), ST_SRID(geom_plain_geojson),
			ST_AsText(geom_wkb), ST_SRID(geom_wkb),
			ST_AsText(geom_line), ST_SRID(geom_line),
			ST_AsText(geom_polygon_native), ST_SRID(geom_polygon_native),
			ST_AsText(geom_multipoint), ST_SRID(geom_multipoint),
			ST_AsText(geom_multiline), ST_SRID(geom_multiline),
			ST_AsText(geom_multipolygon), ST_SRID(geom_multipolygon)
		FROM events
		WHERE name LIKE 'geo-%'
		ORDER BY name
	`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	gotSRID := map[string][]int{}
	for rows.Next() {
		var name string
		values := make([]string, 11)
		srids := make([]int, 11)
		scanArgs := []any{&name}
		for i := range values {
			scanArgs = append(scanArgs, &values[i], &srids[i])
		}
		require.NoError(t, rows.Scan(scanArgs...))
		for i := range values {
			values[i] = compactWKT(values[i])
		}
		got[name] = values
		gotSRID[name] = srids
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
	assert.Equal(t, map[string][]int{
		"geo-a": []int{4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326},
		"geo-b": []int{4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326},
	}, gotSRID)
}

func TestIngest_HTTP_GeometryTypes_ReadThroughHugr(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []string{"geo-read-a", "geo-read-b"}, [][2]float64{{30.5, 50.25}, {-73.935242, 40.730610}})
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
		geometryReadExpected("geo-read-a", [2]float64{30.5, 50.25}, 0, 0),
		geometryReadExpected("geo-read-b", [2]float64{-73.935242, 40.730610}, 1, 1),
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

	values := make([]string, 11)
	srids := make([]int, 11)
	require.NoError(t, env.pgConn.QueryRow(`
		SELECT ST_AsText(geom), ST_SRID(geom),
			ST_AsText(geom_wkt), ST_SRID(geom_wkt),
			ST_AsText(geom_geojson), ST_SRID(geom_geojson),
			ST_AsText(geom_hugr_geojson), ST_SRID(geom_hugr_geojson),
			ST_AsText(geom_plain_geojson), ST_SRID(geom_plain_geojson),
			ST_AsText(geom_wkb), ST_SRID(geom_wkb),
			ST_AsText(geom_line), ST_SRID(geom_line),
			ST_AsText(geom_polygon_native), ST_SRID(geom_polygon_native),
			ST_AsText(geom_multipoint), ST_SRID(geom_multipoint),
			ST_AsText(geom_multiline), ST_SRID(geom_multiline),
			ST_AsText(geom_multipolygon), ST_SRID(geom_multipolygon)
		FROM events
		WHERE name = 'pg-geo-bulk-049999'
	`).Scan(
		&values[0], &srids[0], &values[1], &srids[1], &values[2], &srids[2],
		&values[3], &srids[3], &values[4], &srids[4], &values[5], &srids[5],
		&values[6], &srids[6], &values[7], &srids[7], &values[8], &srids[8],
		&values[9], &srids[9], &values[10], &srids[10],
	))
	for i := range values {
		values[i] = compactWKT(values[i])
	}
	assert.Equal(t, geometryExpected("POINT(99 49)", "99", "49"), values)
	assert.Equal(t, []int{4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326, 4326}, srids)
	assertGeometryReadThroughHugr(t, env.service, env.dsName, `filter: { name: { eq: "pg-geo-bulk-049999" } }`, []map[string]any{
		geometryReadExpected("pg-geo-bulk-049999", [2]float64{99, 49}, 99, 49),
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
	names := rb.Field(0).(*array.StringBuilder)
	values := rb.Field(1).(*array.Float64Builder)
	active := rb.Field(2).(*array.BooleanBuilder)
	payloads := rb.Field(3).(*array.StringBuilder)
	ts := rb.Field(4).(*array.TimestampBuilder)
	for i := 0; i < r.rowsPerBatch; i++ {
		row := r.batchIdx*r.rowsPerBatch + i
		names.Append(fmt.Sprintf("evt-%06d", row))
		values.Append(float64(row) * 0.5)
		active.Append(row%2 == 0)
		if row%5 == 0 {
			payloads.AppendNull()
		} else {
			payloads.Append(fmt.Sprintf(`{"row":%d}`, row))
		}
		ts.Append(arrow.Timestamp(r.base.Add(time.Duration(row) * time.Millisecond).UnixMicro()))
	}
	r.current = rb.NewRecord()
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
