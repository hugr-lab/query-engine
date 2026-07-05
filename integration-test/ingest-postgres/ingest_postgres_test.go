//go:build duckdb_arrow

package ingest_postgres_test

import (
	"context"
	"database/sql"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/require"
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
