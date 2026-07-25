//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/require"
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
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			geom_point_native GEOMETRY,
			geom_line_wkt GEOMETRY,
			geom_polygon_geojson GEOMETRY,
			geom_polygon_hugr_geojson GEOMETRY,
			geom_polygon_plain_geojson GEOMETRY,
			geom_polygon_geojson_struct GEOMETRY,
			geom_point_wkb GEOMETRY,
			geom_point_hexwkb GEOMETRY,
			geom_line_native GEOMETRY,
			geom_line_wkb GEOMETRY,
			geom_polygon_native GEOMETRY,
			geom_polygon_wkb GEOMETRY,
			geom_multipoint_native GEOMETRY,
			geom_multipoint_wkb GEOMETRY,
			geom_multiline_native GEOMETRY,
			geom_multiline_wkb GEOMETRY,
			geom_multipolygon_native GEOMETRY,
			geom_multipolygon_wkb GEOMETRY
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
