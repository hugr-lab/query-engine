//go:build duckdb_arrow

package curation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// The curation UDFs are reachable through the core.catalog module under their
// stable GraphQL names (the DuckDB functions keep their internal `_schema_*`
// names). These tests boot a real engine on an in-memory CoreDB and drive the
// surface end to end: SDL declaration → module root → registered UDF.
//
// The engine still runs the COMPILED-SCHEMA catalog provider, which stores no
// logical model — the logical-kind functions are registered there as refusals
// so the surface is complete in both modes and reports the reason instead of
// failing with an unknown-function error.

func setupEngine(t *testing.T) (*hugr.Service, context.Context) {
	t.Helper()
	ctx := context.Background()
	service, err := hugr.New(hugr.Config{
		DB:     db.Config{Path: ""},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{Allowed: true, Role: "admin"}),
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })
	require.NoError(t, service.Init(ctx))
	return service, auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Role: "admin", UserId: "test"})
}

// call runs one core.catalog mutation function and returns its result.
func call(t *testing.T, s *hugr.Service, ctx context.Context, field, args string) (bool, string) {
	t.Helper()
	res, err := s.Query(ctx, `mutation { function { core { catalog {
		`+field+`(`+args+`) { success message }
	} } } }`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()

	var out struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	require.NoError(t, res.ScanData("function.core.catalog."+field, &out))
	return out.Success, out.Message
}

// TestCurationSurfaceReachable pins the GraphQL wiring: the generated-surface
// curation works on the compiled-schema provider and lands in the schema store.
func TestCurationSurfaceReachable(t *testing.T) {
	s, ctx := setupEngine(t)

	ok, msg := call(t, s, ctx, "annotate_gql_type",
		`name: "embedder_settings", description: "Embedder settings", long_description: ""`)
	assert.True(t, ok, msg)

	ok, msg = call(t, s, ctx, "annotate_gql_field",
		`type_name: "embedder_settings", name: "model", description: "Model id", long_description: ""`)
	assert.True(t, ok, msg)

	ok, msg = call(t, s, ctx, "annotate_module",
		`name: "core", description: "Core module", long_description: ""`)
	assert.True(t, ok, msg)
}

// TestLogicalCurationRefused pins the other half: the logical-model functions
// are declared and callable, and report that they need the entity storage.
func TestLogicalCurationRefused(t *testing.T) {
	s, ctx := setupEngine(t)

	ok, msg := call(t, s, ctx, "annotate_data_object",
		`name: "embedder_settings", description: "x", long_description: ""`)
	assert.False(t, ok)
	assert.Contains(t, msg, "entity catalog storage")

	ok, msg = call(t, s, ctx, "annotate_function",
		`module: "core", name: "checkpoint", description: "x", long_description: ""`)
	assert.False(t, ok)
	assert.Contains(t, msg, "entity catalog storage")
}

// TestMaintenanceSurface pins the maintenance functions' wiring — an unknown
// data source is reported through the result, not as a query error.
func TestMaintenanceSurface(t *testing.T) {
	s, ctx := setupEngine(t)

	ok, msg := call(t, s, ctx, "reset_data_source_version", `name: "nope"`)
	assert.True(t, ok, msg) // the compiled-schema provider treats it as a no-op

	ok, msg = call(t, s, ctx, "reindex_embeddings", `name: "", batch_size: 10`)
	assert.False(t, ok, "no embedder configured")
	assert.Contains(t, msg, "embeddings not configured")
}
