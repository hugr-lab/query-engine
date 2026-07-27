//go:build duckdb_arrow

package entity_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// The engine on the ENTITY catalog storage (design 034): the schema of every
// source lives in the catalog.* tables as a logical model and the GraphQL
// schema is generated from it on the fly. These tests boot a real engine in
// that mode and check that the catalog works at all — runtime sources compile
// and store, their queries run, a user source loads and is queryable, and the
// catalog's own state is visible through the entity views.
//
// MCP is NOT covered: it reads the compiled-schema views, which entity mode
// does not expose. Porting it is separate work.

func setupEngine(t *testing.T, storage hugr.CatalogStorage) (*hugr.Service, context.Context) {
	t.Helper()
	ctx := context.Background()
	service, err := hugr.New(hugr.Config{
		DB:             db.Config{Path: ""},
		CoreDB:         coredb.New(coredb.Config{}),
		CatalogStorage: storage,
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

func query(t *testing.T, s *hugr.Service, ctx context.Context, q string, path string, out any) {
	t.Helper()
	res, err := s.Query(ctx, q, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()
	require.NoError(t, res.ScanData(path, out))
}

func mustQuery(t *testing.T, s *hugr.Service, ctx context.Context, q string, vars map[string]any) {
	t.Helper()
	res, err := s.Query(ctx, q, vars)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	res.Close()
}

func scanQuery(t *testing.T, s *hugr.Service, ctx context.Context, q string, vars map[string]any, path string, out any) {
	t.Helper()
	res, err := s.Query(ctx, q, vars)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()
	require.NoError(t, res.ScanData(path, out))
}

// TestEntityStorageBoots is the smoke test: the engine starts on the entity
// storage, every runtime source's schema is compiled and stored, and the
// generated schema answers a real query over one of them.
func TestEntityStorageBoots(t *testing.T) {
	s, ctx := setupEngine(t, hugr.CatalogStorageEntity)

	// A runtime source's own data — CoreDB's data_sources table.
	var sources []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { data_sources { name } } }`, "core.data_sources", &sources)
	assert.Empty(t, sources, "no user data sources registered yet")

	// The stored logical model: every runtime source registered its catalog.
	var stored []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { entity_data_sources { name } } }`,
		"core.entity_data_sources", &stored)
	names := map[string]bool{}
	for _, r := range stored {
		names[r.Name] = true
	}
	for _, want := range []string{"core", "core.ds", "core.cache", "core.meta"} {
		assert.True(t, names[want], "runtime source %q stored in the catalog", want)
	}

	// The generated surface reaches the data objects of a runtime source.
	var objects []struct {
		Name   string `json:"name"`
		Module string `json:"module"`
	}
	query(t, s, ctx, `query { core { entity_data_objects(filter: {data_source: {eq: "core"}}, order_by: [{field: "name"}]) { name module } } }`,
		"core.entity_data_objects", &objects)
	assert.NotEmpty(t, objects, "CoreDB's own objects are stored")
}

// TestEntityStorageDataSource loads a REAL user source — a catalog file with a
// view-backed object — and queries its data: the write path (compile → store)
// and the read path (generate → plan → execute) end to end on the entity
// storage.
func TestEntityStorageDataSource(t *testing.T) {
	s, ctx := setupEngine(t, hugr.CatalogStorageEntity)
	full := auth.ContextWithFullAccess(ctx)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shop.graphql"), []byte(shopSchema), 0o600))

	mustQuery(t, s, full, `mutation($ds: core_data_sources_mut_input_data!, $cs: core_catalog_sources_mut_input_data!, $link: core_catalogs_mut_input_data!) {
		core {
			insert_data_sources(data: $ds) { name }
			insert_catalog_sources(data: $cs) { name }
			insert_catalogs(data: $link) { success affected_rows }
		}
	}`, map[string]any{
		"ds":   map[string]any{"name": "shop", "type": "duckdb", "prefix": "shop", "as_module": false, "path": ""},
		"cs":   map[string]any{"name": "shop_schema", "type": "localFS", "path": dir},
		"link": map[string]any{"catalog_name": "shop_schema", "data_source_name": "shop"},
	})

	var load struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	scanQuery(t, s, full, `mutation { function { core { load_data_source(name: "shop") { success message } } } }`,
		nil, "function.core.load_data_source", &load)
	require.True(t, load.Success, load.Message)

	// The source's schema is stored as a logical model...
	var objects []struct {
		Name     string `json:"name"`
		Original string `json:"original_name"`
	}
	query(t, s, ctx, `query { core { entity_data_objects(filter: {data_source: {eq: "shop"}}) { name original_name } } }`,
		"core.entity_data_objects", &objects)
	require.Len(t, objects, 1)
	assert.Equal(t, "shop_items", objects[0].Name, "stored under its prefixed name")

	// ...and the schema generated from it answers a real query.
	var items []struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { shop_items(order_by: [{field: "id"}]) { id name } }`, "shop_items", &items)
	require.Len(t, items, 2)
	assert.Equal(t, "second", items[1].Name)

	// Reload is version-gated but must keep serving.
	mustQuery(t, s, full, `mutation { function { core { load_data_source(name: "shop") { success } } } }`, nil)
	query(t, s, ctx, `query { shop_items(order_by: [{field: "id"}]) { id name } }`, "shop_items", &items)
	assert.Len(t, items, 2, "still served after a reload")
}

// shopSchema is a user catalog with no physical tables — the object is a view
// with inline SQL, so the source needs nothing but an attached DuckDB.
const shopSchema = `
type items @view(name: "items", sql: "SELECT 1 AS id, 'first' AS name UNION ALL SELECT 2, 'second'") {
  id: Int! @pk
  name: String
}
`

// TestEntityStorageCuration drives the curation UDFs against the entity
// storage: a logical curation lands and shows through the entity views and the
// generated schema.
func TestEntityStorageCuration(t *testing.T) {
	s, ctx := setupEngine(t, hugr.CatalogStorageEntity)

	res, err := s.Query(ctx, `mutation { function { core { catalog {
		annotate_data_object(name: "core_data_sources", description: "Registered data sources", long_description: "")
		{ success message }
	} } } }`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	var out struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	require.NoError(t, res.ScanData("function.core.catalog.annotate_data_object", &out))
	res.Close()
	require.True(t, out.Success, out.Message)

	var objects []struct {
		Name string `json:"name"`
		Desc string `json:"description"`
	}
	query(t, s, ctx, `query { core { entity_data_objects(filter: {name: {eq: "core_data_sources"}}) { name description } } }`,
		"core.entity_data_objects", &objects)
	require.Len(t, objects, 1)
	assert.Equal(t, "Registered data sources", objects[0].Desc, "curation overrides the stored description")

	// A function is curated per ROOT NAMESPACE: load_data_source is a mutation
	// function, so the curation must name that kind — the same name under the
	// query-function kind is a different operation and leaves this row alone.
	res, err = s.Query(ctx, `mutation { function { core { catalog {
		mut: annotate_function(module: "core", name: "load_data_source", kind: "mutation",
			description: "Load a registered data source", long_description: "") { success message }
		fn: annotate_function(module: "core", name: "load_data_source", kind: "function",
			description: "not this one", long_description: "") { success message }
	} } } }`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	require.NoError(t, res.ScanData("function.core.catalog.mut", &out))
	res.Close()
	require.True(t, out.Success, out.Message)

	var fns []struct {
		Kind string `json:"kind"`
		Desc string `json:"description"`
	}
	query(t, s, ctx, `query { core { entity_functions(filter: {name: {eq: "load_data_source"}}) { kind description } } }`,
		"core.entity_functions", &fns)
	require.Len(t, fns, 1)
	assert.Equal(t, "mutation", fns[0].Kind)
	assert.Equal(t, "Load a registered data source", fns[0].Desc,
		"the mutation function reads its own kind's curation")
}

// TestCompiledStorageKeepsLegacyViews is the other half of the mode switch:
// the default storage still exposes the compiled-schema views (and not the
// entity ones), so nothing changes for the current deployment.
func TestCompiledStorageKeepsLegacyViews(t *testing.T) {
	s, ctx := setupEngine(t, "")

	var types_ []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { catalog { types(filter: {name: {eq: "core_data_sources"}}) { name } } } }`,
		"core.catalog.types", &types_)
	assert.NotEmpty(t, types_, "compiled-schema views are served in the default mode")

	// The entity views are not part of this mode's schema (a validation error
	// surfaces either from Query itself or on the response).
	res, err := s.Query(ctx, `query { core { entity_data_objects { name } } }`, nil)
	if err == nil {
		assert.Error(t, res.Err(), "entity views are not exposed by the compiled-schema storage")
		res.Close()
	}
}
