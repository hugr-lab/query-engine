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

// The engine on the entity catalog storage (design 034), which is the storage:
// the schema of every source lives in the catalog.* tables as a logical model
// and the GraphQL schema is generated from it on the fly. These tests boot a
// real engine and check that the catalog works at all — runtime sources compile
// and store, their queries run, a user source loads and is queryable, and the
// catalog's own state is visible through the entity views.
//
// MCP runs here too (mcp_search_test.go): the catalog-* tools read the logical
// model through the meta-query family and rank over the entity views' own index.

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
	s, ctx := setupEngine(t)

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
	query(t, s, ctx, `query { core { catalog { active_sources { name } } } }`,
		"core.catalog.active_sources", &stored)
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
	query(t, s, ctx, `query { core { catalog { data_objects(filter: {data_source: {eq: "core"}}, order_by: [{field: "name"}]) { name module } } } }`,
		"core.catalog.data_objects", &objects)
	assert.NotEmpty(t, objects, "CoreDB's own objects are stored")
}

// TestEntityStorageDataSource loads a REAL user source — a catalog file with a
// view-backed object — and queries its data: the write path (compile → store)
// and the read path (generate → plan → execute) end to end on the entity
// storage.
func TestEntityStorageDataSource(t *testing.T) {
	s, ctx := setupEngine(t)
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
	query(t, s, ctx, `query { core { catalog { data_objects(filter: {data_source: {eq: "shop"}}) { name original_name } } } }`,
		"core.catalog.data_objects", &objects)
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
	s, ctx := setupEngine(t)

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
	query(t, s, ctx, `query { core { catalog { data_objects(filter: {name: {eq: "core_data_sources"}}) { name description } } } }`,
		"core.catalog.data_objects", &objects)
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
	query(t, s, ctx, `query { core { catalog { functions(filter: {name: {eq: "load_data_source"}}) { kind description } } } }`,
		"core.catalog.functions", &fns)
	require.Len(t, fns, 1)
	assert.Equal(t, "mutation", fns[0].Kind)
	assert.Equal(t, "Load a registered data source", fns[0].Desc,
		"the mutation function reads its own kind's curation")
}

// TestEntityStorageCurationReachesIntrospection is the end-to-end half of the
// same contract: what an operator curates must be what INTROSPECTION answers.
// The _catalog family used to read the stored rows directly, so a deployment
// described itself one way through the entity views and another through
// _dataObject / _module / _function — and longDescription, which exists only
// as curation, had no surface at all.
func TestEntityStorageCurationReachesIntrospection(t *testing.T) {
	s, ctx := setupEngine(t)

	mustQuery(t, s, ctx, `mutation { function { core { catalog {
		obj: annotate_data_object(name: "core_data_sources",
			description: "Registered data sources", long_description: "One row per attached source.")
			{ success message }
		mod: annotate_module(name: "core",
			description: "Engine internals", long_description: "The engine's own catalog and runtime.")
			{ success message }
		fn: annotate_function(module: "core", name: "load_data_source", kind: "mutation",
			description: "Load a registered data source", long_description: "Reloads when already loaded.")
			{ success message }
	} } } }`, nil)

	var probe struct {
		Object *struct {
			Description     string `json:"description"`
			LongDescription string `json:"longDescription"`
		} `json:"obj"`
		Module *struct {
			Description     string `json:"description"`
			LongDescription string `json:"longDescription"`
		} `json:"mod"`
		Function *struct {
			Description     string `json:"description"`
			LongDescription string `json:"longDescription"`
		} `json:"fn"`
	}
	res, err := s.Query(ctx, `{
		obj: _dataObject(name: "core_data_sources") { description longDescription }
		mod: _module(name: "core") { description longDescription }
		fn: _function(module: "core", name: "load_data_source") { description longDescription }
	}`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	require.NoError(t, res.ScanData("", &probe))
	res.Close()

	require.NotNil(t, probe.Object)
	assert.Equal(t, "Registered data sources", probe.Object.Description)
	assert.Equal(t, "One row per attached source.", probe.Object.LongDescription)

	require.NotNil(t, probe.Module)
	assert.Equal(t, "Engine internals", probe.Module.Description)
	assert.Equal(t, "The engine's own catalog and runtime.", probe.Module.LongDescription)

	require.NotNil(t, probe.Function)
	assert.Equal(t, "Load a registered data source", probe.Function.Description)
	assert.Equal(t, "Reloads when already loaded.", probe.Function.LongDescription)
}

// TestCatalogModuleServesLogicalViewsOnly replaces the test that asserted the
// opposite while the compiled-schema storage was the default. There is no
// storage to select any more, so what it guards is the CoreDB catalog module:
// it must expose the catalog views and not regrow the _schema_* ones.
func TestCatalogModuleServesLogicalViewsOnly(t *testing.T) {
	s, ctx := setupEngine(t)

	var objects []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { catalog { data_objects(filter: {name: {eq: "core_data_sources"}}) { name } } } }`,
		"core.catalog.data_objects", &objects)
	assert.NotEmpty(t, objects, "catalog views are served")

	// The compiled-schema views went with the storage that filled them. The
	// probe deliberately names one whose name was NOT reused: this module now
	// serves modules / types / fields / data_objects again, but over the
	// LOGICAL model, so those names prove nothing about the old storage.
	res, err := s.Query(ctx, `query { core { catalog { module_intro { name } } } }`, nil)
	if err == nil {
		assert.Error(t, res.Err(), "compiled-schema views are no longer part of the schema")
		res.Close()
	}
}
