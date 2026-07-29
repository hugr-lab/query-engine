//go:build duckdb_arrow

package entity_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
)

// The lifecycle contract of the entity storage (design 034 §5.1): stored rows
// are EXPENSIVE (a compile plus, with an embedder, one embedding call per
// entity) and visibility is CHEAP (three boolean flags). So every operation
// that does not change a source's SDL must move flags only:
//
//	unload (soft)  → suspended = true, rows untouched
//	load  (again)  → version gate hits, flags repaired, rows untouched
//	app reconnect  → same gate: the SDL came back identical, so nothing is written
//
// Only a CHANGED version — or an explicit hard unload — may rewrite rows.
//
// The probe for "were the rows rewritten?" is data_source_meta.loaded_at: the
// writer stamps it with CURRENT_TIMESTAMP in upsertMeta, which runs only on a
// real write; setFlags never touches it. Row counts alone would not catch a
// delete-and-rewrite that lands on the same shape.

// sourceStats is the observable state of one stored source.
type sourceStats struct {
	LoadedAt  time.Time `json:"loaded_at"`
	Loaded    bool      `json:"loaded"`
	Disabled  bool      `json:"disabled"`
	Suspended bool      `json:"suspended"`
}

func statsOf(t *testing.T, s *hugr.Service, ctx context.Context, name string) sourceStats {
	t.Helper()
	var out []sourceStats
	query(t, s, ctx, `query { core { entity_catalogs(filter: {name: {eq: "`+name+`"}})
		{ loaded_at loaded disabled suspended } } }`, "core.entity_catalogs", &out)
	require.Len(t, out, 1, "source %q has a metadata row", name)
	return out[0]
}

// storedRows counts the entity rows of a source across the tables a rewrite
// would delete and re-insert.
func storedRows(t *testing.T, s *hugr.Service, ctx context.Context, name string) (objects, fields int) {
	t.Helper()
	var objs []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { entity_data_objects(filter: {data_source: {eq: "`+name+`"}}) { name } } }`,
		"core.entity_data_objects", &objs)
	var flds []struct {
		Name string `json:"name"`
	}
	query(t, s, ctx, `query { core { entity_fields(filter: {data_source: {eq: "`+name+`"}}) { name } } }`,
		"core.entity_fields", &flds)
	return len(objs), len(flds)
}

func callFn(t *testing.T, s *hugr.Service, ctx context.Context, field, args string) (bool, string) {
	t.Helper()
	res, err := s.Query(ctx, `mutation { function { core { `+field+`(`+args+`) { success message } } } }`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()
	var out struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	require.NoError(t, res.ScanData("function.core."+field, &out))
	return out.Success, out.Message
}

// TestUnloadReloadKeepsRows walks the flag-only half of the lifecycle: a soft
// unload suspends, a load of an unchanged source repairs the flags, and the
// rows are never touched.
func TestUnloadReloadKeepsRows(t *testing.T) {
	s, ctx := setupEngine(t)
	full := auth.ContextWithFullAccess(ctx)
	registerShop(t, s, full)

	before := statsOf(t, s, ctx, "shop")
	objects, fields := storedRows(t, s, ctx, "shop")
	require.Positive(t, objects, "the source stored its objects")
	require.Positive(t, fields)

	// --- soft unload: visibility off, rows stay ---
	ok, msg := callFn(t, s, full, "unload_data_source", `name: "shop"`)
	require.True(t, ok, msg)

	after := statsOf(t, s, ctx, "shop")
	assert.True(t, after.Suspended, "a soft unload suspends")
	assert.Equal(t, before.LoadedAt, after.LoadedAt, "no rewrite: loaded_at untouched")
	// The entity_* views are activity-gated, so a suspended source's entities
	// are correctly invisible through them — the rows themselves are still
	// stored, which the resumed counts below confirm together with loaded_at.
	gotObjects, gotFields := storedRows(t, s, ctx, "shop")
	assert.Zero(t, gotObjects, "a suspended source's entities are hidden")
	assert.Zero(t, gotFields)

	// The generated schema no longer serves it.
	res, err := s.Query(ctx, `query { shop_items { id } }`, nil)
	if err == nil {
		assert.Error(t, res.Err(), "a suspended source is not queryable")
		res.Close()
	}

	// --- load again: same version, so flags only ---
	ok, msg = callFn(t, s, full, "load_data_source", `name: "shop"`)
	require.True(t, ok, msg)

	reloaded := statsOf(t, s, ctx, "shop")
	assert.False(t, reloaded.Suspended, "the load resumed it")
	assert.Equal(t, before.LoadedAt, reloaded.LoadedAt,
		"an unchanged source is NOT rewritten on load — the version gate holds")
	gotObjects, gotFields = storedRows(t, s, ctx, "shop")
	assert.Equal(t, objects, gotObjects, "the same rows are visible again — they were never deleted")
	assert.Equal(t, fields, gotFields)

	var items []struct {
		Id int `json:"id"`
	}
	query(t, s, ctx, `query { shop_items { id } }`, "shop_items", &items)
	assert.Len(t, items, 2, "served again after the reload")
}

// TestReloadWhileLoadedKeepsRows is the same guarantee for the operation an
// operator actually runs after editing a catalog file: load_data_source on a
// source that is still attached. The schema did not change, so it must not be
// rewritten either — the engine's hard unload-then-load would otherwise drop
// every row (and, with an embedder configured, re-embed the whole source).
func TestReloadWhileLoadedKeepsRows(t *testing.T) {
	s, ctx := setupEngine(t)
	full := auth.ContextWithFullAccess(ctx)
	registerShop(t, s, full)

	before := statsOf(t, s, ctx, "shop")
	objects, fields := storedRows(t, s, ctx, "shop")

	ok, msg := callFn(t, s, full, "load_data_source", `name: "shop"`)
	require.True(t, ok, msg)

	after := statsOf(t, s, ctx, "shop")
	assert.False(t, after.Suspended)
	assert.Equal(t, before.LoadedAt, after.LoadedAt,
		"reloading an unchanged source must not rewrite its rows")
	gotObjects, gotFields := storedRows(t, s, ctx, "shop")
	assert.Equal(t, objects, gotObjects)
	assert.Equal(t, fields, gotFields)

	var items []struct {
		Id int `json:"id"`
	}
	query(t, s, ctx, `query { shop_items { id } }`, "shop_items", &items)
	assert.Len(t, items, 2, "still served after the reload")
}

// TestHardUnloadRemovesRows is the counterpart: an EXPLICIT hard unload is the
// one operation that drops the stored schema.
func TestHardUnloadRemovesRows(t *testing.T) {
	s, ctx := setupEngine(t)
	full := auth.ContextWithFullAccess(ctx)
	registerShop(t, s, full)

	ok, msg := callFn(t, s, full, "unload_data_source", `name: "shop", hard: true`)
	require.True(t, ok, msg)

	var meta []sourceStats
	query(t, s, ctx, `query { core { entity_catalogs(filter: {name: {eq: "shop"}})
		{ loaded_at loaded disabled suspended } } }`, "core.entity_catalogs", &meta)
	assert.Empty(t, meta, "a hard unload removes the metadata row")
	objects, fields := storedRows(t, s, ctx, "shop")
	assert.Zero(t, objects, "and the entity rows")
	assert.Zero(t, fields)
}

// registerShop registers and loads the shop fixture source (a view-backed
// object over an attached DuckDB — no physical storage needed).
func registerShop(t *testing.T, s *hugr.Service, full context.Context) {
	t.Helper()
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

	ok, msg := callFn(t, s, full, "load_data_source", `name: "shop"`)
	require.True(t, ok, msg)
}
