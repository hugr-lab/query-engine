//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Ш5 read-cache checks: served-from-cache pointer stability, per-source
// invalidation on rewrite / delete / flag flips, the cross-source family
// closure (absence dependencies), error-poisoned resolutions staying out of
// the cache, and the NameExists classification.

// cacheTestSchemaV2 is collectTestSchema with the orders.note column added
// and a NEW object — the reshape an invalidated cache must serve.
const cacheTestSchemaV2 = collectTestSchema + `
extend type orders {
  note: String
}

type invoices @module(name: "sales") @table(name: "invoices") {
  id: Int! @pk
}
`

func TestCacheHitAndRewriteInvalidation(t *testing.T) {
	store, ctx := writtenStore(t)

	def1 := store.ForName(ctx, "orders")
	require.NotNil(t, def1)
	assert.Nil(t, def1.Fields.ForName("note"))
	def1b := store.ForName(ctx, "orders")
	assert.True(t, def1 == def1b, "second read is served from the cache")

	filter1 := store.ForName(ctx, "orders_filter")
	require.NotNil(t, filter1)
	assert.Nil(t, filter1.Fields.ForName("note"))

	// The new object is unknown yet.
	assert.Nil(t, store.ForName(ctx, "invoices"))

	// Rewrite the source with a changed version — the cache drops.
	d := collect(ctx, partialSource(t, "test", cacheTestSchemaV2), "test")
	rewritten, err := store.writeSource(ctx, d, SourceState{Name: "test", Version: "v2", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	require.True(t, rewritten)

	def2 := store.ForName(ctx, "orders")
	require.NotNil(t, def2)
	assert.False(t, def1 == def2, "rewrite evicts the cached definition")
	assert.NotNil(t, def2.Fields.ForName("note"), "new column served")

	filter2 := store.ForName(ctx, "orders_filter")
	require.NotNil(t, filter2)
	assert.NotNil(t, filter2.Fields.ForName("note"), "derived type rebuilt")

	require.NotNil(t, store.ForName(ctx, "invoices"), "new object served after the rewrite")
}

func TestCacheVersionGateKeepsEntries(t *testing.T) {
	store, ctx := writtenStore(t)

	def1 := store.ForName(ctx, "orders")
	require.NotNil(t, def1)

	// Same version — writeSource is a no-op and must NOT invalidate.
	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")
	rewritten, err := store.writeSource(ctx, d, SourceState{Name: "test", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	require.False(t, rewritten)

	def2 := store.ForName(ctx, "orders")
	assert.True(t, def1 == def2, "unchanged version keeps the cache")
}

func TestCacheFlagFlipsInvalidate(t *testing.T) {
	store, ctx := writtenStore(t)

	require.NotNil(t, store.ForName(ctx, "orders"))
	require.True(t, store.NameExists(ctx, "orders"))

	require.NoError(t, store.setFlags(ctx, "test", true, true, false))
	assert.Nil(t, store.ForName(ctx, "orders"), "disabled source hides its types")
	assert.False(t, store.NameExists(ctx, "orders"), "disabled source is not servable")

	require.NoError(t, store.setFlags(ctx, "test", true, false, false))
	def := store.ForName(ctx, "orders")
	require.NotNil(t, def, "re-enabled source serves again")
	assert.True(t, def == store.ForName(ctx, "orders"), "and caches again")
}

// TestCacheCrossSourceClosure is the absence-dependency regression: object A
// is cached BEFORE source B exists; B's write stores a relation pointing at
// A, which A's cached definition never read a row of — the write-side family
// closure must evict A anyway.
func TestCacheCrossSourceClosure(t *testing.T) {
	const schemaA = `
type a_obj @module(name: "app") @table(name: "a_obj") {
  id: Int! @pk
}
`
	const schemaB = `
type b_obj @module(name: "app") @table(name: "b_obj")
  @references(name: "b_to_a", references_name: "a_obj",
    source_fields: ["a_id"], references_fields: ["id"],
    query: "a", references_query: "b_items") {
  id: Int! @pk
  a_id: Int!
}
`
	store, ctx := storeFor(t, schemaA)
	srcA := partialSource(t, "test", schemaA)

	before := store.ForName(ctx, "a_obj")
	require.NotNil(t, before)
	require.Nil(t, before.Fields.ForName("b_items"), "no back navigation yet")

	srcB := partialSource(t, "b", schemaB, definitionsOnly{srcA})
	d := collect(ctx, srcB, "b")
	rewritten, err := store.writeSource(ctx, d, SourceState{Name: "b", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	require.True(t, rewritten)

	after := store.ForName(ctx, "a_obj")
	require.NotNil(t, after)
	assert.False(t, before == after, "family closure evicted the foreign endpoint")
	assert.NotNil(t, after.Fields.ForName("b_items"), "back navigation from B's relation")

	// And the derived family of A follows.
	lf := store.ForName(ctx, "a_obj_list_filter")
	assert.NotNil(t, lf, "list filter exists once A is a back-projection target")
}

func TestCacheErrorNotCached(t *testing.T) {
	store, ctx := writtenStore(t)

	// A canceled context fails every read: the error propagates to ForName,
	// which logs, serves nil and caches nothing.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	assert.Nil(t, store.ForName(cctx, "orders"))
	assert.Nil(t, store.cache.get("orders"), "failed resolution never cached")

	def := store.ForName(ctx, "orders")
	require.NotNil(t, def, "healthy context resolves")
	assert.True(t, def == store.ForName(ctx, "orders"), "and caches")
}

func TestNameExistsClassification(t *testing.T) {
	store, ctx := writtenStore(t)

	for _, name := range []string{
		"orders",                       // data object
		"sales_by_country_args",        // residual source type
		"Query",                        // top-level root
		"_module_sales_query",          // module root
		"orders_filter",                // derived over object
		"_orders_aggregation",          // derived aggregation
		"sales_by_country_args_filter", // derived over a structural residual type
		"String",                       // static scalar
		"_join",                        // shared-type stub
	} {
		assert.Truef(t, store.NameExists(ctx, name), "NameExists(%s)", name)
	}

	for _, name := range []string{
		"definitely_not_a_type",
		"nope_filter",
		"_module_missing_query",
		"_nope_aggregation",
	} {
		assert.Falsef(t, store.NameExists(ctx, name), "NameExists(%s)", name)
	}
}
