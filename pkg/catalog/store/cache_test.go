//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
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
	// Cache the derived family BEFORE the write — its eviction is the point.
	filterBefore := store.ForName(ctx, "a_obj_filter")
	require.NotNil(t, filterBefore)
	require.Nil(t, filterBefore.Fields.ForName("b_items"))

	srcB := partialSource(t, "b", schemaB, definitionsOnly{srcA})
	d := collect(ctx, srcB, "b")
	rewritten, err := store.writeSource(ctx, d, SourceState{Name: "b", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	require.True(t, rewritten)

	after := store.ForName(ctx, "a_obj")
	require.NotNil(t, after)
	assert.False(t, before == after, "family closure evicted the foreign endpoint")
	assert.NotNil(t, after.Fields.ForName("b_items"), "back navigation from B's relation")

	// The derived family of A was evicted and rebuilt too.
	filterAfter := store.ForName(ctx, "a_obj_filter")
	require.NotNil(t, filterAfter)
	assert.False(t, filterBefore == filterAfter, "derived family evicted")
	assert.NotNil(t, filterAfter.Fields.ForName("b_items"), "filter nav member from B's relation")
	lf := store.ForName(ctx, "a_obj_list_filter")
	assert.NotNil(t, lf, "list filter exists once A is a back-projection target")

	// deleteSource closes the reverse direction: B's relation disappears.
	require.NoError(t, store.deleteSource(ctx, "b"))
	dropped := store.ForName(ctx, "a_obj")
	require.NotNil(t, dropped)
	assert.Nil(t, dropped.Fields.ForName("b_items"), "back navigation dropped with B")
}

// TestCacheJoinTargetClosure pins the cross-source @join lifecycle: a field an
// EXTENSION source contributes to one source's object, joining to ANOTHER
// source's object, is served exactly while the source its DATA comes from is
// active — and the definition cache re-resolves on every flip instead of
// serving a stale shape.
//
// The join lives in an extension source, the only place the contract allows a
// cross-source artifact; the same shape genMultiFixtures pins (audit_events →
// ro's logs). A cross-source @join declared inline in a plain source, or a
// plain source extending another source's type at all, is refused on the write
// path.
//
// The field used to be served BARE when the target was missing — declared, but
// with no subquery machinery. That was an artifact of the write path not
// recording who the field belongs to: JoinValidator did not walk extension
// fields, so nothing propagated the target's @catalog and the row was
// attributed to the extension. The read side computed the right owner anyway,
// which is why the flag-play contract already said "hidden with its data
// source" while this test said "bare". Both sides agree now.
func TestCacheJoinTargetClosure(t *testing.T) {
	const schemaB = `
type b_stats @module(name: "app") @table(name: "b_stats") {
  order_id: Int! @pk
}
`
	const schemaA = `
type a_orders @module(name: "app") @table(name: "a_orders") {
  id: Int! @pk
}
`
	const schemaExt = `
extend type a_orders {
  stats: [b_stats] @join(references_name: "b_stats", source_fields: ["id"], references_fields: ["order_id"])
}
`
	// The extension compiles against BOTH sources' definitions, but the target
	// source B is not WRITTEN yet — it does not exist in the store.
	srcB := partialSource(t, "b", schemaB)
	srcA := partialSource(t, "test", schemaA)
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)
	_, err = store.writeSource(ctx, collect(ctx, srcA, "test"),
		SourceState{Name: "test", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	srcExt := partialExtensionSource(t, "ext", schemaExt, definitionsOnly{srcA}, definitionsOnly{srcB})
	_, err = store.writeSource(ctx, collect(ctx, srcExt, "ext"),
		SourceState{Name: "ext", Version: "v1", Engine: "duckdb", IsExtension: true, Loaded: true})
	require.NoError(t, err)

	// The base object is always served — only the contributed field follows the
	// target's source.
	assertHidden := func(when string) {
		def := store.ForName(ctx, "a_orders")
		require.NotNilf(t, def, "a_orders served (%s)", when)
		assert.Nilf(t, def.Fields.ForName("stats"), "join field hidden with its data source (%s)", when)
		assert.Nilf(t, def.Fields.ForName("stats_aggregation"), "no twin either (%s)", when)
	}
	assertWired := func(when string) {
		def := store.ForName(ctx, "a_orders")
		require.NotNilf(t, def, "a_orders served (%s)", when)
		fd := def.Fields.ForName("stats")
		require.NotNilf(t, fd, "declared join field present (%s)", when)
		assert.NotEmptyf(t, fd.Arguments, "join field carries subquery args (%s)", when)
		assert.NotNilf(t, def.Fields.ForName("stats_aggregation"), "aggregation twin generated (%s)", when)
	}

	assertHidden("target absent")

	// Register the target source — the field appears, fully wired.
	dB := collect(ctx, srcB, "b")
	_, err = store.writeSource(ctx, dB, SourceState{Name: "b", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	assertWired("target active")

	// ...and goes again when the target does: the cached definition is
	// invalidated by the flip, not served stale.
	require.NoError(t, store.setFlags(ctx, "b", true, true, false))
	assertHidden("target disabled")
}

// TestMultiSourceFlagPlay drives the 5-source golden fixture set through
// source enable/disable cycles: the extension flip drops every contributed
// artifact, the shop flip hides the @dependency view while the extension
// stays active, and the ro / func flips hide the wired fields entirely —
// a virtual field is attributed to the source its DATA comes from
// (write-time resolution), so the standard activity gate applies. Every
// assertion goes through the definition cache — a stale entry after a flip
// fails the test.
func TestMultiSourceFlagPlay(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	assertBaseline := func(when string) {
		items := store.ForName(ctx, "shop_items")
		require.NotNilf(t, items, "shop_items (%s)", when)
		for _, name := range []string{"note", "label", "similar", "similar_aggregation"} {
			assert.NotNilf(t, items.Fields.ForName(name), "shop_items.%s (%s)", name, when)
		}
		audit := store.ForName(ctx, "audit_events")
		require.NotNilf(t, audit, "audit_events (%s)", when)
		assert.NotNilf(t, audit.Fields.ForName("logs"), "audit_events.logs (%s)", when)
		assert.NotNilf(t, audit.Fields.ForName("logs_aggregation"), "audit_events.logs_aggregation (%s)", when)
		require.NotNilf(t, store.ForName(ctx, "shop_overview"), "shop_overview (%s)", when)
		require.NotNilf(t, store.ForName(ctx, "logs"), "logs (%s)", when)
	}
	assertBaseline("initial")

	// --- extension off: every contributed artifact disappears ---
	require.NoError(t, store.setFlags(ctx, "ext", true, true, false))
	items := store.ForName(ctx, "shop_items")
	require.NotNil(t, items, "base object survives the extension flip")
	for _, name := range []string{"note", "label", "similar", "similar_aggregation"} {
		assert.Nilf(t, items.Fields.ForName(name), "shop_items.%s dropped with the extension", name)
	}
	audit := store.ForName(ctx, "audit_events")
	require.NotNil(t, audit)
	assert.Nil(t, audit.Fields.ForName("logs"), "extension join dropped")
	assert.Nil(t, store.ForName(ctx, "shop_overview"), "extension view dropped")
	assert.False(t, store.NameExists(ctx, "shop_overview"))
	require.NoError(t, store.setFlags(ctx, "ext", true, false, false))
	assertBaseline("extension back")

	// --- shop off: the @dependency view hides while the extension is active ---
	require.NoError(t, store.setFlags(ctx, "shop", true, true, false))
	assert.Nil(t, store.ForName(ctx, "shop_items"))
	assert.Nil(t, store.ForName(ctx, "shop_overview"), "declared @dependency gates the view")
	assert.Nil(t, store.ForName(ctx, "shop_overview_filter"), "derived types follow")
	assert.False(t, store.NameExists(ctx, "shop_overview"))
	require.NoError(t, store.setFlags(ctx, "shop", true, false, false))
	assertBaseline("shop back")

	// --- ro off: the extension join field is ATTRIBUTED to ro (its data
	// source) and disappears with it, machinery included ---
	require.NoError(t, store.setFlags(ctx, "ro", true, true, false))
	assert.Nil(t, store.ForName(ctx, "logs"))
	audit = store.ForName(ctx, "audit_events")
	require.NotNil(t, audit)
	assert.Nil(t, audit.Fields.ForName("logs"), "join field hidden with its data source")
	assert.Nil(t, audit.Fields.ForName("logs_aggregation"))
	require.NoError(t, store.setFlags(ctx, "ro", true, false, false))
	assertBaseline("ro back")

	// --- func off: the TFCJ field is ATTRIBUTED to the function's source and
	// disappears with it; the same-source @function_call stays ---
	require.NoError(t, store.setFlags(ctx, "func", true, true, false))
	items = store.ForName(ctx, "shop_items")
	require.NotNil(t, items)
	assert.Nil(t, items.Fields.ForName("similar"), "TFCJ field hidden with the function's source")
	assert.Nil(t, items.Fields.ForName("similar_aggregation"))
	assert.NotNil(t, items.Fields.ForName("label"), "shop's function_call unaffected")
	require.NoError(t, store.setFlags(ctx, "func", true, false, false))
	assertBaseline("func back")
}

// TestObjectDependencyGate pins the @dependency contract on views: the object
// (and everything derived from it) is hidden while ANY declared dependency
// source is inactive or unregistered, and dependency flag flips invalidate
// the cache through the recorded provenance.
func TestObjectDependencyGate(t *testing.T) {
	const schema = `
type v_stats @module(name: "app")
  @view(name: "v_stats", sql: "SELECT 1 AS id")
  @dependency(name: "depsrc") {
  id: Int! @pk
}
`
	store, ctx := storeFor(t, schema)

	// The dependency is unregistered → the view is not served.
	assert.Nil(t, store.ForName(ctx, "v_stats"))
	assert.False(t, store.NameExists(ctx, "v_stats"))
	assert.Nil(t, store.ForName(ctx, "v_stats_filter"), "derived types follow the gate")

	// Register the dependency (meta only) → the view appears.
	_, err := store.writeSource(ctx, newDesired(), SourceState{Name: "depsrc", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	def := store.ForName(ctx, "v_stats")
	require.NotNil(t, def, "view served once the dependency is active")
	require.NotNil(t, def.Directives.ForName("dependency"), "@dependency re-emitted")
	assert.True(t, store.NameExists(ctx, "v_stats"))
	require.NotNil(t, store.ForName(ctx, "v_stats_filter"))

	// Disabling the dependency hides it again — the cached definition was
	// indexed under the dependency source and must be evicted.
	require.NoError(t, store.setFlags(ctx, "depsrc", true, true, false))
	assert.Nil(t, store.ForName(ctx, "v_stats"), "view hidden with its dependency")
	assert.Nil(t, store.ForName(ctx, "v_stats_filter"))
	assert.False(t, store.NameExists(ctx, "v_stats"))

	require.NoError(t, store.setFlags(ctx, "depsrc", true, false, false))
	require.NotNil(t, store.ForName(ctx, "v_stats"), "and back")
}

// TestFieldDependencyGate mirrors the compiled provider's read-time gate on
// dependency_catalog: a field whose @dependency source is inactive is hidden
// from the object and its derived types.
func TestFieldDependencyGate(t *testing.T) {
	store, ctx := writtenStore(t)

	// The dependency source exists and is active first: a field's dependency
	// IS its declaring (extension) source, so writing "ghost" AFTER stamping
	// it would legitimately delete the row (deleteSourceRows removes what the
	// source declared).
	_, err := store.writeSource(ctx, newDesired(),
		SourceState{Name: "ghost", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)

	// Attribute one stored column to that source directly (the compiled path
	// stamps @dependency on virtual / extension fields).
	conn, err := store.pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE core.catalog.fields SET dependency_data_source = 'ghost'
		WHERE type_name = 'orders' AND name = 'amount'`)
	conn.Close()
	require.NoError(t, err)
	store.invalidateAll()

	def := store.ForName(ctx, "orders")
	require.NotNil(t, def)
	fd := def.Fields.ForName("amount")
	require.NotNil(t, fd, "field served while its dependency is active")
	assert.NotNil(t, fd.Directives.ForName("dependency"), "@dependency re-emitted on the field")

	// Disabling the dependency hides it — the closure covers fields BY
	// dependency attribution, so the cached object is evicted.
	require.NoError(t, store.setFlags(ctx, "ghost", true, true, false))
	def = store.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.Nil(t, def.Fields.ForName("amount"), "field hidden while its dependency is inactive")

	require.NoError(t, store.setFlags(ctx, "ghost", true, false, false))
	def = store.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("amount"), "and back")
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
