//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	catsrc "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const managerSchemaV1 = `
type orders @module(name: "sales") @table(name: "orders") {
  id: Int! @pk
  amount: Float
}
`

const managerSchemaV2 = `
type orders @module(name: "sales") @table(name: "orders") {
  id: Int! @pk
  amount: Float
  note: String
}
`

// managerSchemaBadJoin is managerSchemaV1 with a @join whose references_fields
// name a column the target does not have — a same-source declaration the write
// barrier must refuse.
const managerSchemaBadJoin = `
type orders @module(name: "sales") @table(name: "orders") {
  id: Int! @pk
  amount: Float
  vip: [vips] @join(references_name: "vips", source_fields: ["id"], references_fields: ["nope"])
}

type vips @module(name: "sales") @table(name: "vips") {
  order_id: Int! @pk
}
`

// managerExtSchema is an extension source over the "base" source's orders
// object: one extension field on the foreign object plus one own view, both
// declaring @dependency(name: "base").
const managerExtSchema = `
extend type orders @dependency(name: "base") {
  vip: [ext_vip] @join(references_name: "ext_vip", source_fields: ["id"], references_fields: ["order_id"])
}

type ext_vip @view(name: "ext_vip") @dependency(name: "base") @module(name: "ext") {
  order_id: Int! @pk
  level: String
}
`

// managerStore boots a fresh in-memory CoreDB and returns an EMPTY store —
// the manager tests drive it through the CatalogManager surface.
func managerStore(t *testing.T, cfg Config) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, cfg, nil)
	require.NoError(t, err)
	return store, ctx
}

// managerStoreEmbedded is managerStore with an embedder attached, so a test can
// tell whether an operation re-embedded the source's entities.
func managerStoreEmbedded(t *testing.T, cfg Config, e Embedder) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, cfg, e)
	require.NoError(t, err)
	return store, ctx
}

// stringCatalog builds a raw (uncompiled) catalog source — the manager runs
// the partial compile itself.
func stringCatalog(t *testing.T, name, schema string) *catsrc.StringSource {
	t.Helper()
	e := &engines.DuckDB{}
	src, err := catsrc.NewStringSource(name, e, base.Options{
		Name:         name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, schema)
	require.NoError(t, err)
	return src
}

// stringExtCatalog builds a raw EXTENSION catalog source (IsExtension set).
func stringExtCatalog(t *testing.T, name, schema string) *catsrc.StringSource {
	t.Helper()
	e := &engines.DuckDB{}
	src, err := catsrc.NewStringSource(name, e, base.Options{
		Name:         name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
		IsExtension:  true,
	}, schema)
	require.NoError(t, err)
	return src
}

// reloadableSource swaps its inner StringSource on Reload — the manager's
// ReloadCatalog then sees the new content through the SAME handle.
type reloadableSource struct {
	*catsrc.StringSource
	next *catsrc.StringSource
}

func (r *reloadableSource) Reload(context.Context) error {
	if r.next != nil {
		r.StringSource = r.next
		r.next = nil
	}
	return nil
}

func TestManagerAddCatalog(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})

	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))

	def := s.ForName(ctx, "orders")
	require.NotNil(t, def, "stored object served")
	assert.NotNil(t, def.Fields.ForName("amount"))
	assert.True(t, s.ExistsCatalog("shop"))
	assert.False(t, s.IsSuspended("shop"))

	v, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v, "add bumps the cluster version counter")
}

// TestManagerAddRefusesInvalidSchema pins the write barrier: compileAndWrite
// compiles FIRST, so a declaration that does not validate never reaches
// writeSource — and the error carries ErrSchemaInvalid, which is what lets the
// boot path suspend a source instead of dropping its rows.
func TestManagerAddRefusesInvalidSchema(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})

	// A first load that does not validate writes nothing at all.
	err := s.AddCatalog(ctx, "bad", stringCatalog(t, "bad", managerSchemaBadJoin))
	require.Error(t, err)
	assert.ErrorIs(t, err, catalog.ErrSchemaInvalid)
	assert.Contains(t, err.Error(), `references field "nope" not found`)
	assert.False(t, s.ExistsCatalog("bad"))
	assert.Nil(t, s.ForName(ctx, "orders"))
	assert.Nil(t, s.ForName(ctx, "vips"))

	// A RELOAD that does not validate leaves the stored version serving.
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))
	require.NotNil(t, s.ForName(ctx, "orders"))

	err = s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaBadJoin))
	require.Error(t, err)
	assert.ErrorIs(t, err, catalog.ErrSchemaInvalid)

	def := s.ForName(ctx, "orders")
	require.NotNil(t, def, "the stored version still serves")
	assert.NotNil(t, def.Fields.ForName("amount"))
	assert.Nil(t, def.Fields.ForName("vip"), "the refused version left no trace")
	assert.Nil(t, s.ForName(ctx, "vips"))
	assert.False(t, s.IsSuspended("shop"), "the write path reports; suspending is the caller's call")
}

func TestManagerAddVersionGate(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))

	before := s.ForName(ctx, "orders")
	require.NotNil(t, before)
	vBefore, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)

	// Same content, fresh handle — the stored version matches, nothing rewrites.
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))
	after := s.ForName(ctx, "orders")
	assert.True(t, before == after, "unchanged version keeps the cache — no rewrite happened")
	vAfter, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, vBefore, vAfter, "no-op add does not bump the version counter")

	// Changed content rewrites and serves the new shape.
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV2)))
	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("note"), "new column served after version change")
	vChanged, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, vAfter+1, vChanged)
}

func TestManagerSuspendReactivate(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	cat := stringCatalog(t, "shop", managerSchemaV1)
	require.NoError(t, s.AddCatalog(ctx, "shop", cat))
	require.NotNil(t, s.ForName(ctx, "orders"))

	require.NoError(t, s.SuspendCatalog(ctx, "shop"))
	assert.True(t, s.IsSuspended("shop"))
	assert.Nil(t, s.ForName(ctx, "orders"), "suspended source is hidden by the activity gate")
	assert.True(t, s.ExistsCatalog("shop"), "suspension keeps the registration")

	// Suspending again is a no-op.
	require.NoError(t, s.SuspendCatalog(ctx, "shop"))

	// Reactivation with the unchanged version only clears the flag — rows were
	// never dropped.
	require.NoError(t, s.ReactivateCatalog(ctx, "shop", cat))
	assert.False(t, s.IsSuspended("shop"))
	require.NotNil(t, s.ForName(ctx, "orders"), "served again after reactivation")
}

// TestManagerReconnectDoesNotRewrite is the hugr-app disconnect/reconnect
// scenario at the storage level: the heartbeat suspends the catalog when the
// app stops answering and, on recovery, RE-READS the app's SDL over the wire
// and calls ReactivateCatalog with it (pkg/data-sources/service.go). An app
// that comes back unchanged must cost a flag flip — the rows stay AND the
// entities are not re-embedded, which is the expensive half of a rewrite (one
// embedder round trip per entity). Only a changed SDL may rewrite.
func TestManagerReconnectDoesNotRewrite(t *testing.T) {
	salt := 0
	s, ctx := managerStoreEmbedded(t, Config{VecSize: 8}, saltEmbedder{dim: 8, salt: &salt})
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))
	written := loadedAt(t, s, "shop")
	require.NotEmpty(t, written)

	// The app stops answering: the heartbeat suspends it.
	require.NoError(t, s.SuspendCatalog(ctx, "shop"))
	assert.True(t, s.IsSuspended("shop"))

	// It comes back with the SAME SDL. Every vector written from here on would
	// carry the new salt — none may.
	salt = 7
	require.NoError(t, s.ReactivateCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))
	assert.False(t, s.IsSuspended("shop"))
	require.NotNil(t, s.ForName(ctx, "orders"), "served again after the reconnect")
	assert.Equal(t, []string{"0"}, rows(t, s.pool,
		`SELECT DISTINCT vec[2] FROM core.catalog.annotations WHERE vec IS NOT NULL`),
		"an unchanged app is not re-embedded on reconnect")
	assert.Equal(t, written, loadedAt(t, s, "shop"), "and its rows are not rewritten")

	// A RESTARTED app that changed its schema must rewrite — the gate is on the
	// content, not on the reconnect.
	require.NoError(t, s.ReactivateCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV2)))
	require.NotNil(t, s.ForName(ctx, "orders").Fields.ForName("note"), "new column served")
	assert.Equal(t, []string{"7"}, rows(t, s.pool,
		`SELECT vec[2] FROM core.catalog.annotations
			WHERE entity_kind = 'data_object' AND entity_key = 'orders'`),
		"a changed app re-embeds its entities (modules stay insert-only by design)")
	assert.NotEqual(t, written, loadedAt(t, s, "shop"))
}

// loadedAt reads the write stamp of a source — moved only by a real write
// (upsertMeta), never by a flag flip.
func loadedAt(t *testing.T, s *Store, name string) string {
	t.Helper()
	got := rows(t, s.pool, `SELECT CAST(loaded_at AS VARCHAR) FROM core.catalog.data_source_meta
		WHERE data_source = `+lit(name))
	require.Len(t, got, 1)
	return got[0]
}

func TestManagerRemoveCatalog(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", managerSchemaV1)))
	require.NotNil(t, s.ForName(ctx, "orders"))

	require.NoError(t, s.RemoveCatalog(ctx, "shop"))
	assert.Nil(t, s.ForName(ctx, "orders"))
	assert.False(t, s.ExistsCatalog("shop"))
	_, ok, err := s.sourceMeta(ctx, "shop")
	require.NoError(t, err)
	assert.False(t, ok, "meta row removed")
}

func TestManagerReloadCatalog(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})

	src := &reloadableSource{
		StringSource: stringCatalog(t, "shop", managerSchemaV1),
		next:         stringCatalog(t, "shop", managerSchemaV2),
	}
	require.NoError(t, s.AddCatalog(ctx, "shop", src))
	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	require.Nil(t, def.Fields.ForName("note"))

	require.NoError(t, s.ReloadCatalog(ctx, "shop"))
	def = s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("note"), "reload served the refreshed content")

	// Reloading an unknown catalog errors.
	require.Error(t, s.ReloadCatalog(ctx, "unknown"))
}

func TestManagerReadOnly(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8, IsReadonly: true})
	cat := stringCatalog(t, "shop", managerSchemaV1)

	assert.ErrorIs(t, s.AddCatalog(ctx, "shop", cat), ErrReadOnly)
	assert.ErrorIs(t, s.RemoveCatalog(ctx, "shop"), ErrReadOnly)
	assert.ErrorIs(t, s.ReloadCatalog(ctx, "shop"), ErrReadOnly)
	assert.ErrorIs(t, s.SuspendCatalog(ctx, "shop"), ErrReadOnly)
	assert.ErrorIs(t, s.ReactivateCatalog(ctx, "shop", cat), ErrReadOnly)
}

func TestManagerDependencyCascade(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", managerSchemaV1)))
	require.NoError(t, s.AddCatalog(ctx, "ext", stringExtCatalog(t, "ext", managerExtSchema)))

	deps, err := s.dependentsOf(ctx, "base")
	require.NoError(t, err)
	assert.Equal(t, []string{"ext"}, deps, "@dependency persisted")

	require.NotNil(t, s.ForName(ctx, "ext_vip"), "extension view served")
	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("vip"), "extension field on the base object")

	// Removing the base suspends the dependent; its rows stay.
	require.NoError(t, s.RemoveCatalog(ctx, "base"))
	assert.True(t, s.IsSuspended("ext"))
	assert.Nil(t, s.ForName(ctx, "ext_vip"), "suspended dependent hidden")

	// The base returns — the dependent auto-reactivates (recompiled).
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", managerSchemaV1)))
	assert.False(t, s.IsSuspended("ext"))
	require.NotNil(t, s.ForName(ctx, "ext_vip"), "reactivated dependent served")
	def = s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("vip"), "extension field back after reactivation")
}

func TestManagerExtensionBeforeBase(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})

	// The extension loads FIRST: rows land (order-independent), the source is
	// stored SUSPENDED — its declared dependency is not active yet.
	require.NoError(t, s.AddCatalog(ctx, "ext", stringExtCatalog(t, "ext", managerExtSchema)))
	assert.True(t, s.IsSuspended("ext"))
	assert.Nil(t, s.ForName(ctx, "ext_vip"))

	// The base arrives — reactivateSuspended recompiles the extension and
	// clears the flag.
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", managerSchemaV1)))
	assert.False(t, s.IsSuspended("ext"))
	require.NotNil(t, s.ForName(ctx, "ext_vip"))
	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("vip"), "extension field served once the base exists")
}

func TestManagerRefreshDependents(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", managerSchemaV1)))
	require.NoError(t, s.AddCatalog(ctx, "ext", stringExtCatalog(t, "ext", managerExtSchema)))

	// A base CONTENT change force-refreshes the active dependent — it stays
	// visible and consistent against the new base.
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", managerSchemaV2)))
	assert.False(t, s.IsSuspended("ext"))
	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("note"), "new base column served")
	assert.NotNil(t, def.Fields.ForName("vip"), "extension field survived the base rewrite")
	require.NotNil(t, s.ForName(ctx, "ext_vip"))
}

func TestSchemaVersionCounter(t *testing.T) {
	s, ctx := managerStore(t, Config{})

	v, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), v, "bootstrap seeds the counter at 0")

	v, err = s.IncrementSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)

	v, err = s.IncrementSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), v)

	got, err := s.GetSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), got)
}
