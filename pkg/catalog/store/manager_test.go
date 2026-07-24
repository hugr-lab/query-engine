//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
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

// stringCatalog builds a raw (uncompiled) catalog source — the manager runs
// the partial compile itself.
func stringCatalog(t *testing.T, name, schema string) *catsrc.StringSource {
	t.Helper()
	e := &engines.DuckDB{}
	src, err := catsrc.NewStringSource(name, e, compiler.Options{
		Name:         name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
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
