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

// TestSeedVectorsAndSweep pins the D24 seed/sweep lifecycle. writeSource seeds a
// vec-only annotation row (text NULL) for every logical entity of the source
// without clobbering a pre-existing curation; deleteSource (the unregister
// primitive) sweeps the seeds and keeps the curated rows. Both use the
// cross-engine statement shapes pinned by the CoreDB inventory.
func TestSeedVectorsAndSweep(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, fakeEmbedder{dim: 8})
	require.NoError(t, err)

	// Curate one data object AND one module BEFORE the source loads — the seed
	// must clobber neither (modules are insert-only; the object seed is guarded).
	require.NoError(t, store.SetDataObjectDescription(ctx, "orders", "Curated orders", ""))
	require.NoError(t, store.SetModuleDescription(ctx, "sales", "Curated sales module", ""))

	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")
	state := SourceState{Name: "test", Version: "v1", Engine: "duckdb", Prefix: "shop", AsModule: true, Loaded: true}
	changed, err := store.writeSource(ctx, d, state)
	require.NoError(t, err)
	require.True(t, changed)

	seedRow := func(kind, key string) []string {
		return rows(t, pool, `SELECT vec IS NOT NULL, description IS NOT NULL FROM core.catalog.annotations
			WHERE entity_kind = `+lit(kind)+` AND entity_key = `+lit(key))
	}

	// Seeds landed: vector present, text NULL — for the source, an object and a function.
	assert.Equal(t, []string{"true|false"}, seedRow(kindDataSource, "test"))
	assert.Equal(t, []string{"true|false"}, seedRow(kindDataObject, "customers"))
	assert.Equal(t, []string{"true|false"}, seedRow(kindFunction, "sales.order_status"))
	// A field seed keyed owner.field.
	assert.Equal(t, []string{"true|false"}, seedRow(kindField, "customers.id"))

	// The curated object survived: text kept, vec is the curated one (not reseeded).
	assert.Equal(t, []string{"Curated orders"}, rows(t, pool,
		`SELECT description FROM core.catalog.annotations WHERE entity_kind = 'data_object' AND entity_key = 'orders'`))

	// Modules: the curated one is untouched (insert-only), the other is a fresh
	// vec-only seed.
	assert.Equal(t, []string{"Curated sales module"}, rows(t, pool,
		`SELECT description FROM core.catalog.annotations WHERE entity_kind = 'module' AND entity_key = 'sales'`))
	assert.Equal(t, []string{"true|false"}, seedRow(kindModule, "sales.reports"))

	// A curated row is NOT double-counted as a seed; there are many seeds overall.
	seedCount := rows(t, pool, `SELECT count(*) FROM core.catalog.annotations WHERE description IS NULL AND vec IS NOT NULL`)
	assert.NotEqual(t, []string{"0"}, seedCount)

	// Reload of the same version is a no-op — seeds are not rewritten, curation stays.
	changed, err = store.writeSource(ctx, d, state)
	require.NoError(t, err)
	require.False(t, changed)

	// --- Unregister sweeps seeds, keeps curation ---
	require.NoError(t, store.deleteSource(ctx, "test"))

	assert.Empty(t, seedRow(kindDataSource, "test"), "data-source seed swept")
	assert.Empty(t, seedRow(kindDataObject, "customers"), "object seed swept")
	assert.Empty(t, seedRow(kindFunction, "sales.order_status"), "function seed swept")
	assert.Empty(t, seedRow(kindField, "customers.id"), "field seed swept")

	// The curated object survives; module seeds stay as tolerated orphans —
	// modules are shared and not source-attributable, and a vec-only orphan is
	// invisible through entity_modules (the module row is gone) and is revived
	// by the seed upsert if the module is recreated.
	assert.Equal(t, []string{"data_object|orders", "module|sales", "module|sales.reports"},
		rows(t, pool, `SELECT entity_kind, entity_key FROM core.catalog.annotations
			ORDER BY entity_kind, entity_key`),
		"curated row plus tolerated module-seed orphans")
}

// TestSeedNoEmbedderNoop confirms the seed path is inert without an embedder:
// writeSource persists entity rows but writes no annotation seed rows.
func TestSeedNoEmbedderNoop(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, nil) // no embedder
	require.NoError(t, err)

	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")
	_, err = store.writeSource(ctx, d, SourceState{Name: "test", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)

	assert.Equal(t, []string{"0"}, rows(t, pool, `SELECT count(*) FROM core.catalog.annotations`),
		"no embedder → no seed rows")
}
