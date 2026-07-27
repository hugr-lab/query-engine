//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	qetypes "github.com/hugr-lab/query-engine/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLogicalAnnotatorWrite exercises the write→read curation loop through the
// LogicalAnnotator surface: a Set*Description upserts the annotation and evicts
// the affected type, so a subsequent ForName reflects the change even when the
// type was cached before the write. Writing an empty description clears the
// curation (the generated text shows through again).
func TestLogicalAnnotatorWrite(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	// Prime the cache with the pre-annotation definitions.
	require.Empty(t, store.ForName(ctx, "shop_items").Description)
	require.Equal(t, "Query shop_items objects",
		store.ForName(ctx, "_module_shop_query").Fields.ForName("items").Description)

	// Data-object description — the cached definition is evicted, the read reflects it.
	require.NoError(t, store.SetDataObjectDescription(ctx, "shop_items", "Curated item", "long form"))
	assert.Equal(t, "Curated item", store.ForName(ctx, "shop_items").Description)

	// Object field description. The derived types were CACHED above the write —
	// the family evict makes them reflect the inherited curation too.
	require.NotNil(t, store.ForName(ctx, "shop_items_filter")) // prime the derived cache
	require.NoError(t, store.SetObjectFieldDescription(ctx, "shop_items", "name", "The item name", ""))
	assert.Equal(t, "The item name", store.ForName(ctx, "shop_items").Fields.ForName("name").Description)
	assert.Equal(t, "The item name",
		store.ForName(ctx, "shop_items_filter").Fields.ForName("name").Description,
		"cached filter evicted with the base — inherits the new curation")

	// Function description keyed module.name under its kind, surfaced on the
	// module function root.
	require.NoError(t, store.SetFunctionDescription(ctx, "tools", "slugify", "function", "Slugify a string", ""))
	assert.Equal(t, "Slugify a string",
		store.ForName(ctx, "_module_tools_function").Fields.ForName("slugify").Description)

	// Clearing: an empty description reverts to the generated text.
	require.NoError(t, store.SetDataObjectDescription(ctx, "shop_items", "", ""))
	assert.Empty(t, store.ForName(ctx, "shop_items").Description, "empty write clears the curation")

	// Module / data-source curation lands as rows (no ForName surface yet).
	require.NoError(t, store.SetModuleDescription(ctx, "shop", "The shop module", ""))
	require.NoError(t, store.SetDataSourceDescription(ctx, "shop", "The shop source", ""))
	assert.Equal(t, []string{"module|shop|The shop module", "data_source|shop|The shop source"},
		annotationRows(t, store.pool, "shop"))

	// Upsert semantics: a second write to the same key replaces, not duplicates.
	require.NoError(t, store.SetModuleDescription(ctx, "shop", "Updated module", ""))
	assert.Equal(t, []string{"module|shop|Updated module", "data_source|shop|The shop source"},
		annotationRows(t, store.pool, "shop"))
}

// TestGraphQLAnnotatorWrite curates the generated surface (a root field and its
// argument) and reads the change back through ForName.
func TestGraphQLAnnotatorWrite(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	require.NoError(t, store.SetFieldDescription(ctx, "_module_shop_query", "items", "Curated items", ""))
	require.NoError(t, store.SetArgumentDescription(ctx, "_module_shop_query", "items", "limit", "Max rows", ""))

	items := store.ForName(ctx, "_module_shop_query").Fields.ForName("items")
	require.NotNil(t, items)
	assert.Equal(t, "Curated items", items.Description)
	assert.Equal(t, "Max rows", items.Arguments.ForName("limit").Description)
}

// TestAnnotatorReadonly rejects curation on a read-only store, on both surfaces.
func TestAnnotatorReadonly(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8, IsReadonly: true}, nil)
	require.NoError(t, err)

	assert.ErrorIs(t, store.SetDataObjectDescription(ctx, "shop_items", "x", ""), errReadonly)
	assert.ErrorIs(t, store.SetModuleDescription(ctx, "shop", "x", ""), errReadonly)
	assert.ErrorIs(t, store.SetDefinitionDescription(ctx, "shop_items_filter", "x", ""), errReadonly)
}

// TestAnnotatorEmbeds pins the vector side: with an embedder configured, a
// curation write stores an embedding vector in the annotation row. A clearing
// write (empty text) leaves the vec column UNTOUCHED — reconciling a stale
// curated vector with the entity's load-time seed is the seed step's job.
func TestAnnotatorEmbeds(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, fakeEmbedder{dim: 8})
	require.NoError(t, err)

	vecPresent := func() []string {
		return rows(t, store.pool, `SELECT vec IS NOT NULL, description IS NOT NULL
			FROM core.catalog.annotations WHERE entity_kind = 'module' AND entity_key = 'shop'`)
	}

	require.NoError(t, store.SetModuleDescription(ctx, "shop", "The shop module", ""))
	assert.Equal(t, []string{"true|true"}, vecPresent(), "curation stores text and vector")

	// Clearing nulls the text but leaves the vector untouched.
	require.NoError(t, store.SetModuleDescription(ctx, "shop", "", ""))
	assert.Equal(t, []string{"true|false"}, vecPresent(), "clear nulls text, keeps vec")
}

// fakeEmbedder returns a fixed-size zero vector — enough to exercise the write
// path without a real embedding backend.
type fakeEmbedder struct{ dim int }

func (f fakeEmbedder) CreateEmbedding(_ context.Context, _ string) (*qetypes.EmbeddingResult, error) {
	return &qetypes.EmbeddingResult{Vector: make(qetypes.Vector, f.dim)}, nil
}

func (f fakeEmbedder) CreateEmbeddings(_ context.Context, inputs []string) (*qetypes.EmbeddingsResult, error) {
	out := make([]qetypes.Vector, len(inputs))
	for i := range out {
		out[i] = make(qetypes.Vector, f.dim)
	}
	return &qetypes.EmbeddingsResult{Vectors: out}, nil
}

// annotationRows returns "entity_kind|entity_key|description" for a key.
func annotationRows(t *testing.T, pool *db.Pool, key string) []string {
	t.Helper()
	return rows(t, pool, `SELECT entity_kind, entity_key, description FROM core.catalog.annotations
		WHERE entity_key = `+lit(key)+` ORDER BY entity_kind DESC`)
}
