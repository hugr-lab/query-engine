//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnnotationOverlay pins the curation overlay applied at the end of
// ForName: a catalog.annotations row overrides the description of a type, a
// field or an argument by its dot-path key, over both a source-provided and a
// default (generated) description. A NULL-text seed row leaves the generated
// text intact.
func TestAnnotationOverlay(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	annotate(t, store.pool, `('type', 'shop_items', NULL, 'Curated shop item type')`)
	annotate(t, store.pool, `('field', 'shop_items.name', 'shop_items', 'Curated name field')`)
	// A default-described synthetic member (module-root select field) — the
	// overlay must win over the "Query shop_items objects" default.
	annotate(t, store.pool, `('field', '_module_shop_query.items', '_module_shop_query', 'Curated items query')`)
	// An argument of that same root field.
	annotate(t, store.pool, `('argument', '_module_shop_query.items.limit', '_module_shop_query.items', 'At most N items')`)
	// A seed row (NULL text) must NOT blank the generated description.
	annotate(t, store.pool, `('field', 'shop_items.price', 'shop_items', NULL)`)

	items := store.ForName(ctx, "shop_items")
	require.NotNil(t, items)
	assert.Equal(t, "Curated shop item type", items.Description)
	assert.Equal(t, "Curated name field", items.Fields.ForName("name").Description)
	assert.Empty(t, items.Fields.ForName("price").Description, "seed row leaves the field blank")

	root := store.ForName(ctx, "_module_shop_query")
	require.NotNil(t, root)
	itemsField := root.Fields.ForName("items")
	require.NotNil(t, itemsField)
	assert.Equal(t, "Curated items query", itemsField.Description, "overlay wins over the default")
	assert.Equal(t, "At most N items", itemsField.Arguments.ForName("limit").Description)

	// The pre-finalise generation carries neither the default nor the overlay.
	rawItems := store.forNameRaw(ctx, "shop_items")
	require.NotNil(t, rawItems)
	assert.Empty(t, rawItems.Description, "overlay is a finalise-step concern")
}

// annotate inserts one VALUES tuple into catalog.annotations.
func annotate(t *testing.T, pool *db.Pool, valuesTuple string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Exec(ctx, `INSERT INTO core.catalog.annotations
		(entity_kind, entity_key, parent, description) VALUES `+valuesTuple)
	require.NoError(t, err)
}
