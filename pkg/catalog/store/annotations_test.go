//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnnotationOverlay pins the layered curation overlay applied at the end of
// ForName. The LOGICAL layer keys by the source-level entity: data_object /
// type on the definition, field on data-object fields (incl. the default-filled
// synthetic ones), function on the module-root function fields. The GRAPHQL
// layer keys the generated surface (gql_type / gql_field / gql_argument) and
// runs AFTER the logical layer, so it wins. A NULL-text seed row is ignored.
func TestAnnotationOverlay(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	// LOGICAL — a data object and one of its fields.
	annotate(t, store.pool, `('data_object', 'shop_items', NULL, 'Curated shop item type')`)
	annotate(t, store.pool, `('field', 'shop_items.name', 'shop_items', 'Curated name field')`)
	// A seed row (NULL text) must NOT blank the generated description.
	annotate(t, store.pool, `('field', 'shop_items.price', 'shop_items', NULL)`)
	// LOGICAL — a function keyed module.name, surfaced on the module function root.
	annotate(t, store.pool, `('function', 'tools.slugify', 'tools', 'Slugify a string')`)

	// GRAPHQL — a generated root field (overrides the "Query shop_items objects"
	// default) and one of its arguments.
	annotate(t, store.pool, `('gql_field', '_module_shop_query.items', '_module_shop_query', 'Curated items query')`)
	annotate(t, store.pool, `('gql_argument', '_module_shop_query.items.limit', '_module_shop_query.items', 'At most N items')`)

	items := store.ForName(ctx, "shop_items")
	require.NotNil(t, items)
	assert.Equal(t, "Curated shop item type", items.Description)
	assert.Equal(t, "Curated name field", items.Fields.ForName("name").Description)
	assert.Equal(t, "The item price", items.Fields.ForName("price").Description,
		"seed row leaves the source-provided text intact")

	tools := store.ForName(ctx, "_module_tools_function")
	require.NotNil(t, tools)
	assert.Equal(t, "Slugify a string", tools.Fields.ForName("slugify").Description,
		"logical function overlay keyed module.name")

	root := store.ForName(ctx, "_module_shop_query")
	require.NotNil(t, root)
	itemsField := root.Fields.ForName("items")
	require.NotNil(t, itemsField)
	assert.Equal(t, "Curated items query", itemsField.Description, "gql overlay wins over the default")
	assert.Equal(t, "At most N items", itemsField.Arguments.ForName("limit").Description)

	// The pre-finalise generation carries neither the default nor the overlay.
	rawItems := store.forNameRaw(ctx, "shop_items")
	require.NotNil(t, rawItems)
	assert.Empty(t, rawItems.Description, "overlay is a finalise-step concern")
}

// TestAnnotationDerivedInheritance pins the implicit inheritance: derived types
// (filters / mutation inputs / aggregations) read their base through ForName —
// already carrying the curation — so a curated base field shows on the derived
// types' same-named fields without any per-derived-type rows. A direct
// gql_field row on the derived type still wins.
func TestAnnotationDerivedInheritance(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	annotate(t, store.pool, `('field', 'shop_items.name', 'shop_items', 'Curated name field')`)
	// A direct gql curation on the FILTER's other field must survive inheritance.
	annotate(t, store.pool, `('gql_field', 'shop_items_filter.price', 'shop_items_filter', 'Filter by price')`)

	filter := store.ForName(ctx, "shop_items_filter")
	require.NotNil(t, filter)
	assert.Equal(t, "Curated name field", filter.Fields.ForName("name").Description,
		"filter field inherits the base field curation")
	assert.Equal(t, "Filter by price", filter.Fields.ForName("price").Description,
		"direct gql curation on the derived type wins")

	agg := store.ForName(ctx, "_shop_items_aggregation")
	require.NotNil(t, agg)
	assert.Equal(t, "Curated name field", agg.Fields.ForName("name").Description,
		"aggregation twin inherits the base field curation")
	assert.Equal(t, "The item price", agg.Fields.ForName("price").Description,
		"aggregation twin inherits the SOURCE description too — no curation needed")

	mut := store.ForName(ctx, "shop_items_mut_input_data")
	require.NotNil(t, mut)
	assert.Equal(t, "Curated name field", mut.Fields.ForName("name").Description,
		"mutation input inherits the base field curation")
}

// TestAnnotationGraphQLWinsLogical pins the layer precedence: a gql_type
// curation overrides a data_object curation on the same reconstructed type.
func TestAnnotationGraphQLWinsLogical(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	annotate(t, store.pool, `('data_object', 'shop_items', NULL, 'Logical description')`)
	assert.Equal(t, "Logical description", store.ForName(ctx, "shop_items").Description)

	store.evictType("shop_items")
	annotate(t, store.pool, `('gql_type', 'shop_items', NULL, 'GraphQL description')`)
	assert.Equal(t, "GraphQL description", store.ForName(ctx, "shop_items").Description,
		"the GraphQL layer runs last and wins")
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
