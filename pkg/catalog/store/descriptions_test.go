//go:build duckdb_arrow

package store

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// TestDefaultDescriptions pins the store-only description enrichment applied at
// the end of ForName: synthetic query members that the source schema leaves
// blank (module-root data-object fields, shared-type members, their structural
// arguments) receive default descriptions, while the pre-finalise generation
// (forNameRaw) stays blank — so the enrichment is a finalise-step concern, not
// baked into the parity-checked generation.
func TestDefaultDescriptions(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)

	fieldDesc := func(typeName, fieldName string) string {
		def := store.ForName(ctx, typeName)
		require.NotNil(t, def, "type %s served", typeName)
		f := def.Fields.ForName(fieldName)
		require.NotNil(t, f, "%s.%s present", typeName, fieldName)
		return f.Description
	}

	// Module-root data-object fields, worded by their query/mutation shape.
	assert.Equal(t, "Query shop_items objects", fieldDesc("_module_shop_query", "items"))
	assert.Equal(t, "Insert shop_items objects", fieldDesc("_module_shop_mutation", "insert_items"))
	assert.Equal(t, "Update shop_items objects", fieldDesc("_module_shop_mutation", "update_items"))
	assert.Equal(t, "Delete shop_items objects", fieldDesc("_module_shop_mutation", "delete_items"))

	// Aggregation root fields already carry a generated description — the
	// default must NOT overwrite it.
	assert.Equal(t, "The aggregation for items", fieldDesc("_module_shop_query", "items_aggregation"))

	// Shared-type members, worded by the shared kind and member subtype.
	assert.Equal(t, "The shop_items records joined to the query",
		fieldDesc("_join", "shop_items"))
	assert.Equal(t, "Aggregation of shop_items records joined to the query",
		fieldDesc("_join", "shop_items_aggregation"))
	assert.Equal(t, "Bucket aggregation of shop_items records joined to the query",
		fieldDesc("_join", "shop_items_bucket_aggregation"))
	assert.Equal(t, "Aggregation of shop_items records joined to the query",
		fieldDesc("_join_aggregation", "shop_items"))

	// Structural arguments the generators leave bare.
	insertItems := store.ForName(ctx, "_module_shop_mutation").Fields.ForName("insert_items")
	require.NotNil(t, insertItems)
	assert.Equal(t, base.DescMutationData, argDesc(t, insertItems, "data"))
	updateItems := store.ForName(ctx, "_module_shop_mutation").Fields.ForName("update_items")
	require.NotNil(t, updateItems)
	assert.Equal(t, base.DescFilter, argDesc(t, updateItems, "filter"))

	// The pre-finalise generation is blank on exactly these members — the
	// enrichment is a finalise-step concern, invisible to the parity oracle.
	rawJoin := store.forNameRaw(ctx, "_join")
	require.NotNil(t, rawJoin)
	assert.Empty(t, rawJoin.Fields.ForName("shop_items").Description,
		"pre-finalise shared member carries no description")
	rawQuery := store.forNameRaw(ctx, "_module_shop_query")
	require.NotNil(t, rawQuery)
	assert.Empty(t, rawQuery.Fields.ForName("items").Description,
		"pre-finalise root field carries no description")
}

func argDesc(t *testing.T, f *ast.FieldDefinition, name string) string {
	t.Helper()
	a := f.Arguments.ForName(name)
	require.NotNil(t, a, "argument %s present on %s", name, f.Name)
	return a.Description
}
