//go:build duckdb_arrow

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One NAME can live in several root namespaces: query functions, mutation
// functions and subscriptions are independent surfaces. Real sources do it —
// an HTTP source exposes every operation as both a function and a mutation
// function, and core.models exposes `completion` as a function AND as a
// streaming subscription. Function identity therefore includes the KIND, both
// in the collected set and in the catalog.functions primary key.
const sameNameKindsSchema = `
type orders @module(name: "shop") @table(name: "orders") {
  id: Int! @pk
  amount: Float
}

extend type Function {
  completion(prompt: String!): String @module(name: "shop") @function(name: "completion")
}

extend type MutationFunction {
  completion(prompt: String!): String @module(name: "shop") @function(name: "completion")
}

extend type Subscription {
  completion(prompt: String!): orders @module(name: "shop")
}
`

func TestFunctionIdentityIncludesKind(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", sameNameKindsSchema)))

	assert.Equal(t, []string{"function", "mutation", "subscription"},
		rows(t, s.pool, `SELECT kind FROM core.catalog.functions
			WHERE module = 'shop' AND name = 'completion' ORDER BY kind`),
		"all three kinds stored under one name")

	// Each root namespace serves its own version.
	for _, tc := range []struct{ root, want string }{
		{"_module_shop_function", "String"},
		{"_module_shop_mut_function", "String"},
		{"_module_shop_subscription", "orders"},
	} {
		def := s.ForName(ctx, tc.root)
		require.NotNil(t, def, tc.root)
		f := def.Fields.ForName("completion")
		require.NotNil(t, f, "%s.completion", tc.root)
		assert.Equal(t, tc.want, f.Type.Name(), tc.root)
	}

	// A reload rewrites the rows wholesale — no duplicate-key on the composite PK.
	require.NoError(t, s.ClearSourceVersion(ctx, "shop"))
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", sameNameKindsSchema)))
	assert.Equal(t, []string{"3"}, rows(t, s.pool,
		`SELECT count(*) FROM core.catalog.functions WHERE module = 'shop' AND name = 'completion'`))
}

// Curation follows the same identity: the annotation's entity_kind IS the
// function's kind, so each root namespace carries its own description and one
// curation never bleeds into a same-named operation next door.
func TestFunctionCurationPerKind(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "shop", stringCatalog(t, "shop", sameNameKindsSchema)))

	require.NoError(t, s.SetFunctionDescription(ctx, "shop", "completion", "function", "Ask once", ""))
	require.NoError(t, s.SetFunctionDescription(ctx, "shop", "completion", "subscription", "Stream tokens", ""))

	// Three rows share the key, one per namespace; only the two curated ones
	// carry text (the mutation function keeps its generated description).
	assert.Equal(t, []string{"function|Ask once", "subscription|Stream tokens"},
		rows(t, s.pool, `SELECT entity_kind, description FROM core.catalog.annotations
			WHERE entity_key = 'shop.completion' AND description IS NOT NULL ORDER BY entity_kind`))

	for _, tc := range []struct{ root, want string }{
		{"_module_shop_function", "Ask once"},
		{"_module_shop_subscription", "Stream tokens"},
	} {
		def := s.ForName(ctx, tc.root)
		require.NotNil(t, def, tc.root)
		assert.Equal(t, tc.want, def.Fields.ForName("completion").Description, tc.root)
	}
	assert.NotEqual(t, "Ask once",
		s.ForName(ctx, "_module_shop_mut_function").Fields.ForName("completion").Description,
		"the uncurated namespace keeps its generated text")

	// An unknown kind is refused rather than written to an unreachable key.
	require.Error(t, s.SetFunctionDescription(ctx, "shop", "completion", "query", "x", ""))
}

// extBaseSchema / extJoinSchema mirror a cross-source extension: the extension
// declares a @join field ON THE BASE object. The field's data_source is
// resolved to the source whose DATA it reads and its dependency_data_source
// keeps the DECLARING extension — the pair that decides whose reload removes
// the row.
const extBaseSchema = `
type orders @module(name: "shop") @table(name: "orders") {
  id: Int! @pk
  customer_id: Int!
  amount: Float
}

type customers @module(name: "shop") @table(name: "customers") {
  id: Int! @pk
  name: String
}
`

// The join target belongs to the BASE source, so the field's data attribution
// (base) differs from its declaring source (ext) — the live shape that broke.
const extJoinSchema = `
extend type orders @dependency(name: "base") {
  cust: [customers] @join(references_name: "customers", source_fields: ["customer_id"], references_fields: ["id"])
}
`

func TestExtensionFieldReload(t *testing.T) {
	s, ctx := managerStore(t, Config{VecSize: 8})
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", extBaseSchema)))
	require.NoError(t, s.AddCatalog(ctx, "ext", stringExtCatalog(t, "ext", extJoinSchema)))

	// The extension's field on the base object: attributed to the source whose
	// data it reads, declared by the extension.
	assert.Equal(t, []string{"base|ext"}, rows(t, s.pool,
		`SELECT data_source, dependency_data_source FROM core.catalog.fields
			WHERE type_name = 'orders' AND name = 'cust'`),
		"data attribution follows the join target, declaration stays with the extension")

	// RELOADING THE EXTENSION must rewrite its own rows — the field row is not
	// attributed to it, so a delete by data_source alone left it behind and the
	// re-insert hit the (type_name, name) primary key.
	require.NoError(t, s.ClearSourceVersion(ctx, "ext"))
	require.NoError(t, s.AddCatalog(ctx, "ext", stringExtCatalog(t, "ext", extJoinSchema)))
	assert.Equal(t, []string{"1"}, rows(t, s.pool,
		`SELECT count(*) FROM core.catalog.fields WHERE type_name = 'orders' AND name = 'cust'`),
		"extension reload rewrites its own field, no duplicate")

	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("cust"), "still served after the extension reloaded")

	// RELOADING THE BASE must NOT remove a field it never declared.
	require.NoError(t, s.ClearSourceVersion(ctx, "base"))
	require.NoError(t, s.AddCatalog(ctx, "base", stringCatalog(t, "base", extBaseSchema)))
	assert.Equal(t, []string{"1"}, rows(t, s.pool,
		`SELECT count(*) FROM core.catalog.fields WHERE type_name = 'orders' AND name = 'cust'`),
		"the base source does not delete the extension's field")
	def = s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("cust"), "still served after the base reloaded")
}
