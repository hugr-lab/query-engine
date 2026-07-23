//go:build duckdb_arrow

package store

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

// The golden frame for the generation layer (Ш4): the SAME fixture goes
// through (a) the partial compiler → collect → store → ForName and (b) the
// FULL compiler (GENERATE included) → static provider. Every name in
// genParityNames must produce identical definitions on both sides; each Ш4.x
// sub-step moves names from genPendingNames into genParityNames.

// genParityNames — generated names the store already serves at full parity
// with the compiled reference.
var genParityNames = []string{
	// Static prelude smoke: the same binary layer feeds both sides, proving
	// the compare pipe end to end.
	"Geometry",
	"JSON",
	// Residual source types: stored SDL vs compiler passthrough.
	"sales_by_country_args",
	// Ш4.1 — filter family: plain FK both directions, m2m endpoints, the
	// junction (no nav), the parameterized view.
	"orders_filter",
	"orders_list_filter",
	"customers_filter",
	"tags_filter",
	"tags_list_filter",
	"order_tags_filter",
	"sales_by_country_filter",
	// Ш4.2 — mutation inputs: columns minus computed/virtual, relation
	// subqueries on insert (FWD single / BACK+M2M list, same catalog), the
	// junction without forward fields.
	"orders_mut_input_data",
	"orders_mut_data",
	"customers_mut_input_data",
	"customers_mut_data",
	"tags_mut_input_data",
	"order_tags_mut_input_data",
	"order_tags_mut_data",
	// Ш4.3 — aggregation family: scalar/extra twins, relation members both
	// directions + m2m, @join virtual twins, bucket, sub-aggregation depth 1-2.
	"_orders_aggregation",
	"_orders_aggregation_bucket",
	"_orders_aggregation_sub_aggregation",
	"_orders_aggregation_sub_aggregation_sub_aggregation",
	"_customers_aggregation",
	"_customers_aggregation_sub_aggregation",
	"_tags_aggregation",
	"_order_tags_aggregation",
	"_sales_by_country_aggregation",
	"_sales_by_country_aggregation_bucket",
	// Ш4.4/4.5 — the data objects themselves: markers, enriched references,
	// nav fields + twins, extras, shared members.
	"orders",
	"customers",
	"tags",
	"order_tags",
	"sales_by_country",
	// Ш4.6 — shared cross-source types.
	"_join",
	"_join_aggregation",
	"_spatial",
	"_spatial_aggregation",
	"_h3_data_query",
	// Ш4.7 — module roots.
	"Query",
	"Mutation",
	"Function",
	"MutationFunction",
	"Subscription",
	"_module_sales_query",
	"_module_sales_mutation",
	"_module_sales_function",
	"_module_sales_mut_function",
	"_module_sales_reports_query",
}

// genPendingNames — names the reference generates that the store must learn
// to serve, keyed by the sub-step that delivers them.
var genPendingNames = map[string][]string{}

// goldenRef compiles a fixture with the FULL rule set into a fresh static
// provider — the reference the generation layer converges to, name by name.
func goldenRef(t *testing.T, name, schema string) *static.Provider {
	t.Helper()
	ctx := context.Background()
	target, err := static.New()
	require.NoError(t, err)
	e := &engines.DuckDB{}
	src, err := sources.NewStringSource(name, e, compiler.Options{
		Name:         name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, schema)
	require.NoError(t, err)
	compiled, err := compiler.New(rules.RegisterAll()...).Compile(ctx, target, src, src.CompileOptions())
	require.NoError(t, err)
	require.NoError(t, target.Update(ctx, compiled))
	return target
}

func TestGenGoldenHarness(t *testing.T) {
	store, ctx := writtenStore(t)
	ref := goldenRef(t, "test", collectTestSchema)

	// The reference side really generates the artifacts the sub-steps target —
	// a pending name disappearing here means the fixture (or the expected
	// name) drifted, not that the work is done.
	for step, names := range genPendingNames {
		for _, name := range names {
			assert.NotNil(t, ref.ForName(ctx, name), "reference must generate %s (%s)", name, step)
		}
	}

	assertGenParity(t, ctx, store, ref, genParityNames)

	// Negative parity: names the compiler does NOT generate must stay absent
	// on the store side too (views take no mutation inputs; no subscriptions
	// or reports-module mutations in the fixture).
	for _, name := range []string{"sales_by_country_mut_input_data", "sales_by_country_mut_data",
		"_module_sales_subscription", "_module_sales_reports_mutation"} {
		assert.Nil(t, ref.ForName(ctx, name), "reference must not generate %s", name)
		assert.Nil(t, store.ForName(ctx, name), "store must not serve %s", name)
	}
}

// genStructSchema exercises the nested-structural branches: struct fields on
// a table (single / list / scalar list), struct-in-struct nesting, and the
// residual passthrough types themselves.
const genStructSchema = `
type products @module(name: "sales") @table(name: "products") {
  id: Int! @pk
  name: String!
  specs: ProductSpecs
  variants: [ProductVariant]
  tags: [String]
}

type ProductSpecs {
  weight: Float
  size: BoxSize
  labels: [String]
}

type BoxSize {
  w: Float
  h: Float
}

type ProductVariant {
  sku: String!
  price: Float
}
`

// TestGenGoldenStructs pins the struct-nesting parity: table filter/inputs
// reference the nested names, struct filters stay scalar-only, struct inputs
// nest Object members, and the passthrough types round-trip.
func TestGenGoldenStructs(t *testing.T) {
	store, ctx := storeFor(t, genStructSchema)
	ref := goldenRef(t, "test", genStructSchema)

	assertGenParity(t, ctx, store, ref, []string{
		// Residual passthrough types (stored SDL vs compiler passthrough).
		"ProductSpecs",
		"BoxSize",
		"ProductVariant",
		// Table filter nests struct members; struct filters are scalar-only
		// (size: BoxSize_filter must NOT appear inside ProductSpecs_filter).
		"products_filter",
		"ProductSpecs_filter",
		"BoxSize_filter",
		"ProductVariant_filter",
		"ProductVariant_list_filter",
		// Mutation inputs nest Object members on both levels.
		"products_mut_input_data",
		"products_mut_data",
		"ProductSpecs_mut_input_data",
		"ProductSpecs_mut_data",
		"BoxSize_mut_input_data",
		"ProductVariant_mut_input_data",
		// Aggregations: table agg nests struct members, struct aggs are
		// scalar+nested, struct sub-aggs drop the nested members.
		"_products_aggregation",
		"_products_aggregation_bucket",
		"_products_aggregation_sub_aggregation",
		"_ProductSpecs_aggregation",
		"_ProductSpecs_aggregation_sub_aggregation",
		"_BoxSize_aggregation",
		"_ProductVariant_aggregation",
		// The table itself: struct fields keep their types, markers attach.
		"products",
	})

	// Struct aggregations take no bucket and stop at sub-depth 1.
	for _, name := range []string{
		"_ProductSpecs_aggregation_bucket",
		"_ProductSpecs_aggregation_sub_aggregation_sub_aggregation",
	} {
		assert.Nil(t, ref.ForName(ctx, name), "reference must not generate %s", name)
		assert.Nil(t, store.ForName(ctx, name), "store must not serve %s", name)
	}
}

// assertGenParity: each covered name must be served by the store structurally
// identical to the fully-compiled reference (member order is not part of the
// contract — definitions are normalized before comparison).
func assertGenParity(t *testing.T, ctx context.Context, s *Store, ref *static.Provider, names []string) {
	t.Helper()
	for _, name := range names {
		want := ref.ForName(ctx, name)
		require.NotNil(t, want, "reference must contain %s", name)
		got := s.ForName(ctx, name)
		require.NotNil(t, got, "store must serve %s", name)
		assert.Equal(t, goldenSDL(want), goldenSDL(got), "definition parity for %s", name)
	}
}

// goldenSDL formats a normalized definition for comparison and readable diffs.
func goldenSDL(def *ast.Definition) string {
	var buf bytes.Buffer
	f := formatter.NewFormatter(&buf, formatter.WithIndent("  "))
	f.FormatSchemaDocument(&ast.SchemaDocument{Definitions: ast.DefinitionList{normalizeDef(def)}})
	return strings.TrimSpace(buf.String()) + "\n"
}

// normalizeDef deep-copies a definition with every order-insensitive member
// list (fields, args, directives, enum values, object-value children) sorted
// and positions cleared.
func normalizeDef(def *ast.Definition) *ast.Definition {
	d := *def
	d.Position = nil
	d.Directives = normalizeDirectives(def.Directives)
	d.Interfaces = slices.Sorted(slices.Values(def.Interfaces))
	d.Fields = make(ast.FieldList, len(def.Fields))
	for i, f := range def.Fields {
		d.Fields[i] = normalizeFieldDef(f)
	}
	slices.SortFunc(d.Fields, func(a, b *ast.FieldDefinition) int {
		return strings.Compare(a.Name, b.Name)
	})
	if len(def.EnumValues) > 0 {
		d.EnumValues = slices.Clone(def.EnumValues)
		for i, ev := range d.EnumValues {
			nev := *ev
			nev.Position = nil
			nev.Directives = normalizeDirectives(ev.Directives)
			d.EnumValues[i] = &nev
		}
		slices.SortFunc(d.EnumValues, func(a, b *ast.EnumValueDefinition) int {
			return strings.Compare(a.Name, b.Name)
		})
	}
	return &d
}

func normalizeFieldDef(f *ast.FieldDefinition) *ast.FieldDefinition {
	nf := *f
	nf.Position = nil
	nf.Directives = normalizeDirectives(f.Directives)
	nf.DefaultValue = normalizeValue(f.DefaultValue)
	nf.Arguments = normalizeArgDefs(f.Arguments)
	return &nf
}

func normalizeArgDefs(args ast.ArgumentDefinitionList) ast.ArgumentDefinitionList {
	if len(args) == 0 {
		return nil
	}
	out := make(ast.ArgumentDefinitionList, len(args))
	for i, a := range args {
		na := *a
		na.Position = nil
		na.Directives = normalizeDirectives(a.Directives)
		na.DefaultValue = normalizeValue(a.DefaultValue)
		out[i] = &na
	}
	slices.SortFunc(out, func(a, b *ast.ArgumentDefinition) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func normalizeDirectives(dl ast.DirectiveList) ast.DirectiveList {
	if len(dl) == 0 {
		return nil
	}
	out := make(ast.DirectiveList, len(dl))
	for i, dir := range dl {
		nd := *dir
		nd.Position = nil
		nd.Definition = nil
		nd.ParentDefinition = nil
		nd.Arguments = slices.Clone(dir.Arguments)
		for j, a := range nd.Arguments {
			na := *a
			na.Position = nil
			na.Value = normalizeValue(a.Value)
			nd.Arguments[j] = &na
		}
		slices.SortFunc(nd.Arguments, func(a, b *ast.Argument) int {
			return strings.Compare(a.Name, b.Name)
		})
		out[i] = &nd
	}
	// Repeatable directives (@references, @query, @unique …) share a name —
	// tie-break on the serialized arguments for a stable order.
	slices.SortStableFunc(out, func(a, b *ast.Directive) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(directiveKey(a), directiveKey(b))
	})
	return out
}

func directiveKey(d *ast.Directive) string {
	parts := make([]string, len(d.Arguments))
	for i, a := range d.Arguments {
		parts[i] = a.Name + ":" + a.Value.String()
	}
	return strings.Join(parts, ",")
}

func normalizeValue(v *ast.Value) *ast.Value {
	if v == nil {
		return nil
	}
	nv := *v
	nv.Position = nil
	nv.Definition = nil
	nv.ExpectedType = nil
	nv.VariableDefinition = nil
	if len(v.Children) > 0 {
		nv.Children = slices.Clone(v.Children)
		for i, c := range nv.Children {
			nc := *c
			nc.Position = nil
			nc.Value = normalizeValue(c.Value)
			nv.Children[i] = &nc
		}
		if v.Kind == ast.ObjectValue {
			slices.SortFunc(nv.Children, func(a, b *ast.ChildValue) int {
				return strings.Compare(a.Name, b.Name)
			})
		}
	}
	return &nv
}

// TestClassifyModuleRootName pins the syntactic inverse of sdl.ModuleTypeName,
// including the underscore-ambiguity cases the dispatcher must disambiguate
// against the modules table.
func TestClassifyModuleRootName(t *testing.T) {
	cases := []struct {
		name string
		want []moduleRootRef
	}{
		{"Query", []moduleRootRef{{Kind: base.ModuleQuery}}},
		{"Mutation", []moduleRootRef{{Kind: base.ModuleMutation}}},
		{"Function", []moduleRootRef{{Kind: base.ModuleFunction}}},
		{"MutationFunction", []moduleRootRef{{Kind: base.ModuleMutationFunction}}},
		{"Subscription", []moduleRootRef{{Kind: base.ModuleSubscription}}},
		{"_module_sales_query", []moduleRootRef{{Module: "sales", Kind: base.ModuleQuery}}},
		{"_module_sales_reports_mutation", []moduleRootRef{{Module: "sales_reports", Kind: base.ModuleMutation}}},
		{"_module_sales_subscription", []moduleRootRef{{Module: "sales", Kind: base.ModuleSubscription}}},
		// _mut_function wins specificity, plain _function stays a candidate.
		{"_module_x_mut_function", []moduleRootRef{
			{Module: "x", Kind: base.ModuleMutationFunction},
			{Module: "x_mut", Kind: base.ModuleFunction},
		}},
		{"_module_mut_function", []moduleRootRef{{Module: "mut", Kind: base.ModuleFunction}}},
		// Not module roots at all.
		{"orders", nil},
		{"_module_", nil},
		{"_join", nil},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, classifyModuleRootName(c.name), "name %s", c.name)
	}
}
