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
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
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

// genVectorSchema exercises the vector-search axes: a plain Vector field
// (similarity) and @embeddings (semantic + _distance_to_query + mutation
// summary shape).
const genVectorSchema = `
type docs @module(name: "ai") @table(name: "docs") {
  id: Int! @pk
  title: String!
  vec: Vector @dim(len: 8)
}

type notes @module(name: "ai") @table(name: "notes") @embeddings(model: "openai", vector: "vec", distance: Cosine) {
  id: Int! @pk
  body: String
  vec: Vector @dim(len: 8)
}

extend type Subscription {
  note_stream(note_id: Int!): notes @module(name: "ai")
  heartbeat(interval_ms: Int): String
}
`

// TestGenGoldenVector pins similarity/semantic root arguments, the
// _distance_to_query members and the embeddings mutation shape (nullable
// data + summary).
func TestGenGoldenVector(t *testing.T) {
	store, ctx := storeFor(t, genVectorSchema)
	ref := goldenRef(t, "test", genVectorSchema)

	assertGenParity(t, ctx, store, ref, []string{
		"docs",
		"notes",
		"docs_filter",
		"notes_filter",
		"docs_mut_input_data",
		"notes_mut_input_data",
		"notes_mut_data",
		"_docs_aggregation",
		"_notes_aggregation",
		"_notes_aggregation_sub_aggregation",
		"_join",
		"_join_aggregation",
		"_module_ai_query",
		"_module_ai_mutation",
		// Subscriptions: the module root gets decorated members
		// (@catalog + @subscription), the top root keeps the raw
		// root-level field + the module gateway.
		"_module_ai_subscription",
		"Subscription",
		"Query",
	})
}

// genCubeSchema exercises the cube/hypertable argument decorators.
const genCubeSchema = `
type sales_cube @module(name: "bi") @cube @table(name: "sales_cube") {
  id: Int! @pk
  region: String!
  revenue: Float @measurement
  quantity: Int @measurement
}

type readings @module(name: "bi") @hypertable @table(name: "readings") {
  id: Int! @pk
  recorded_at: Timestamp @timescale_key
  temperature: Float
}
`

// TestGenGoldenCube pins measurement_func on @cube @measurement fields and
// gapfill on @hypertable @timescale_key fields — on the objects and their
// aggregation twins.
func TestGenGoldenCube(t *testing.T) {
	store, ctx := storeFor(t, genCubeSchema)
	ref := goldenRef(t, "test", genCubeSchema)

	assertGenParity(t, ctx, store, ref, []string{
		"sales_cube",
		"readings",
		"sales_cube_filter",
		"readings_filter",
		"_sales_cube_aggregation",
		"_sales_cube_aggregation_bucket",
		"_sales_cube_aggregation_sub_aggregation",
		"_readings_aggregation",
		"_readings_aggregation_sub_aggregation",
		"sales_cube_mut_input_data",
		"_module_bi_query",
	})
}

// genUniqueSchema exercises @unique SELECT_ONE variants.
const genUniqueSchema = `
type users @module(name: "crm") @table(name: "users")
  @unique(fields: ["email"], query_suffix: "by_email")
  @unique(fields: ["first_name", "last_name"], query_suffix: "by_full_name") {
  id: Int! @pk
  email: String!
  first_name: String!
  last_name: String!
}
`

// TestGenGoldenUnique pins the @unique SELECT_ONE root fields and their
// def-level markers.
func TestGenGoldenUnique(t *testing.T) {
	store, ctx := storeFor(t, genUniqueSchema)
	ref := goldenRef(t, "test", genUniqueSchema)

	assertGenParity(t, ctx, store, ref, []string{
		"users",
		"users_filter",
		"_module_crm_query",
	})
}

// genTFCJSchema exercises @table_function_call_join: a declared call
// parameter (radius) plus a mapped one (iata), twins on the object and its
// aggregation reuse the declared arguments.
const genTFCJSchema = `
type airports @module(name: "geo") @table(name: "airports") {
  iata: String! @pk
  name: String!
}

extend type Function {
  find_nearby(iata: String!, radius: Float): [airports]
    @module(name: "geo") @function(name: "find_nearby")
}

extend type airports {
  nearby(radius: Float): [airports]
    @table_function_call_join(references_name: "find_nearby", module: "geo", args: {iata: "iata"})
}
`

// TestGenGoldenTFCJ pins the table_function_call_join surface: declared field
// arguments round-trip (f6 fields.args), the aggregation twin pairs on the
// object and the aggregation type carry the declared arguments.
func TestGenGoldenTFCJ(t *testing.T) {
	store, ctx := storeFor(t, genTFCJSchema)
	ref := goldenRef(t, "test", genTFCJSchema)

	assertGenParity(t, ctx, store, ref, []string{
		"airports",
		"airports_filter",
		"_airports_aggregation",
		"_airports_aggregation_sub_aggregation",
		"airports_mut_input_data",
		"_module_geo_query",
		"_module_geo_function",
	})
}

// fixtureSource describes one source of a multi-source golden fixture.
type fixtureSource struct {
	name        string
	schema      string
	engineType  string // "" = duckdb
	prefix      string
	asModule    bool
	readOnly    bool
	isExtension bool
}

// fixtureEngine maps an engine type string to its engine (harness-side
// factory; the live corpus spans several engines).
func fixtureEngine(engineType string) engines.Engine {
	switch engineType {
	case "postgres":
		return engines.NewPostgres()
	case "mssql":
		return engines.NewMssql()
	case "mysql":
		return engines.NewMySql()
	case "http":
		return engines.NewHttp()
	default:
		return engines.NewDuckDB()
	}
}

func fixtureOpts(fs fixtureSource, e engines.Engine) compiler.Options {
	return compiler.Options{
		Name:         fs.name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
		Prefix:       fs.prefix,
		AsModule:     fs.asModule,
		ReadOnly:     fs.readOnly,
		IsExtension:  fs.isExtension,
	}
}

// storeForSources writes the fixture sources in ORDER through the partial
// pipeline — each later source compiles against the earlier ones (cross-source
// extends resolve), states carry prefix/as_module/read_only.
func storeForSources(t *testing.T, fixtures []fixtureSource) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)

	target, err := static.New()
	require.NoError(t, err)
	for _, fs := range fixtures {
		e := fixtureEngine(fs.engineType)
		src, err := sources.NewStringSource(fs.name, e, fixtureOpts(fs, e), fs.schema)
		require.NoError(t, err)
		_, err = compiler.New(partialRules()...).Compile(ctx, target, src, src.CompileOptions())
		require.NoError(t, err)
		// Seed ONLY the definitions: later sources need the prior types for
		// their extends to validate; leftover extensions (e.g. unmerged
		// same-source virtual-field extends) are collect's job, not the seed's.
		require.NoError(t, target.Update(ctx, definitionsOnly{src}))
		d := collect(ctx, src, fs.name)
		_, err = store.writeSource(ctx, d, SourceState{
			Name: fs.name, Version: "v1", Engine: string(e.Type()),
			Prefix: fs.prefix, AsModule: fs.asModule, ReadOnly: fs.readOnly,
			IsExtension: fs.isExtension, Loaded: true,
		})
		require.NoError(t, err)
	}
	return store, ctx
}

// definitionsOnly hides a source's Extensions from the seed provider.
type definitionsOnly struct{ base.DefinitionsSource }

// goldenRefSources compiles the same fixtures with the FULL rule set,
// sequentially into one static provider (multi-catalog reference).
func goldenRefSources(t *testing.T, fixtures []fixtureSource) *static.Provider {
	t.Helper()
	ctx := context.Background()
	target, err := static.New()
	require.NoError(t, err)
	for i, fs := range fixtures {
		e := fixtureEngine(fs.engineType)
		src, err := sources.NewStringSource(fs.name, e, fixtureOpts(fs, e), fs.schema)
		require.NoError(t, err)
		var p base.Provider
		if i > 0 {
			p = target
		}
		compiled, err := compiler.New(rules.RegisterAll()...).Compile(ctx, p, src, src.CompileOptions())
		require.NoError(t, err)
		require.NoError(t, target.Update(ctx, compiled))
	}
	return target
}

// genMultiFixtures covers the deferred source-option axes: a prefixed
// AsModule source, a read-only source, plain sources and a cross-source
// extension. The CONTRACT pinned here: a regular source describes ONLY its
// own data — every cross-source schema artifact (@join / @function_call /
// @table_function_call_join wiring, cross-source views) lives in the
// EXTENSION source.
var genMultiFixtures = []fixtureSource{
	{
		name: "shop", prefix: "shop", asModule: true,
		// categories declared BEFORE items: the compiler's insert-relation
		// subqueries are created lazily in declaration order (the target's
		// mutation input must already exist).
		schema: `
type categories @table(name: "categories") {
  id: Int! @pk
  title: String!
}

type items @table(name: "items") {
  id: Int! @pk
  category_id: Int @field_references(references_name: "categories", field: "id", query: "category", references_query: "items")
  name: String!
  price: Float
}

extend type Function {
  item_label(id: Int!): String @function(name: "item_label")
}
`,
	},
	{
		name: "ro", readOnly: true,
		schema: `
type logs @module(name: "ro_mod") @table(name: "logs") {
  id: Int! @pk
  message: String
}
`,
	},
	{
		name: "audit",
		schema: `
type audit_events @module(name: "audit") @table(name: "audit_events") {
  id: Int! @pk
  log_id: Int
}
`,
	},
	{
		// A function-only source; similar_items RETURNS another source's
		// objects — the TFCJ wiring onto shop_items happens in the extension.
		name: "func",
		schema: `
extend type Function {
  similar_items(item_id: Int!, limit: Int = 5): [shop_items] @function(name: "similar_items")
  slugify(s: String!): String @module(name: "tools") @function(name: "slugify")
}
`,
	},
	{
		// The extension owns EVERY cross-source artifact: the @function_call
		// wiring to shop's function, the @table_function_call_join wiring to
		// the func source's function, the cross-source @join from audit's
		// object to ro's logs, and a view with an explicit @dependency.
		name: "ext", isExtension: true,
		schema: `
extend type shop_items {
  note: String
  label: String @function_call(references_name: "item_label", module: "shop", args: {id: "id"})
  similar: [shop_items] @table_function_call_join(references_name: "similar_items", args: {item_id: "id"})
}

extend type audit_events {
  logs: [logs] @join(references_name: "logs", source_fields: ["log_id"], references_fields: ["id"])
}

type shop_overview
  @view(name: "shop_overview", sql: "SELECT category_id, count(*) AS items_count FROM shop.main.items GROUP BY category_id")
  @dependency(name: "shop") {
  category_id: Int @pk
  items_count: BigInt
}
`,
	},
}

// TestGenGoldenMultiSource pins the prefix + AsModule + read-only + extension
// axes: prefixed compiled names with original-name markers and roots,
// mutation suppression for the read-only source, extension field attribution.
func TestGenGoldenMultiSource(t *testing.T) {
	store, ctx := storeForSources(t, genMultiFixtures)
	ref := goldenRefSources(t, genMultiFixtures)

	assertGenParity(t, ctx, store, ref, []string{
		// Prefixed objects: @original_name, markers with ORIGINAL names,
		// prefixed nav/derived names, the extension field on shop_items.
		"shop_items",
		"shop_categories",
		"shop_items_filter",
		"shop_categories_filter",
		"shop_items_list_filter",
		"shop_items_mut_input_data",
		"shop_items_mut_data",
		"_shop_items_aggregation",
		"_shop_items_aggregation_sub_aggregation",
		"_shop_categories_aggregation",
		// Read-only source: no mutation surface at all.
		"logs",
		"logs_filter",
		"_logs_aggregation",
		// Cross-source @join: the declared field's @catalog is PROPAGATED from
		// the referenced object ("ro"), computed via references_name on read.
		"audit_events",
		"_audit_events_aggregation",
		// Extension view with an explicit @dependency (round-trips through the
		// properties bag) and its derived types.
		"shop_overview",
		"shop_overview_filter",
		"_shop_overview_aggregation",
		// Roots: AsModule module named after the source, original-name
		// members; ro_mod contributes no mutation root. Functions route into
		// the source module (inline @module nests: shop.tools).
		"Query",
		"Mutation",
		"Function",
		"_module_shop_query",
		"_module_shop_mutation",
		"_module_shop_function",
		"_module_tools_function",
		"_module_ro_mod_query",
		"_module_audit_query",
		// Shared types span all three sources.
		"_join",
		"_join_aggregation",
	})

	for _, name := range []string{
		"logs_mut_input_data",
		"logs_mut_data",
		"_module_ro_mod_mutation",
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
		// Per-name asserts (no require): one missing name must not hide the
		// diffs of the remaining ones.
		want := ref.ForName(ctx, name)
		if !assert.NotNil(t, want, "reference must contain %s", name) {
			continue
		}
		got := s.ForName(ctx, name)
		if !assert.NotNil(t, got, "store must serve %s", name) {
			continue
		}
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
	// tie-break on the serialized arguments for a stable order. IDENTICAL
	// duplicates collapse (the old compiled path double-tags @catalog on some
	// residual types).
	slices.SortStableFunc(out, func(a, b *ast.Directive) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(directiveKey(a), directiveKey(b))
	})
	out = slices.CompactFunc(out, func(a, b *ast.Directive) bool {
		return a.Name == b.Name && directiveKey(a) == directiveKey(b)
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
