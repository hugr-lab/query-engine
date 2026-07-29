//go:build duckdb_arrow

package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/ingest"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

// The golden frame for the generation layer: a fixture goes through
// collect → store → ForName, and every name in genParityNames must come back
// identical to its frozen snapshot in testdata/golden.
//
// The snapshots were AUTHORED BY THE COMPILER, whose GENERATE / ASSEMBLE rules
// design-036 then deleted — this file is what is left of that oracle. It is
// exactly as strong as it was for existing behaviour (the text is unchanged and
// reviewed in the diff) and weaker for new behaviour, where a regenerated
// section is written by the same code it checks.

// genParityNames — generated names covered by the harness snapshot.
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

func TestGenGoldenHarness(t *testing.T) {
	store, ctx := writtenStore(t)

	assertGenParity(t, ctx, store, "harness", genParityNames)

	// Negative parity: names that must stay ABSENT — a view takes no mutation
	// inputs, and the fixture has no subscriptions and no reports-module
	// mutations. A snapshot cannot say "this does not exist", so the absences
	// are asserted by name.
	for _, name := range []string{"sales_by_country_mut_input_data", "sales_by_country_mut_data",
		"_module_sales_subscription", "_module_sales_reports_mutation"} {
		assert.Nil(t, store.ForName(ctx, name), "store must not serve %s", name)
	}

	assertInputTypesSound(t, ctx, store)
}

// assertInputTypesSound is the invariant ArgumentTypeValidator enforced over
// compiled output, restated where the schema is now produced: every field
// argument and every input-object field must name an INPUT type — a scalar, an
// enum or an input object. An object type leaking into either position is
// illegal GraphQL and surfaces far from its cause, as "Introspection must
// provide input type for arguments".
//
// The rule could only see one source's compilation; this walks the WHOLE served
// schema — the reachability closure from its roots — so it also covers what the
// store assembles across sources.
func assertInputTypesSound(t *testing.T, ctx context.Context, s *Store) {
	t.Helper()
	sound := func(name string) bool {
		if types.IsScalar(name) {
			return true
		}
		switch name {
		case "String", "Int", "Float", "Boolean", "ID":
			return true
		}
		def := s.ForName(ctx, name)
		if def == nil {
			// Unresolvable from here — absence is another assertion's subject.
			return true
		}
		switch def.Kind {
		case ast.Scalar, ast.Enum, ast.InputObject:
			return true
		}
		return false
	}
	for def := range s.Definitions(ctx) {
		for _, f := range def.Fields {
			for _, arg := range f.Arguments {
				assert.Truef(t, sound(arg.Type.Name()),
					"%s.%s argument %q: %s is not an input type", def.Name, f.Name, arg.Name, arg.Type.Name())
			}
			if def.Kind == ast.InputObject {
				assert.Truef(t, sound(f.Type.Name()),
					"input %s.%s: %s is not an input type", def.Name, f.Name, f.Type.Name())
			}
		}
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

	assertGenParity(t, ctx, store, "structs", []string{
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

	assertGenParity(t, ctx, store, "vector", []string{
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

	assertGenParity(t, ctx, store, "cube", []string{
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

	assertGenParity(t, ctx, store, "unique", []string{
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

	assertGenParity(t, ctx, store, "tfcj", []string{
		"airports",
		"airports_filter",
		"_airports_aggregation",
		"_airports_aggregation_sub_aggregation",
		"airports_mut_input_data",
		"_module_geo_query",
		"_module_geo_function",
	})
}

// genJoinViewSchema exercises a @join whose TARGET is a parameterized view,
// declared inside its own source (the internal-extend path, not the
// cross-source extension one). Everything that reaches the view has to carry
// its args — the join field itself and its aggregation twins alike — and a
// NonNull member of the args input makes them required all the way down.
const genJoinViewSchema = `
type regions @module(name: "geo") @table(name: "regions") {
  id: Int! @pk
  name: String!
  stats: [region_stats] @join(references_name: "region_stats", source_fields: ["id"], references_fields: ["region_id"])
}

type region_stats @module(name: "geo")
  @view(name: "region_stats", sql: "SELECT 1 AS region_id, 2 AS total")
  @args(name: "region_stats_args") {
  region_id: Int @pk
  total: Int
}

input region_stats_args {
  since: Timestamp!
}
`

// TestGenGoldenJoinView pins the args profile of a @join onto a parameterized
// view: the field takes the view's args (NonNull here — the input has a NonNull
// member) on top of the standard subquery arguments, and so do the
// {name}_aggregation / {name}_bucket_aggregation twins.
func TestGenGoldenJoinView(t *testing.T) {
	store, ctx := storeFor(t, genJoinViewSchema)

	assertGenParity(t, ctx, store, "joinview", []string{
		"regions",
		"region_stats",
		"region_stats_args",
		"regions_filter",
		"region_stats_filter",
		"_regions_aggregation",
		"_region_stats_aggregation",
		"_module_geo_query",
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

func fixtureOpts(fs fixtureSource, e engines.Engine) base.Options {
	return base.Options{
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

	for _, fs := range fixtures {
		e := fixtureEngine(fs.engineType)
		src, err := sources.NewStringSource(fs.name, e, fixtureOpts(fs, e), fs.schema)
		require.NoError(t, err)
		// The STORE is the compile target, as in production (compileAndWrite):
		// each later source resolves the earlier ones through the store's
		// on-demand reconstruction — including the module function roots, which
		// a static seed of raw definitions cannot produce.
		_, err = ingest.New(ingest.Default()...).Compile(ctx, store, src, src.CompileOptions())
		require.NoError(t, err)
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
  "The item price"
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

	assertGenParity(t, ctx, store, "multi", []string{
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
		assert.Nil(t, store.ForName(ctx, name), "store must not serve %s", name)
	}

	assertInputTypesSound(t, ctx, store)
}

// --- frozen snapshots --------------------------------------------------------
//
// The oracle for the generation layer used to be the compiler itself: the same
// fixture compiled with the FULL rule set, compared definition by definition.
// Design-036 retires that rule set, so the oracle is FROZEN into
// testdata/golden — text AUTHORED BY THE COMPILER, regenerated with
// UPDATE_GOLDEN=1. While the rule set is still here every assertion checks the
// snapshot against BOTH sides, so a drifted snapshot fails instead of being
// silently rewritten; once the rules go, the reference half drops out and the
// frozen text carries the contract alone.

const goldenDir = "testdata/golden"

// goldenMark opens a named section inside a fixture snapshot.
const goldenMark = "# === "

func goldenPath(fixture string) string {
	return filepath.Join(goldenDir, fixture+".graphql")
}

// readGoldenSections parses a fixture snapshot into name → normalized SDL.
func readGoldenSections(t *testing.T, fixture string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(goldenPath(fixture))
	if err != nil {
		t.Fatalf("read golden %q: %v (UPDATE_GOLDEN=1 to create)", fixture, err)
	}
	out := map[string]string{}
	name := ""
	var body strings.Builder
	flush := func() {
		if name != "" {
			out[name] = strings.TrimSpace(body.String()) + "\n"
		}
		body.Reset()
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if after, ok := strings.CutPrefix(line, goldenMark); ok {
			flush()
			name = strings.TrimSpace(after)
			continue
		}
		body.WriteString(line)
		body.WriteString("\n")
	}
	flush()
	return out
}

// writeGoldenSections rewrites a fixture snapshot, sections in the given order.
func writeGoldenSections(t *testing.T, fixture string, names []string, sdl map[string]string) {
	t.Helper()
	var buf strings.Builder
	buf.WriteString("# Golden snapshot of the generated schema — authored by the schema\n")
	buf.WriteString("# compiler, regenerate with UPDATE_GOLDEN=1. One section per definition\n")
	buf.WriteString("# the entity store must reproduce; see gen_golden_test.go.\n")
	for _, name := range names {
		s, ok := sdl[name]
		if !ok {
			continue
		}
		buf.WriteString("\n")
		buf.WriteString(goldenMark)
		buf.WriteString(name)
		buf.WriteString("\n")
		buf.WriteString(s)
	}
	require.NoError(t, os.MkdirAll(goldenDir, 0o755))
	require.NoError(t, os.WriteFile(goldenPath(fixture), []byte(buf.String()), 0o644))
}

// assertGenParity: each covered name must be served by the store structurally
// identical to the frozen snapshot (member order is not part of the contract —
// definitions are normalized before comparison). The comparison is against the
// store's PRE-FINALISE generation (forNameRaw): the store enriches descriptions
// on synthetic query members beyond what the compiler produced, and that
// store-only enrichment is verified separately (descriptions_test.go), not by
// the base-generation oracle.
//
// UPDATE_GOLDEN now rewrites the snapshot from the STORE, which is the whole
// difference the rules' deletion makes: the text is no longer authored by an
// independent implementation. For existing behaviour that costs nothing — the
// frozen text is the same contract it was. For NEW behaviour it means a wrong
// implementation writes a wrong golden, so a regenerated section has to be READ
// in the diff, not waved through.
func assertGenParity(t *testing.T, ctx context.Context, s *Store, fixture string, names []string) {
	t.Helper()

	golden := map[string]string{}
	if os.Getenv("UPDATE_GOLDEN") != "" {
		for _, name := range names {
			def := s.forNameRaw(ctx, name)
			require.NotNil(t, def, "store must serve %s", name)
			golden[name] = goldenSDL(def)
		}
		writeGoldenSections(t, fixture, names, golden)
	} else {
		golden = readGoldenSections(t, fixture)
	}

	for _, name := range names {
		// Per-name asserts (no require): one missing name must not hide the
		// diffs of the remaining ones.
		want, ok := golden[name]
		if !assert.True(t, ok, "golden %q has no section for %s (UPDATE_GOLDEN=1 to add)", fixture, name) {
			continue
		}
		got := s.forNameRaw(ctx, name)
		if !assert.NotNil(t, got, "store must serve %s", name) {
			continue
		}
		assert.Equal(t, want, goldenSDL(got), "definition parity for %s", name)
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
