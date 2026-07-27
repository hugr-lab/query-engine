//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// partialSource runs the partial compiler over a schema and returns the source
// itself: VALIDATE+PREPARE mutate its definitions in place (prefix, catalog and
// module tags) and leave the physical directives intact (no GENERATE). Data
// objects / source types live in Definitions(); function roots and cross-source
// `extend type` blocks (promoted) live in Extensions(). seeds are prior sources'
// definitions merged into the compile target so cross-source extends resolve.
func partialSource(t *testing.T, name, schema string, seeds ...base.DefinitionsSource) base.ExtensionsSource {
	t.Helper()
	ctx := context.Background()
	target, err := static.New()
	require.NoError(t, err)
	for _, seed := range seeds {
		require.NoError(t, target.Update(ctx, seed))
	}
	e := &engines.DuckDB{}
	src, err := sources.NewStringSource(name, e, compiler.Options{
		Name:         name,
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, schema)
	require.NoError(t, err)
	_, err = compiler.New(partialRules()...).Compile(ctx, target, src, src.CompileOptions())
	require.NoError(t, err)
	return src
}

const collectTestSchema = `
type customers @module(name: "sales") @table(name: "customers") {
  id: Int! @pk
  name: String! @cache(ttl: "10m", tags: ["crm"])
  customer_orders: [orders] @join(references_name: "orders", source_fields: ["id"], references_fields: ["customer_id"])
}

type orders @module(name: "sales")
  @table(name: "orders", soft_delete: true,
    soft_delete_cond: "deleted_at IS NULL", soft_delete_set: "deleted_at = now()")
  @references(name: "order_customer", references_name: "customers",
    source_fields: ["customer_id"], references_fields: ["id"],
    query: "customer", references_query: "orders") {
  id: Int! @pk
  customer_id: Int!
  amount: Float @deprecated(reason: "use total_with_tax")
  total_with_tax: Float @sql(exp: "amount * 1.1")
  created_at: Timestamp @default(insert_exp: "now()")
  deleted_at: Timestamp
  area: Geometry @geometry_info(type: POLYGON, srid: 4326) @dim(len: 3) @unique_rule(rule: INTERSECTS)
  status_label: String @function_call(references_name: "order_status", module: "sales", args: {id: "id"})
}

type tags @module(name: "sales") @table(name: "tags") {
  name: String! @pk
}

type order_tags @module(name: "sales") @table(name: "order_tags", is_m2m: true) {
  order_id: Int! @pk @field_references(references_name: "orders", field: "id", query: "order", references_query: "tags")
  tag_name: String! @pk @field_references(references_name: "tags", field: "name", query: "tag", references_query: "orders")
}

type sales_by_country @module(name: "sales.reports")
  @view(name: "sales_by_country", sql: "SELECT 1 AS country, 2 AS total")
  @args(name: "sales_by_country_args") {
  country: String @pk
  total: Float
}

input sales_by_country_args {
  country: String!
}

extend type Function {
  order_status(id: Int!, mode: String @deprecated(reason: "unused")): String
    @module(name: "sales") @function(name: "order_status") @deprecated
}

extend type MutationFunction {
  reprice_orders(factor: Float!): customers @module(name: "sales") @function(name: "reprice_orders")
}
`

func TestCollect(t *testing.T) {
	ctx := context.Background()
	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")

	// --- modules + parent ---
	require.Contains(t, d.modules, "sales")
	require.Contains(t, d.modules, "sales.reports")
	assert.Equal(t, "", d.modules["sales"].Parent)
	assert.Equal(t, "sales", d.modules["sales.reports"].Parent, "parent derived from dotted name")

	// --- module→source closure: the child's source rolls up to the parent and
	// the root ''; kind flags recorded per row (hierarchy flattened at save) ---
	assert.Contains(t, d.moduleSources, pkKey("sales", "test"))
	assert.Contains(t, d.moduleSources, pkKey("sales.reports", "test"),
		"submodule backed by its view source")
	require.Contains(t, d.moduleSources, pkKey("", "test"), "root row covers the whole tree")
	sales := d.moduleSources[pkKey("sales", "test")]
	assert.True(t, sales.HasDataObjects)
	assert.True(t, sales.HasTables)
	assert.True(t, sales.HasFunctions)
	assert.True(t, sales.HasMutFunctions)
	assert.False(t, sales.HasSubscriptions)
	reports := d.moduleSources[pkKey("sales.reports", "test")]
	assert.True(t, reports.HasDataObjects)
	assert.False(t, reports.HasTables, "a view is not mutation-capable")
	root := d.moduleSources[pkKey("", "test")]
	assert.True(t, root.HasTables && root.HasFunctions && root.HasMutFunctions)

	// --- data objects ---
	require.Contains(t, d.dataObjects, "orders")
	assert.Equal(t, "table", d.dataObjects["orders"].Kind)
	assert.Equal(t, "view", d.dataObjects["sales_by_country"].Kind)
	assert.Equal(t, "sales", d.dataObjects["orders"].Module)
	assert.NotEmpty(t, d.dataObjects["orders"].DataSource)

	// --- fields (declared columns; incl. junction fields, excl. generated) ---
	assert.Contains(t, d.fields, pkKey("orders", "customer_id"))
	assert.Contains(t, d.fields, pkKey("order_tags", "order_id"))
	assert.True(t, d.fields[pkKey("customers", "id")].IsPK)

	// --- relations: PHYSICAL refs, no m2m generation ---
	// fk from orders → customers.
	fk := d.relations[pkKey("orders", "order_customer")]
	require.NotNil(t, fk)
	assert.Equal(t, "fk", fk.Kind)
	assert.Equal(t, "customers", fk.Destination)
	assert.Equal(t, []string{"customer_id"}, fk.SourceKeys)
	assert.Equal(t, []string{"id"}, fk.DestinationKeys)

	// m2m junction: two ordinary fk legs whose SOURCE is the junction table.
	leg1 := d.relations[pkKey("order_tags", "orders_order_id")]
	require.NotNil(t, leg1, "junction leg → orders")
	assert.Equal(t, "order_tags", leg1.Source)
	assert.Equal(t, "orders", leg1.Destination)
	assert.Equal(t, []string{"order_id"}, leg1.SourceKeys)
	assert.Equal(t, []string{"id"}, leg1.DestinationKeys)
	leg2 := d.relations[pkKey("order_tags", "tags_tag_name")]
	require.NotNil(t, leg2, "junction leg → tags")
	assert.Equal(t, "tags", leg2.Destination)
	// No synthesized m2m edge stored — derived on read.
	assert.NotContains(t, d.relations, pkKey("orders", "tags"))

	// --- functions (module roots not stored; members extracted) ---
	fn := d.functions[pkKey("sales", "order_status", "function")]
	require.NotNil(t, fn)
	assert.Equal(t, "function", fn.Kind)
	assert.False(t, fn.IsTable, "scalar-returning function")
	mut := d.functions[pkKey("sales", "reprice_orders", "mutation")]
	require.NotNil(t, mut)
	assert.Equal(t, "mutation", mut.Kind)
	assert.True(t, mut.IsTable, "returns a data object")

	// --- residual source type ---
	require.Contains(t, d.types, "sales_by_country_args")
	assert.Equal(t, "input", d.types["sales_by_country_args"].Kind)

	// --- property bags (M2.3 mappers) ---
	orders := d.dataObjects["orders"].Properties
	require.NotNil(t, orders)
	assert.Equal(t, "orders", orders.Name, "physical @table name")

	view := d.dataObjects["sales_by_country"].Properties
	require.NotNil(t, view)
	assert.Contains(t, view.SQL, "SELECT", "@view(sql:) body stored")
	assert.Equal(t, "sales_by_country_args", view.ArgsTypeName)

	assert.True(t, d.dataObjects["order_tags"].Properties.IsM2M, "junction marked is_m2m")

	// computed field: @sql(exp:) → typed member.
	comp := d.fields[pkKey("orders", "total_with_tax")].Properties
	require.NotNil(t, comp)
	assert.True(t, comp.Computed)
	assert.Equal(t, "amount * 1.1", comp.SQL)

	// soft-delete raw expressions stored on the object bag.
	assert.True(t, orders.SoftDelete)
	assert.Equal(t, "deleted_at IS NULL", orders.SoftDeleteCond)
	assert.Equal(t, "deleted_at = now()", orders.SoftDeleteSet)

	// geometry field: @geometry_info + @dim + @unique_rule.
	area := d.fields[pkKey("orders", "area")].Properties
	require.NotNil(t, area)
	require.NotNil(t, area.Geometry)
	assert.Equal(t, "POLYGON", area.Geometry.Type)
	assert.EqualValues(t, 3, area.Dim)
	assert.Equal(t, "INTERSECTS", area.UniqueRule)

	// @deprecated → column value; bare directive stores the spec default.
	assert.Equal(t, "use total_with_tax", d.fields[pkKey("orders", "amount")].DeprecationReason)
	assert.Empty(t, d.fields[pkKey("orders", "id")].DeprecationReason)
	assert.Equal(t, "No longer supported", fn.DeprecationReason)

	// declared-field bags: @join, @function_call, @default, @cache.
	join := d.fields[pkKey("customers", "customer_orders")].Properties.Join
	require.NotNil(t, join)
	assert.Equal(t, "orders", join.ReferencesName)
	assert.Equal(t, []string{"id"}, join.SourceFields)
	fc := d.fields[pkKey("orders", "status_label")].Properties.FunctionCall
	require.NotNil(t, fc)
	assert.Equal(t, "order_status", fc.Function.Name)
	assert.Equal(t, "sales", fc.Function.Module)
	require.NotNil(t, d.fields[pkKey("orders", "created_at")].Properties.Default)
	assert.Equal(t, "now()", d.fields[pkKey("orders", "created_at")].Properties.Default.InsertExp)
	cache := d.fields[pkKey("customers", "name")].Properties.Cache
	require.NotNil(t, cache)
	assert.Equal(t, "10m", cache.TTL)
	assert.Equal(t, []string{"crm"}, cache.Tags)

	// function config + ordered args.
	fp := d.functions[pkKey("sales", "order_status", "function")].Properties
	require.NotNil(t, fp)
	require.NotNil(t, fp.Function)
	assert.Equal(t, "order_status", fp.Function.Name)
	fargs := d.functions[pkKey("sales", "order_status", "function")].Args
	require.Len(t, fargs, 2)
	assert.Equal(t, "id", fargs[0].Name)
	assert.Equal(t, "Int!", fargs[0].Type)
	assert.Empty(t, fargs[0].DeprecationReason)
	assert.Equal(t, "unused", fargs[1].DeprecationReason)
}

// TestCollectPrefixedOriginalName verifies a prefixed source keeps its
// unprefixed name in OriginalName (@original_name added by the prefixer).
func TestCollectPrefixedOriginalName(t *testing.T) {
	ctx := context.Background()
	target, err := static.New()
	require.NoError(t, err)
	e := &engines.DuckDB{}
	src, err := sources.NewStringSource("shop", e, compiler.Options{
		Name: "shop", Prefix: "shop", EngineType: string(e.Type()), Capabilities: e.Capabilities(),
	}, `type products @module(name: "shop") @table(name: "products") { id: Int! @pk }`)
	require.NoError(t, err)
	_, err = compiler.New(partialRules()...).Compile(ctx, target, src, src.CompileOptions())
	require.NoError(t, err)

	d := collect(ctx, src, "shop")
	require.Len(t, d.dataObjects, 1)
	for _, obj := range d.dataObjects {
		assert.Equal(t, "products", obj.OriginalName, "unprefixed name preserved")
		assert.NotEqual(t, "products", obj.Name, "stored name is prefixed")
	}
}

// TestCollectExtensionFields covers a cross-source extension: source A owns the
// object, source B adds fields via `extend type`. B contributes ONLY the field
// rows (attributed to B), no object row, no module backing.
func TestCollectExtensionFields(t *testing.T) {
	ctx := context.Background()
	const baseSchema = `type customers @module(name: "sales") @table(name: "customers") {
  id: Int! @pk
  name: String!
}`
	const extSchema = `extend type customers {
  note: String
}`
	srcA := partialSource(t, "A", baseSchema)
	srcB := partialSource(t, "B", extSchema, srcA) // B compiled with A's customers seeded

	dA := collect(ctx, srcA, "A")
	require.Contains(t, dA.dataObjects, "customers")
	assert.Equal(t, "A", dA.dataObjects["customers"].DataSource)
	assert.Contains(t, dA.fields, pkKey("customers", "id"))

	dB := collect(ctx, srcB, "B")
	// B adds only the extension field — no object row, attributed to B.
	assert.NotContains(t, dB.dataObjects, "customers", "extension source does not own the object")
	require.Contains(t, dB.fields, pkKey("customers", "note"))
	note := dB.fields[pkKey("customers", "note")]
	assert.Equal(t, "customers", note.TypeName)
	assert.Equal(t, "B", note.DataSource)
	// B does not back the module — customers' module is owned by A.
	assert.NotContains(t, dB.moduleSources, pkKey("sales", "B"))
	assert.Empty(t, dB.dataObjects)
}

// TestCollectFieldReferencesCompositePK mirrors the compiler's PK-default rule
// (gen_references.go): an argless @field_references(field:) defaults to the
// target PK only when the key is a SINGLE column — a composite-PK target
// yields no relation instead of a fabricated single-leg join.
func TestCollectFieldReferencesCompositePK(t *testing.T) {
	ctx := context.Background()
	const schema = `type parts @table(name: "parts") {
  vendor_id: Int! @pk
  part_no: Int! @pk
  name: String
}

type shipments @table(name: "shipments") {
  id: Int! @pk
  part_no: Int @field_references(references_name: "parts")
  vendor_ref: Int @field_references(references_name: "parts", field: "vendor_id")
}`
	d := collect(ctx, partialSource(t, "test", schema), "test")
	assert.NotContains(t, d.relations, pkKey("shipments", "parts_part_no"),
		"composite PK target: no default field, relation dropped")
	require.Contains(t, d.relations, pkKey("shipments", "parts_vendor_ref"),
		"explicit field: unaffected by the composite PK")
	assert.Equal(t, []string{"vendor_id"},
		d.relations[pkKey("shipments", "parts_vendor_ref")].DestinationKeys)
}
