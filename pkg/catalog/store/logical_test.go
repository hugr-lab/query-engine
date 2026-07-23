//go:build duckdb_arrow

package store

import (
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLogicalModules covers the module tree: existence, description, direct
// children and the direct-member data sources.
func TestLogicalModules(t *testing.T) {
	store, ctx := writtenStore(t)

	sales := store.Module(ctx, "sales")
	require.NotNil(t, sales)
	assert.Equal(t, []string{"test"}, sales.DataSources, "direct members' sources")
	// RootTypes carries NAMES (kind presence from one member-kinds query):
	// tables → query+mutation, function + mutation function members present.
	assert.Equal(t, map[sdl.ModuleObjectType]string{
		sdl.ModuleQuery:            "_module_sales_query",
		sdl.ModuleMutation:         "_module_sales_mutation",
		sdl.ModuleFunction:         "_module_sales_function",
		sdl.ModuleMutationFunction: "_module_sales_mut_function",
	}, sales.RootTypes)

	reports := store.Module(ctx, "sales.reports")
	require.NotNil(t, reports)
	assert.Equal(t, map[sdl.ModuleObjectType]string{
		sdl.ModuleQuery: "_module_sales_reports_query",
	}, reports.RootTypes, "a view is read-only — query kind only")

	assert.Nil(t, store.Module(ctx, "nope"))
	root := store.Module(ctx, "")
	require.NotNil(t, root, "root module always resolves")
	assert.Equal(t, "Query", root.RootTypes[sdl.ModuleQuery])
	assert.Equal(t, "Mutation", root.RootTypes[sdl.ModuleMutation], "subtree roll-up reaches the root")

	var top []string
	for m := range store.Modules(ctx, "") {
		top = append(top, m.Name)
	}
	assert.Equal(t, []string{"sales"}, top)

	var children []string
	for m := range store.Modules(ctx, "sales") {
		children = append(children, m.Name)
	}
	assert.Equal(t, []string{"sales.reports"}, children)
}

// TestLogicalDataObjects covers object resolution and per-module listing.
func TestLogicalDataObjects(t *testing.T) {
	store, ctx := writtenStore(t)

	orders := store.DataObject(ctx, "orders")
	require.NotNil(t, orders)
	assert.Equal(t, "test", orders.Catalog, "@catalog name = data source")
	assert.Nil(t, store.DataObject(ctx, "sales_by_country_args"), "input type is not a data object")

	var names []string
	for o := range store.DataObjects(ctx, "sales") {
		names = append(names, o.Name)
	}
	assert.Equal(t, []string{"customers", "order_tags", "orders", "tags"}, names)

	names = names[:0]
	for o := range store.DataObjects(ctx, "sales.reports") {
		names = append(names, o.Name)
	}
	assert.Equal(t, []string{"sales_by_country"}, names)
}

// TestLogicalFunctions covers callable-member resolution and kind ordering.
func TestLogicalFunctions(t *testing.T) {
	store, ctx := writtenStore(t)

	fn := store.Function(ctx, "sales", "order_status")
	require.NotNil(t, fn)
	assert.Equal(t, sdl.ModuleFunction, fn.Kind)
	assert.False(t, fn.IsTable)
	assert.Equal(t, "test", fn.DataSource)
	require.NotNil(t, fn.Field)
	assert.Len(t, fn.Field.Arguments, 2)

	var listedNames []string
	var listedKinds []sdl.ModuleObjectType
	for entry := range store.Functions(ctx, "sales") {
		listedNames = append(listedNames, entry.Field.Name)
		listedKinds = append(listedKinds, entry.Kind)
	}
	assert.Equal(t, []string{"order_status", "reprice_orders"}, listedNames)
	assert.Equal(t, []sdl.ModuleObjectType{sdl.ModuleFunction, sdl.ModuleMutationFunction}, listedKinds)

	assert.Nil(t, store.Function(ctx, "sales", "nope"))
}

// TestLogicalRelations covers the FORWARD / BACK / M2M / JOIN synthesis from
// the physical legs (semantics verified against the live etalon).
func TestLogicalRelations(t *testing.T) {
	store, ctx := writtenStore(t)

	rels := func(object string) map[string]*catalog.RelationInfo {
		out := map[string]*catalog.RelationInfo{}
		for r := range store.Relations(ctx, object) {
			out[r.Name+"|"+string(r.Kind)+"|"+string(r.Direction)] = r
		}
		return out
	}

	// orders: own FK FORWARD + M2M FORWARD via the junction; NO FK BACK for the
	// junction leg.
	orders := rels("orders")
	fk := orders["order_customer|FK|FORWARD"]
	require.NotNil(t, fk)
	assert.Equal(t, "customer", fk.FieldName)
	assert.Equal(t, "customers", fk.DataObject)
	assert.Equal(t, []string{"customer_id"}, fk.SourceKeys)
	assert.Equal(t, []string{"id"}, fk.DestinationKeys)

	m2m := orders["orders_order_id|M2M|FORWARD"]
	require.NotNil(t, m2m, "junction leg becomes M2M FORWARD on the endpoint")
	assert.Equal(t, "tags", m2m.FieldName, "leg's references_query names the m2m nav")
	assert.Equal(t, "tags", m2m.DataObject, "co-endpoint, not the junction")
	assert.Equal(t, "order_tags", m2m.Through)
	assert.Equal(t, []string{"id"}, m2m.SourceKeys, "endpoint→junction orientation")
	assert.Equal(t, []string{"order_id"}, m2m.DestinationKeys)
	require.NotContains(t, orders, "orders_order_id|FK|BACK", "no FK BACK for a junction leg")

	// the junction itself: two plain FK FORWARD legs.
	junction := rels("order_tags")
	require.Len(t, junction, 2)
	require.NotNil(t, junction["orders_order_id|FK|FORWARD"])
	assert.Equal(t, "order", junction["orders_order_id|FK|FORWARD"].FieldName)
	require.NotNil(t, junction["tags_tag_name|FK|FORWARD"])

	// customers: FK BACK from orders (canonical keys) + own JOIN edge.
	customers := rels("customers")
	back := customers["order_customer|FK|BACK"]
	require.NotNil(t, back)
	assert.Equal(t, "orders", back.FieldName, "references_query names the back nav")
	assert.Equal(t, "orders", back.DataObject)
	assert.Equal(t, []string{"customer_id"}, back.SourceKeys, "canonical source→destination keys")
	assert.Equal(t, []string{"id"}, back.DestinationKeys)

	join := customers["customer_orders|JOIN|FORWARD"]
	require.NotNil(t, join)
	assert.Equal(t, "orders", join.DataObject)
	assert.Equal(t, []string{"id"}, join.SourceKeys)
	assert.Equal(t, []string{"customer_id"}, join.DestinationKeys)
}

// TestLogicalTypes covers the type surfaces: ForName chain, source types and
// the static system layer.
func TestLogicalTypes(t *testing.T) {
	store, ctx := writtenStore(t)

	require.NotNil(t, store.Type(ctx, "orders"))

	var sourceTypes []string
	for name := range store.SourceTypes(ctx) {
		sourceTypes = append(sourceTypes, name)
	}
	assert.Contains(t, sourceTypes, "sales_by_country_args")

	found := false
	for name := range store.SystemTypes(ctx) {
		if name == "String" {
			found = true
			break
		}
	}
	assert.True(t, found, "static prelude serves the system types")
}

// TestLogicalActivityGate proves the whole logical surface disappears when the
// source is suspended and returns when re-enabled — rows never move.
func TestLogicalActivityGate(t *testing.T) {
	store, ctx := writtenStore(t)

	require.NoError(t, store.setFlags(ctx, "test", false, false, true))
	assert.Nil(t, store.Module(ctx, "sales"))
	assert.Nil(t, store.DataObject(ctx, "orders"))
	assert.Nil(t, store.ForName(ctx, "orders"))
	assert.Empty(t, slices.Collect(store.Relations(ctx, "orders")))
	assert.Empty(t, slices.Collect(store.Functions(ctx, "sales")))
	var sourceTypes []string
	for name := range store.SourceTypes(ctx) {
		sourceTypes = append(sourceTypes, name)
	}
	assert.Empty(t, sourceTypes)

	require.NoError(t, store.setFlags(ctx, "test", true, false, false))
	require.NotNil(t, store.Module(ctx, "sales"))
	require.NotNil(t, store.DataObject(ctx, "orders"))
}
