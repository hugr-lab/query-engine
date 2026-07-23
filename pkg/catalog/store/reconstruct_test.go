//go:build duckdb_arrow

package store

import (
	"context"
	"testing"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// writtenStore boots an in-memory CoreDB, writes the collect fixture and returns
// the store — the read-side test base.
func writtenStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)
	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")
	_, err = store.writeSource(ctx, d, SourceState{Name: "test", Version: "v1", Engine: "duckdb", Loaded: true})
	require.NoError(t, err)
	return store, ctx
}

// TestForNameM31 covers M3.1: ForName serves system types from the static layer
// and reconstructs residual source types from their stored SDL.
func TestForNameM31(t *testing.T) {
	store, ctx := writtenStore(t)

	// System layer — scalars come from the static prelude, not the DB.
	assert.NotNil(t, store.ForName(ctx, "String"))
	assert.NotNil(t, store.ForName(ctx, "Int"))

	// Residual source type reconstructed from stored SDL.
	def := store.ForName(ctx, "sales_by_country_args")
	require.NotNil(t, def, "input type reconstructed from catalog.types")
	assert.Equal(t, ast.InputObject, def.Kind)
	require.NotNil(t, def.Fields.ForName("country"))

	// Absent name → nil.
	assert.Nil(t, store.ForName(ctx, "definitely_not_a_type"))
}

// TestReconstructDataObjectBase covers the base data-object reconstruction: the
// object shell, core directives (@original_name/@module/@table/@view) and base
// fields with @pk / @field_source.
func TestReconstructDataObjectBase(t *testing.T) {
	store, ctx := writtenStore(t)

	orders := store.ForName(ctx, "orders")
	require.NotNil(t, orders)
	assert.Equal(t, ast.Object, orders.Kind)
	tblDir := orders.Directives.ForName("table")
	require.NotNil(t, tblDir, "@table reattached")
	assert.Equal(t, "orders", dirArg(tblDir, "name"), "physical name")
	assert.Equal(t, "true", dirArg(tblDir, "soft_delete"))
	assert.Equal(t, "deleted_at IS NULL", dirArg(tblDir, "soft_delete_cond"))
	assert.Equal(t, "deleted_at = now()", dirArg(tblDir, "soft_delete_set"))
	assert.Equal(t, "orders", dirArg(orders.Directives.ForName("original_name"), "name"))
	assert.Equal(t, "sales", dirArg(orders.Directives.ForName("module"), "name"))

	// @catalog(name, engine) re-attached from data_source_meta.
	cat := orders.Directives.ForName("catalog")
	require.NotNil(t, cat)
	assert.Equal(t, "test", dirArg(cat, "name"))
	assert.Equal(t, "duckdb", dirArg(cat, "engine"))

	// @references re-emitted from the physical relation row.
	ref := orders.Directives.ForName("references")
	require.NotNil(t, ref)
	assert.Equal(t, "order_customer", dirArg(ref, "name"))
	assert.Equal(t, "customers", dirArg(ref, "references_name"))
	assert.Equal(t, "customer", dirArg(ref, "query"))
	assert.Equal(t, "orders", dirArg(ref, "references_query"))

	// Base fields: id is @pk, customer_id present, generated fields NOT here.
	id := orders.Fields.ForName("id")
	require.NotNil(t, id)
	assert.Equal(t, "Int!", id.Type.String())
	assert.NotNil(t, id.Directives.ForName("pk"))
	require.NotNil(t, orders.Fields.ForName("customer_id"))
	assert.Nil(t, orders.Fields.ForName("customer"), "nav field is generated, not stored")

	// @deprecated round-trips through the deprecation_reason column.
	amount := orders.Fields.ForName("amount")
	require.NotNil(t, amount)
	assert.Equal(t, "use total_with_tax", dirArg(amount.Directives.ForName("deprecated"), "reason"))
	assert.Nil(t, id.Directives.ForName("deprecated"))

	// Field bag directives re-emitted by the pair table.
	computed := orders.Fields.ForName("total_with_tax")
	require.NotNil(t, computed)
	assert.Equal(t, "amount * 1.1", dirArg(computed.Directives.ForName("sql"), "exp"))
	area := orders.Fields.ForName("area")
	require.NotNil(t, area)
	assert.Equal(t, "POLYGON", dirArg(area.Directives.ForName("geometry_info"), "type"))
	assert.Equal(t, "4326", dirArg(area.Directives.ForName("geometry_info"), "srid"))
	assert.Equal(t, "3", dirArg(area.Directives.ForName("dim"), "len"))
	assert.Equal(t, "INTERSECTS", dirArg(area.Directives.ForName("unique_rule"), "rule"))
	created := orders.Fields.ForName("created_at")
	require.NotNil(t, created)
	assert.Equal(t, "now()", dirArg(created.Directives.ForName("default"), "insert_exp"))
	statusLabel := orders.Fields.ForName("status_label")
	require.NotNil(t, statusLabel)
	fcDir := statusLabel.Directives.ForName("function_call")
	require.NotNil(t, fcDir)
	assert.Equal(t, "order_status", dirArg(fcDir, "references_name"))
	assert.Equal(t, "sales", dirArg(fcDir, "module"))
	require.NotNil(t, fcDir.Arguments.ForName("args"), "@function_call args map re-emitted")

	// Declared @join field on customers.
	customers := store.ForName(ctx, "customers")
	require.NotNil(t, customers)
	joinField := customers.Fields.ForName("customer_orders")
	require.NotNil(t, joinField, "declared @join field is a stored column row")
	joinDir := joinField.Directives.ForName("join")
	require.NotNil(t, joinDir)
	assert.Equal(t, "orders", dirArg(joinDir, "references_name"))
	nameField := customers.Fields.ForName("name")
	require.NotNil(t, nameField)
	assert.Equal(t, "10m", dirArg(nameField.Directives.ForName("cache"), "ttl"))

	// A view keeps its @view(sql:).
	view := store.ForName(ctx, "sales_by_country")
	require.NotNil(t, view)
	require.NotNil(t, view.Directives.ForName("view"))
	assert.Contains(t, dirArg(view.Directives.ForName("view"), "sql"), "SELECT")

	// The m2m junction is @table(is_m2m: true).
	junction := store.ForName(ctx, "order_tags")
	require.NotNil(t, junction)
	tbl := junction.Directives.ForName("table")
	require.NotNil(t, tbl)
	assert.Equal(t, "true", tbl.Arguments.ForName("is_m2m").Value.Raw)
}

// TestReconstructFunction covers the function pair table: the stored root-field
// definition round-trips with @function / @module / @deprecated and the ordered
// arguments incl. argument-level @deprecated.
func TestReconstructFunction(t *testing.T) {
	store, ctx := writtenStore(t)

	fn := reconstructFunction(ctx, store, "sales", "order_status")
	require.NotNil(t, fn)
	assert.Equal(t, "order_status", fn.Name)
	assert.Equal(t, "String", fn.Type.String())
	assert.Equal(t, "order_status", dirArg(fn.Directives.ForName("function"), "name"))
	assert.Equal(t, "sales", dirArg(fn.Directives.ForName("module"), "name"))
	assert.Equal(t, "No longer supported", dirArg(fn.Directives.ForName("deprecated"), "reason"))

	require.Len(t, fn.Arguments, 2)
	id := fn.Arguments.ForName("id")
	require.NotNil(t, id)
	assert.Equal(t, "Int!", id.Type.String())
	mode := fn.Arguments.ForName("mode")
	require.NotNil(t, mode)
	assert.Equal(t, "unused", dirArg(mode.Directives.ForName("deprecated"), "reason"))

	// Absent function → nil.
	assert.Nil(t, reconstructFunction(ctx, store, "sales", "no_such_fn"))
}

func dirArg(d *ast.Directive, name string) string {
	if d == nil {
		return ""
	}
	if a := d.Arguments.ForName(name); a != nil {
		return a.Value.Raw
	}
	return ""
}
