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
	_, err = store.writeSource(ctx, d, SourceState{Name: "test", Version: "v1", Loaded: true})
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
	require.NotNil(t, orders.Directives.ForName("table"), "@table reattached")
	assert.Equal(t, "orders", dirArg(orders.Directives.ForName("table"), "name"), "physical name")
	assert.Equal(t, "orders", dirArg(orders.Directives.ForName("original_name"), "name"))
	assert.Equal(t, "sales", dirArg(orders.Directives.ForName("module"), "name"))

	// Base fields: id is @pk, customer_id present, generated fields NOT here.
	id := orders.Fields.ForName("id")
	require.NotNil(t, id)
	assert.Equal(t, "Int!", id.Type.String())
	assert.NotNil(t, id.Directives.ForName("pk"))
	require.NotNil(t, orders.Fields.ForName("customer_id"))
	assert.Nil(t, orders.Fields.ForName("customer"), "nav field is generated, not stored")

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

func dirArg(d *ast.Directive, name string) string {
	if d == nil {
		return ""
	}
	if a := d.Arguments.ForName(name); a != nil {
		return a.Value.Raw
	}
	return ""
}
