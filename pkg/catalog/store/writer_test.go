//go:build duckdb_arrow

package store

import (
	"context"
	"fmt"
	"strings"
	"testing"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteSourceRoundTrip covers the write path: collect the fixture, write it
// into a fresh in-memory CoreDB, read the rows back, and confirm a re-write of
// the same version is a no-op.
func TestWriteSourceRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))

	store, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)

	d := collect(ctx, partialSource(t, "test", collectTestSchema), "test")
	state := SourceState{Name: "test", Version: "v1", Engine: "duckdb", Prefix: "shop", AsModule: true, Loaded: true}

	changed, err := store.writeSource(ctx, d, state)
	require.NoError(t, err)
	assert.True(t, changed, "first write persists the source")

	// --- entity rows landed ---
	assert.Equal(t, []string{"5"}, rows(t, pool, `SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))
	assert.Equal(t, []string{"table|orders"}, rows(t, pool,
		`SELECT kind, name FROM core.catalog.data_objects WHERE name = 'orders'`))
	assert.Equal(t, []string{"view"}, rows(t, pool,
		`SELECT kind FROM core.catalog.data_objects WHERE name = 'sales_by_country'`))

	// --- property bags round-trip through the STRUCT column ---
	assert.Equal(t, []string{"orders"}, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.name') FROM core.catalog.data_objects WHERE name = 'orders'`))
	assert.Equal(t, []string{"true"}, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.is_m2m') FROM core.catalog.data_objects WHERE name = 'order_tags'`))
	assert.Equal(t, []string{"true|amount * 1.1"}, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.computed'), json_extract_string(properties::JSON, '$.sql')
		 FROM core.catalog.fields WHERE type_name = 'orders' AND name = 'total_with_tax'`))
	assert.Contains(t, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.sql') FROM core.catalog.data_objects WHERE name = 'sales_by_country'`)[0],
		"SELECT", "@view sql stored")

	// --- step-1 members: soft-delete raw expressions, @dim/@unique_rule, @deprecated ---
	assert.Equal(t, []string{"deleted_at IS NULL|deleted_at = now()"}, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.soft_delete_cond'), json_extract_string(properties::JSON, '$.soft_delete_set')
		 FROM core.catalog.data_objects WHERE name = 'orders'`))
	assert.Equal(t, []string{"3|INTERSECTS"}, rows(t, pool,
		`SELECT json_extract_string(properties::JSON, '$.dim'), json_extract_string(properties::JSON, '$.unique_rule')
		 FROM core.catalog.fields WHERE type_name = 'orders' AND name = 'area'`))
	assert.Equal(t, []string{"use total_with_tax"}, rows(t, pool,
		`SELECT deprecation_reason FROM core.catalog.fields WHERE type_name = 'orders' AND name = 'amount'`))
	assert.Equal(t, []string{"No longer supported|unused"}, rows(t, pool,
		`SELECT deprecation_reason, json_extract_string(args::JSON, '$[1].deprecation_reason')
		 FROM core.catalog.functions WHERE name = 'order_status'`))

	// --- physical relations: fk + two m2m legs (source = junction) ---
	assert.Equal(t, []string{"order_tags|orders|fk", "order_tags|tags|fk", "orders|customers|fk"},
		rows(t, pool, `SELECT source, destination, kind FROM core.catalog.relations ORDER BY source, destination`))
	assert.Equal(t, []string{`["customer_id"]|["id"]`}, rows(t, pool,
		`SELECT source_keys::JSON::VARCHAR, destination_keys::JSON::VARCHAR FROM core.catalog.relations WHERE name = 'order_customer'`))

	// --- functions + ordered args ---
	assert.Equal(t, []string{"sales|order_status|function|false", "sales|reprice_orders|mutation|true"},
		rows(t, pool, `SELECT module, name, kind, is_table FROM core.catalog.functions ORDER BY name`))
	assert.Equal(t, []string{"id|Int!"}, rows(t, pool,
		`SELECT json_extract_string(args::JSON, '$[0].name'), json_extract_string(args::JSON, '$[0].type')
		 FROM core.catalog.functions WHERE name = 'order_status'`))

	// --- modules + parent + closure (incl. the root '' row) with kind flags ---
	assert.Equal(t, []string{"sales|<nil>", "sales.reports|sales"}, rows(t, pool,
		`SELECT name, parent FROM core.catalog.modules ORDER BY name`))
	assert.Equal(t, []string{"", "sales", "sales.reports"}, rows(t, pool,
		`SELECT module FROM core.catalog.module_data_sources WHERE data_source = 'test' ORDER BY module`))
	assert.Equal(t, []string{"true|true|true|true|false"}, rows(t, pool,
		`SELECT has_data_objects, has_tables, has_functions, has_mut_functions, has_subscriptions
		 FROM core.catalog.module_data_sources WHERE module = 'sales'`))
	assert.Equal(t, []string{"true|false|false|false|false"}, rows(t, pool,
		`SELECT has_data_objects, has_tables, has_functions, has_mut_functions, has_subscriptions
		 FROM core.catalog.module_data_sources WHERE module = 'sales.reports'`),
		"the view-only submodule contributes no table/function kinds")

	// --- meta stamped with the writer-format version + options ---
	assert.Equal(t, []string{"f3|v1|true|false|false"}, rows(t, pool,
		`SELECT version, loaded, disabled, suspended FROM core.catalog.data_source_meta WHERE data_source = 'test'`))
	assert.Equal(t, []string{"duckdb|false|shop|true"}, rows(t, pool,
		`SELECT engine, read_only, prefix, as_module FROM core.catalog.data_source_meta WHERE data_source = 'test'`))

	// --- original_name stored (equals name for the unprefixed fixture) ---
	assert.Equal(t, []string{"orders|orders"}, rows(t, pool,
		`SELECT name, original_name FROM core.catalog.data_objects WHERE name = 'orders'`))

	// --- idempotency: same version is a no-op ---
	changed2, err := store.writeSource(ctx, d, state)
	require.NoError(t, err)
	assert.False(t, changed2, "unchanged version → no rewrite")

	// --- version bump rewrites the source ---
	state.Version = "v2"
	changed3, err := store.writeSource(ctx, d, state)
	require.NoError(t, err)
	assert.True(t, changed3)
	assert.Equal(t, []string{"5"}, rows(t, pool, `SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))
	assert.Equal(t, []string{"f3|v2"}, rows(t, pool,
		`SELECT version FROM core.catalog.data_source_meta WHERE data_source = 'test'`))

	// --- setFlags mirrors flags into the meta; rows stay ---
	require.NoError(t, store.setFlags(ctx, "test", false, false, true))
	assert.Equal(t, []string{"false|false|true"}, rows(t, pool,
		`SELECT loaded, disabled, suspended FROM core.catalog.data_source_meta WHERE data_source = 'test'`))
	assert.Equal(t, []string{"5"}, rows(t, pool, `SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))

	// --- DeleteSource removes the rows and prunes the modules ---
	require.NoError(t, store.deleteSource(ctx, "test"))
	assert.Equal(t, []string{"0"}, rows(t, pool, `SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))
	assert.Equal(t, []string{"0"}, rows(t, pool, `SELECT count(*) FROM core.catalog.modules`))
	assert.Empty(t, rows(t, pool, `SELECT version FROM core.catalog.data_source_meta WHERE data_source = 'test'`))
}

// TestWriteExtensionComposition proves per-source writes compose: source A owns
// the object, source B adds a field, and reloading A does not lose B's field.
func TestWriteExtensionComposition(t *testing.T) {
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	store, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)

	const baseSchema = `type customers @module(name: "sales") @table(name: "customers") {
  id: Int! @pk
  name: String!
}`
	const extSchema = `extend type customers { note: String }`
	srcA := partialSource(t, "A", baseSchema)
	srcB := partialSource(t, "B", extSchema, srcA)

	dA := collect(ctx, srcA, "A")
	dB := collect(ctx, srcB, "B")

	_, err = store.writeSource(ctx, dA, SourceState{Name: "A", Version: "v1", Loaded: true})
	require.NoError(t, err)
	_, err = store.writeSource(ctx, dB, SourceState{Name: "B", Version: "v1", Loaded: true})
	require.NoError(t, err)

	// customers has A's two columns + B's added field.
	assert.Equal(t, []string{"id|A", "name|A", "note|B"}, rows(t, pool,
		`SELECT name, data_source FROM core.catalog.fields WHERE type_name = 'customers' ORDER BY name`))

	// Reloading A (new version) rewrites only A's rows — B's field survives.
	_, err = store.writeSource(ctx, dA, SourceState{Name: "A", Version: "v2", Loaded: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"id|A", "name|A", "note|B"}, rows(t, pool,
		`SELECT name, data_source FROM core.catalog.fields WHERE type_name = 'customers' ORDER BY name`),
		"reload of A keeps B's extension field")

	// Unregistering B removes only its field.
	require.NoError(t, store.deleteSource(ctx, "B"))
	assert.Equal(t, []string{"id|A", "name|A"}, rows(t, pool,
		`SELECT name, data_source FROM core.catalog.fields WHERE type_name = 'customers' ORDER BY name`))
}

func rows(t *testing.T, pool *db.Pool, query string) []string {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	rs, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rs.Close()
	var out []string
	for rs.Next() {
		cols, err := rs.Columns()
		require.NoError(t, err)
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rs.Scan(ptrs...))
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	require.NoError(t, rs.Err())
	return out
}
