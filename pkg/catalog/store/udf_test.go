//go:build duckdb_arrow

package store

import (
	"context"
	"fmt"
	"testing"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	qetypes "github.com/hugr-lab/query-engine/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// udfStore boots a store with the schema-management UDFs registered over a
// single loaded catalog (collectTestSchema — objects, relations, functions,
// modules and a residual type, so every curation kind has a target).
func udfStore(t *testing.T, embedder Embedder, checker CatalogChecker) (*Store, *db.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	s, err := New(ctx, pool, Config{VecSize: 8}, embedder)
	require.NoError(t, err)
	require.NoError(t, s.RegisterUDFs(ctx, checker))
	require.NoError(t, s.AddCatalog(ctx, "test", stringCatalog(t, "test", collectTestSchema)))
	return s, pool, ctx
}

// udfCall runs one UDF and returns its OperationResult rendered as text
// ({'success': true, ..., 'message': ...}).
func udfCall(t *testing.T, pool *db.Pool, expr string) string {
	t.Helper()
	got := rows(t, pool, `SELECT (`+expr+`)::VARCHAR`)
	require.Len(t, got, 1)
	return got[0]
}

// fakeChecker stands in for catalog.Service's loaded-catalog gate.
type fakeChecker struct{ loaded bool }

func (c *fakeChecker) ExistsCatalog(string) bool { return c.loaded }

// TestUDFCurationSurfaces pins the two curation surfaces the UDFs reach: the
// legacy names write the GENERATED GraphQL kinds (what the summarizer has
// always curated) and the new names write the LOGICAL kinds — the same entity
// name therefore carries two independent rows.
func TestUDFCurationSurfaces(t *testing.T) {
	_, pool, _ := udfStore(t, nil, nil)

	calls := []struct {
		expr string
		kind string
		key  string
	}{
		{`_schema_update_type_desc('orders', 'gql type', 'gql type long')`, kindGQLType, "orders"},
		{`_schema_update_field_desc('orders', 'amount', 'gql field', '')`, kindGQLField, "orders.amount"},
		{`_schema_update_argument_desc('Query', 'orders', 'limit', 'gql arg', '')`, kindGQLArgument, "Query.orders.limit"},
		{`_schema_update_module_desc('sales', 'module', '')`, kindModule, "sales"},
		{`_schema_update_catalog_desc('test', 'catalog', '')`, kindDataSource, "test"},
		{`_schema_update_data_object_desc('orders', 'object', 'object long')`, kindDataObject, "orders"},
		{`_schema_update_data_object_field_desc('orders', 'amount', 'object field', '')`, kindField, "orders.amount"},
		{`_schema_update_source_type_desc('sales_by_country_args', 'source type', '')`, kindType, "sales_by_country_args"},
		{`_schema_update_function_desc('sales', 'order_status', 'function', 'the query fn', '')`,
			kindFunction, "sales.order_status"},
		// The kind selects the root namespace: the same name curated as a mutation
		// function is an independent row.
		{`_schema_update_function_desc('sales', 'order_status', 'mutation', 'the mutation fn', '')`,
			kindMutationFunction, "sales.order_status"},
	}
	for _, c := range calls {
		assert.Contains(t, udfCall(t, pool, c.expr), "'success': true", c.expr)
		assert.Equal(t, []string{c.kind + "|" + c.key},
			rows(t, pool, `SELECT entity_kind, entity_key FROM core.catalog.annotations
				WHERE entity_kind = `+lit(c.kind)+` AND entity_key = `+lit(c.key)+`
				AND description IS NOT NULL`), c.expr)
	}

	// The same name curated on both surfaces keeps two independent rows.
	assert.Equal(t, []string{"data_object|object|object long", "gql_type|gql type|gql type long"},
		rows(t, pool, `SELECT entity_kind, description, coalesce(long_description, '-')
			FROM core.catalog.annotations WHERE entity_key = 'orders' ORDER BY entity_kind`))

	// The two function kinds hold their own text side by side.
	assert.Equal(t, []string{"function|the query fn", "mutation|the mutation fn"},
		rows(t, pool, `SELECT entity_kind, description FROM core.catalog.annotations
			WHERE entity_key = 'sales.order_status' ORDER BY entity_kind`))

	// An unknown kind is refused through the result, not by failing the statement.
	assert.Contains(t, udfCall(t, pool, `_schema_update_function_desc('sales', 'order_status', 'query', 'x', '')`),
		"unknown function kind")

	// An empty description clears the curation (NULL, generated text shows again).
	assert.Contains(t, udfCall(t, pool, `_schema_update_data_object_desc('orders', '', '')`), "'success': true")
	assert.Equal(t, []string{"true"}, rows(t, pool, `SELECT description IS NULL
		FROM core.catalog.annotations WHERE entity_kind = 'data_object' AND entity_key = 'orders'`))
}

// TestUDFCurationReachesReader confirms a UDF curation is visible through the
// reader immediately — the annotator evicts the affected cached definitions.
func TestUDFCurationReachesReader(t *testing.T) {
	s, pool, ctx := udfStore(t, nil, nil)

	require.NotNil(t, s.ForName(ctx, "orders")) // populate the cache first
	assert.Contains(t, udfCall(t, pool, `_schema_update_data_object_desc('orders', 'Curated orders', '')`),
		"'success': true")

	def := s.ForName(ctx, "orders")
	require.NotNil(t, def)
	assert.Equal(t, "Curated orders", def.Description)

	assert.Contains(t, udfCall(t, pool, `_schema_update_data_object_field_desc('orders', 'amount', 'Curated amount', '')`),
		"'success': true")
	def = s.ForName(ctx, "orders")
	require.NotNil(t, def)
	require.NotNil(t, def.Fields.ForName("amount"))
	assert.Equal(t, "Curated amount", def.Fields.ForName("amount").Description)
}

// TestUDFVersionClean pins the version escape hatch: an unknown catalog is
// reported through the OperationResult, a known one has its stored version
// invalidated so the next load rewrites its rows instead of taking the gate.
func TestUDFVersionClean(t *testing.T) {
	s, pool, ctx := udfStore(t, nil, nil)

	res := udfCall(t, pool, `_schema_version_clean('nope')`)
	assert.Contains(t, res, "'success': false")
	assert.Contains(t, res, `catalog "nope" not found`)

	assert.Contains(t, udfCall(t, pool, `_schema_version_clean('test')`), "'success': true")
	assert.Equal(t, []string{""}, rows(t, pool,
		`SELECT version FROM core.catalog.data_source_meta WHERE data_source = 'test'`))

	// The cleared version misses the gate: the same source is rewritten.
	require.NoError(t, s.AddCatalog(ctx, "test", stringCatalog(t, "test", collectTestSchema)))
	assert.NotEqual(t, []string{""}, rows(t, pool,
		`SELECT version FROM core.catalog.data_source_meta WHERE data_source = 'test'`))
}

// TestUDFHardRemove pins the unregister gate: a loaded catalog is refused, an
// unloaded one is removed with all of its rows.
func TestUDFHardRemove(t *testing.T) {
	checker := &fakeChecker{loaded: true}
	_, pool, _ := udfStore(t, nil, checker)

	res := udfCall(t, pool, `_schema_hard_remove('test')`)
	assert.Contains(t, res, "'success': false")
	assert.Contains(t, res, "catalog is loaded")
	assert.NotEqual(t, []string{"0"}, rows(t, pool,
		`SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))

	checker.loaded = false
	assert.Contains(t, udfCall(t, pool, `_schema_hard_remove('test')`), "'success': true")
	assert.Equal(t, []string{"0"}, rows(t, pool,
		`SELECT count(*) FROM core.catalog.data_objects WHERE data_source = 'test'`))
	assert.Equal(t, []string{"0"}, rows(t, pool,
		`SELECT count(*) FROM core.catalog.data_source_meta WHERE data_source = 'test'`))
}

// TestUDFReindex pins the vector maintenance path: every entity in scope is
// re-embedded from its CURRENT text — the curation where there is one, the SDL
// or synthetic description otherwise.
func TestUDFReindex(t *testing.T) {
	salt := 0
	_, pool, _ := udfStore(t, saltEmbedder{dim: 8, salt: &salt}, nil)

	// Curate one object; both this row and the untouched seeds carry salt 0.
	assert.Contains(t, udfCall(t, pool, `_schema_update_data_object_desc('orders', 'short', 'a curated long text')`),
		"'success': true")
	assert.Equal(t, []string{"0"}, rows(t, pool, `SELECT vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'data_object' AND entity_key = 'orders'`))

	// A new salt marks every vector the reindex rewrites.
	salt = 7
	res := udfCall(t, pool, `_schema_reindex('', 50)`)
	assert.Contains(t, res, "'success': true")
	assert.Contains(t, res, "reindexed")

	// The curated row was re-embedded from the CURATION (long description wins).
	assert.Equal(t, []string{"19|7"}, rows(t, pool, `SELECT vec[1], vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'data_object' AND entity_key = 'orders'`),
		"len('a curated long text') with the new salt")

	// An uncurated entity was re-embedded from its synthetic description.
	synthetic := syntheticDescription("", "customers", "", "sales", "test")
	assert.Equal(t, []string{fmt.Sprint(len(synthetic)) + "|7"},
		rows(t, pool, `SELECT vec[1], vec[2] FROM core.catalog.annotations
			WHERE entity_kind = 'data_object' AND entity_key = 'customers'`))

	// A relation navigation field (generated: no catalog.fields row) is covered.
	assert.Equal(t, []string{"7"}, rows(t, pool, `SELECT vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'field' AND entity_key = 'customers.orders'`))

	// A GraphQL-surface curation has no entity row and is reindexed globally.
	assert.Contains(t, udfCall(t, pool, `_schema_update_type_desc('orders_filter', 'generated type', '')`),
		"'success': true")
	salt = 9
	assert.Contains(t, udfCall(t, pool, `_schema_reindex('', 50)`), "'success': true")
	assert.Equal(t, []string{"14|9"}, rows(t, pool, `SELECT vec[1], vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'gql_type' AND entity_key = 'orders_filter'`))

	// A SCOPED run covers the source's own entities and leaves the rest alone.
	salt = 11
	assert.Contains(t, udfCall(t, pool, `_schema_reindex('other', 50)`), "'success': true")
	assert.Equal(t, []string{"9"}, rows(t, pool, `SELECT vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'data_object' AND entity_key = 'customers'`),
		"another source's scope does not touch these rows")
	assert.Contains(t, udfCall(t, pool, `_schema_reindex('test', 50)`), "'success': true")
	assert.Equal(t, []string{"11"}, rows(t, pool, `SELECT vec[2] FROM core.catalog.annotations
		WHERE entity_kind = 'data_object' AND entity_key = 'customers'`))
}

// TestUDFReindexNoEmbedder confirms reindexing reports the missing embedder
// through the OperationResult instead of failing the statement.
func TestUDFReindexNoEmbedder(t *testing.T) {
	_, pool, _ := udfStore(t, nil, nil)

	res := udfCall(t, pool, `_schema_reindex('', 50)`)
	assert.Contains(t, res, "'success': false")
	assert.Contains(t, res, "embeddings not configured")
}

// saltEmbedder encodes the embedded TEXT LENGTH in vec[0] and a caller-settable
// salt in vec[1], so a test can tell which text a vector was computed from and
// when it was recomputed.
type saltEmbedder struct {
	dim  int
	salt *int
}

func (e saltEmbedder) vector(text string) qetypes.Vector {
	v := make(qetypes.Vector, e.dim)
	v[0] = float64(len(text))
	v[1] = float64(*e.salt)
	return v
}

func (e saltEmbedder) CreateEmbedding(_ context.Context, input string) (*qetypes.EmbeddingResult, error) {
	return &qetypes.EmbeddingResult{Vector: e.vector(input)}, nil
}

func (e saltEmbedder) CreateEmbeddings(_ context.Context, inputs []string) (*qetypes.EmbeddingsResult, error) {
	out := make([]qetypes.Vector, len(inputs))
	for i, in := range inputs {
		out[i] = e.vector(in)
	}
	return &qetypes.EmbeddingsResult{Vectors: out}, nil
}
