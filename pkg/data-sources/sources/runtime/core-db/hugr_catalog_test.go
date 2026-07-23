//go:build duckdb_arrow

package coredb

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the PROVIDER DML CONTRACT for the catalog namespace: the
// provider talks to CoreDB with direct SQL through the pool — no planner, no
// GraphQL — and the SAME statement texts must work on both backends:
//   * DuckDB — native STRUCT property columns, implicit VARCHAR→STRUCT cast;
//   * attached PostgreSQL — JSONB + INOUT assignment casts (writes go through
//     the postgres extension COPY path).
// probeCatalogStatements below IS the statement inventory (insert / upsert /
// conditional upsert / update / delete / insert-select seeding / symmetric
// reads). Step-2 writer code must reuse these exact statement shapes.

// TestFreshCoreDBDuckDB boots an IN-MEMORY CoreDB exactly the way the dev
// server does (Source.Attach → applySchema; empty path = ':memory:') and runs
// the full statement inventory, plus DuckDB-native STRUCT member reads.
func TestFreshCoreDBDuckDB(t *testing.T) {
	ctx := t.Context()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	s := New(Config{VectorSize: 8}) // empty path → in-memory CoreDB
	require.NoError(t, s.Attach(ctx, pool))

	got := queryStrings(t, pool, `SELECT count(*) FROM duckdb_tables() WHERE database_name = 'core' AND schema_name = 'catalog'`)
	assert.Equal(t, []string{"10"}, got, "all catalog tables created by InitSchema")
	got = queryStrings(t, pool, `SELECT version FROM core."version"`)
	assert.Equal(t, []string{Version}, got)

	probeCatalogStatements(t, pool)

	// Native STRUCT member access (the planner read path) — no JSON functions.
	// Final probe state: probe_rich properties = {"cache":{"ttl":"10m"}}.
	got = queryStrings(t, pool, `SELECT properties.cache.ttl, properties."unique" IS NULL, properties.soft_delete
		FROM core.catalog.data_objects WHERE name = 'probe_rich'`)
	assert.Equal(t, []string{"10m|true|<nil>"}, got, "absent JSON keys are NULL STRUCT members")
	got = queryStrings(t, pool, `SELECT properties.soft_delete FROM core.catalog.data_objects WHERE name = 'probe_partial'`)
	assert.Equal(t, []string{"true"}, got)
	got = queryStrings(t, pool, `SELECT args[1].name, args[1]."type", args[2].description
		FROM core.catalog.functions WHERE module = 'probe' AND name = 'fn'`)
	assert.Equal(t, []string{"id|Int!|<nil>"}, got, "args is a native LIST of STRUCT")
	got = queryStrings(t, pool, `SELECT properties.function_call."function".name,
		properties.function_call.args::VARCHAR, properties."default".value::VARCHAR
		FROM core.catalog.fields WHERE type_name = 'probe_rich' AND name = 'linked'`)
	assert.Equal(t, []string{`f|{"id":"$id"}|{"a":1}`}, got, "JSON members inside native STRUCT")
}

// TestFreshCoreDBPostgres runs the SAME statement inventory against an
// attached PostgreSQL CoreDB.
//
// Requires HUGR_TEST_PG_PATH — a postgres:// DSN of a DEDICATED disposable
// database: on first run Source.Attach initializes it with VectorSize 8; the
// probe cleans its own rows, so reruns are safe. Skipped when unset.
func TestFreshCoreDBPostgres(t *testing.T) {
	dsn := os.Getenv("HUGR_TEST_PG_PATH")
	if dsn == "" {
		t.Skip("HUGR_TEST_PG_PATH not set; PG statement-inventory probe skipped")
	}
	ctx := t.Context()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	s := New(Config{Path: dsn, VectorSize: 8})
	require.NoError(t, s.Attach(ctx, pool))

	got := queryStrings(t, pool, `SELECT version FROM core."version"`)
	assert.Equal(t, []string{Version}, got)

	probeCatalogStatements(t, pool)
}

// TestAttachExistingCoreDB attaches an ALREADY INITIALIZED CoreDB — a DuckDB
// file or a postgres:// DSN from HUGR_TEST_CORE_DB_PATH — through the regular
// pool and runs the statement inventory against it. The probe touches only its
// own 'probe' keys and leaves the final rows in place for manual inspection.
func TestAttachExistingCoreDB(t *testing.T) {
	path := os.Getenv("HUGR_TEST_CORE_DB_PATH")
	if path == "" {
		t.Skip("HUGR_TEST_CORE_DB_PATH not set; existing-CoreDB probe skipped")
	}
	ctx := t.Context()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	s := New(Config{Path: path})
	require.NoError(t, s.Attach(ctx, pool))

	got := queryStrings(t, pool, `SELECT version FROM core."version"`)
	assert.Equal(t, []string{Version}, got)

	probeCatalogStatements(t, pool)
}

// probeCatalogStatements is the provider statement inventory. ONE SQL dialect,
// ONE statement text per operation, executed directly on the pool.
func probeCatalogStatements(t *testing.T, pool *db.Pool) {
	t.Helper()
	ctx := t.Context()
	exec := func(q string) {
		t.Helper()
		_, err := pool.Exec(ctx, q)
		require.NoError(t, err, q)
	}

	// The probe owns its keys — rerun-safe on a persistent database.
	exec(`DELETE FROM core.catalog.data_source_meta WHERE data_source = 'probe'`)
	exec(`DELETE FROM core.catalog.data_objects WHERE data_source = 'probe'`)
	exec(`DELETE FROM core.catalog.fields WHERE data_source = 'probe'`)
	exec(`DELETE FROM core.catalog.functions WHERE module = 'probe'`)
	exec(`DELETE FROM core.catalog.annotations WHERE entity_key LIKE 'probe%'`)
	exec(`DELETE FROM core.catalog.module_data_sources WHERE data_source = 'probe'`)
	exec(`DELETE FROM core.catalog.modules WHERE name LIKE 'probe%'`)

	// ---- A. INSERT --------------------------------------------------------
	// Every NOT NULL column listed explicitly (no DEFAULTs in the schema —
	// they never fire on the attached-PG COPY path); nullable columns may be
	// omitted and are NULL on both backends.

	// A1: multi-row insert, property bags as plain JSON text: rich (nested
	// struct, list of struct, list of varchar), partial (one key), SQL NULL.
	exec(`INSERT INTO core.catalog.data_objects (name, original_name, data_source, module, kind, properties) VALUES
		('probe_rich', 'probe_rich', 'probe', '', 'table',
		 '{"cache":{"ttl":"5m","tags":["a","b"]},"unique":[{"fields":["name"],"query_suffix":"by_name"},{"fields":["id","name"],"skip_query":true}],"at":{"version":42}}'),
		('probe_partial', 'probe_partial', 'probe', '', 'view', '{"soft_delete":true}'),
		('probe_null', 'probe_null', 'probe', '', 'table', NULL)`)

	// A2: fields.properties — the trickiest bag shape: JSON-typed members
	// INSIDE the struct (default.value, function_call.args).
	exec(`INSERT INTO core.catalog.fields (type_name, name, field_type, properties, data_source, is_pk, ordinal) VALUES
		('probe_rich', 'linked', 'probe_other',
		 '{"function_call":{"function":{"module":"m","name":"f"},"args":{"id":"$id"},"source_fields":["id"]},"default":{"value":{"a":1}}}',
		 'probe', false, 3)`)

	// A3: functions.args — a LIST of structs written as one JSON array text.
	exec(`INSERT INTO core.catalog.functions (module, name, kind, data_source, returns, is_table, args) VALUES
		('probe', 'fn', 'function', 'probe', 'String', false,
		 '[{"name":"id","type":"Int!","description":"identifier"},{"name":"mode","type":"String"}]')`)

	// A4: modules (parent derived from the dotted name at insert) + the
	// module→source CLOSURE (submodule contributions recorded on the parent).
	exec(`INSERT INTO core.catalog.modules (name, parent, description) VALUES
		('probe_mod', NULL, NULL), ('probe_mod.sub', 'probe_mod', 'sub module')`)
	exec(`INSERT INTO core.catalog.module_data_sources (module, data_source) VALUES
		('probe_mod', 'probe'), ('probe_mod.sub', 'probe')`)
	got := queryStrings(t, pool, `SELECT name FROM core.catalog.modules WHERE parent = 'probe_mod'`)
	assert.Equal(t, []string{"probe_mod.sub"}, got, "child selection is a plain equality on parent")

	// ---- B. UPSERT --------------------------------------------------------

	// B1: meta upsert — the same statement drives both the insert and the
	// update path (version gate / flag reconcile).
	meta := `INSERT INTO core.catalog.data_source_meta (data_source, version, capabilities, engine, read_only, as_module, loaded, disabled, suspended, loaded_at)
		VALUES ('probe', '%s', '{"read_only":false}', 'duckdb', false, false, true, false, false, CURRENT_TIMESTAMP)
		ON CONFLICT (data_source) DO UPDATE SET version = EXCLUDED.version, capabilities = EXCLUDED.capabilities`
	exec(fmt.Sprintf(meta, "v1"))
	exec(fmt.Sprintf(meta, "v2"))
	got = queryStrings(t, pool, `SELECT version, loaded, disabled, suspended,
		json_extract_string(capabilities::JSON, '$.read_only')
		FROM core.catalog.data_source_meta WHERE data_source = 'probe'`)
	assert.Equal(t, []string{"v2|true|false|false|false"}, got)

	// B2: upserting the property bag itself — replaced wholesale.
	exec(`INSERT INTO core.catalog.data_objects (name, original_name, data_source, module, kind, properties) VALUES
		('probe_rich', 'probe_rich', 'probe', '', 'table', '{"cache":{"ttl":"10m"}}')
		ON CONFLICT (name) DO UPDATE SET properties = EXCLUDED.properties`)
	got = queryStrings(t, pool, `SELECT json_extract_string(properties::JSON, '$.cache.ttl'),
		json_extract_string(properties::JSON, '$.unique[0].query_suffix')
		FROM core.catalog.data_objects WHERE name = 'probe_rich'`)
	assert.Equal(t, []string{"10m|<nil>"}, got, "bag replaced wholesale by the upsert")

	// ---- C. UPDATE --------------------------------------------------------

	// C1: flag flip (unload / suspend are UPDATEs, rows stay). While the
	// source is off, the module-activity gate hides its modules.
	exec(`UPDATE core.catalog.data_source_meta SET loaded = false, suspended = true WHERE data_source = 'probe'`)
	got = queryStrings(t, pool, `SELECT loaded, suspended FROM core.catalog.data_source_meta WHERE data_source = 'probe'`)
	assert.Equal(t, []string{"false|true"}, got)
	activeModules := `SELECT mm.name FROM core.catalog.modules mm
		WHERE EXISTS (SELECT 1 FROM core.catalog.module_data_sources md
			JOIN core.catalog.data_source_meta m ON m.data_source = md.data_source
			WHERE md.module = mm.name AND m.loaded AND NOT m.disabled AND NOT m.suspended)
		AND mm.name LIKE 'probe%' ORDER BY mm.name`
	got = queryStrings(t, pool, activeModules)
	assert.Empty(t, got, "modules of an unloaded source are filtered out")
	exec(`UPDATE core.catalog.data_source_meta SET loaded = true, suspended = false WHERE data_source = 'probe'`)
	got = queryStrings(t, pool, activeModules)
	assert.Equal(t, []string{"probe_mod", "probe_mod.sub"}, got, "module gate is a plain semi-join")

	// C2: bag replace via plain UPDATE (annotation-style RMW writes the whole
	// JSON text back).
	exec(`UPDATE core.catalog.functions SET description = 'probed', args = '[{"name":"id","type":"Int!"},{"name":"mode","type":"String"}]'
		WHERE module = 'probe' AND name = 'fn'`)
	got = queryStrings(t, pool, `SELECT description, json_extract_string(args::JSON, '$[0].description')
		FROM core.catalog.functions WHERE module = 'probe' AND name = 'fn'`)
	assert.Equal(t, []string{"probed|<nil>"}, got, "args rewritten wholesale, description column set")

	// ---- D. READS ---------------------------------------------------------
	// Unified read expressions: ::JSON paths work over native STRUCT (DuckDB)
	// and JSONB (PG) alike.

	got = queryStrings(t, pool, `SELECT
		json_extract_string(properties::JSON, '$.soft_delete'),
		json_valid(properties::JSON::VARCHAR)
		FROM core.catalog.data_objects WHERE name = 'probe_partial'`)
	assert.Equal(t, []string{"true|true"}, got, "bag round-trips to valid JSON text")
	got = queryStrings(t, pool, `SELECT properties IS NULL FROM core.catalog.data_objects WHERE name = 'probe_null'`)
	assert.Equal(t, []string{"true"}, got, "empty bag is SQL NULL — not json-null and not '{}'")
	got = queryStrings(t, pool, `SELECT
		json_extract_string(properties::JSON, '$.function_call.function.name'),
		json_extract_string(properties::JSON, '$.function_call.args.id'),
		json_extract_string(properties::JSON, '$.default.value.a'),
		is_pk, ordinal
		FROM core.catalog.fields WHERE type_name = 'probe_rich' AND name = 'linked'`)
	assert.Equal(t, []string{"f|$id|1|false|3"}, got, "JSON members inside the struct bag survive")

	// D1: stored-signature read (tier-2 diff shape) — canonical JSON text per
	// row, ordered by key.
	got = queryStrings(t, pool, `SELECT name, coalesce(properties::JSON::VARCHAR, '')
		FROM core.catalog.data_objects WHERE data_source = 'probe' ORDER BY name`)
	require.Len(t, got, 3)
	assert.Equal(t, "probe_null|", got[0], "NULL bag reads as empty signature")

	// D2: state-gate semi-join (the entity views' visibility shape).
	got = queryStrings(t, pool, `SELECT o.name FROM core.catalog.data_objects o
		WHERE EXISTS (SELECT 1 FROM core.catalog.data_source_meta m
			WHERE m.data_source = o.data_source AND m.loaded AND NOT m.disabled AND NOT m.suspended)
		AND o.data_source = 'probe' ORDER BY o.name`)
	assert.Equal(t, []string{"probe_null", "probe_partial", "probe_rich"}, got)

	// ---- E. ANNOTATIONS: vec, conditional upsert, seed insert-select ------
	// Vector dimension follows the attached database (seeded at init).
	dims := queryStrings(t, pool, `SELECT json_extract_string(value::JSON, '$.vec_size') FROM core._schema_settings WHERE key = 'config'`)
	require.Len(t, dims, 1, "vec_size seeded at initialization")
	dim, err := strconv.Atoi(dims[0])
	require.NoError(t, err)
	vecText := func(x string) string {
		return "[" + strings.Join(slices.Repeat([]string{x}, dim), ",") + "]"
	}

	// E1: curated upsert with a text vector literal.
	exec(`INSERT INTO core.catalog.annotations (entity_kind, entity_key, description, vec) VALUES
		('data_object', 'probe_rich', 'curated', '` + vecText("0.1") + `')
		ON CONFLICT (entity_kind, entity_key) DO UPDATE SET description = EXCLUDED.description, vec = EXCLUDED.vec`)
	got = queryStrings(t, pool, `SELECT vec IS NOT NULL FROM core.catalog.annotations WHERE entity_key = 'probe_rich'`)
	assert.Equal(t, []string{"true"}, got, "text vector literal accepted")

	// E2: conditional seed upsert — must NOT overwrite curated rows.
	exec(`INSERT INTO core.catalog.annotations (entity_kind, entity_key, vec) VALUES
		('data_object', 'probe_rich', '` + vecText("0.9") + `')
		ON CONFLICT (entity_kind, entity_key) DO UPDATE SET vec = EXCLUDED.vec
		WHERE description IS NULL AND long_description IS NULL`)
	got = queryStrings(t, pool, `SELECT description FROM core.catalog.annotations WHERE entity_key = 'probe_rich'`)
	assert.Equal(t, []string{"curated"}, got, "conditional upsert skips curated rows")

	// E3: seed shape — INSERT ... SELECT with an anti-join over stored rows.
	exec(`INSERT INTO core.catalog.annotations (entity_kind, entity_key)
		SELECT 'data_object', o.name FROM core.catalog.data_objects o
		WHERE o.data_source = 'probe'
		AND NOT EXISTS (SELECT 1 FROM core.catalog.annotations a
			WHERE a.entity_kind = 'data_object' AND a.entity_key = o.name)`)
	got = queryStrings(t, pool, `SELECT count(*) FROM core.catalog.annotations WHERE entity_key LIKE 'probe%'`)
	assert.Equal(t, []string{"3"}, got, "anti-join seeded the two missing objects")

	// E4: curation overlay read (the entity views' COALESCE shape).
	got = queryStrings(t, pool, `SELECT o.name, coalesce(a.description, o.description, '-')
		FROM core.catalog.data_objects o
		LEFT JOIN core.catalog.annotations a ON a.entity_kind = 'data_object' AND a.entity_key = o.name
		WHERE o.data_source = 'probe' ORDER BY o.name`)
	assert.Equal(t, []string{"probe_null|-", "probe_partial|-", "probe_rich|curated"}, got)

	// ---- F. DELETE --------------------------------------------------------

	// F1: row-value IN delete (composite-PK batch delete shape).
	exec(`INSERT INTO core.catalog.fields (type_name, name, field_type, properties, data_source, is_pk, ordinal) VALUES
		('probe_rich', 'tmp_del', 'Int', NULL, 'probe', false, 4)`)
	exec(`DELETE FROM core.catalog.fields WHERE (type_name, name) IN (('probe_rich', 'tmp_del'))`)
	got = queryStrings(t, pool, `SELECT count(*) FROM core.catalog.fields WHERE data_source = 'probe'`)
	assert.Equal(t, []string{"1"}, got, "row-value IN removed exactly the listed key")

	// F2: predicate sweep (seed rows go, curated rows stay).
	exec(`DELETE FROM core.catalog.annotations WHERE entity_key LIKE 'probe%'
		AND description IS NULL AND long_description IS NULL`)
	got = queryStrings(t, pool, `SELECT entity_key FROM core.catalog.annotations WHERE entity_key LIKE 'probe%'`)
	assert.Equal(t, []string{"probe_rich"}, got, "sweep removed seeds, kept curated")
}

func queryStrings(t *testing.T, pool *db.Pool, query string) []string {
	t.Helper()
	conn, err := pool.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.Query(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close()
	var res []string
	for rows.Next() {
		cols, err := rows.Columns()
		require.NoError(t, err)
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprint(v)
		}
		res = append(res, strings.Join(parts, "|"))
	}
	require.NoError(t, rows.Err())
	return res
}
