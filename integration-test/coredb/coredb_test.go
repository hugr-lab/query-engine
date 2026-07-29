//go:build duckdb_arrow

package coredb_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// catalogTables lists the catalog entity-namespace tables — the CoreDB schema
// storage since design-034, and the only one since design-036 dropped the
// eleven _schema_* tables the compiled-schema provider used.
var catalogTables = []string{
	"data_source_meta",
	"modules",
	"module_data_sources",
	"data_objects",
	"fields",
	"relations",
	"functions",
	"types",
	"annotations",
	"data_source_dependencies",
}

// legacySchemaTables must NOT be created by the init schema any more. A fresh
// CoreDB that grows them again means the DDL was resurrected; an upgraded one
// is the 0.0.20 migration's business, not this test's.
var legacySchemaTables = []string{
	"_schema_catalogs",
	"_schema_catalog_dependencies",
	"_schema_types",
	"_schema_fields",
	"_schema_arguments",
	"_schema_enum_values",
	"_schema_directives",
	"_schema_modules",
	"_schema_module_type_catalogs",
	"_schema_data_objects",
	"_schema_data_object_queries",
}

// vectorTables lists tables that have a vec column. The annotations overlay is
// the only one left: the legacy tables carried their own vectors, and semantic
// search now ranks over the annotations.
var vectorTables = []string{
	"annotations",
}

// ─── DuckDB tests ───────────────────────────────────────────────────────────

func TestDuckDB_InitSchema(t *testing.T) {
	pool, err := db.NewPool("")
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// check version
	var version string
	err = conn.QueryRow(ctx, `SELECT "version" FROM "version" LIMIT 1;`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, coredb.Version, version)

	// the catalog namespace exists and is queryable
	for _, table := range catalogTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE schema_name = 'catalog' AND table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "catalog.%s should exist", table)

		var count int
		err = conn.QueryRow(ctx, `SELECT count(*) FROM catalog."`+table+`";`).Scan(&count)
		require.NoError(t, err, "count %s", table)
		assert.Equal(t, 0, count, "catalog.%s should be empty", table)
	}

	// ...and the compiled-schema tables do not come back
	for _, table := range legacySchemaTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.False(t, exists, "%s must not be created any more", table)
	}

	// _schema_settings is NOT one of them: it carries the schema_version counter.
	var settings bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(FROM duckdb_tables() WHERE table_name = '_schema_settings');",
	).Scan(&settings)
	require.NoError(t, err)
	assert.True(t, settings, "_schema_settings stays")
}

func TestDuckDB_VectorColumns(t *testing.T) {
	pool, err := db.NewPool("")
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 768,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for _, table := range vectorTables {
		var colType string
		err = conn.QueryRow(ctx,
			`SELECT data_type FROM duckdb_columns()
			 WHERE schema_name = 'catalog' AND table_name = $1 AND column_name = 'vec';`, table,
		).Scan(&colType)
		require.NoError(t, err, "vec type on %s", table)
		assert.Equal(t, "FLOAT[768]", colType, "vec column on %s", table)
	}
}

func TestDuckDB_AttachedMode(t *testing.T) {
	pool, err := db.NewPool("")
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	// attach in-memory DB named "core" — this is the real runtime path
	_, err = pool.Exec(ctx, "ATTACH ':memory:' AS core;")
	require.NoError(t, err)

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBAttachedDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for _, table := range catalogTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE database_name = 'core' AND schema_name = 'catalog' AND table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "core.catalog.%s", table)
		assert.True(t, exists, "core.catalog.%s should exist", table)
	}

	var version string
	err = conn.QueryRow(ctx, `SELECT "version" FROM core."version" LIMIT 1;`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, coredb.Version, version)
}

// ─── PostgreSQL tests ───────────────────────────────────────────────────────

func pgDSN(t *testing.T) string {
	dsn := os.Getenv("COREDB_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("COREDB_TEST_PG_DSN not set, skipping PostgreSQL tests")
	}
	return dsn
}

func openPG(t *testing.T) *sql.DB {
	dsn := pgDSN(t)
	conn, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	err = conn.Ping()
	require.NoError(t, err, "cannot connect to PostgreSQL at %s", dsn)
	return conn
}

// cleanPG drops all _schema_* tables and the version/core tables if they exist.
func cleanPG(t *testing.T, conn *sql.DB) {
	// drop in reverse dependency order
	tables := []string{
		"_schema_data_object_queries",
		"_schema_data_objects",
		"_schema_module_type_catalogs",
		"_schema_modules",
		"_schema_directives",
		"_schema_enum_values",
		"_schema_arguments",
		"_schema_fields",
		"_schema_types",
		"_schema_catalog_dependencies",
		"_schema_catalogs",
		"permissions",
		"api_keys",
		"data_source_catalogs",
		"data_sources",
		"catalog_sources",
		"roles",
		"version",
	}
	for _, table := range tables {
		_, _ = conn.Exec(`DROP TABLE IF EXISTS "` + table + `" CASCADE;`)
	}
}

func TestPostgres_InitSchema(t *testing.T) {
	conn := openPG(t)
	defer conn.Close()

	cleanPG(t, conn)

	// ensure pgvector extension
	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS vector;")
	require.NoError(t, err)

	// parse and apply init schema for PostgreSQL
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBPostgres, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = conn.Exec(sqlStr)
	require.NoError(t, err)

	// check version
	var version string
	err = conn.QueryRow(`SELECT "version" FROM "version" LIMIT 1;`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, coredb.Version, version)

	// the catalog namespace exists
	for _, table := range catalogTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'catalog');`,
			table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "catalog.%s should exist", table)
	}

	// ...and the compiled-schema tables do not come back
	for _, table := range legacySchemaTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public');`,
			table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.False(t, exists, "%s must not be created any more", table)
	}

	// check tables are queryable
	for _, table := range catalogTables {
		var count int
		err = conn.QueryRow(`SELECT count(*) FROM catalog."` + table + `";`).Scan(&count)
		require.NoError(t, err, "count %s", table)
		assert.Equal(t, 0, count, "catalog.%s should be empty", table)
	}

	// the property bags are JSONB on PostgreSQL — the write path sends them as
	// plain JSON text and relies on the assignment cast.
	jsonbColumns := []struct{ table, column string }{
		{"data_objects", "properties"},
		{"fields", "properties"},
		{"functions", "properties"},
		{"relations", "properties"},
		{"data_source_meta", "capabilities"},
	}
	for _, c := range jsonbColumns {
		var dataType string
		err = conn.QueryRow(
			`SELECT data_type FROM information_schema.columns
			 WHERE table_schema = 'catalog' AND table_name = $1 AND column_name = $2;`, c.table, c.column,
		).Scan(&dataType)
		require.NoError(t, err, "%s.%s", c.table, c.column)
		assert.Equal(t, "jsonb", dataType, "catalog.%s.%s should be JSONB", c.table, c.column)
	}

	cleanPG(t, conn)
}

func TestPostgres_VectorColumns(t *testing.T) {
	conn := openPG(t)
	defer conn.Close()

	cleanPG(t, conn)

	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS vector;")
	require.NoError(t, err)

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBPostgres, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 768,
	})
	require.NoError(t, err)

	_, err = conn.Exec(sqlStr)
	require.NoError(t, err)

	// check vec column exists and has correct type (USER-DEFINED for pgvector)
	for _, table := range vectorTables {
		var dataType string
		err = conn.QueryRow(
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = $1 AND column_name = 'vec';`, table,
		).Scan(&dataType)
		require.NoError(t, err, "vec column on %s", table)
		assert.Equal(t, "USER-DEFINED", dataType, "vec on %s should be vector type", table)

		// check the actual UDT name
		var udtName string
		err = conn.QueryRow(
			`SELECT udt_name FROM information_schema.columns
			 WHERE table_name = $1 AND column_name = 'vec';`, table,
		).Scan(&udtName)
		require.NoError(t, err)
		assert.Equal(t, "vector", udtName, "vec UDT on %s should be vector", table)
	}

	cleanPG(t, conn)
}
