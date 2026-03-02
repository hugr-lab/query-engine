//go:build duckdb_arrow

package coredb_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// allSchemaTables lists all 11 _schema_* tables.
var allSchemaTables = []string{
	"_schema_catalogs",
	"_schema_catalog_dependencies",
	"_schema_types",
	"_schema_fields",
	"_schema_arguments",
	"_schema_enum_values",
	"_schema_directives",
	"_schema_modules",
	"_schema_module_catalogs",
	"_schema_data_objects",
	"_schema_data_object_queries",
}

// vectorTables lists tables that have a vec column.
var vectorTables = []string{
	"_schema_catalogs",
	"_schema_types",
	"_schema_fields",
	"_schema_modules",
}

func migrationFilePath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("HUGR_MIGRATIONS_PATH"); p != "" {
		return filepath.Join(p, "0.0.9", "1-add-schema-tables.sql")
	}
	// fallback: sibling repo relative to test package dir (go test cwd = package dir)
	root := filepath.Join("..", "..", "..", "hugr", "migrations")
	p := filepath.Join(root, "0.0.9", "1-add-schema-tables.sql")
	if _, err := os.Stat(p); err != nil {
		t.Skipf("migration file not found at %s (set HUGR_MIGRATIONS_PATH)", p)
	}
	return p
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

	// check all 11 _schema_* tables exist and are queryable
	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "table %s should exist", table)

		var count int
		err = conn.QueryRow(ctx, `SELECT count(*) FROM "`+table+`";`).Scan(&count)
		require.NoError(t, err, "count %s", table)
		assert.Equal(t, 0, count, "table %s should be empty", table)
	}
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
			 WHERE table_name = $1 AND column_name = 'vec';`, table,
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

	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE database_name = 'core' AND table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "core.%s", table)
		assert.True(t, exists, "core.%s should exist", table)
	}

	var version string
	err = conn.QueryRow(ctx, `SELECT "version" FROM core."version" LIMIT 1;`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, coredb.Version, version)
}

func TestDuckDB_Migration(t *testing.T) {
	pool, err := db.NewPool("")
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	// create full DB then drop _schema_* tables to simulate 0.0.8
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for i := len(allSchemaTables) - 1; i >= 0; i-- {
		_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS "`+allSchemaTables[i]+`";`)
		require.NoError(t, err)
	}
	_, err = conn.Exec(ctx, `UPDATE "version" SET "version" = '0.0.8';`)
	require.NoError(t, err)

	// apply migration
	migrationSQL, err := os.ReadFile(migrationFilePath(t))
	require.NoError(t, err, "migration file not found")

	parsed, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, string(migrationSQL), coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = conn.Exec(ctx, parsed)
	require.NoError(t, err)

	// verify tables
	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(ctx,
			"SELECT EXISTS(FROM duckdb_tables() WHERE table_name = $1);", table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "table %s should exist after migration", table)
	}

	// verify vec columns
	for _, table := range vectorTables {
		var exists bool
		err = conn.QueryRow(ctx,
			`SELECT EXISTS(FROM duckdb_columns() WHERE table_name = $1 AND column_name = 'vec');`, table,
		).Scan(&exists)
		require.NoError(t, err, "vec on %s", table)
		assert.True(t, exists, "vec column on %s after migration", table)
	}
}

func TestDuckDB_MigrationIdempotent(t *testing.T) {
	pool, err := db.NewPool("")
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()

	// apply full init schema (already has _schema_* tables)
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	// apply migration on top — should be idempotent
	migrationSQL, err := os.ReadFile(migrationFilePath(t))
	require.NoError(t, err)

	parsed, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, string(migrationSQL), coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(ctx, parsed)
	require.NoError(t, err, "migration should be idempotent")
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
		"_schema_module_catalogs",
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

	// check all _schema_* tables exist
	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public');`,
			table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "table %s should exist", table)
	}

	// check tables are queryable
	for _, table := range allSchemaTables {
		var count int
		err = conn.QueryRow(`SELECT count(*) FROM "` + table + `";`).Scan(&count)
		require.NoError(t, err, "count %s", table)
		assert.Equal(t, 0, count, "table %s should be empty", table)
	}

	// check JSONB columns on tables that have directives
	jsonbTables := []string{"_schema_types", "_schema_fields", "_schema_arguments", "_schema_enum_values"}
	for _, table := range jsonbTables {
		var dataType string
		err = conn.QueryRow(
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = $1 AND column_name = 'directives';`, table,
		).Scan(&dataType)
		require.NoError(t, err, "directives type on %s", table)
		assert.Equal(t, "jsonb", dataType, "directives on %s should be JSONB", table)
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

func TestPostgres_Migration(t *testing.T) {
	conn := openPG(t)
	defer conn.Close()

	cleanPG(t, conn)

	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS vector;")
	require.NoError(t, err)

	// create full DB then drop _schema_* tables to simulate 0.0.8
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBPostgres, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)
	_, err = conn.Exec(sqlStr)
	require.NoError(t, err)

	// drop _schema_* tables
	for i := len(allSchemaTables) - 1; i >= 0; i-- {
		_, err = conn.Exec(`DROP TABLE IF EXISTS "` + allSchemaTables[i] + `" CASCADE;`)
		require.NoError(t, err)
	}
	_, err = conn.Exec(`UPDATE "version" SET "version" = '0.0.8';`)
	require.NoError(t, err)

	// verify tables are gone
	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public');`,
			table,
		).Scan(&exists)
		require.NoError(t, err)
		assert.False(t, exists, "table %s should not exist before migration", table)
	}

	// apply migration
	migrationSQL, err := os.ReadFile(migrationFilePath(t))
	require.NoError(t, err)

	parsed, err := db.ParseSQLScriptTemplate(db.SDBPostgres, string(migrationSQL), coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = conn.Exec(parsed)
	require.NoError(t, err)

	// verify all tables now exist
	for _, table := range allSchemaTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public');`,
			table,
		).Scan(&exists)
		require.NoError(t, err, "table %s", table)
		assert.True(t, exists, "table %s should exist after migration", table)
	}

	// verify vec columns
	for _, table := range vectorTables {
		var exists bool
		err = conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'vec');`,
			table,
		).Scan(&exists)
		require.NoError(t, err, "vec on %s", table)
		assert.True(t, exists, "vec column on %s after migration", table)
	}

	cleanPG(t, conn)
}

func TestPostgres_MigrationIdempotent(t *testing.T) {
	conn := openPG(t)
	defer conn.Close()

	cleanPG(t, conn)

	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS vector;")
	require.NoError(t, err)

	// apply full init schema
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBPostgres, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)
	_, err = conn.Exec(sqlStr)
	require.NoError(t, err)

	// apply migration on top — should be idempotent
	migrationSQL, err := os.ReadFile(migrationFilePath(t))
	require.NoError(t, err)

	parsed, err := db.ParseSQLScriptTemplate(db.SDBPostgres, string(migrationSQL), coredb.SchemaTemplateParams{
		VectorSize: coredb.DefaultVectorSize,
	})
	require.NoError(t, err)

	_, err = conn.Exec(parsed)
	require.NoError(t, err, "migration should be idempotent on PostgreSQL")

	cleanPG(t, conn)
}
