package iceberg

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/ducklake"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// IntrospectSchema discovers tables in an attached Iceberg catalog and returns
// table/column definitions using the same types as DuckLake.
//
// Strategy: duckdb_columns() doesn't return proper column info for iceberg catalogs
// (returns a single "__" column with UNKNOWN type). Instead we:
//  1. List tables via duckdb_tables() (works for iceberg)
//  2. For each table, DESCRIBE SELECT * to get column names and types
func IntrospectSchema(ctx context.Context, pool *db.Pool, prefix string, filter *ducklake.IntrospectFilter) ([]ducklake.DuckLakeTable, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("iceberg: get connection failed: %w", err)
	}
	defer conn.Close()

	// Step 1: List all tables in the iceberg catalog
	tableQuery := fmt.Sprintf(`
		SELECT schema_name, table_name
		FROM duckdb_tables()
		WHERE database_name = '%s'
			AND schema_name NOT IN ('information_schema')
		ORDER BY schema_name, table_name;
	`, escapeSQLString(prefix))

	tableRows, err := conn.Query(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("iceberg: list tables failed: %w", err)
	}
	defer tableRows.Close()

	type tableInfo struct {
		schema, table string
	}
	var tableInfos []tableInfo

	for tableRows.Next() {
		var schema, table string
		if err := tableRows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("iceberg: scan table info failed: %w", err)
		}
		if !filter.Match(schema, table) {
			continue
		}
		tableInfos = append(tableInfos, tableInfo{schema, table})
	}
	if err := tableRows.Err(); err != nil {
		return nil, fmt.Errorf("iceberg: table list error: %w", err)
	}

	// Step 2: DESCRIBE each table to get column definitions
	var tables []ducklake.DuckLakeTable
	for _, ti := range tableInfos {
		cols, err := describeTable(ctx, conn, prefix, ti.schema, ti.table)
		if err != nil {
			return nil, fmt.Errorf("iceberg: describe table %s.%s failed: %w", ti.schema, ti.table, err)
		}
		if len(cols) == 0 {
			continue
		}
		tables = append(tables, ducklake.DuckLakeTable{
			SchemaName: ti.schema,
			TableName:  ti.table,
			Columns:    cols,
		})
	}

	return tables, nil
}

// describeTable uses DESCRIBE to get column names and types for an iceberg table.
func describeTable(ctx context.Context, conn *db.Connection, prefix, schema, table string) ([]ducklake.DuckLakeColumn, error) {
	qualifiedName := fmt.Sprintf(`"%s"."%s"."%s"`, prefix, schema, table)
	descQuery := fmt.Sprintf("DESCRIBE SELECT * FROM %s", qualifiedName)

	rows, err := conn.Query(ctx, descQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []ducklake.DuckLakeColumn
	for rows.Next() {
		var colName, colType, isNull string
		var key, defaultVal, extra sql.NullString
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultVal, &extra); err != nil {
			return nil, err
		}
		cols = append(cols, ducklake.DuckLakeColumn{
			Name:       colName,
			Type:       colType,
			IsNullable: isNull == "YES",
		})
	}
	return cols, rows.Err()
}
