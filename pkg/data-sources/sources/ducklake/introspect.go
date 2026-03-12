package ducklake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// IntrospectFilter holds optional regexp filters for schema/table names.
type IntrospectFilter struct {
	SchemaFilter *regexp.Regexp // If set, only schemas matching this regexp are included
	TableFilter  *regexp.Regexp // If set, only tables matching this regexp are included
}

// NewIntrospectFilter compiles schema/table filter regexps. Empty strings mean no filter.
func NewIntrospectFilter(schemaFilter, tableFilter string) (*IntrospectFilter, error) {
	f := &IntrospectFilter{}
	if schemaFilter != "" {
		re, err := regexp.Compile(schemaFilter)
		if err != nil {
			return nil, fmt.Errorf("ducklake: invalid schema_filter regexp %q: %w", schemaFilter, err)
		}
		f.SchemaFilter = re
	}
	if tableFilter != "" {
		re, err := regexp.Compile(tableFilter)
		if err != nil {
			return nil, fmt.Errorf("ducklake: invalid table_filter regexp %q: %w", tableFilter, err)
		}
		f.TableFilter = re
	}
	return f, nil
}

// Match returns true if the schema/table name passes the filters.
func (f *IntrospectFilter) Match(schemaName, tableName string) bool {
	if f == nil {
		return true
	}
	if f.SchemaFilter != nil && !f.SchemaFilter.MatchString(schemaName) {
		return false
	}
	if f.TableFilter != nil && !f.TableFilter.MatchString(tableName) {
		return false
	}
	return true
}

// metaCatalogIdent returns the properly quoted metadata catalog identifier.
// DuckLake creates an internal catalog named __ducklake_metadata_<catalog_name>.
// If the catalog name contains dots or other special characters, the identifier must be quoted.
func metaCatalogIdent(prefix string) string {
	escaped := strings.ReplaceAll(prefix, `"`, `""`)
	return fmt.Sprintf(`"%s"`, "__ducklake_metadata_"+escaped)
}

// DuckLakeTable represents a table discovered from DuckLake metadata.
type DuckLakeTable struct {
	SchemaName string
	TableName  string
	Columns    []DuckLakeColumn
}

// DuckLakeColumn represents a column discovered from DuckLake metadata.
type DuckLakeColumn struct {
	Name       string
	Type       string
	IsNullable bool
	IsPK       bool
}

// IntrospectResult holds the results of schema introspection.
type IntrospectResult struct {
	Tables []DuckLakeTable
	Views  []DuckLakeView
}

// DuckLakeView represents a view discovered from DuckLake metadata.
type DuckLakeView struct {
	SchemaName string
	ViewName   string
	Columns    []DuckLakeColumn
}

// IntrospectSchema queries DuckLake metadata tables and returns table/column definitions.
// The prefix is the DuckDB catalog name (from ATTACH AS prefix).
func IntrospectSchema(ctx context.Context, pool *db.Pool, prefix string) ([]DuckLakeTable, error) {
	return IntrospectSchemaFiltered(ctx, pool, prefix, nil)
}

// IntrospectSchemaFiltered queries DuckLake metadata tables and returns table/column definitions,
// optionally filtering by schema/table name regexps.
func IntrospectSchemaFiltered(ctx context.Context, pool *db.Pool, prefix string, filter *IntrospectFilter) ([]DuckLakeTable, error) {
	result, err := IntrospectAll(ctx, pool, prefix, filter)
	if err != nil {
		return nil, err
	}
	return result.Tables, nil
}

// IntrospectAll queries DuckLake metadata tables and returns both tables and views.
func IntrospectAll(ctx context.Context, pool *db.Pool, prefix string, filter *IntrospectFilter) (*IntrospectResult, error) {
	tables, err := introspectTables(ctx, pool, prefix, filter)
	if err != nil {
		return nil, err
	}
	views, err := introspectViews(ctx, pool, prefix, filter)
	if err != nil {
		// Views are optional — some DuckLake versions may not support them
		views = nil
	}
	return &IntrospectResult{Tables: tables, Views: views}, nil
}

// introspectTables queries DuckLake metadata tables and returns table/column definitions.
func introspectTables(ctx context.Context, pool *db.Pool, prefix string, filter *IntrospectFilter) ([]DuckLakeTable, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("ducklake: get connection failed: %w", err)
	}
	defer conn.Close()

	metaCatalog := metaCatalogIdent(prefix)

	// Query tables with their columns using DuckLake metadata tables.
	// DuckLake metadata tables use begin_snapshot/end_snapshot for temporal validity.
	// Active objects have end_snapshot IS NULL.
	query := fmt.Sprintf(`
		SELECT
			s.schema_name,
			t.table_name,
			c.column_name,
			c.column_type,
			c.nulls_allowed
		FROM %[1]s.ducklake_table t
		JOIN %[1]s.ducklake_schema s ON t.schema_id = s.schema_id
			AND s.end_snapshot IS NULL
		JOIN %[1]s.ducklake_column c ON c.table_id = t.table_id
			AND c.end_snapshot IS NULL
		WHERE t.end_snapshot IS NULL
		ORDER BY s.schema_name, t.table_name, c.column_id
	`, metaCatalog)

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ducklake: introspect query failed: %w", err)
	}
	defer rows.Close()

	type tableKey struct {
		schema, table string
	}
	tableMap := make(map[tableKey]*DuckLakeTable)
	var tableOrder []tableKey

	for rows.Next() {
		var schemaName, tableName, colName, colType string
		var isNullable bool
		if err := rows.Scan(&schemaName, &tableName, &colName, &colType, &isNullable); err != nil {
			return nil, fmt.Errorf("ducklake: introspect scan failed: %w", err)
		}

		// Apply schema/table filter
		if !filter.Match(schemaName, tableName) {
			continue
		}

		key := tableKey{schemaName, tableName}
		tbl, ok := tableMap[key]
		if !ok {
			tbl = &DuckLakeTable{
				SchemaName: schemaName,
				TableName:  tableName,
			}
			tableMap[key] = tbl
			tableOrder = append(tableOrder, key)
		}

		tbl.Columns = append(tbl.Columns, DuckLakeColumn{
			Name:       colName,
			Type:       colType,
			IsNullable: isNullable,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake: introspect rows error: %w", err)
	}

	// Try to get primary key info from DuckLake constraints
	pkQuery := fmt.Sprintf(`
		SELECT
			s.schema_name,
			t.table_name,
			tc.column_names
		FROM %[1]s.ducklake_table_constraint tc
		JOIN %[1]s.ducklake_table t ON tc.table_id = t.table_id
			AND t.end_snapshot IS NULL
		JOIN %[1]s.ducklake_schema s ON t.schema_id = s.schema_id
			AND s.end_snapshot IS NULL
		WHERE tc.end_snapshot IS NULL
			AND tc.constraint_type = 'PRIMARY KEY'
	`, metaCatalog)

	pkRows, err := conn.Query(ctx, pkQuery)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var schemaName, tableName string
			var columnNames []string
			if err := pkRows.Scan(&schemaName, &tableName, &columnNames); err != nil {
				// Skip PK info on scan errors
				break
			}
			key := tableKey{schemaName, tableName}
			tbl, ok := tableMap[key]
			if !ok {
				continue
			}
			pkCols := make(map[string]bool, len(columnNames))
			for _, cn := range columnNames {
				pkCols[cn] = true
			}
			for i := range tbl.Columns {
				if pkCols[tbl.Columns[i].Name] {
					tbl.Columns[i].IsPK = true
				}
			}
		}
	}
	// PK info is optional — some DuckLake versions may not have constraints table

	tables := make([]DuckLakeTable, 0, len(tableOrder))
	for _, key := range tableOrder {
		tables = append(tables, *tableMap[key])
	}

	return tables, nil
}

// SchemaVersion returns the current schema version from DuckLake metadata.
func SchemaVersion(ctx context.Context, pool *db.Pool, prefix string) (int, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("ducklake: get connection failed: %w", err)
	}
	defer conn.Close()

	metaCatalog := metaCatalogIdent(prefix)
	query := fmt.Sprintf("SELECT COALESCE(MAX(schema_version), 0) FROM %s.ducklake_schema_versions", metaCatalog)

	var version int
	err = conn.QueryRow(ctx, query).Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("ducklake: schema version query failed: %w", err)
	}
	return version, nil
}

// GenerateSchemaDocument generates a GraphQL AST schema document from introspected tables.
func GenerateSchemaDocument(tables []DuckLakeTable) *ast.SchemaDocument {
	return GenerateSchemaDocumentFull(tables, nil)
}

// GenerateSchemaDocumentFull generates a GraphQL AST schema document from tables and views.
func GenerateSchemaDocumentFull(tables []DuckLakeTable, views []DuckLakeView) *ast.SchemaDocument {
	doc := &ast.SchemaDocument{}

	for _, tbl := range tables {
		def := tableToDefinition(tbl)
		if def != nil {
			doc.Definitions = append(doc.Definitions, def)
		}
	}

	for _, vw := range views {
		def := viewToDefinition(vw)
		if def != nil {
			doc.Definitions = append(doc.Definitions, def)
		}
	}

	return doc
}

func tableToDefinition(tbl DuckLakeTable) *ast.Definition {
	if len(tbl.Columns) == 0 {
		return nil
	}

	tableSQLName := dataObjectName(tbl.SchemaName, tbl.TableName)
	hasModule := hasSchemaModule(tbl.SchemaName)
	typeName := identGraphQL(rawObjectName(tbl.SchemaName, tbl.TableName))

	def := &ast.Definition{
		Name:     typeName,
		Kind:     ast.Object,
		Position: base.CompiledPos("ducklake-self-described"),
	}

	// @table directive
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "table",
		Arguments: ast.ArgumentList{
			&ast.Argument{
				Name:     "name",
				Value:    &ast.Value{Raw: tableSQLName, Kind: ast.StringValue, Position: base.CompiledPos("ducklake-table")},
				Position: base.CompiledPos("ducklake-table"),
			},
		},
		Position: base.CompiledPos("ducklake-table"),
	})

	// @module directive for non-default schemas
	if hasModule {
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "module",
			Arguments: ast.ArgumentList{
				&ast.Argument{
					Name:     "name",
					Value:    &ast.Value{Raw: identGraphQL(tbl.SchemaName), Kind: ast.StringValue, Position: base.CompiledPos("ducklake-module")},
					Position: base.CompiledPos("ducklake-module"),
				},
			},
			Position: base.CompiledPos("ducklake-module"),
		})
	}

	for _, col := range tbl.Columns {
		fd := columnToFieldDef(col)
		if fd != nil {
			def.Fields = append(def.Fields, fd)
		}
	}

	if len(def.Fields) == 0 {
		return nil
	}

	return def
}

func viewToDefinition(vw DuckLakeView) *ast.Definition {
	if len(vw.Columns) == 0 {
		return nil
	}

	viewSQLName := dataObjectName(vw.SchemaName, vw.ViewName)
	hasModule := hasSchemaModule(vw.SchemaName)
	typeName := identGraphQL(rawObjectName(vw.SchemaName, vw.ViewName))

	def := &ast.Definition{
		Name:     typeName,
		Kind:     ast.Object,
		Position: base.CompiledPos("ducklake-self-described"),
	}

	// @view directive (read-only, references the existing DuckLake view)
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "view",
		Arguments: ast.ArgumentList{
			&ast.Argument{
				Name:     "name",
				Value:    &ast.Value{Raw: viewSQLName, Kind: ast.StringValue, Position: base.CompiledPos("ducklake-view")},
				Position: base.CompiledPos("ducklake-view"),
			},
		},
		Position: base.CompiledPos("ducklake-view"),
	})

	// @module directive for non-default schemas
	if hasModule {
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "module",
			Arguments: ast.ArgumentList{
				&ast.Argument{
					Name:     "name",
					Value:    &ast.Value{Raw: identGraphQL(vw.SchemaName), Kind: ast.StringValue, Position: base.CompiledPos("ducklake-module")},
					Position: base.CompiledPos("ducklake-module"),
				},
			},
			Position: base.CompiledPos("ducklake-module"),
		})
	}

	for _, col := range vw.Columns {
		fd := columnToFieldDef(col)
		if fd != nil {
			def.Fields = append(def.Fields, fd)
		}
	}

	if len(def.Fields) == 0 {
		return nil
	}

	return def
}

func columnToFieldDef(col DuckLakeColumn) *ast.FieldDefinition {
	gqlType := duckDBTypeToGraphQL(col.Type)
	if gqlType == nil {
		return nil
	}

	if !col.IsNullable {
		gqlType.NonNull = true
	}

	fd := &ast.FieldDefinition{
		Name:     identGraphQL(col.Name),
		Type:     gqlType,
		Position: base.CompiledPos("ducklake-self-described"),
	}

	// Add @field_source if name was sanitized
	if fd.Name != col.Name {
		fd.Directives = append(fd.Directives, base.FieldSourceDirective(col.Name))
	}

	// Add @pk directive
	if col.IsPK {
		fd.Directives = append(fd.Directives, &ast.Directive{
			Name:     "pk",
			Position: base.CompiledPos("ducklake-pk"),
		})
	}

	return fd
}

// duckDBTypeToGraphQL maps DuckDB column type strings to GraphQL types.
func duckDBTypeToGraphQL(duckType string) *ast.Type {
	pos := base.CompiledPos("ducklake-type")

	// Handle array types like "INTEGER[]"
	if strings.HasSuffix(duckType, "[]") {
		elemType := duckDBTypeToGraphQL(strings.TrimSuffix(duckType, "[]"))
		if elemType == nil {
			return nil
		}
		return ast.ListType(elemType, pos)
	}

	// Handle parameterized types like "DECIMAL(10,2)", "VARCHAR(255)"
	upper := strings.ToUpper(duckType)
	if idx := strings.Index(upper, "("); idx >= 0 {
		upper = strings.TrimSpace(upper[:idx])
	}

	switch upper {
	case "INT", "INTEGER", "INT4", "SIGNED", "INT32":
		return ast.NamedType("Int", pos)
	// Note: INT8 maps to Int (8-bit TINYINT) in DuckLake metadata, not BigInt.
	// DuckLake uses physical type names: int8=TINYINT, int16=SMALLINT, int32=INTEGER, int64=BIGINT.
	case "BIGINT", "LONG", "HUGEINT", "UINTEGER", "UBIGINT", "UHUGEINT", "INT64":
		return ast.NamedType("BigInt", pos)
	case "SMALLINT", "INT2", "SHORT", "TINYINT", "INT8", "UTINYINT", "USMALLINT", "INT16":
		return ast.NamedType("Int", pos)
	case "FLOAT", "REAL", "FLOAT4", "DOUBLE", "FLOAT8", "DECIMAL", "NUMERIC", "FLOAT32", "FLOAT64":
		return ast.NamedType("Float", pos)
	case "BOOLEAN", "BOOL", "LOGICAL":
		return ast.NamedType("Boolean", pos)
	case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR":
		return ast.NamedType("String", pos)
	case "UUID":
		return ast.NamedType("String", pos)
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		return ast.NamedType("String", pos)
	case "BIT", "BITSTRING":
		return ast.NamedType("String", pos)
	case "DATE":
		return ast.NamedType("Date", pos)
	case "TIME", "TIMETZ", "TIME WITH TIME ZONE":
		return ast.NamedType("Time", pos)
	case "TIMESTAMP", "DATETIME", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE",
		"TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS":
		return ast.NamedType("Timestamp", pos)
	case "INTERVAL":
		return ast.NamedType("Interval", pos)
	case "JSON":
		return ast.NamedType("JSON", pos)
	case "GEOMETRY", "GEOGRAPHY", "WKB_BLOB":
		return ast.NamedType("Geometry", pos)
	case "ENUM":
		return ast.NamedType("String", pos)
	case "STRUCT", "MAP", "UNION":
		// Complex types — expose as JSON
		return ast.NamedType("JSON", pos)
	default:
		return nil
	}
}

// SchemaChanges describes tables that changed between two schema versions.
type SchemaChanges struct {
	// Added contains tables that were created after fromSnapshot.
	Added []DuckLakeTable
	// Dropped contains tables (name only) that were dropped after fromSnapshot.
	Dropped []DuckLakeTable
	// Modified contains tables whose columns changed after fromSnapshot.
	// Each entry has the full current column set for the table.
	Modified []DuckLakeTable
}

// IntrospectChanges queries DuckLake metadata for schema changes between
// fromSnapshot and the current state. Only touches tables/columns with
// begin_snapshot > fromSnapshot or end_snapshot > fromSnapshot.
// This is O(changed) instead of O(all) compared to full IntrospectSchema + DiffSchemas.
func IntrospectChanges(ctx context.Context, pool *db.Pool, prefix string, fromSnapshot int) (*SchemaChanges, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("ducklake: get connection failed: %w", err)
	}
	defer conn.Close()

	metaCatalog := metaCatalogIdent(prefix)
	changes := &SchemaChanges{}

	// 1. Find table IDs that were touched since fromSnapshot:
	//    - table created:  begin_snapshot > fromSnapshot AND end_snapshot IS NULL
	//    - table dropped:  end_snapshot > fromSnapshot AND end_snapshot IS NOT NULL
	//    - table modified: column begin_snapshot > fromSnapshot OR column end_snapshot > fromSnapshot
	//      (but table itself still active: t.end_snapshot IS NULL)

	// Dropped tables (active before, dropped after fromSnapshot)
	droppedQuery := fmt.Sprintf(`
		SELECT s.schema_name, t.table_name
		FROM %[1]s.ducklake_table t
		JOIN %[1]s.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE t.end_snapshot IS NOT NULL
			AND t.end_snapshot > %[2]d
			AND t.begin_snapshot <= %[2]d
	`, metaCatalog, fromSnapshot)

	droppedRows, err := conn.Query(ctx, droppedQuery)
	if err != nil {
		return nil, fmt.Errorf("ducklake: query dropped tables: %w", err)
	}
	defer droppedRows.Close()
	for droppedRows.Next() {
		var schema, table string
		if err := droppedRows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("ducklake: scan dropped table: %w", err)
		}
		changes.Dropped = append(changes.Dropped, DuckLakeTable{SchemaName: schema, TableName: table})
	}
	if err := droppedRows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake: dropped tables rows error: %w", err)
	}

	// Added tables (created after fromSnapshot, still active)
	addedQuery := fmt.Sprintf(`
		SELECT s.schema_name, t.table_name, t.table_id
		FROM %[1]s.ducklake_table t
		JOIN %[1]s.ducklake_schema s ON t.schema_id = s.schema_id
			AND s.end_snapshot IS NULL
		WHERE t.end_snapshot IS NULL
			AND t.begin_snapshot > %[2]d
	`, metaCatalog, fromSnapshot)

	addedRows, err := conn.Query(ctx, addedQuery)
	if err != nil {
		return nil, fmt.Errorf("ducklake: query added tables: %w", err)
	}
	defer addedRows.Close()

	var addedTableIDs []int
	type addedInfo struct {
		schema, table string
	}
	addedMap := make(map[int]addedInfo)

	for addedRows.Next() {
		var schema, table string
		var tableID int
		if err := addedRows.Scan(&schema, &table, &tableID); err != nil {
			return nil, fmt.Errorf("ducklake: scan added table: %w", err)
		}
		addedTableIDs = append(addedTableIDs, tableID)
		addedMap[tableID] = addedInfo{schema, table}
	}
	if err := addedRows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake: added tables rows error: %w", err)
	}

	// Modified tables: existing tables (begin_snapshot <= fromSnapshot, end_snapshot IS NULL)
	// that have column changes after fromSnapshot
	modifiedQuery := fmt.Sprintf(`
		SELECT DISTINCT t.table_id, s.schema_name, t.table_name
		FROM %[1]s.ducklake_table t
		JOIN %[1]s.ducklake_schema s ON t.schema_id = s.schema_id
			AND s.end_snapshot IS NULL
		JOIN %[1]s.ducklake_column c ON c.table_id = t.table_id
		WHERE t.end_snapshot IS NULL
			AND t.begin_snapshot <= %[2]d
			AND (c.begin_snapshot > %[2]d OR (c.end_snapshot IS NOT NULL AND c.end_snapshot > %[2]d))
	`, metaCatalog, fromSnapshot)

	modifiedRows, err := conn.Query(ctx, modifiedQuery)
	if err != nil {
		return nil, fmt.Errorf("ducklake: query modified tables: %w", err)
	}
	defer modifiedRows.Close()

	var modifiedTableIDs []int
	modifiedMap := make(map[int]addedInfo)

	for modifiedRows.Next() {
		var tableID int
		var schema, table string
		if err := modifiedRows.Scan(&tableID, &schema, &table); err != nil {
			return nil, fmt.Errorf("ducklake: scan modified table: %w", err)
		}
		modifiedTableIDs = append(modifiedTableIDs, tableID)
		modifiedMap[tableID] = addedInfo{schema, table}
	}
	if err := modifiedRows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake: modified tables rows error: %w", err)
	}

	// Fetch current columns for added + modified tables
	allIDs := append(addedTableIDs, modifiedTableIDs...)
	if len(allIDs) > 0 {
		idList := intListSQL(allIDs)
		colQuery := fmt.Sprintf(`
			SELECT c.table_id, c.column_name, c.column_type, c.nulls_allowed
			FROM %s.ducklake_column c
			WHERE c.table_id IN (%s)
				AND c.end_snapshot IS NULL
			ORDER BY c.table_id, c.column_id
		`, metaCatalog, idList)

		colRows, err := conn.Query(ctx, colQuery)
		if err != nil {
			return nil, fmt.Errorf("ducklake: query columns for changed tables: %w", err)
		}
		defer colRows.Close()

		tableColumns := make(map[int][]DuckLakeColumn)
		for colRows.Next() {
			var tableID int
			var colName, colType string
			var isNullable bool
			if err := colRows.Scan(&tableID, &colName, &colType, &isNullable); err != nil {
				return nil, fmt.Errorf("ducklake: scan column: %w", err)
			}
			tableColumns[tableID] = append(tableColumns[tableID], DuckLakeColumn{
				Name:       colName,
				Type:       colType,
				IsNullable: isNullable,
			})
		}
		if err := colRows.Err(); err != nil {
			return nil, fmt.Errorf("ducklake: columns rows error: %w", err)
		}

		// Try to get PK info for changed tables
		pkQuery := fmt.Sprintf(`
			SELECT tc.table_id, tc.column_names
			FROM %s.ducklake_table_constraint tc
			WHERE tc.table_id IN (%s)
				AND tc.end_snapshot IS NULL
				AND tc.constraint_type = 'PRIMARY KEY'
		`, metaCatalog, idList)

		pkRows, err := conn.Query(ctx, pkQuery)
		if err == nil {
			defer pkRows.Close()
			for pkRows.Next() {
				var tableID int
				var columnNames []string
				if err := pkRows.Scan(&tableID, &columnNames); err != nil {
					break
				}
				pkCols := make(map[string]bool, len(columnNames))
				for _, cn := range columnNames {
					pkCols[cn] = true
				}
				for i := range tableColumns[tableID] {
					if pkCols[tableColumns[tableID][i].Name] {
						tableColumns[tableID][i].IsPK = true
					}
				}
			}
		}

		// Build added tables
		for _, id := range addedTableIDs {
			info := addedMap[id]
			changes.Added = append(changes.Added, DuckLakeTable{
				SchemaName: info.schema,
				TableName:  info.table,
				Columns:    tableColumns[id],
			})
		}

		// Build modified tables
		for _, id := range modifiedTableIDs {
			info := modifiedMap[id]
			changes.Modified = append(changes.Modified, DuckLakeTable{
				SchemaName: info.schema,
				TableName:  info.table,
				Columns:    tableColumns[id],
			})
		}
	}

	return changes, nil
}

// introspectViews queries DuckLake metadata for view definitions and resolves their columns
// via DuckDB's DESCRIBE mechanism.
func introspectViews(ctx context.Context, pool *db.Pool, prefix string, filter *IntrospectFilter) ([]DuckLakeView, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("ducklake: get connection failed: %w", err)
	}
	defer conn.Close()

	metaCatalog := metaCatalogIdent(prefix)

	// Get active views from DuckLake metadata
	viewQuery := fmt.Sprintf(`
		SELECT s.schema_name, v.view_name
		FROM %[1]s.ducklake_view v
		JOIN %[1]s.ducklake_schema s ON v.schema_id = s.schema_id
			AND s.end_snapshot IS NULL
		WHERE v.end_snapshot IS NULL
		ORDER BY s.schema_name, v.view_name
	`, metaCatalog)

	viewRows, err := conn.Query(ctx, viewQuery)
	if err != nil {
		return nil, fmt.Errorf("ducklake: query views: %w", err)
	}
	defer viewRows.Close()

	type viewInfo struct {
		schema, name string
	}
	var viewInfos []viewInfo

	for viewRows.Next() {
		var schema, name string
		if err := viewRows.Scan(&schema, &name); err != nil {
			return nil, fmt.Errorf("ducklake: scan view: %w", err)
		}
		if !filter.Match(schema, name) {
			continue
		}
		viewInfos = append(viewInfos, viewInfo{schema, name})
	}
	if err := viewRows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake: view rows error: %w", err)
	}

	// Resolve columns for each view using DESCRIBE
	var views []DuckLakeView
	for _, vi := range viewInfos {
		cols, err := describeViewColumns(ctx, conn, prefix, vi.schema, vi.name)
		if err != nil {
			// Skip views whose columns cannot be resolved
			continue
		}
		if len(cols) == 0 {
			continue
		}
		views = append(views, DuckLakeView{
			SchemaName: vi.schema,
			ViewName:   vi.name,
			Columns:    cols,
		})
	}

	return views, nil
}

// describeViewColumns resolves column names and types for a DuckLake view.
func describeViewColumns(ctx context.Context, conn *db.Connection, prefix, schema, viewName string) ([]DuckLakeColumn, error) {
	qualifiedName := fmt.Sprintf("%s.%s.%s",
		fmt.Sprintf(`"%s"`, prefix),
		fmt.Sprintf(`"%s"`, schema),
		fmt.Sprintf(`"%s"`, viewName),
	)
	descQuery := fmt.Sprintf("DESCRIBE SELECT * FROM %s", qualifiedName)

	rows, err := conn.Query(ctx, descQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []DuckLakeColumn
	for rows.Next() {
		var colName, colType, isNull string
		var key, defaultVal, extra sql.NullString
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultVal, &extra); err != nil {
			return nil, err
		}
		gqlType := duckDBTypeToGraphQL(colType)
		if gqlType == nil {
			continue
		}
		cols = append(cols, DuckLakeColumn{
			Name:       colName,
			Type:       colType,
			IsNullable: isNull == "YES",
		})
	}
	return cols, rows.Err()
}

func intListSQL(ids []int) string {
	var sb strings.Builder
	for i, id := range ids {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%d", id)
	}
	return sb.String()
}

// ContentHash computes a stable hash of the introspected schema for version tracking.
func ContentHash(tables []DuckLakeTable) string {
	return ContentHashFull(tables, nil)
}

// ContentHashFull computes a stable hash including both tables and views.
func ContentHashFull(tables []DuckLakeTable, views []DuckLakeView) string {
	h := sha256.New()

	sorted := make([]DuckLakeTable, len(tables))
	copy(sorted, tables)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].SchemaName != sorted[j].SchemaName {
			return sorted[i].SchemaName < sorted[j].SchemaName
		}
		return sorted[i].TableName < sorted[j].TableName
	})

	for _, tbl := range sorted {
		fmt.Fprintf(h, "table:%s.%s\n", tbl.SchemaName, tbl.TableName)
		for _, col := range tbl.Columns {
			fmt.Fprintf(h, "col:%s:%s:%v:%v\n", col.Name, col.Type, col.IsNullable, col.IsPK)
		}
	}

	sortedViews := make([]DuckLakeView, len(views))
	copy(sortedViews, views)
	sort.Slice(sortedViews, func(i, j int) bool {
		if sortedViews[i].SchemaName != sortedViews[j].SchemaName {
			return sortedViews[i].SchemaName < sortedViews[j].SchemaName
		}
		return sortedViews[i].ViewName < sortedViews[j].ViewName
	})

	for _, vw := range sortedViews {
		fmt.Fprintf(h, "view:%s.%s\n", vw.SchemaName, vw.ViewName)
		for _, col := range vw.Columns {
			fmt.Fprintf(h, "col:%s:%s:%v:%v\n", col.Name, col.Type, col.IsNullable, col.IsPK)
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// identGraphQL makes valid GraphQL identifier from a name.
func identGraphQL(name string) string {
	name = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '_' {
			return r
		}
		return '_'
	}, name)
	if len(name) > 0 && name[0] == '_' {
		name = "dl_" + name[1:]
	}
	return name
}

// skipSchemaModules lists schemas that should NOT create a GraphQL module.
// DuckLake uses "main" as the default schema (DuckDB convention).
// See also: pkg/catalog/sources/db_info.go which adds "public" for PostgreSQL sources.
var skipSchemaModules = map[string]bool{
	"main": true,
}

// hasSchemaModule returns true if the schema creates a GraphQL module.
func hasSchemaModule(schema string) bool {
	if schema == "" {
		return false
	}
	_, skip := skipSchemaModules[schema]
	return !skip
}

// rawObjectName returns the unquoted schema.name for GraphQL type name generation.
func rawObjectName(schema, name string) string {
	if schema == "" {
		return name
	}
	if _, ok := skipSchemaModules[schema]; ok {
		return name
	}
	return schema + "." + name
}

// dataObjectName returns the SQL-quoted schema.name for @table/@view directives.
func dataObjectName(schema, name string) string {
	if schema == "" {
		return engines.Ident(name)
	}
	if _, ok := skipSchemaModules[schema]; ok {
		return engines.Ident(name)
	}
	return engines.Ident(schema) + "." + engines.Ident(name)
}

