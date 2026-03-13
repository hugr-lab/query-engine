package ducklake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

func (s *Source) registerUDF(ctx context.Context) error {
	// --- Mutation functions ---

	if err := s.registerExpireSnapshots(ctx); err != nil {
		return err
	}
	if err := s.registerMergeAdjacentFiles(ctx); err != nil {
		return err
	}
	if err := s.registerRewriteDataFiles(ctx); err != nil {
		return err
	}
	if err := s.registerCleanupOldFiles(ctx); err != nil {
		return err
	}
	if err := s.registerSimpleCall(ctx, "hugr_ducklake_flush_inlined_data", "CALL ducklake_flush_inlined_data(%s)", "Inlined data flushed"); err != nil {
		return err
	}
	if err := s.registerSimpleCall(ctx, "hugr_ducklake_checkpoint", "CHECKPOINT %s", "Checkpoint completed"); err != nil {
		return err
	}
	if err := s.registerSetOption(ctx); err != nil {
		return err
	}

	// --- DDL functions ---

	if err := s.registerDDL2(ctx, "hugr_ducklake_create_schema",
		func(name, arg string) string {
			return fmt.Sprintf("CREATE SCHEMA %s.%s", engines.Ident(name), engines.Ident(arg))
		}, "Schema created"); err != nil {
		return err
	}
	if err := s.registerDDL2(ctx, "hugr_ducklake_drop_schema",
		func(name, arg string) string {
			return fmt.Sprintf("DROP SCHEMA %s.%s", engines.Ident(name), engines.Ident(arg))
		}, "Schema dropped"); err != nil {
		return err
	}
	if err := s.registerCreateTable(ctx); err != nil {
		return err
	}
	if err := s.registerDDL3(ctx, "hugr_ducklake_drop_table",
		func(name, tableName, schemaName string) string {
			return fmt.Sprintf("DROP TABLE %s", qualifiedTable(name, schemaName, tableName))
		}, "Table dropped"); err != nil {
		return err
	}
	if err := s.registerAddColumn(ctx); err != nil {
		return err
	}
	if err := s.registerDDL4(ctx, "hugr_ducklake_drop_column",
		func(name, tableName, colName, schemaName string) string {
			return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
				qualifiedTable(name, schemaName, tableName), engines.Ident(colName))
		}, "Column dropped"); err != nil {
		return err
	}
	if err := s.registerRenameColumn(ctx); err != nil {
		return err
	}
	if err := s.registerRenameTable(ctx); err != nil {
		return err
	}
	// --- Bridge functions ---

	if err := s.registerIcebergToDucklake(ctx); err != nil {
		return err
	}

	// --- Query functions ---

	if err := s.registerInfo(ctx); err != nil {
		return err
	}
	if err := s.registerCurrentSnapshot(ctx); err != nil {
		return err
	}

	return nil
}

// qualifiedTable builds a fully qualified table identifier: "catalog"."schema"."table"
// If schema is empty, defaults to "main".
func qualifiedTable(catalog, schema, table string) string {
	if schema == "" {
		schema = "main"
	}
	return fmt.Sprintf("%s.%s.%s", engines.Ident(catalog), engines.Ident(schema), engines.Ident(table))
}

// registerSimpleCall registers a mutation UDF that takes a single name argument
// and executes a DuckLake management SQL command.
func (s *Source) registerSimpleCall(ctx context.Context, name, sqlTmpl, successMsg string) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: name,
		Execute: func(ctx context.Context, catalogName string) (*types.OperationResult, error) {
			sql := fmt.Sprintf(sqlTmpl, engines.Ident(catalogName))
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(successMsg, 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			return args[0].(string), nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: db.DuckDBOperationResult(),
	})
}

type expireArgs struct {
	name      string
	versions  string
	olderThan string
	dryRun    bool
}

func (s *Source) registerExpireSnapshots(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[expireArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_expire_snapshots",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args expireArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("CALL ducklake_expire_snapshots(%s", engines.Ident(args.name))
			if args.versions != "" {
				sql += fmt.Sprintf(", versions := %s", args.versions)
			}
			if args.olderThan != "" {
				sql += fmt.Sprintf(", older_than := '%s'::TIMESTAMP WITH TIME ZONE", escapeSQLString(args.olderThan))
			}
			if args.dryRun {
				sql += ", dry_run := true"
			}
			sql += ")"
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Snapshots expired", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (expireArgs, error) {
			if len(args) != 4 {
				return expireArgs{}, fmt.Errorf("invalid number of arguments: expected 4, got %d", len(args))
			}
			ea := expireArgs{
				name: args[0].(string),
			}
			if args[1] != nil {
				ea.versions = args[1].(string)
			}
			if args[2] != nil {
				ea.olderThan = args[2].(string)
			}
			if args[3] != nil {
				ea.dryRun = args[3].(bool)
			}
			return ea, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// mergeArgs for ducklake_merge_adjacent_files(catalog, [table_name], [schema => ...])
type mergeArgs struct {
	name       string
	tableName  string
	schemaName string
}

func (s *Source) registerMergeAdjacentFiles(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[mergeArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_merge_adjacent_files",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args mergeArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("CALL ducklake_merge_adjacent_files(%s", engines.Ident(args.name))
			if args.tableName != "" {
				sql += fmt.Sprintf(", '%s'", escapeSQLString(args.tableName))
				if args.schemaName != "" {
					sql += fmt.Sprintf(", schema => '%s'", escapeSQLString(args.schemaName))
				}
			}
			sql += ")"
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Files merged", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (mergeArgs, error) {
			if len(args) != 3 {
				return mergeArgs{}, fmt.Errorf("invalid number of arguments: expected 3, got %d", len(args))
			}
			ma := mergeArgs{
				name: args[0].(string),
			}
			if args[1] != nil {
				ma.tableName = args[1].(string)
			}
			if args[2] != nil {
				ma.schemaName = args[2].(string)
			}
			return ma, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// rewriteArgs for ducklake_rewrite_data_files(catalog, [table_name], [schema => ...], [delete_threshold => ...])
type rewriteArgs struct {
	name            string
	tableName       string
	schemaName      string
	deleteThreshold float64
}

func (s *Source) registerRewriteDataFiles(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[rewriteArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_rewrite_data_files",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args rewriteArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("CALL ducklake_rewrite_data_files(%s", engines.Ident(args.name))
			if args.tableName != "" {
				sql += fmt.Sprintf(", '%s'", escapeSQLString(args.tableName))
				if args.schemaName != "" {
					sql += fmt.Sprintf(", schema => '%s'", escapeSQLString(args.schemaName))
				}
			}
			if args.deleteThreshold > 0 {
				sql += fmt.Sprintf(", delete_threshold => %g", args.deleteThreshold)
			}
			sql += ")"
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Files rewritten", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (rewriteArgs, error) {
			if len(args) != 4 {
				return rewriteArgs{}, fmt.Errorf("invalid number of arguments: expected 4, got %d", len(args))
			}
			ra := rewriteArgs{
				name: args[0].(string),
			}
			if args[1] != nil {
				ra.tableName = args[1].(string)
			}
			if args[2] != nil {
				ra.schemaName = args[2].(string)
			}
			if args[3] != nil {
				ra.deleteThreshold = args[3].(float64)
			}
			return ra, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("DOUBLE"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type cleanupArgs struct {
	name       string
	olderThan  string
	cleanupAll bool
	dryRun     bool
}

func (s *Source) registerCleanupOldFiles(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[cleanupArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_cleanup_old_files",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args cleanupArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("CALL ducklake_cleanup_old_files(%s", engines.Ident(args.name))
			if args.olderThan != "" {
				sql += fmt.Sprintf(", older_than := '%s'::TIMESTAMP WITH TIME ZONE", escapeSQLString(args.olderThan))
			}
			if args.cleanupAll {
				sql += ", cleanup_all := true"
			}
			if args.dryRun {
				sql += ", dry_run := true"
			}
			sql += ")"
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Files cleaned up", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (cleanupArgs, error) {
			if len(args) != 4 {
				return cleanupArgs{}, fmt.Errorf("invalid number of arguments: expected 4, got %d", len(args))
			}
			ca := cleanupArgs{
				name: args[0].(string),
			}
			if args[1] != nil {
				ca.olderThan = args[1].(string)
			}
			if args[2] != nil {
				ca.cleanupAll = args[2].(bool)
			}
			if args[3] != nil {
				ca.dryRun = args[3].(bool)
			}
			return ca, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type setOptionArgs struct {
	name       string
	option     string
	value      string
	schemaName string
	tableName  string
}

// registerSetOption uses catalog-scoped syntax: CALL "catalog".set_option(...)
func (s *Source) registerSetOption(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[setOptionArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_set_option",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args setOptionArgs) (*types.OperationResult, error) {
			// DuckLake set_option is catalog-scoped: CALL "catalog".set_option('key', 'value', ...)
			sql := fmt.Sprintf("CALL %s.set_option('%s', '%s'",
				engines.Ident(args.name),
				escapeSQLString(args.option),
				escapeSQLString(args.value),
			)
			if args.schemaName != "" {
				sql += fmt.Sprintf(", schema => '%s'", escapeSQLString(args.schemaName))
			}
			if args.tableName != "" {
				sql += fmt.Sprintf(", table_name => '%s'", escapeSQLString(args.tableName))
			}
			sql += ")"
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Option set", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (setOptionArgs, error) {
			if len(args) != 5 {
				return setOptionArgs{}, fmt.Errorf("invalid number of arguments: expected 5, got %d", len(args))
			}
			sa := setOptionArgs{
				name:   args[0].(string),
				option: args[1].(string),
				value:  args[2].(string),
			}
			if args[3] != nil {
				sa.schemaName = args[3].(string)
			}
			if args[4] != nil {
				sa.tableName = args[4].(string)
			}
			return sa, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// --- Query functions ---

type duckLakeInfo struct {
	Name            string `json:"name"`
	SnapshotCount   int64  `json:"snapshot_count"`
	CurrentSnapshot int64  `json:"current_snapshot"`
	TableCount      int64  `json:"table_count"`
	SchemaCount     int64  `json:"schema_count"`
	ViewCount       int64  `json:"view_count"`
	SchemaVersion   int64  `json:"schema_version"`
	MetadataBackend string `json:"metadata_backend"`
	DuckLakeVersion string `json:"ducklake_version"`
	DataPath        string `json:"data_path"`
	CreatedAt       string `json:"created_at"`
	LastModifiedAt  string `json:"last_modified_at"`
}

var duckDBDuckLakeInfoType = runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
	"name":             duckdb.TYPE_VARCHAR,
	"snapshot_count":   duckdb.TYPE_BIGINT,
	"current_snapshot": duckdb.TYPE_BIGINT,
	"table_count":      duckdb.TYPE_BIGINT,
	"schema_count":     duckdb.TYPE_BIGINT,
	"view_count":       duckdb.TYPE_BIGINT,
	"schema_version":   duckdb.TYPE_BIGINT,
	"metadata_backend": duckdb.TYPE_VARCHAR,
	"ducklake_version": duckdb.TYPE_VARCHAR,
	"data_path":        duckdb.TYPE_VARCHAR,
	"created_at":       duckdb.TYPE_VARCHAR,
	"last_modified_at": duckdb.TYPE_VARCHAR,
})

func (s *Source) registerInfo(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *duckLakeInfo]{
		Name: "hugr_ducklake_info",
		Execute: func(ctx context.Context, catalogName string) (*duckLakeInfo, error) {
			conn, err := s.db.Conn(ctx)
			if err != nil {
				return nil, fmt.Errorf("ducklake_info: connection failed: %w", err)
			}
			defer conn.Close()

			metaCatalog := metaCatalogIdent(catalogName)
			metaCatalogStr := "__ducklake_metadata_" + catalogName

			info := &duckLakeInfo{Name: catalogName}

			query := fmt.Sprintf(`
				SELECT
					(SELECT COUNT(*) FROM %[1]s.ducklake_snapshot),
					(SELECT COALESCE(MAX(snapshot_id), 0) FROM %[1]s.ducklake_snapshot),
					(SELECT COUNT(*) FROM %[1]s.ducklake_table WHERE end_snapshot IS NULL),
					(SELECT COUNT(*) FROM %[1]s.ducklake_schema WHERE end_snapshot IS NULL),
					(SELECT COALESCE(MAX(schema_version), 0) FROM %[1]s.ducklake_schema_versions),
					(SELECT database_name FROM duckdb_databases() WHERE database_name = '%[2]s'),
					(SELECT COALESCE(value, '') FROM %[1]s.ducklake_metadata WHERE key = 'version'),
					(SELECT COALESCE(value, '') FROM %[1]s.ducklake_metadata WHERE key = 'data_path'),
					(SELECT COALESCE(MIN(snapshot_time)::VARCHAR, '') FROM %[1]s.ducklake_snapshot),
					(SELECT COALESCE(MAX(snapshot_time)::VARCHAR, '') FROM %[1]s.ducklake_snapshot)
			`, metaCatalog, escapeSQLString(metaCatalogStr))

			if err := conn.QueryRow(ctx, query).Scan(
				&info.SnapshotCount,
				&info.CurrentSnapshot,
				&info.TableCount,
				&info.SchemaCount,
				&info.SchemaVersion,
				&info.MetadataBackend,
				&info.DuckLakeVersion,
				&info.DataPath,
				&info.CreatedAt,
				&info.LastModifiedAt,
			); err != nil {
				return nil, fmt.Errorf("ducklake_info: query failed: %w", err)
			}

			// View count is optional — ducklake_view may not exist in older versions
			var viewCount int64
			viewQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s.ducklake_view WHERE end_snapshot IS NULL`, metaCatalog)
			if err := conn.QueryRow(ctx, viewQuery).Scan(&viewCount); err == nil {
				info.ViewCount = viewCount
			}

			return info, nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			return args[0].(string), nil
		},
		ConvertOutput: func(out *duckLakeInfo) (any, error) {
			if out == nil {
				return nil, nil
			}
			return map[string]any{
				"name":             out.Name,
				"snapshot_count":   out.SnapshotCount,
				"current_snapshot": out.CurrentSnapshot,
				"table_count":      out.TableCount,
				"schema_count":     out.SchemaCount,
				"view_count":       out.ViewCount,
				"schema_version":   out.SchemaVersion,
				"metadata_backend": out.MetadataBackend,
				"ducklake_version": out.DuckLakeVersion,
				"data_path":        out.DataPath,
				"created_at":       out.CreatedAt,
				"last_modified_at": out.LastModifiedAt,
			}, nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: duckDBDuckLakeInfoType,
	})
}

func (s *Source) registerCurrentSnapshot(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, int32]{
		Name: "hugr_ducklake_current_snapshot",
		Execute: func(ctx context.Context, catalogName string) (int32, error) {
			conn, err := s.db.Conn(ctx)
			if err != nil {
				return 0, fmt.Errorf("ducklake_current_snapshot: connection failed: %w", err)
			}
			defer conn.Close()

			metaCatalog := metaCatalogIdent(catalogName)
			query := fmt.Sprintf("SELECT COALESCE(MAX(snapshot_id), 0) FROM %s.ducklake_snapshot", metaCatalog)

			var snapshotID int32
			if err := conn.QueryRow(ctx, query).Scan(&snapshotID); err != nil {
				return 0, fmt.Errorf("ducklake_current_snapshot: query failed: %w", err)
			}
			return snapshotID, nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			return args[0].(string), nil
		},
		ConvertOutput: func(out int32) (any, error) {
			return out, nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: runtime.DuckDBTypeInfoByNameMust("INTEGER"),
	})
}

// --- DDL UDF helpers ---

// registerDDL2 registers a DDL UDF with 2 VARCHAR args: (name, arg).
func (s *Source) registerDDL2(ctx context.Context, name string, sqlFn func(name, arg string) string, successMsg string) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[[2]string, *types.OperationResult]{
		Name: name,
		Execute: func(ctx context.Context, args [2]string) (*types.OperationResult, error) {
			sql := sqlFn(args[0], args[1])
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(successMsg, 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) ([2]string, error) {
			if len(args) != 2 {
				return [2]string{}, fmt.Errorf("expected 2 arguments, got %d", len(args))
			}
			return [2]string{args[0].(string), args[1].(string)}, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// registerDDL3 registers a DDL UDF with 3 VARCHAR args: (name, objectName, schemaName?).
func (s *Source) registerDDL3(ctx context.Context, name string, sqlFn func(name, objectName, schemaName string) string, successMsg string) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[[3]string, *types.OperationResult]{
		Name:                  name,
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args [3]string) (*types.OperationResult, error) {
			sql := sqlFn(args[0], args[1], args[2])
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(successMsg, 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) ([3]string, error) {
			if len(args) != 3 {
				return [3]string{}, fmt.Errorf("expected 3 arguments, got %d", len(args))
			}
			r := [3]string{args[0].(string), args[1].(string)}
			if args[2] != nil {
				r[2] = args[2].(string)
			}
			return r, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// registerDDL4 registers a DDL UDF with 4 VARCHAR args: (name, objectName, colName, schemaName?).
func (s *Source) registerDDL4(ctx context.Context, name string, sqlFn func(name, objectName, colName, schemaName string) string, successMsg string) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[[4]string, *types.OperationResult]{
		Name:                  name,
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args [4]string) (*types.OperationResult, error) {
			sql := sqlFn(args[0], args[1], args[2], args[3])
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(successMsg, 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) ([4]string, error) {
			if len(args) != 4 {
				return [4]string{}, fmt.Errorf("expected 4 arguments, got %d", len(args))
			}
			r := [4]string{args[0].(string), args[1].(string), args[2].(string)}
			if args[3] != nil {
				r[3] = args[3].(string)
			}
			return r, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type ddlField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// allowedColumnTypes is the set of valid column type enum values for DDL operations.
var allowedColumnTypes = map[string]bool{
	"BOOLEAN": true, "TINYINT": true, "SMALLINT": true, "INTEGER": true,
	"BIGINT": true, "HUGEINT": true, "FLOAT": true, "DOUBLE": true,
	"DECIMAL": true, "VARCHAR": true, "CHAR": true, "BLOB": true,
	"DATE": true, "TIME": true, "TIMESTAMP": true, "TIMESTAMPTZ": true,
	"INTERVAL": true, "UUID": true, "JSON": true, "GEOMETRY": true,
}

func validateColumnType(t string) error {
	if !allowedColumnTypes[strings.ToUpper(t)] {
		return fmt.Errorf("unsupported column type %q", t)
	}
	return nil
}

type createTableArgs struct {
	name       string
	tableName  string
	columns    []ddlField
	schemaName string
}

func (s *Source) registerCreateTable(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[createTableArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_create_table",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args createTableArgs) (*types.OperationResult, error) {
			if len(args.columns) == 0 {
				return types.ErrResult(fmt.Errorf("columns list is empty")), nil
			}
			var colDefs []string
			for _, col := range args.columns {
				if err := validateColumnType(col.Type); err != nil {
					return types.ErrResult(err), nil
				}
				colDefs = append(colDefs, fmt.Sprintf("%s %s", engines.Ident(col.Name), strings.ToUpper(col.Type)))
			}
			sql := fmt.Sprintf("CREATE TABLE %s (%s)",
				qualifiedTable(args.name, args.schemaName, args.tableName),
				strings.Join(colDefs, ", "),
			)
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Table created", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (createTableArgs, error) {
			if len(args) != 4 {
				return createTableArgs{}, fmt.Errorf("expected 4 arguments, got %d", len(args))
			}
			a := createTableArgs{
				name:      args[0].(string),
				tableName: args[1].(string),
			}
			// columns come as JSON-encoded VARCHAR
			columnsJSON := args[2].(string)
			if err := json.Unmarshal([]byte(columnsJSON), &a.columns); err != nil {
				return createTableArgs{}, fmt.Errorf("invalid columns JSON: %w", err)
			}
			if args[3] != nil {
				a.schemaName = args[3].(string)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type addColumnArgs struct {
	name         string
	tableName    string
	columnName   string
	columnType   string
	schemaName   string
	defaultValue string
}

func (s *Source) registerAddColumn(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[addColumnArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_add_column",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args addColumnArgs) (*types.OperationResult, error) {
			if err := validateColumnType(args.columnType); err != nil {
				return types.ErrResult(err), nil
			}
			sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				qualifiedTable(args.name, args.schemaName, args.tableName),
				engines.Ident(args.columnName),
				strings.ToUpper(args.columnType),
			)
			if args.defaultValue != "" {
				sql += fmt.Sprintf(" DEFAULT '%s'", escapeSQLString(args.defaultValue))
			}
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Column added", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (addColumnArgs, error) {
			if len(args) != 6 {
				return addColumnArgs{}, fmt.Errorf("expected 6 arguments, got %d", len(args))
			}
			a := addColumnArgs{
				name:       args[0].(string),
				tableName:  args[1].(string),
				columnName: args[2].(string),
				columnType: args[3].(string),
			}
			if args[4] != nil {
				a.schemaName = args[4].(string)
			}
			if args[5] != nil {
				a.defaultValue = args[5].(string)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type renameColumnArgs struct {
	name       string
	tableName  string
	oldName    string
	newName    string
	schemaName string
}

func (s *Source) registerRenameColumn(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[renameColumnArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_rename_column",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args renameColumnArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("ALTER TABLE %s RENAME %s TO %s",
				qualifiedTable(args.name, args.schemaName, args.tableName),
				engines.Ident(args.oldName),
				engines.Ident(args.newName),
			)
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Column renamed", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (renameColumnArgs, error) {
			if len(args) != 5 {
				return renameColumnArgs{}, fmt.Errorf("expected 5 arguments, got %d", len(args))
			}
			a := renameColumnArgs{
				name:      args[0].(string),
				tableName: args[1].(string),
				oldName:   args[2].(string),
				newName:   args[3].(string),
			}
			if args[4] != nil {
				a.schemaName = args[4].(string)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type renameTableArgs struct {
	name       string
	oldName    string
	newName    string
	schemaName string
}

func (s *Source) registerRenameTable(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[renameTableArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_rename_table",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args renameTableArgs) (*types.OperationResult, error) {
			sql := fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
				qualifiedTable(args.name, args.schemaName, args.oldName),
				engines.Ident(args.newName),
			)
			_, err := s.db.Exec(ctx, sql)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Table renamed", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (renameTableArgs, error) {
			if len(args) != 4 {
				return renameTableArgs{}, fmt.Errorf("expected 4 arguments, got %d", len(args))
			}
			a := renameTableArgs{
				name:    args[0].(string),
				oldName: args[1].(string),
				newName: args[2].(string),
			}
			if args[3] != nil {
				a.schemaName = args[3].(string)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

// metaCatalogIdent returns the properly quoted metadata catalog identifier.
// DuckLake creates an internal catalog named __ducklake_metadata_<catalog_name>.
func metaCatalogIdent(catalogName string) string {
	escaped := strings.ReplaceAll(catalogName, `"`, `""`)
	return fmt.Sprintf(`"%s"`, "__ducklake_metadata_"+escaped)
}

// escapeSQLString is a local alias for sources.EscapeSQLString.
var escapeSQLString = sources.EscapeSQLString

type icebergToDucklakeArgs struct {
	icebergCatalog  string
	ducklakeCatalog string
	skipTables      string // JSON array or empty
	clear           bool
}

func (s *Source) registerIcebergToDucklake(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[icebergToDucklakeArgs, *types.OperationResult]{
		Name:                  "hugr_ducklake_iceberg_to_ducklake",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args icebergToDucklakeArgs) (*types.OperationResult, error) {
			// If clear=true, delete DuckLake metadata tables first
			if args.clear {
				meta := metaCatalogIdent(args.ducklakeCatalog)
				// Delete order respects foreign key constraints (children first).
				// Table names match DuckLake 0.4 schema.
				clearSQL := fmt.Sprintf(`
					DELETE FROM %[1]s.ducklake_snapshot_changes;
					DELETE FROM %[1]s.ducklake_file_partition_value;
					DELETE FROM %[1]s.ducklake_partition_column;
					DELETE FROM %[1]s.ducklake_partition_info;
					DELETE FROM %[1]s.ducklake_file_column_stats;
					DELETE FROM %[1]s.ducklake_file_variant_stats;
					DELETE FROM %[1]s.ducklake_table_column_stats;
					DELETE FROM %[1]s.ducklake_table_stats;
					DELETE FROM %[1]s.ducklake_column_mapping;
					DELETE FROM %[1]s.ducklake_name_mapping;
					DELETE FROM %[1]s.ducklake_data_file;
					DELETE FROM %[1]s.ducklake_delete_file;
					DELETE FROM %[1]s.ducklake_column_tag;
					DELETE FROM %[1]s.ducklake_column;
					DELETE FROM %[1]s.ducklake_sort_expression;
					DELETE FROM %[1]s.ducklake_sort_info;
					DELETE FROM %[1]s.ducklake_inlined_data_tables;
					DELETE FROM %[1]s.ducklake_files_scheduled_for_deletion;
					DELETE FROM %[1]s.ducklake_macro_parameters;
					DELETE FROM %[1]s.ducklake_macro_impl;
					DELETE FROM %[1]s.ducklake_macro;
					DELETE FROM %[1]s.ducklake_table;
					DELETE FROM %[1]s.ducklake_view;
					DELETE FROM %[1]s.ducklake_tag;
					DELETE FROM %[1]s.ducklake_schema;
					DELETE FROM %[1]s.ducklake_snapshot;
					DELETE FROM %[1]s.ducklake_schema_versions;
				`, meta)
				_, err := s.db.Exec(ctx, clearSQL)
				if err != nil {
					return types.ErrResult(fmt.Errorf("clear DuckLake catalog: %w", err)), nil
				}
			}

			// Build iceberg_to_ducklake call
			callSQL := fmt.Sprintf("CALL iceberg_to_ducklake(%s, %s",
				engines.Ident(args.icebergCatalog),
				engines.Ident(args.ducklakeCatalog),
			)

			// Parse skip_tables if provided
			if args.skipTables != "" {
				var tables []string
				if err := json.Unmarshal([]byte(args.skipTables), &tables); err == nil && len(tables) > 0 {
					var quoted []string
					for _, t := range tables {
						quoted = append(quoted, fmt.Sprintf("'%s'", escapeSQLString(t)))
					}
					callSQL += fmt.Sprintf(", skip_tables=[%s]", strings.Join(quoted, ", "))
				}
			}

			callSQL += ")"

			_, err := s.db.Exec(ctx, callSQL)
			if err != nil {
				return types.ErrResult(fmt.Errorf("iceberg_to_ducklake: %w", err)), nil
			}
			return types.Result("Iceberg metadata imported to DuckLake", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (icebergToDucklakeArgs, error) {
			if len(args) != 4 {
				return icebergToDucklakeArgs{}, errors.New("expected 4 arguments")
			}
			a := icebergToDucklakeArgs{
				icebergCatalog:  args[0].(string),
				ducklakeCatalog: args[1].(string),
			}
			if args[2] != nil {
				a.skipTables = args[2].(string)
			}
			if args[3] != nil {
				a.clear = args[3].(bool)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) { return out.ToDuckdb(), nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}
