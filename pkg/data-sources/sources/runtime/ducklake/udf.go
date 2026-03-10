package ducklake

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
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

	// --- Query functions ---

	if err := s.registerInfo(ctx); err != nil {
		return err
	}
	if err := s.registerCurrentSnapshot(ctx); err != nil {
		return err
	}

	return nil
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
		OutputType: types.DuckDBOperationResult(),
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
		OutputType: types.DuckDBOperationResult(),
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
		OutputType: types.DuckDBOperationResult(),
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
		OutputType: types.DuckDBOperationResult(),
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
		OutputType: types.DuckDBOperationResult(),
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
		OutputType: types.DuckDBOperationResult(),
	})
}

// --- Query functions ---

type duckLakeInfo struct {
	Name            string `json:"name"`
	SnapshotCount   int64  `json:"snapshot_count"`
	CurrentSnapshot int64  `json:"current_snapshot"`
	TableCount      int64  `json:"table_count"`
	MetadataBackend string `json:"metadata_backend"`
}

var duckDBDuckLakeInfoType = runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
	"name":             duckdb.TYPE_VARCHAR,
	"snapshot_count":   duckdb.TYPE_BIGINT,
	"current_snapshot": duckdb.TYPE_BIGINT,
	"table_count":     duckdb.TYPE_BIGINT,
	"metadata_backend": duckdb.TYPE_VARCHAR,
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
					(SELECT database_name FROM duckdb_databases() WHERE database_name = '%[2]s')
			`, metaCatalog, escapeSQLString(metaCatalogStr))

			if err := conn.QueryRow(ctx, query).Scan(
				&info.SnapshotCount,
				&info.CurrentSnapshot,
				&info.TableCount,
				&info.MetadataBackend,
			); err != nil {
				return nil, fmt.Errorf("ducklake_info: query failed: %w", err)
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
				"table_count":     out.TableCount,
				"metadata_backend": out.MetadataBackend,
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

// metaCatalogIdent returns the properly quoted metadata catalog identifier.
func metaCatalogIdent(catalogName string) string {
	return fmt.Sprintf(`"%s"`, "__ducklake_metadata_"+catalogName)
}

func escapeSQLString(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
