package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

const attachAlias = "_coredb_import"

func (s *Source) registerFunctions(ctx context.Context) error {
	if err := s.registerExport(ctx); err != nil {
		return err
	}
	if err := s.registerImport(ctx); err != nil {
		return err
	}
	if err := s.registerImportDescriptions(ctx); err != nil {
		return err
	}
	if err := s.registerRecreateIndexes(ctx); err != nil {
		return err
	}
	if err := s.registerResetSchema(ctx); err != nil {
		return err
	}
	return s.registerResetSystemTypes(ctx)
}

// --- Export ---

func (s *Source) registerExport(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "hugr_coredb_export",
		Execute: func(ctx context.Context, path string) (*types.OperationResult, error) {
			if err := s.exportDatabase(ctx, path); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(fmt.Sprintf("Database exported to %s", path), 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) < 1 || args[0] == nil {
				return "", fmt.Errorf("path is required")
			}
			return args[0].(string), nil
		},
		ConvertOutput: opResultOutput,
		InputTypes:    []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    db.DuckDBOperationResult(),
	})
}

func (s *Source) exportDatabase(ctx context.Context, path string) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf("ATTACH DATABASE '%s' AS _coredb_export", escapeSQLString(path)))
	if err != nil {
		return fmt.Errorf("attach export target: %w", err)
	}
	defer s.db.Exec(ctx, "DETACH _coredb_export") //nolint:errcheck

	// Table-by-table export ensures consistent schema (main) regardless of CoreDB type (DuckDB vs PostgreSQL)
	for _, table := range coreDBTables {
		if _, err := s.db.Exec(ctx, fmt.Sprintf("CREATE TABLE _coredb_export.\"%s\" AS SELECT * FROM core.\"%s\"", table, table)); err != nil {
			return fmt.Errorf("export table %s: %w", table, err)
		}
	}
	return nil
}

// --- Import ---

type importArgs struct {
	path       string
	backupPath string
}

func (s *Source) registerImport(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[importArgs, *types.OperationResult]{
		Name:                  "hugr_coredb_import",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args importArgs) (*types.OperationResult, error) {
			if args.backupPath != "" {
				if err := s.exportDatabase(ctx, args.backupPath); err != nil {
					return types.ErrResult(fmt.Errorf("pre-import backup failed: %w", err)), nil
				}
			}
			if err := s.importDatabase(ctx, args.path); err != nil {
				return types.ErrResult(err), nil
			}
			msg := fmt.Sprintf("Database imported from %s", args.path)
			if args.backupPath != "" {
				msg += fmt.Sprintf(" (backup at %s)", args.backupPath)
			}
			return types.Result(msg, 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (importArgs, error) {
			if len(args) < 1 || args[0] == nil {
				return importArgs{}, fmt.Errorf("path is required")
			}
			ia := importArgs{path: args[0].(string)}
			if len(args) > 1 && args[1] != nil {
				ia.backupPath = args[1].(string)
			}
			return ia, nil
		},
		ConvertOutput: opResultOutput,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

func (s *Source) importDatabase(ctx context.Context, path string) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf("ATTACH DATABASE '%s' AS %s (READ_ONLY)", escapeSQLString(path), attachAlias))
	if err != nil {
		return fmt.Errorf("attach import source: %w", err)
	}
	defer s.db.Exec(ctx, fmt.Sprintf("DETACH %s", attachAlias)) //nolint:errcheck

	// Validate version
	if err := s.validateVersion(ctx); err != nil {
		return err
	}

	// Clear + insert each table
	for _, table := range coreDBTables {
		if _, err := s.db.Exec(ctx, fmt.Sprintf("DELETE FROM core.\"%s\"", table)); err != nil {
			return fmt.Errorf("clear table %s: %w", table, err)
		}
		if _, err := s.db.Exec(ctx, fmt.Sprintf("INSERT INTO core.\"%s\" SELECT * FROM %s.\"%s\"", table, attachAlias, table)); err != nil {
			return fmt.Errorf("import table %s: %w", table, err)
		}
	}
	return nil
}

// --- Import Descriptions ---

type importDescArgs struct {
	path                string
	includeEmbeddings   bool
	recomputeEmbeddings bool
}

func (s *Source) registerImportDescriptions(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[importDescArgs, *types.OperationResult]{
		Name:                  "hugr_coredb_import_descriptions",
		IsSpecialNullHandling: true,
		Execute: func(ctx context.Context, args importDescArgs) (*types.OperationResult, error) {
			msg, err := s.importDescriptions(ctx, args)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(msg, 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (importDescArgs, error) {
			if len(args) < 1 || args[0] == nil {
				return importDescArgs{}, fmt.Errorf("path is required")
			}
			ida := importDescArgs{
				path:              args[0].(string),
				includeEmbeddings: true,
			}
			if len(args) > 1 && args[1] != nil {
				ida.includeEmbeddings = args[1].(bool)
			}
			if len(args) > 2 && args[2] != nil {
				ida.recomputeEmbeddings = args[2].(bool)
			}
			return ida, nil
		},
		ConvertOutput: opResultOutput,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

func (s *Source) importDescriptions(ctx context.Context, args importDescArgs) (string, error) {
	// Attach source
	_, err := s.db.Exec(ctx, fmt.Sprintf("ATTACH DATABASE '%s' AS %s (READ_ONLY)", escapeSQLString(args.path), attachAlias))
	if err != nil {
		return "", fmt.Errorf("attach source: %w", err)
	}
	defer s.db.Exec(ctx, fmt.Sprintf("DETACH %s", attachAlias)) //nolint:errcheck

	// Validate version
	if err := s.validateVersion(ctx); err != nil {
		return "", err
	}

	vecCol := ""
	if args.includeEmbeddings && !args.recomputeEmbeddings {
		vecCol = ", vec = src.vec"
	}

	// Update descriptions for each table
	updates := []struct {
		table string
		where string
		extra string // additional SET columns beyond description/long_description/is_summarized
	}{
		{"_schema_catalogs", "core._schema_catalogs.name = src.name", vecCol},
		{"_schema_types", "core._schema_types.name = src.name", vecCol},
		{"_schema_fields", "core._schema_fields.type_name = src.type_name AND core._schema_fields.name = src.name", vecCol},
		{"_schema_arguments", "core._schema_arguments.type_name = src.type_name AND core._schema_arguments.field_name = src.field_name AND core._schema_arguments.name = src.name", ""},
		{"_schema_modules", "core._schema_modules.name = src.name", vecCol},
	}

	var warnings []string
	for _, u := range updates {
		setCols := "description = src.description, long_description = src.long_description, is_summarized = src.is_summarized"
		if u.table == "_schema_arguments" {
			setCols = "description = src.description"
		}
		if u.extra != "" {
			setCols += u.extra
		}

		sql := fmt.Sprintf("UPDATE core.%s SET %s FROM %s.%s src WHERE %s",
			u.table, setCols, attachAlias, u.table, u.where)
		if _, err := s.db.Exec(ctx, sql); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", u.table, err))
		}
	}

	msg := "Descriptions imported"
	if len(warnings) > 0 {
		msg += " (warnings: " + strings.Join(warnings, "; ") + ")"
	}

	// Recompute embeddings if requested
	if args.recomputeEmbeddings {
		msg += " — recomputing embeddings not yet supported via UDF (use Provider.ReindexEmbeddings)"
	}

	return msg, nil
}

// --- Recreate Indexes ---

func (s *Source) registerRecreateIndexes(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionNoArgs[*types.OperationResult]{
		Name: "hugr_coredb_recreate_indexes",
		Execute: func(ctx context.Context) (*types.OperationResult, error) {
			if err := s.recreateIndexes(ctx); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Indexes recreated", 1, 0), nil
		},
		ConvertOutput: opResultOutput,
		OutputType:    db.DuckDBOperationResult(),
		IsVolatile:    true,
	})
}

func (s *Source) recreateIndexes(ctx context.Context) error {
	isPG := s.isPostgresCoreDB(ctx)

	// Common indexes (both DuckDB and PostgreSQL)
	for _, idx := range commonIndexes {
		if isPG {
			// PostgreSQL: execute DDL via postgres_execute
			ddl := fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				idx.name, idx.name, idx.table, idx.cols)
			if _, err := s.db.Exec(ctx, fmt.Sprintf("CALL postgres_execute('core', '%s')", escapeSQLString(ddl))); err != nil {
				return fmt.Errorf("pg create index %s: %w", idx.name, err)
			}
		} else {
			// DuckDB: direct DDL with core. prefix
			s.db.Exec(ctx, fmt.Sprintf("DROP INDEX IF EXISTS core.%s", idx.name)) //nolint:errcheck
			if _, err := s.db.Exec(ctx, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON core.%s (%s)", idx.name, idx.table, idx.cols)); err != nil {
				return fmt.Errorf("create index %s: %w", idx.name, err)
			}
		}
	}

	// PostgreSQL-specific: HNSW vector indexes (only if PG CoreDB)
	if isPG {
		for _, idx := range pgVectorIndexes {
			ddl := fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (vec vector_cosine_ops)",
				idx.name, idx.name, idx.table)
			// Ignore errors — vec column may not exist if embeddings not configured
			s.db.Exec(ctx, fmt.Sprintf("CALL postgres_execute('core', '%s')", escapeSQLString(ddl))) //nolint:errcheck
		}
	}

	return nil
}

func (s *Source) isPostgresCoreDB(ctx context.Context) bool {
	var rows []struct {
		Type string `db:"type"`
	}
	if err := s.db.QueryTableToSlice(ctx, &rows, "SELECT type FROM duckdb_databases() WHERE database_name = 'core'"); err != nil {
		return false
	}
	return len(rows) > 0 && rows[0].Type == "postgres"
}

// --- Reset Schema ---

func (s *Source) registerResetSchema(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionNoArgs[*types.OperationResult]{
		Name: "hugr_coredb_reset_schema",
		Execute: func(ctx context.Context) (*types.OperationResult, error) {
			if err := s.resetSchema(ctx); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Schema metadata cleared, restart to recompile", 1, 0), nil
		},
		ConvertOutput: opResultOutput,
		OutputType:    db.DuckDBOperationResult(),
		IsVolatile:    true,
	})
}

func (s *Source) resetSchema(ctx context.Context) error {
	for _, table := range schemaTables {
		if _, err := s.db.Exec(ctx, fmt.Sprintf("DELETE FROM core.%s", table)); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	// Reset schema_version
	_, err := s.db.Exec(ctx, "UPDATE core._schema_settings SET value = '\"0\"' WHERE key = 'schema_version'")
	return err
}

// --- Reset System Types ---

func (s *Source) registerResetSystemTypes(ctx context.Context) error {
	return s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionNoArgs[*types.OperationResult]{
		Name: "hugr_coredb_reset_system_types",
		Execute: func(ctx context.Context) (*types.OperationResult, error) {
			_, err := s.db.Exec(ctx, "DELETE FROM core._schema_settings WHERE key = 'system_types_version'")
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("System types version cleared, restart to recompile", 1, 0), nil
		},
		ConvertOutput: opResultOutput,
		OutputType:    db.DuckDBOperationResult(),
		IsVolatile:    true,
	})
}

// --- Helpers ---

type versionRow struct {
	Version string `db:"version"`
}

func (s *Source) validateVersion(ctx context.Context) error {
	var rows []versionRow
	err := s.db.QueryTableToSlice(ctx, &rows, fmt.Sprintf("SELECT version FROM %s.version LIMIT 1", attachAlias))
	if err != nil {
		return fmt.Errorf("read source version: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("source has no version record")
	}
	if rows[0].Version != coredb.Version {
		return fmt.Errorf("version mismatch: source=%s, current=%s", rows[0].Version, coredb.Version)
	}
	return nil
}

func opResultOutput(out *types.OperationResult) (any, error) {
	if out == nil {
		return nil, nil
	}
	return out.ToDuckdb(), nil
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
