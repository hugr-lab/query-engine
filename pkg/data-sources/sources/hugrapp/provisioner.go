package hugrapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

const metaTableSQL = `
CREATE TABLE IF NOT EXISTS _hugr_app_meta (
    app_name     TEXT PRIMARY KEY,
    version      TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);`

// Querier is sources.Querier — provides GraphQL query access.
type Querier = sources.Querier

// TemplateParams holds system-level parameters available for Go template
// expansion in app's SQL (init/migrate) and SDL (HugrSchema).
// Follows the same pattern as CoreDB's SchemaTemplateParams.
type TemplateParams struct {
	VectorSize   int    // Embedding vector dimension (0 = no embeddings)
	EmbedderName string // Name of the system embedder (empty = not configured)
}

// applyTemplate renders a Go text/template string with TemplateParams.
func applyTemplate(tmpl string, params TemplateParams) (string, error) {
	if tmpl == "" {
		return "", nil
	}
	return db.ParseSQLScriptTemplate(db.SDBPostgres, tmpl, params)
}

// ProvisionDataSources reads data sources from _mount.data_sources,
// cleans up stale registrations, ensures DB schemas, then registers and loads all DS.
// Two-pass approach:
//  1. ensureDB for PostgreSQL sources (create tables, migrate)
//  2. register + load all sources (any type)
func ProvisionDataSources(
	ctx context.Context,
	pool *db.Pool,
	appSource *Source,
	querier Querier,
	tmplParams TemplateParams,
) error {
	dataSources, err := readMountDataSources(ctx, pool, appSource.Name())
	if err != nil {
		return fmt.Errorf("provision: read data_sources: %w", err)
	}

	if len(dataSources) == 0 {
		return nil
	}

	appName := appSource.Name()

	// Clean up stale DS registrations — unload + delete all app DS
	if err := cleanupAppDataSources(ctx, querier, appName); err != nil {
		slog.Warn("cleanup app data sources failed (non-fatal)", "app", appName, "error", err)
		return fmt.Errorf("cleanup provision data sources for app %s: %w", appName, err)
	}

	// Pass 1: ensure DB schemas for PostgreSQL sources (only if app manages DB)
	if appSource.appInfo != nil && appSource.appInfo.IsDBInitializer {
		for _, dsInfo := range dataSources {
			if !strings.EqualFold(dsInfo.Type, "postgres") {
				continue // only PostgreSQL gets DB provisioning
			}
			fullName := appName + "." + dsInfo.Name
			slog.Info("ensuring DB schema", "app", appName, "ds", fullName, "version", dsInfo.Version)
			if _, err := ensureDBSchema(ctx, pool, appSource, dsInfo, fullName, tmplParams); err != nil {
				return fmt.Errorf("ensure DB schema %s: %w", fullName, err)
			}
		}
	}

	// Pass 2: register + load all sources (any type)
	for _, dsInfo := range dataSources {
		fullName := appName + "." + dsInfo.Name
		slog.Info("registering app DS", "app", appName, "ds", fullName)
		if err := registerAppDataSource(ctx, querier, fullName, dsInfo, tmplParams); err != nil {
			return fmt.Errorf("register DS %s: %w", fullName, err)
		}
		slog.Info("loading app DS", "app", appName, "ds", fullName)
		if err := loadDataSource(ctx, querier, fullName); err != nil {
			return fmt.Errorf("load DS %s: %w", fullName, err)
		}
	}

	return nil
}

// registerAppDataSource registers a new data source in hugr via GraphQL.
// If HugrSchema is provided, it's templated with TemplateParams before use.
func registerAppDataSource(ctx context.Context, querier Querier, fullName string, dsInfo DataSourceInfo, tmplParams TemplateParams) error {
	slog.Info("registering new app DS", "ds", fullName)
	prefix := strings.ReplaceAll(fullName, ".", "_")
	selfDefined := dsInfo.HugrSchema == ""

	data := map[string]any{
		"name":         fullName,
		"type":         dsInfo.Type,
		"description":  dsInfo.Description,
		"prefix":       prefix,
		"path":         dsInfo.Path,
		"as_module":    true,
		"self_defined": selfDefined,
		"read_only":    dsInfo.ReadOnly,
	}
	// If app provides custom SDL, register it as a text catalog source
	if dsInfo.HugrSchema != "" {
		sdl, err := applyTemplate(dsInfo.HugrSchema, tmplParams)
		if err != nil {
			return fmt.Errorf("template SDL for %s: %w", fullName, err)
		}
		data["catalogs"] = []any{
			map[string]any{
				"name": fullName,
				"type": "text",
				"path": sdl,
			},
		}
	}

	registerQuery := `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`
	resp, err := querier.Query(ctx, registerQuery, map[string]any{"data": data})
	if err != nil {
		return fmt.Errorf("register DS %s: %w", fullName, err)
	}
	resp.Close()

	return nil
}

// ensureDBSchema connects directly to the PostgreSQL database, checks
// the schema version, and runs init or migration as needed.
// SQL templates from the app are expanded with TemplateParams.
// Returns true if the schema was changed (init or migrate happened).
func ensureDBSchema(
	ctx context.Context,
	pool *db.Pool,
	appSource *Source,
	dsInfo DataSourceInfo,
	fullName string,
	tmplParams TemplateParams,
) (schemaChanged bool, err error) {
	// Resolve env vars in path: postgres://[$DB_USER]:[$DB_PASS]@host/db
	resolvedPath, err := sources.ApplyEnvVars(dsInfo.Path)
	if err != nil {
		return false, fmt.Errorf("resolve env vars in DS path %s: %w", fullName, err)
	}

	pgConn, err := sql.Open("pgx", resolvedPath)
	if err != nil {
		return false, fmt.Errorf("connect to %s: %w", fullName, err)
	}
	defer pgConn.Close()

	if err := pgConn.PingContext(ctx); err != nil {
		return false, fmt.Errorf("database not reachable at %s, check DataSourceInfo.Path: %w", fullName, err)
	}

	// Ensure meta table exists
	if _, err := pgConn.ExecContext(ctx, metaTableSQL); err != nil {
		return false, fmt.Errorf("create _hugr_app_meta in %s: %w", fullName, err)
	}

	// Check current version
	var currentVersion string
	err = pgConn.QueryRowContext(ctx,
		"SELECT version FROM _hugr_app_meta WHERE app_name = $1",
		appSource.Name(),
	).Scan(&currentVersion)

	switch {
	case err == sql.ErrNoRows:
		slog.Info("initializing schema for app DS", "ds", fullName, "version", dsInfo.Version)
		if err := initSchema(ctx, pool, pgConn, appSource, dsInfo, tmplParams); err != nil {
			return false, err
		}
		return true, nil

	case err != nil:
		return false, fmt.Errorf("check version for %s: %w", fullName, err)

	case currentVersion == dsInfo.Version:
		slog.Info("app DS schema up to date", "ds", fullName, "version", currentVersion)
		return false, nil

	default:
		slog.Info("migrating app DS schema", "ds", fullName, "from", currentVersion, "to", dsInfo.Version)
		if err := migrateSchema(ctx, pool, pgConn, appSource, dsInfo, currentVersion, tmplParams); err != nil {
			return false, err
		}
		return true, nil
	}
}

// unloadDataSource unloads a data source via GraphQL mutation.
func unloadDataSource(ctx context.Context, querier Querier, name string) error {
	q := `mutation($name: String!) {
		function { core { unload_data_source(name: $name) { success message } } }
	}`
	resp, err := querier.Query(ctx, q, map[string]any{"name": name})
	if resp != nil {
		resp.Close()
	}
	return err
}

// loadDataSource loads a data source via GraphQL mutation.
func loadDataSource(ctx context.Context, querier Querier, name string) error {
	q := `mutation($name: String!) {
		function { core { load_data_source(name: $name) { success message } } }
	}`
	resp, err := querier.Query(ctx, q, map[string]any{"name": name})
	if resp != nil {
		resp.Close()
	}
	return err
}

func initSchema(ctx context.Context, pool *db.Pool, pgConn *sql.DB, appSource *Source, dsInfo DataSourceInfo, tmplParams TemplateParams) error {
	rawSQL, err := readMountInitDSSchema(ctx, pool, appSource.Name(), dsInfo.Name)
	if err != nil {
		return fmt.Errorf("get init_ds_schema: %w", err)
	}

	// Apply template: {{ .VectorSize }}, {{ .EmbedderName }} etc.
	sqlStr, err := applyTemplate(rawSQL, tmplParams)
	if err != nil {
		return fmt.Errorf("template init_ds_schema: %w", err)
	}

	// Execute init SQL in transaction
	tx, err := pgConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin init transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlStr); err != nil {
		return fmt.Errorf("execute init_ds_schema: %w", err)
	}

	// Record version
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO _hugr_app_meta (app_name, version) VALUES ($1, $2) ON CONFLICT (app_name) DO UPDATE SET version = $2, updated_at = now()",
		appSource.Name(), dsInfo.Version,
	); err != nil {
		return fmt.Errorf("record version: %w", err)
	}

	return tx.Commit()
}

func migrateSchema(ctx context.Context, pool *db.Pool, pgConn *sql.DB, appSource *Source, dsInfo DataSourceInfo, fromVersion string, tmplParams TemplateParams) error {
	if !appSource.appInfo.IsDBMigrator {
		return fmt.Errorf("app %s does not support migrations (version %s → %s)", appSource.Name(), fromVersion, dsInfo.Version)
	}

	rawSQL, err := readMountMigrateDSSchema(ctx, pool, appSource.Name(), dsInfo.Name, fromVersion)
	if err != nil {
		return fmt.Errorf("get migrate_ds_schema: %w", err)
	}

	// Apply template
	sqlStr, err := applyTemplate(rawSQL, tmplParams)
	if err != nil {
		return fmt.Errorf("template migrate_ds_schema: %w", err)
	}

	// Execute migration in transaction
	tx, err := pgConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migrate transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlStr); err != nil {
		return fmt.Errorf("execute migrate_ds_schema: %w", err)
	}

	// Update version
	if _, err := tx.ExecContext(ctx,
		"UPDATE _hugr_app_meta SET version = $1, updated_at = now() WHERE app_name = $2",
		dsInfo.Version, appSource.Name(),
	); err != nil {
		return fmt.Errorf("update version: %w", err)
	}

	return tx.Commit()
}

// cleanupAppDataSources finds all DS registered for this app (name LIKE 'appName.%'),
// unloads loaded ones, and deletes from data_sources (cascade removes catalog_sources link).
func cleanupAppDataSources(ctx context.Context, querier Querier, appName string) error {
	pattern := appName + ".%"
	q := `query($pattern: String!) {
		core { data_sources(filter: { name: { like: $pattern } }) { name } }
	}`
	resp, err := querier.Query(ctx, q, map[string]any{"pattern": pattern})
	if err != nil {
		return err
	}
	if resp == nil || len(resp.Errors) != 0 {
		if resp != nil {
			resp.Close()
		}
		return nil
	}
	defer resp.Close()

	var ds []struct {
		Name string `json:"name"`
	}
	if err := resp.ScanData("core.data_sources", &ds); err != nil || len(ds) == 0 {
		return nil
	}

	for _, d := range ds {
		slog.Info("cleaning up app DS", "app", appName, "ds", d.Name)
		_ = unloadDataSource(ctx, querier, d.Name)
	}

	delQ := `mutation($name: String!) {
			core { 
				delete_data_sources(filter: { name: { like: $name } }) { success } 
				delete_data_source_catalogs(filter: { data_source_name: { like: $name } }) { success }
				delete_catalogs(filter: { name: { like: $name } }) { success } }
			}
		}`
	delResp, err := querier.Query(ctx, delQ, map[string]any{"name": pattern})
	if delResp != nil {
		delResp.Close()
	}
	if err != nil {
		slog.Warn("failed to delete app DS", "hugr-app", pattern, "error", err)
	}

	return nil
}
