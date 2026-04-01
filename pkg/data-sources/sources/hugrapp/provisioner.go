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

// ProvisionDataSources reads data sources from _mount.data_sources and
// ensures each one is registered, initialized, and migrated as needed.
// Uses Querier to register/load data sources via GraphQL API.
// TemplateParams provides system-level parameters for SQL/SDL templating.
func ProvisionDataSources(
	ctx context.Context,
	pool *db.Pool,
	appSource *Source,
	querier Querier,
	tmplParams TemplateParams,
) error {
	if appSource.appInfo == nil || !appSource.appInfo.IsDBInitializer {
		return nil // app doesn't manage databases
	}

	dataSources, err := readMountDataSources(ctx, pool, appSource.Name())
	if err != nil {
		return fmt.Errorf("provision: read data_sources: %w", err)
	}

	if len(dataSources) == 0 {
		return nil
	}

	appName := appSource.Name()

	for _, dsInfo := range dataSources {
		fullName := appName + "." + dsInfo.Name

		// Only PostgreSQL supported for now
		if !strings.EqualFold(dsInfo.Type, "postgres") {
			return fmt.Errorf("DB provisioning not supported for type %q; only PostgreSQL is currently supported (data source: %s)", dsInfo.Type, fullName)
		}

		slog.Info("provisioning app data source", "app", appName, "ds", fullName, "version", dsInfo.Version)

		if err := provisionOne(ctx, pool, appSource, dsInfo, fullName, querier, tmplParams); err != nil {
			return fmt.Errorf("provision %s: %w", fullName, err)
		}
	}

	return nil
}

func provisionOne(
	ctx context.Context,
	pool *db.Pool,
	appSource *Source,
	dsInfo DataSourceInfo,
	fullName string,
	querier Querier,
	tmplParams TemplateParams,
) error {
	// 1. Check if DS already registered in hugr and its status
	status, exists := queryDataSourceStatus(ctx, querier, fullName)

	// 2. Register in hugr if not exists
	if !exists {
		if err := registerAppDataSource(ctx, querier, fullName, dsInfo, tmplParams); err != nil {
			return err
		}
	}

	// 3. Handle DB schema (connect, version check, init/migrate)
	schemaChanged, err := ensureDBSchema(ctx, pool, appSource, dsInfo, fullName, tmplParams)
	if err != nil {
		return err
	}

	// 4. If schema changed and DS was loaded — unload first for clean reload
	if schemaChanged && status == "loaded" {
		slog.Info("unloading DS for schema reload", "ds", fullName)
		_ = unloadDataSource(ctx, querier, fullName)
	}

	// 5. Ensure DS is loaded
	if status != "loaded" || schemaChanged {
		slog.Info("loading app DS", "ds", fullName)
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
		data["catalogs"] = []map[string]any{{
			"name": fullName,
			"type": "text",
			"path": sdl,
		}}
	}

	registerQuery := `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`
	if _, err := querier.Query(ctx, registerQuery, map[string]any{"data": data}); err != nil {
		return fmt.Errorf("register DS %s: %w", fullName, err)
	}

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

// queryDataSourceStatus checks if a data source exists and returns its status.
func queryDataSourceStatus(ctx context.Context, querier Querier, name string) (status string, exists bool) {
	q := `{ function { core { data_source_status(name: $name) } } }`
	vars := map[string]any{"name": name}

	resp, err := querier.Query(ctx, q, vars)
	if err != nil || resp == nil {
		return "", false
	}

	// Navigate response: data.function.core.data_source_status
	data, ok := resp.Data["data"]
	if !ok {
		return "", false
	}
	dataMap, ok := data.(map[string]any)
	if !ok {
		return "", false
	}
	fn, ok := dataMap["function"]
	if !ok {
		return "", false
	}
	fnMap, ok := fn.(map[string]any)
	if !ok {
		return "", false
	}
	core, ok := fnMap["core"]
	if !ok {
		return "", false
	}
	coreMap, ok := core.(map[string]any)
	if !ok {
		return "", false
	}
	s, ok := coreMap["data_source_status"]
	if !ok || s == nil {
		return "", false
	}
	statusStr, ok := s.(string)
	if !ok {
		return "", false
	}
	return statusStr, true
}

// unloadDataSource unloads a data source via GraphQL mutation.
func unloadDataSource(ctx context.Context, querier Querier, name string) error {
	q := `mutation($name: String!) {
		function { core { unload_data_source(name: $name) { success message } } }
	}`
	_, err := querier.Query(ctx, q, map[string]any{"name": name})
	return err
}

// loadDataSource loads a data source via GraphQL mutation.
func loadDataSource(ctx context.Context, querier Querier, name string) error {
	q := `mutation($name: String!) {
		function { core { load_data_source(name: $name) { success message } } }
	}`
	_, err := querier.Query(ctx, q, map[string]any{"name": name})
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
