package hugrapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

const metaTableSQL = `
CREATE TABLE IF NOT EXISTS _hugr_app_meta (
    app_name     TEXT PRIMARY KEY,
    version      TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);`

// Querier executes GraphQL queries for data source management.
type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error)
}

// ProvisionDataSources reads data sources from _mount.data_sources and
// ensures each one is registered, initialized, and migrated as needed.
// Uses Querier to register/load data sources via GraphQL API.
func ProvisionDataSources(
	ctx context.Context,
	pool *db.Pool,
	appSource *Source,
	querier Querier,
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

		if err := provisionOne(ctx, pool, appSource, dsInfo, fullName, querier); err != nil {
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
) error {
	path := dsInfo.Path // env vars resolved by hugr at attach time

	// Connect to the target PostgreSQL database
	pgConn, err := sql.Open("pgx", path)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", fullName, err)
	}
	defer pgConn.Close()

	if err := pgConn.PingContext(ctx); err != nil {
		return fmt.Errorf("database not reachable at %s, check DataSourceInfo.Path: %w", fullName, err)
	}

	// Ensure meta table exists
	if _, err := pgConn.ExecContext(ctx, metaTableSQL); err != nil {
		return fmt.Errorf("create _hugr_app_meta in %s: %w", fullName, err)
	}

	// 1. Check if DS already registered in hugr and its status
	status, exists := queryDataSourceStatus(ctx, querier, fullName)

	// 2. Register in hugr if not exists
	if !exists {
		slog.Info("registering new app DS", "ds", fullName)
		prefix := strings.ReplaceAll(fullName, ".", "_")

		// If app provides SDL (HugrSchema) → use text catalog source, not self_defined
		selfDefined := dsInfo.HugrSchema == ""
		catalogsBlock := ""
		if dsInfo.HugrSchema != "" {
			catalogsBlock = fmt.Sprintf(`, catalogs: [{name: %q, type: "text", path: %q}]`,
				fullName, dsInfo.HugrSchema)
		}

		registerQuery := fmt.Sprintf(`mutation {
			core {
				insert_data_sources(data: {
					name: %q
					type: %q
					description: %q
					prefix: %q
					path: %q
					as_module: true
					self_defined: %t
					read_only: %t
					%s
				}) { name }
			}
		}`, fullName, dsInfo.Type, dsInfo.Description, prefix, dsInfo.Path,
			selfDefined, dsInfo.ReadOnly, catalogsBlock)

		if _, err := querier.Query(ctx, registerQuery, nil); err != nil {
			return fmt.Errorf("register DS %s: %w", fullName, err)
		}
	}

	// 3. Check DB schema version
	var currentVersion string
	err = pgConn.QueryRowContext(ctx,
		"SELECT version FROM _hugr_app_meta WHERE app_name = $1",
		appSource.Name(),
	).Scan(&currentVersion)

	needsReload := false

	switch {
	case err == sql.ErrNoRows:
		// First time — initialize schema
		slog.Info("initializing schema for app DS", "ds", fullName, "version", dsInfo.Version)
		if err := initSchema(ctx, pool, pgConn, appSource, dsInfo); err != nil {
			return err
		}
		needsReload = true

	case err != nil:
		return fmt.Errorf("check version for %s: %w", fullName, err)

	case currentVersion == dsInfo.Version:
		slog.Info("app DS schema up to date", "ds", fullName, "version", currentVersion)

	default:
		// Version mismatch — unload if loaded, then migrate, then reload
		slog.Info("migrating app DS schema", "ds", fullName, "from", currentVersion, "to", dsInfo.Version)
		if status == "loaded" {
			slog.Info("unloading DS before migration", "ds", fullName)
			_ = unloadDataSource(ctx, querier, fullName)
		}
		if err := migrateSchema(ctx, pool, pgConn, appSource, dsInfo, currentVersion); err != nil {
			return err
		}
		needsReload = true
	}

	// 4. Ensure DS is loaded
	if status != "loaded" || needsReload {
		slog.Info("loading app DS", "ds", fullName)
		if err := loadDataSource(ctx, querier, fullName); err != nil {
			return fmt.Errorf("load DS %s: %w", fullName, err)
		}
	}

	return nil
}

// queryDataSourceStatus checks if a data source exists and returns its status.
func queryDataSourceStatus(ctx context.Context, querier Querier, name string) (status string, exists bool) {
	q := fmt.Sprintf(`{
		function { core { data_source_status(name: %q) } }
	}`, name)

	resp, err := querier.Query(ctx, q, nil)
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
	q := fmt.Sprintf(`mutation {
		function { core { unload_data_source(name: %q) { success message } } }
	}`, name)
	_, err := querier.Query(ctx, q, nil)
	return err
}

// loadDataSource loads a data source via GraphQL mutation.
func loadDataSource(ctx context.Context, querier Querier, name string) error {
	q := fmt.Sprintf(`mutation {
		function { core { load_data_source(name: %q) { success message } } }
	}`, name)

	_, err := querier.Query(ctx, q, nil)
	return err
}

func initSchema(ctx context.Context, pool *db.Pool, pgConn *sql.DB, appSource *Source, dsInfo DataSourceInfo) error {
	sqlTemplate, err := readMountInitDSSchema(ctx, pool, appSource.Name(), dsInfo.Name)
	if err != nil {
		return fmt.Errorf("get init_ds_schema: %w", err)
	}

	// Execute init SQL in transaction
	tx, err := pgConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin init transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlTemplate); err != nil {
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

func migrateSchema(ctx context.Context, pool *db.Pool, pgConn *sql.DB, appSource *Source, dsInfo DataSourceInfo, fromVersion string) error {
	if !appSource.appInfo.IsDBMigrator {
		return fmt.Errorf("app %s does not support migrations (version %s → %s)", appSource.Name(), fromVersion, dsInfo.Version)
	}

	sqlTemplate, err := readMountMigrateDSSchema(ctx, pool, appSource.Name(), dsInfo.Name, fromVersion)
	if err != nil {
		return fmt.Errorf("get migrate_ds_schema: %w", err)
	}

	// Execute migration in transaction
	tx, err := pgConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migrate transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlTemplate); err != nil {
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
