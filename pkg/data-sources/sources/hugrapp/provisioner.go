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

	// Check current version
	var currentVersion string
	err = pgConn.QueryRowContext(ctx,
		"SELECT version FROM _hugr_app_meta WHERE app_name = $1",
		appSource.Name(),
	).Scan(&currentVersion)

	switch {
	case err == sql.ErrNoRows:
		// First time — initialize schema
		slog.Info("initializing schema for app DS", "ds", fullName, "version", dsInfo.Version)
		if err := initSchema(ctx, pool, pgConn, appSource, dsInfo); err != nil {
			return err
		}

	case err != nil:
		return fmt.Errorf("check version for %s: %w", fullName, err)

	case currentVersion == dsInfo.Version:
		slog.Info("app DS schema up to date", "ds", fullName, "version", currentVersion)

	default:
		// Version mismatch — migrate
		slog.Info("migrating app DS schema", "ds", fullName, "from", currentVersion, "to", dsInfo.Version)
		if err := migrateSchema(ctx, pool, pgConn, appSource, dsInfo, currentVersion); err != nil {
			return err
		}
	}

	// Register and load via GraphQL API
	prefix := strings.ReplaceAll(fullName, ".", "_")
	registerQuery := fmt.Sprintf(`mutation {
		core {
			insert_data_sources(data: {
				name: %q
				type: %q
				description: %q
				prefix: %q
				path: %q
				as_module: true
				self_defined: true
				read_only: %t
			}) { name }
		}
	}`, fullName, dsInfo.Type, dsInfo.Description, prefix, dsInfo.Path, dsInfo.ReadOnly)

	if _, err := querier.Query(ctx, registerQuery, nil); err != nil {
		slog.Debug("register DS via GraphQL (may already exist)", "ds", fullName, "error", err)
	}

	loadQuery := fmt.Sprintf(`mutation {
		function { core { load_data_source(name: %q) { success message } } }
	}`, fullName)

	if _, err := querier.Query(ctx, loadQuery, nil); err != nil {
		slog.Warn("load DS via GraphQL failed", "ds", fullName, "error", err)
	}

	return nil
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
