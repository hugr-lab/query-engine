package hugrapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

// escapeSQLString escapes single quotes for safe interpolation in DuckDB SQL.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// readMountInfo queries _mount.info() from the attached hugr-app source
// and returns the parsed AppInfo struct.
func readMountInfo(ctx context.Context, pool *db.Pool, sourceName string) (*AppInfo, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("readMountInfo: %w", err)
	}
	defer conn.Close()

	var name, description, version, uri string
	var isDBInit, isDBMigrator bool

	query := fmt.Sprintf(
		`SELECT (s).name, (s).description, (s).version, (s).uri, (s).is_db_initializer, (s).is_db_migrator
		 FROM (SELECT %s._mount.info() AS s)`,
		engines.Ident(sourceName),
	)

	err = conn.QueryRow(ctx, query).Scan(&name, &description, &version, &uri, &isDBInit, &isDBMigrator)
	if err != nil {
		return nil, fmt.Errorf("readMountInfo for %s: %w", sourceName, err)
	}

	return &AppInfo{
		Name:            name,
		Description:     description,
		Version:         version,
		URI:             uri,
		IsDBInitializer: isDBInit,
		IsDBMigrator:    isDBMigrator,
	}, nil
}

// DataSourceInfo holds a data source declared by a hugr-app.
type DataSourceInfo struct {
	Name        string
	Type        string
	Description string
	ReadOnly    bool
	Path        string
	Version     string
	HugrSchema  string
}

// readMountDataSources queries _mount.data_sources table from the attached hugr-app.
func readMountDataSources(ctx context.Context, pool *db.Pool, sourceName string) ([]DataSourceInfo, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("readMountDataSources: %w", err)
	}
	defer conn.Close()

	query := fmt.Sprintf(
		`SELECT name, type, description, read_only, path, version, hugr_schema
		 FROM %s._mount.data_sources`,
		engines.Ident(sourceName),
	)

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("readMountDataSources for %s: %w", sourceName, err)
	}
	defer rows.Close()

	var result []DataSourceInfo
	for rows.Next() {
		var ds DataSourceInfo
		if err := rows.Scan(&ds.Name, &ds.Type, &ds.Description, &ds.ReadOnly, &ds.Path, &ds.Version, &ds.HugrSchema); err != nil {
			return nil, fmt.Errorf("readMountDataSources scan: %w", err)
		}
		result = append(result, ds)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("readMountDataSources iteration: %w", err)
	}
	return result, nil
}

// readMountSchemaSDL queries _mount.schema_sdl() from the attached hugr-app.
func readMountSchemaSDL(ctx context.Context, pool *db.Pool, sourceName string) (string, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("readMountSchemaSDL: %w", err)
	}
	defer conn.Close()

	var sdl string
	query := fmt.Sprintf(
		`SELECT %s._mount.schema_sdl()`,
		engines.Ident(sourceName),
	)
	err = conn.QueryRow(ctx, query).Scan(&sdl)
	if err != nil {
		return "", fmt.Errorf("readMountSchemaSDL for %s: %w", sourceName, err)
	}
	return sdl, nil
}

// readMountInitDSSchema calls _mount.init_ds_schema(name) → returns SQL template.
func readMountInitDSSchema(ctx context.Context, pool *db.Pool, sourceName, dsName string) (string, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	var sql string
	query := fmt.Sprintf(
		`SELECT %s._mount.init_ds_schema('%s')`,
		engines.Ident(sourceName), escapeSQLString(dsName),
	)
	err = conn.QueryRow(ctx, query).Scan(&sql)
	if err != nil {
		return "", fmt.Errorf("init_ds_schema(%s) for %s: %w", dsName, sourceName, err)
	}
	return sql, nil
}

// readMountMigrateDSSchema calls _mount.migrate_ds_schema(name, version) → returns migration SQL.
// callMountInit calls _mount.init() to notify the app that provisioning is complete.
func callMountInit(ctx context.Context, pool *db.Pool, sourceName string) error {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	query := fmt.Sprintf(`SELECT %s._mount.init()`, engines.Ident(sourceName))
	var result bool
	err = conn.QueryRow(ctx, query).Scan(&result)
	if err != nil {
		return fmt.Errorf("mount init for %s: %w", sourceName, err)
	}
	return nil
}

func readMountMigrateDSSchema(ctx context.Context, pool *db.Pool, sourceName, dsName, fromVersion string) (string, error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	var sql string
	query := fmt.Sprintf(
		`SELECT %s._mount.migrate_ds_schema('%s', '%s')`,
		engines.Ident(sourceName), escapeSQLString(dsName), escapeSQLString(fromVersion),
	)
	err = conn.QueryRow(ctx, query).Scan(&sql)
	if err != nil {
		return "", fmt.Errorf("migrate_ds_schema(%s, %s) for %s: %w", dsName, fromVersion, sourceName, err)
	}
	return sql, nil
}
