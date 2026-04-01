package hugrapp

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

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
