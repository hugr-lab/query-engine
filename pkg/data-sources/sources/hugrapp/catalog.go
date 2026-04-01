package hugrapp

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	dsources "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

var _ dsources.SelfDescriber = &Source{}

// CatalogSource implements sources.SelfDescriber by reading _mount.schema_sdl()
// from the attached hugr-app and parsing the returned GraphQL SDL.
func (s *Source) CatalogSource(ctx context.Context, pool *db.Pool) (cs.Catalog, error) {
	sdl, err := readMountSchemaSDL(ctx, pool, s.ds.Name)
	if err != nil {
		return nil, fmt.Errorf("hugr-app catalog for %s: %w", s.ds.Name, err)
	}
	if sdl == "" {
		return nil, nil
	}

	opts := base.Options{
		Name:       s.ds.Name,
		Prefix:     s.ds.Prefix,
		EngineType: string(s.engine.Type()),
		AsModule:   s.ds.AsModule,
		ReadOnly:   s.ds.ReadOnly,
	}
	return cs.NewStringSource(s.ds.Name, s.engine, opts, sdl)
}

// readMountSchemaSDL queries _mount.schema_sdl() from the attached hugr-app source.
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
