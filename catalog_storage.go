package hugr

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	catalogdb "github.com/hugr-lab/query-engine/pkg/catalog/db"
	catalogstore "github.com/hugr-lab/query-engine/pkg/catalog/store"
	"github.com/hugr-lab/query-engine/pkg/cluster"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

// CatalogStorage selects how the engine keeps the schema in CoreDB.
type CatalogStorage string

const (
	// CatalogStorageCompiled stores the COMPILED GraphQL schema — one row per
	// generated type, field and argument in the _schema_* tables
	// (pkg/catalog/db). The default.
	CatalogStorageCompiled CatalogStorage = "db"
	// CatalogStorageEntity stores the sources' LOGICAL model in the catalog.*
	// tables and generates the GraphQL schema on the fly from it
	// (pkg/catalog/store, design 034).
	CatalogStorageEntity CatalogStorage = "entity"
)

func (c CatalogStorage) resolve() (CatalogStorage, error) {
	switch c {
	case "", CatalogStorageCompiled:
		return CatalogStorageCompiled, nil
	case CatalogStorageEntity:
		return CatalogStorageEntity, nil
	}
	return "", fmt.Errorf("unknown catalog storage %q (want %q or %q)",
		string(c), CatalogStorageCompiled, CatalogStorageEntity)
}

// catalogStorage is what the engine needs from a catalog storage whichever one
// runs: the read provider catalog.Service is built on, the lifecycle
// operations Init drives, and the surface the cluster polls. Implemented by
// *catalogdb.Provider and *catalogstore.Store.
type catalogStorage interface {
	catalog.Provider
	cluster.CatalogProvider

	// RegisterUDFs installs the schema-management functions; the checker
	// refuses to hard-remove a catalog that still has an engine.
	RegisterUDFs(ctx context.Context, checker catalog.CatalogChecker) error
	// RemoveCatalog drops a catalog's stored schema.
	RemoveCatalog(ctx context.Context, name string) error
	// CleanOrphanedCatalogs drops schemas of data sources that no longer exist.
	CleanOrphanedCatalogs(ctx context.Context) error
}

var (
	_ catalogStorage = (*catalogdb.Provider)(nil)
	_ catalogStorage = (*catalogstore.Store)(nil)
)

// initCatalogStorage creates the catalog storage for the resolved mode. The
// compiled-schema provider also persists the system types; the entity storage
// keeps that layer in memory and never writes it. A read-only node skips the
// write either way — the writer node did it.
func (s *Service) initCatalogStorage(ctx context.Context, mode CatalogStorage, embedder catalogdb.Embedder, isReadonly bool) (catalogStorage, error) {
	if mode == CatalogStorageEntity {
		st, err := catalogstore.New(ctx, s.db, catalogstore.Config{
			VecSize:    s.config.Embedder.VectorSize,
			IsReadonly: isReadonly,
			Cache: catalogstore.CacheConfig{
				MaxEntries: s.config.SchemaCacheMaxEntries,
				TTL:        s.config.SchemaCacheTTL,
			},
		}, embedder)
		if err != nil {
			return nil, fmt.Errorf("create catalog store: %w", err)
		}
		return st, nil
	}

	cacheConfig := catalogdb.CacheConfig{
		MaxEntries: s.config.SchemaCacheMaxEntries,
		TTL:        s.config.SchemaCacheTTL,
	}
	if cacheConfig.MaxEntries == 0 && cacheConfig.TTL == 0 {
		cacheConfig = catalogdb.DefaultCacheConfig()
	}
	p, err := catalogdb.NewWithCompiler(ctx, s.db, catalogdb.Config{
		TablePrefix: "core.",
		Cache:       cacheConfig,
		IsPostgres:  s.config.CoreDB.Info().Type == sources.Postgres,
		IsReadonly:  isReadonly,
		VecSize:     s.config.Embedder.VectorSize,
	}, embedder, compiler.New(compiler.GlobalRules()...))
	if err != nil {
		return nil, fmt.Errorf("create db provider: %w", err)
	}
	if !isReadonly {
		// Version-checked; skips when unchanged on restart.
		if err := p.InitSystemTypes(ctx); err != nil {
			return nil, fmt.Errorf("init system types: %w", err)
		}
	}
	return p, nil
}
