package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	catsrc "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"golang.org/x/sync/singleflight"
)

// logReadErr reports a read failure at a Provider surface (ForName, the
// logical model, NameExists). The read interfaces have no error channel, so the
// surface logs the propagated error here and serves EMPTY; the failed result
// is never cached (absence is a nil WITHOUT an error).
func logReadErr(op string, err error) {
	slog.Error("catalog store: read failed", "op", op, "error", err)
}

// Embedder creates embedding vectors for annotation seeding at insert time.
// Same method set as the engine's embedding source (pkg/catalog/db.Embedder) —
// a concrete embedder satisfies both structurally. nil when embeddings are not
// configured; the writer then persists entities without vectors.
type Embedder interface {
	CreateEmbedding(ctx context.Context, input string) (*sources.EmbeddingResult, error)
	CreateEmbeddings(ctx context.Context, inputs []string) (*sources.EmbeddingsResult, error)
}

// Config holds the entity-storage provider configuration.
type Config struct {
	// VecSize is the annotations.vec dimension (fixed at CoreDB init). 0
	// disables vector operations.
	VecSize int
	// IsReadonly rejects all writes (cluster workers read the already-populated
	// catalog schema).
	IsReadonly bool
	// Cache configures the read-side definition cache; the zero value takes
	// the defaults (set Disabled to switch it off).
	Cache CacheConfig
}

// Store is the entity-storage catalog provider. It holds the in-memory system
// layer (never persisted) and talks to the CoreDB `catalog` schema through the
// pool with direct SQL — the statement forms proven by the provider DML
// inventory (core-db/hugr_catalog_test.go: probeCatalogStatements).
//
// Method surfaces are added incrementally: M2 adds the writer (MutableProvider
// / CatalogManager), M3 adds ForName/Types synthesis and the read cache.
type Store struct {
	pool       *db.Pool
	vecSize    int
	isReadonly bool

	// embedder seeds annotation vectors on insert; nil disables vectors.
	embedder Embedder
	// static is the binary-owned system layer (scalars, __*, @system types,
	// base directives) assembled at startup. Source entities are stored in
	// catalog.*; system types live only here.
	static *static.Provider

	// cache holds compiled definitions by name (nil = disabled); gen is the
	// invalidation clock its installs check against; sf collapses concurrent
	// resolutions of the same name.
	cache *defCache
	gen   atomic.Uint64
	sf    singleflight.Group

	// catMu guards catalogs — the live source handles the manager keeps for
	// ReloadCatalog and reactivation (manager.go).
	catMu    sync.RWMutex
	catalogs map[string]catsrc.Catalog
}

// New assembles the system layer and returns a Store bound to the CoreDB pool.
// It performs no DDL — the catalog schema is delivered by coredb.InitSchema
// (new databases) or the hugr 0.0.19 migration (existing databases).
func New(_ context.Context, pool *db.Pool, cfg Config, embedder Embedder) (*Store, error) {
	sp, err := static.New()
	if err != nil {
		return nil, fmt.Errorf("catalog store: assemble system types: %w", err)
	}
	return &Store{
		pool:       pool,
		vecSize:    cfg.VecSize,
		isReadonly: cfg.IsReadonly,
		embedder:   embedder,
		static:     sp,
		cache:      newDefCache(cfg.Cache),
		catalogs:   map[string]catsrc.Catalog{},
	}, nil
}

// invalidateSource drops every cached artifact a write to the source could
// have changed: the definition cache's source and allSources buckets, plus
// the affected object FAMILIES (an object plus its derived types — the
// write-side closure for cross-source absence dependencies: relations
// pointing at foreign objects and module functions returning lists of them
// influence definitions whose builds saw no row of this source). The
// invalidation clock is bumped FIRST so in-flight resolutions discard their
// results.
func (s *Store) invalidateSource(source string, affected map[string]struct{}) {
	s.gen.Add(1)
	if len(affected) == 0 {
		s.cache.invalidate(source, nil)
		return
	}
	s.cache.invalidate(source, func(name string) bool {
		if _, ok := affected[name]; ok {
			return true
		}
		for i := range derivedRules {
			if base, ok := derivedRules[i].match(name); ok {
				if _, hit := affected[base]; hit {
					return true
				}
			}
		}
		return false
	})
}

// invalidateAll is the conservative fallback when the affected-object set
// could not be computed (a read failure during the write path).
func (s *Store) invalidateAll() {
	s.gen.Add(1)
	s.cache.invalidateAll()
}

// evictType drops one type from the definition cache — the targeted
// invalidation for a type/field/argument annotation write, which changes only
// that type's assembled definition. The clock is bumped FIRST so an in-flight
// resolution that read the pre-write annotations discards its result instead
// of racing a stale definition back into the cache after the evict.
func (s *Store) evictType(name string) {
	s.gen.Add(1)
	s.cache.evict(name)
}

// evictTypeFamily drops a base type AND its cached derived types (filters,
// mutation inputs, aggregation types) — the write-side closure for a FIELD
// curation, whose description the derived types inherit onto their same-named
// fields at reconstruction. Every derived rule is checked (suffixes overlap:
// x_list_filter parses as both x+list_filter and x_list+filter).
func (s *Store) evictTypeFamily(name string) {
	s.gen.Add(1)
	s.cache.evictFamily(name, func(candidate string) bool {
		for i := range derivedRules {
			if b, ok := derivedRules[i].match(candidate); ok && b == name {
				return true
			}
		}
		return false
	})
}

// System returns the in-memory system-type layer. The writer skips
// sdl.IsSystemType(def) and the reader delegates system-type resolution here
// (except Query/Mutation/Subscription, which are synthesized from catalog.*).
func (s *Store) System() *static.Provider { return s.static }
