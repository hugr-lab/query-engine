package store

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
)

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
	}, nil
}

// System returns the in-memory system-type layer. The writer skips
// sdl.IsSystemType(def) and the reader delegates system-type resolution here
// (except Query/Mutation/Subscription, which are synthesized from catalog.*).
func (s *Store) System() *static.Provider { return s.static }
