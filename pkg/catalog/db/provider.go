package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
	"golang.org/x/sync/singleflight"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// Compile-time check that Provider implements base.MutableProvider.
var _ base.MutableProvider = (*Provider)(nil)

// ErrReadOnly is returned when a write operation is attempted on a read-only provider.
var ErrReadOnly = errors.New("schema store is in read-only mode")

// CacheConfig controls the LRU cache behavior.
type CacheConfig struct {
	MaxEntries int
	TTL        time.Duration
}

// DefaultCacheConfig returns the default cache configuration.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxEntries: 10000,
		TTL:        10 * time.Minute,
	}
}

// Config holds all provider configuration.
type Config struct {
	Cache       CacheConfig
	TablePrefix string // "core." for attached DuckDB, "" for native
	VecSize     int    // Embedding vector dimension; 0 = skip vec operations
	IsPostgres  bool   // true when CoreDB is PostgreSQL (affects vec column DDL)
	IsReadonly  bool   // true when CoreDB is read-only (rejects all writes)
}

// Provider is the DB-backed schema provider.
// It stores compiled GraphQL types in _schema_* tables and serves
// lookups via an LRU cache with DB fallback.
//
// When a *compiler.Compiler is set (via NewWithCompiler), the provider
// also implements CatalogManager — managing catalog lifecycle (add/remove/
// reload/disable/enable) with version-based skip-if-unchanged logic.
type Provider struct {
	pool     *db.Pool
	prefix   string
	embedder Embedder
	vecSize  int

	isPostgres bool
	isReadonly bool

	cache *schemaCache

	// Root type pointers, loaded on demand.
	queryType    *ast.Definition
	mutationType *ast.Definition
	rootsLoaded  bool

	// CatalogManager support: compiler for self-contained compilation,
	// catalogs map for runtime source handles only (all state in DB).
	compiler *compiler.Compiler
	catalogs map[string]sources.Catalog

	sf singleflight.Group
	mu sync.RWMutex
}

// New creates a new DB-backed schema provider.
//
// It initializes the _schema_meta table (if not exists), checks vector
// dimension consistency, and prepares the LRU cache.
//
// pool: *db.Pool for raw SQL access to CoreDB
// cfg: provider configuration (prefix, cache, vec size)
// embedder: optional (nil when embeddings not configured)
func New(ctx context.Context, pool *db.Pool, cfg Config, embedder Embedder) (*Provider, error) {
	p := &Provider{
		pool:       pool,
		prefix:     cfg.TablePrefix,
		embedder:   embedder,
		vecSize:    cfg.VecSize,
		isPostgres: cfg.IsPostgres,
		isReadonly: cfg.IsReadonly,
		cache:      newSchemaCache(cfg.Cache),
	}

	// Ensure _schema_settings table exists
	if err := p.ensureSettings(ctx); err != nil {
		return nil, fmt.Errorf("db provider init: %w", err)
	}

	// Check and migrate vector dimensions if needed
	if err := p.ensureVectorSize(ctx); err != nil {
		return nil, fmt.Errorf("db provider init: %w", err)
	}

	return p, nil
}

// NewWithCompiler creates a DB-backed provider with CatalogManager support.
// The compiler is used for self-contained catalog compilation via AddCatalog/ReloadCatalog.
func NewWithCompiler(ctx context.Context, pool *db.Pool, cfg Config, embedder Embedder, c *compiler.Compiler) (*Provider, error) {
	p, err := New(ctx, pool, cfg, embedder)
	if err != nil {
		return nil, err
	}
	p.compiler = c
	p.catalogs = make(map[string]sources.Catalog)
	return p, nil
}

// IsReadonly returns whether this provider is in read-only mode.
func (p *Provider) IsReadonly() bool {
	return p.isReadonly
}

// TablePrefix returns the SQL table prefix (e.g. "core." for attached DuckDB).
func (p *Provider) TablePrefix() string {
	return p.prefix
}

// HasEmbeddings returns true when the provider has an embedder and vec columns.
func (p *Provider) HasEmbeddings() bool {
	return p.vecSize > 0 && p.embedder != nil
}

// VecSize returns the embedding vector dimension.
func (p *Provider) VecSize() int {
	return p.vecSize
}

// Description returns a static provider description.
func (p *Provider) Description(_ context.Context) string {
	return "DB-backed schema provider"
}

// SetDefinitionDescription updates a type's description and long description,
// and recomputes its embedding vector (if embedder is available).
func (p *Provider) SetDefinitionDescription(ctx context.Context, name, desc, longDesc string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set description: %w", err)
	}
	defer conn.Close()

	if p.vecSize > 0 && p.embedder != nil {
		synth := SyntheticDescription("", name, "", "", "")
		text := EmbeddingText(longDesc, desc, synth)
		vec, embErr := p.embedder.CreateEmbedding(ctx, text)
		if embErr != nil {
			return fmt.Errorf("set description embedding: %w", embErr)
		}
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true, vec=$4 WHERE name=$1`,
			p.table("_schema_types"),
		), name, desc, longDesc, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true WHERE name=$1`,
			p.table("_schema_types"),
		), name, desc, longDesc)
	}
	if err != nil {
		return fmt.Errorf("set description: %w", err)
	}

	p.cache.evictType(name)
	return nil
}

// SetFieldDescription updates a field's description and long description,
// and recomputes its embedding vector (if embedder is available).
func (p *Provider) SetFieldDescription(ctx context.Context, typeName, fieldName, desc, longDesc string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set field description: %w", err)
	}
	defer conn.Close()

	if p.vecSize > 0 && p.embedder != nil {
		synth := SyntheticDescription("", fieldName, typeName, "", "")
		text := EmbeddingText(longDesc, desc, synth)
		vec, embErr := p.embedder.CreateEmbedding(ctx, text)
		if embErr != nil {
			return fmt.Errorf("set field description embedding: %w", embErr)
		}
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$3, long_description=$4, is_summarized=true, vec=$5 WHERE type_name=$1 AND name=$2`,
			p.table("_schema_fields"),
		), typeName, fieldName, desc, longDesc, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$3, long_description=$4, is_summarized=true WHERE type_name=$1 AND name=$2`,
			p.table("_schema_fields"),
		), typeName, fieldName, desc, longDesc)
	}
	if err != nil {
		return fmt.Errorf("set field description: %w", err)
	}

	p.cache.evictType(typeName)
	return nil
}

// SetModuleDescription updates a module's description and long description,
// and recomputes its embedding vector (if embedder is available).
func (p *Provider) SetModuleDescription(ctx context.Context, name, desc, longDesc string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set module description: %w", err)
	}
	defer conn.Close()

	if p.vecSize > 0 && p.embedder != nil {
		synth := SyntheticDescription("module", name, "", "", "")
		text := EmbeddingText(longDesc, desc, synth)
		vec, embErr := p.embedder.CreateEmbedding(ctx, text)
		if embErr != nil {
			return fmt.Errorf("set module description embedding: %w", embErr)
		}
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true, vec=$4 WHERE name=$1`,
			p.table("_schema_modules"),
		), name, desc, longDesc, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true WHERE name=$1`,
			p.table("_schema_modules"),
		), name, desc, longDesc)
	}
	if err != nil {
		return fmt.Errorf("set module description: %w", err)
	}
	return nil
}

// SetCatalogDescription updates a catalog's description and long description,
// and recomputes its embedding vector (if embedder is available).
func (p *Provider) SetCatalogDescription(ctx context.Context, name, desc, longDesc string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set catalog description: %w", err)
	}
	defer conn.Close()

	if p.vecSize > 0 && p.embedder != nil {
		synth := SyntheticDescription("catalog", name, "", "", "")
		text := EmbeddingText(longDesc, desc, synth)
		vec, embErr := p.embedder.CreateEmbedding(ctx, text)
		if embErr != nil {
			return fmt.Errorf("set catalog description embedding: %w", embErr)
		}
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true, vec=$4 WHERE name=$1`,
			p.table("_schema_catalogs"),
		), name, desc, longDesc, vec)
	} else {
		_, err = p.execWrite(ctx, conn, fmt.Sprintf(
			`UPDATE %s SET description=$2, long_description=$3, is_summarized=true WHERE name=$1`,
			p.table("_schema_catalogs"),
		), name, desc, longDesc)
	}
	if err != nil {
		return fmt.Errorf("set catalog description: %w", err)
	}
	return nil
}

// ResetSummarized clears the is_summarized flag so the summarizer re-processes entities.
//
// scope controls granularity:
//   - "all"     — reset everything (name is ignored)
//   - "catalog" — reset all types/fields/modules in the named catalog, plus the catalog record
//   - "type"    — reset the named type and its fields
func (p *Provider) ResetSummarized(ctx context.Context, name, scope string) (int, error) {
	if p.isReadonly {
		return 0, ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("reset summarized: %w", err)
	}
	defer conn.Close()

	var total int64
	exec := func(query string, args ...any) {
		if err != nil {
			return
		}
		var res interface{ RowsAffected() (int64, error) }
		res, err = p.execWrite(ctx, conn, query, args...)
		if err == nil {
			n, _ := res.RowsAffected()
			total += n
		}
	}

	switch scope {
	case "type":
		if name == "" {
			return 0, fmt.Errorf("reset summarized: name is required for scope=type")
		}
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE name=$1`, p.table("_schema_types")), name)
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE type_name=$1`, p.table("_schema_fields")), name)

	case "catalog":
		if name == "" {
			return 0, fmt.Errorf("reset summarized: name is required for scope=catalog")
		}
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE name=$1`, p.table("_schema_catalogs")), name)
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE catalog=$1`, p.table("_schema_types")), name)
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE catalog=$1`, p.table("_schema_fields")), name)
		exec(fmt.Sprintf(
			`UPDATE %s SET is_summarized=false WHERE name IN (
				SELECT DISTINCT m.name FROM %s m
				INNER JOIN %s mtc ON m.name = mtc.module_name
				WHERE mtc.catalog_name = $1
			)`, p.table("_schema_modules"), p.table("_schema_modules"), p.table("_schema_module_type_catalogs"),
		), name)

	default: // "all"
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE is_summarized=true`, p.table("_schema_catalogs")))
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE is_summarized=true`, p.table("_schema_types")))
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE is_summarized=true`, p.table("_schema_fields")))
		exec(fmt.Sprintf(`UPDATE %s SET is_summarized=false WHERE is_summarized=true`, p.table("_schema_modules")))
	}

	if err != nil {
		return 0, fmt.Errorf("reset summarized: %w", err)
	}
	return int(total), nil
}

// InvalidateCatalog evicts all cached entries for the given catalog name.
// Also evicts root types (Query, Mutation) since their fields may include
// module fields from the invalidated catalog.
// Types from other catalogs that had extension fields from this catalog
// will be refreshed from DB on next access (cache TTL or LRU eviction).
func (p *Provider) InvalidateCatalog(catalog string) {
	p.cache.invalidateCatalog(catalog)
	// Root types aggregate fields from all modules; must be evicted
	// so they're re-read from DB without the dropped catalog's fields.
	p.cache.evictType("Query")
	p.cache.evictType("Mutation")
	// Reset root type pointers so they're reloaded on next access.
	p.mu.Lock()
	p.rootsLoaded = false
	p.queryType = nil
	p.mutationType = nil
	p.mu.Unlock()
}

// InvalidateAll purges the entire cache.
func (p *Provider) InvalidateAll() {
	p.cache.invalidateAll()
	p.mu.Lock()
	p.rootsLoaded = false
	p.queryType = nil
	p.mutationType = nil
	p.mu.Unlock()
}

// IncrementSchemaVersion atomically increments the schema version counter
// in _schema_settings and returns the new value. Called by management node
// after AddCatalog/RemoveCatalog/ReloadCatalog.
func (p *Provider) IncrementSchemaVersion(ctx context.Context) (int64, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("increment schema version: %w", err)
	}
	defer conn.Close()

	// Two-step update+select: RETURNING is not supported for PostgreSQL tables
	// accessed via DuckDB's postgres scanner.
	tbl := p.table("_schema_settings")
	// Cast to JSON for PostgreSQL (value column is jsonb), VARCHAR for DuckDB.
	castType := "VARCHAR"
	if p.isPostgres {
		castType = "JSON"
	}
	_, err = conn.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET value = CAST((CAST(TRIM(value, '"') AS BIGINT) + 1) AS %s)
		 WHERE key = 'schema_version'`, tbl, castType))
	if err != nil {
		return 0, fmt.Errorf("increment schema version: %w", err)
	}

	var newVersion int64
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT CAST(TRIM(value, '"') AS BIGINT) FROM %s WHERE key = 'schema_version'`, tbl,
	)).Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("increment schema version (read): %w", err)
	}
	return newVersion, nil
}

// GetSchemaVersion returns the current schema version counter.
func (p *Provider) GetSchemaVersion(ctx context.Context) (int64, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("get schema version: %w", err)
	}
	defer conn.Close()

	var version int64
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT CAST(TRIM(value, '"') AS BIGINT) FROM %s WHERE key = 'schema_version'`,
		p.table("_schema_settings"),
	)).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("get schema version: %w", err)
	}
	return version, nil
}
