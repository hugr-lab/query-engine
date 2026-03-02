package db

import (
	"sync"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/vektah/gqlparser/v2/ast"
)

// cacheEntry holds a cached type definition along with its catalog name
// for tag-based invalidation.
type cacheEntry struct {
	def     *ast.Definition
	catalog string
}

// dirCacheEntry holds a cached directive definition.
type dirCacheEntry struct {
	def *ast.DirectiveDefinition
}

// schemaCache manages LRU caches for type and directive definitions.
type schemaCache struct {
	types      *expirable.LRU[string, *cacheEntry]
	directives *expirable.LRU[string, *dirCacheEntry]
	implements *expirable.LRU[string, []string] // interface/union name → implementor names

	// catalogIndex maps catalog name → set of cached type names.
	// Protected by mu.
	catalogIndex map[string]map[string]struct{}
	mu           sync.RWMutex
}

// newSchemaCache creates a new schema cache with the given configuration.
func newSchemaCache(cfg CacheConfig) *schemaCache {
	sc := &schemaCache{
		catalogIndex: make(map[string]map[string]struct{}),
	}
	sc.types = expirable.NewLRU(cfg.MaxEntries, sc.onEvict, cfg.TTL)
	sc.directives = expirable.NewLRU[string, *dirCacheEntry](cfg.MaxEntries, nil, cfg.TTL)
	sc.implements = expirable.NewLRU[string, []string](cfg.MaxEntries, nil, cfg.TTL)
	return sc
}

// onEvict is called when a type cache entry is evicted.
// It removes the entry from the catalogIndex.
func (sc *schemaCache) onEvict(key string, value *cacheEntry) {
	if value == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if names, ok := sc.catalogIndex[value.catalog]; ok {
		delete(names, key)
		if len(names) == 0 {
			delete(sc.catalogIndex, value.catalog)
		}
	}
}

// getType returns a cached type definition, or nil if not found.
func (sc *schemaCache) getType(name string) *ast.Definition {
	entry, ok := sc.types.Get(name)
	if !ok {
		return nil
	}
	return entry.def
}

// putType stores a type definition in the cache.
func (sc *schemaCache) putType(name, catalog string, def *ast.Definition) {
	sc.types.Add(name, &cacheEntry{def: def, catalog: catalog})
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.catalogIndex[catalog] == nil {
		sc.catalogIndex[catalog] = make(map[string]struct{})
	}
	sc.catalogIndex[catalog][name] = struct{}{}
}

// getDirective returns a cached directive definition, or nil if not found.
func (sc *schemaCache) getDirective(name string) *ast.DirectiveDefinition {
	entry, ok := sc.directives.Get(name)
	if !ok {
		return nil
	}
	return entry.def
}

// putDirective stores a directive definition in the cache.
func (sc *schemaCache) putDirective(name string, def *ast.DirectiveDefinition) {
	sc.directives.Add(name, &dirCacheEntry{def: def})
}

// getImplements returns cached implementor names for an interface/union, or nil if not cached.
func (sc *schemaCache) getImplements(name string) ([]string, bool) {
	names, ok := sc.implements.Get(name)
	return names, ok
}

// putImplements caches implementor names for an interface/union.
func (sc *schemaCache) putImplements(name string, implementors []string) {
	sc.implements.Add(name, implementors)
}

// evictType removes a single type from the cache.
func (sc *schemaCache) evictType(name string) {
	sc.types.Remove(name)
}

// invalidateCatalog removes all cached entries for the given catalog.
func (sc *schemaCache) invalidateCatalog(catalog string) {
	sc.mu.Lock()
	names := sc.catalogIndex[catalog]
	delete(sc.catalogIndex, catalog)
	sc.mu.Unlock()

	for name := range names {
		sc.types.Remove(name)
	}
	// Adding/removing types can change interface implementations
	sc.implements.Purge()
}

// invalidateAll purges the entire cache.
func (sc *schemaCache) invalidateAll() {
	sc.types.Purge()
	sc.directives.Purge()
	sc.implements.Purge()
	sc.mu.Lock()
	sc.catalogIndex = make(map[string]map[string]struct{})
	sc.mu.Unlock()
}
