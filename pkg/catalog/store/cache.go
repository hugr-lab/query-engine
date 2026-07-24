package store

import (
	"sync"
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/vektah/gqlparser/v2/ast"
)

// CacheConfig controls the read-side definition cache (the store's port of
// the compiled provider's schemaCache).
type CacheConfig struct {
	// MaxEntries is the LRU capacity; 0 selects the default.
	MaxEntries int
	// TTL bounds staleness the invalidation closure cannot see; 0 selects
	// the default.
	TTL time.Duration
	// Disabled turns the cache off entirely (every ForName resolves from
	// CoreDB).
	Disabled bool
}

// DefaultCacheConfig mirrors the compiled provider's defaults.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{MaxEntries: 10000, TTL: 10 * time.Minute}
}

// allSources is the pseudo-source of definitions built from the WHOLE
// active-source set (module roots, shared types): every invalidation evicts
// its bucket.
const allSources = "*"

// defEntry is one cached compiled definition plus the data sources it was
// built from — the index membership used for scoped invalidation.
type defEntry struct {
	def     *ast.Definition
	sources []string
}

// defCache is an expirable LRU over compiled definitions keyed by type name,
// with a source→names index scoping invalidation to the sources a definition
// actually read rows from. A nil *defCache is valid and inert (cache
// disabled). Cached *ast.Definition values are shared with callers — the
// same read-only contract the compiled provider's cache has.
type defCache struct {
	types *expirable.LRU[string, *defEntry]

	mu          sync.Mutex
	sourceIndex map[string]map[string]struct{}
}

// newDefCache builds the cache; zero-valued fields take the defaults, and
// Disabled returns nil (all methods are nil-receiver safe).
func newDefCache(cfg CacheConfig) *defCache {
	if cfg.Disabled {
		return nil
	}
	def := DefaultCacheConfig()
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = def.MaxEntries
	}
	if cfg.TTL <= 0 {
		cfg.TTL = def.TTL
	}
	c := &defCache{sourceIndex: map[string]map[string]struct{}{}}
	c.types = expirable.NewLRU(cfg.MaxEntries, c.onEvict, cfg.TTL)
	return c
}

// onEvict drops the evicted entry's index memberships (also fired by Remove).
func (c *defCache) onEvict(name string, e *defEntry) {
	if e == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, src := range e.sources {
		if names, ok := c.sourceIndex[src]; ok {
			delete(names, name)
			if len(names) == 0 {
				delete(c.sourceIndex, src)
			}
		}
	}
}

func (c *defCache) get(name string) *ast.Definition {
	if c == nil {
		return nil
	}
	e, ok := c.types.Get(name)
	if !ok {
		return nil
	}
	return e.def
}

// put installs a definition under its source memberships. stillValid is the
// caller's invalidation-clock check (Store.gen captured when the resolution
// started): it is verified before indexing AND re-verified after the LRU add,
// so an invalidation racing the install either sees the indexed name or the
// post-add check removes the stale entry. The memberships are re-asserted
// AFTER the add too — the LRU's background expiry can evict the PREVIOUS
// entry under this name between our index write and the add, and its onEvict
// would strip the fresh memberships.
func (c *defCache) put(name string, sources []string, def *ast.Definition, stillValid func() bool) {
	if c == nil {
		return
	}
	index := func() {
		for _, src := range sources {
			if c.sourceIndex[src] == nil {
				c.sourceIndex[src] = map[string]struct{}{}
			}
			c.sourceIndex[src][name] = struct{}{}
		}
	}
	c.mu.Lock()
	if !stillValid() {
		c.mu.Unlock()
		return
	}
	index()
	c.mu.Unlock()

	c.types.Add(name, &defEntry{def: def, sources: sources})

	c.mu.Lock()
	index()
	c.mu.Unlock()
	if !stillValid() {
		c.types.Remove(name)
	}
}

// invalidate evicts everything a write to the source could have changed: its
// index bucket, the allSources bucket, and any remaining key alsoEvict
// matches (the write-side family closure for cross-source absence
// dependencies — see Store.invalidateSource).
func (c *defCache) invalidate(source string, alsoEvict func(name string) bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	names := make([]string, 0, len(c.sourceIndex[source])+len(c.sourceIndex[allSources]))
	for _, bucket := range []map[string]struct{}{c.sourceIndex[source], c.sourceIndex[allSources]} {
		for name := range bucket {
			names = append(names, name)
		}
	}
	delete(c.sourceIndex, source)
	delete(c.sourceIndex, allSources)
	c.mu.Unlock()

	for _, name := range names {
		c.types.Remove(name)
	}
	if alsoEvict == nil {
		return
	}
	for _, name := range c.types.Keys() {
		if alsoEvict(name) {
			c.types.Remove(name)
		}
	}
}

// evict drops a single cached definition by name — the targeted invalidation
// for an annotation write, which changes only that one type's assembled
// definition (its own description and its fields'/arguments' descriptions).
// Remove fires onEvict, which strips the entry's source-index memberships.
func (c *defCache) evict(name string) {
	if c == nil {
		return
	}
	c.types.Remove(name)
}

// evictFamily drops a base definition plus every cached name isDerived reports
// as derived from it — the write-side closure for a field curation, whose
// description is INHERITED by the derived types' same-named fields.
func (c *defCache) evictFamily(base string, isDerived func(name string) bool) {
	if c == nil {
		return
	}
	c.types.Remove(base)
	for _, name := range c.types.Keys() {
		if isDerived(name) {
			c.types.Remove(name)
		}
	}
}

// invalidateAll purges the cache.
func (c *defCache) invalidateAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.sourceIndex = map[string]map[string]struct{}{}
	c.mu.Unlock()
	c.types.Purge()
}
