package schema

import (
	"context"
	"errors"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
)

var _ CatalogManager = (*memoryCatalog)(nil)

type memoryCatalog struct {
	mu       sync.RWMutex
	catalogs map[string]registeredCatalog
	provider Provider
}

func newMemoryCatalogManager(provider Provider) *memoryCatalog {
	return &memoryCatalog{
		catalogs: make(map[string]registeredCatalog),
		provider: provider,
	}
}

// SetProvider replaces the Provider (e.g. on catalog change).
// NOT thread-safe — call under external lock.
func (c *memoryCatalog) SetProvider(p Provider) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.provider = p
}

type registeredCatalog struct {
	source  Catalog
	version string
}

var (
	ErrCatalogNotFound      = errors.New("catalog not found")
	ErrCatalogAlreadyExists = errors.New("catalog already exists")
)

// AddCatalog adds a new catalog to the manager. If a catalog with the same name already exists, it will be replaced.
func (c *memoryCatalog) AddCatalog(ctx context.Context, name string, catalog Catalog) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cat, ok := c.catalogs[name]
	if ok {
		return ErrCatalogAlreadyExists
	}

	c.catalogs[name] = cat
	err = c.incrementalUpdate(ctx, catalog)
	if err != nil {
		delete(c.catalogs, name)
		return err
	}
	return nil
}

func (c *memoryCatalog) RemoveCatalog(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.catalogs[name]
	if !ok {
		return ErrCatalogNotFound
	}
	err := c.removeCatalog(ctx, name)
	if err != nil {
		return err
	}
	delete(c.catalogs, name)
	return nil
}

func (c *memoryCatalog) incrementalUpdate(ctx context.Context, catalog Catalog) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("catalog provider does not support mutable operations, cannot update catalog")
	}
	// no change support, replace whole catalog
	compiled, err := compiler.Compile(ctx, p, catalog, catalog.CompileOptions())
	if err != nil {
		return err
	}
	return p.Update(ctx, compiled)
}

func (c *memoryCatalog) removeCatalog(ctx context.Context, name string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("catalog provider does not support mutable operations, cannot remove catalog")
	}
	return p.DropCatalog(ctx, name, true)
}
