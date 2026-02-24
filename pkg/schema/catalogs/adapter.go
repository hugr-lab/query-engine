package catalogs

import (
	"context"
	"sync"

	oldcatalogs "github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/schema"
)

// compile-time check
var _ schema.Manager = (*Adapter)(nil)

// Adapter wraps *catalogs.Service to satisfy the Catalogs interface.
// It bridges the old two-step API (NewCatalog + AddCatalog) into the
// single-step AddCatalog(ctx, name, prefix, engine, source, ...) call.
type Adapter struct {
	inner      *oldcatalogs.Service
	mu         sync.Mutex
	extensions map[string]struct{}
}

func NewAdapter(inner *oldcatalogs.Service) *Adapter {
	return &Adapter{inner: inner, extensions: make(map[string]struct{})}
}

func (a *Adapter) AddCatalog(ctx context.Context, name string, engine engines.Engine, catalog schema.Catalog) error {
	opts := catalog.CompileOptions()
	if opts.IsExtension {
		ext, err := a.inner.NewExtension(ctx, name, catalog.(sources.Source))
		if err != nil {
			return err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		a.extensions[name] = struct{}{}
		return a.inner.AddExtension(ctx, ext)
	}
	c, err := oldcatalogs.NewCatalog(ctx, name, opts.Prefix, engine, catalog.(sources.Source), opts.AsModule, opts.ReadOnly)
	if err != nil {
		return err
	}
	return a.inner.AddCatalog(ctx, name, c)
}

func (a *Adapter) RemoveCatalog(ctx context.Context, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, isExt := a.extensions[name]; isExt {
		delete(a.extensions, name)
		return a.inner.RemoveExtension(ctx, name)
	}
	return a.inner.RemoveCatalog(ctx, name)
}

func (a *Adapter) ExistsCatalog(name string) bool {
	return a.inner.ExistsCatalog(name)
}

func (a *Adapter) Engine(name string) (engines.Engine, error) {
	return a.inner.Engine(name)
}
