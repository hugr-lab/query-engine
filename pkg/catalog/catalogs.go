package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
)

var _ CatalogManager = (*memoryCatalog)(nil)

type memoryCatalog struct {
	mu       sync.RWMutex
	catalogs map[string]registeredCatalog
	provider Provider
	compiler *compiler.Compiler
}

func newMemoryCatalogManager(provider Provider, compiler *compiler.Compiler) *memoryCatalog {
	return &memoryCatalog{
		catalogs: make(map[string]registeredCatalog),
		provider: provider,
		compiler: compiler,
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
	source       Catalog
	version      string
	dependencies []string // catalogs this one depends on (from compilation)
	suspended    bool     // true when removed from provider because a dependency was removed
}

var (
	ErrCatalogNotFound      = errors.New("catalog not found")
	ErrCatalogAlreadyExists = errors.New("catalog already exists")
)

// AddCatalog adds a new catalog to the manager. If a catalog with the same name already exists, it will be replaced.
func (c *memoryCatalog) AddCatalog(ctx context.Context, name string, catalog Catalog) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reg, ok := c.catalogs[name]; ok && !reg.suspended {
		return ErrCatalogAlreadyExists
	}

	deps, err := c.compileAndApply(ctx, name, catalog)
	if err != nil {
		return err
	}
	version, _ := catalog.Version(ctx)
	slog.Info("catalog added", "catalog", name, "dependencies", deps, "version", version)
	c.catalogs[name] = registeredCatalog{source: catalog, version: version, dependencies: deps}

	// Re-activate any suspended catalogs whose dependencies are now all satisfied.
	c.reactivateSuspended(ctx)
	return nil
}

func (c *memoryCatalog) RemoveCatalog(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reg, ok := c.catalogs[name]
	if !ok {
		return ErrCatalogNotFound
	}

	// Suspend dependent catalogs first (remove from provider, keep source for re-activation).
	c.suspendDependents(ctx, name)

	if !reg.suspended {
		_ = c.removeCatalog(ctx, name)
	}
	delete(c.catalogs, name)
	return nil
}

func (c *memoryCatalog) ExistsCatalog(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.catalogs[name]
	return ok
}

// ReloadCatalog reloads a catalog by name. If the source supports incremental
// changes (IncrementalCatalog), only the delta is compiled and applied.
// Otherwise falls back to full recompilation (drop + recompile).
func (c *memoryCatalog) ReloadCatalog(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reg, ok := c.catalogs[name]
	if !ok {
		return ErrCatalogNotFound
	}
	if reg.suspended {
		return errors.New("cannot reload suspended catalog")
	}

	// Refresh source data if the catalog supports it.
	if rc, ok := reg.source.(ReloadableCatalog); ok {
		if err := rc.Reload(ctx); err != nil {
			return fmt.Errorf("reload source: %w", err)
		}
	}

	// Check version — skip if unchanged.
	newVersion, err := reg.source.Version(ctx)
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	if newVersion == reg.version {
		slog.Debug("catalog version unchanged, skipping reload", "catalog", name, "version", newVersion)
		return nil
	}

	// Try incremental compilation if supported.
	if ic, ok := reg.source.(IncrementalCatalog); ok {
		if err := c.reloadIncremental(ctx, name, &reg, ic); err != nil {
			slog.Warn("incremental reload failed, falling back to full recompilation",
				"catalog", name, "error", err)
		} else {
			return nil
		}
	}

	// Full recompilation fallback: drop existing catalog, recompile from scratch.
	return c.reloadFull(ctx, name, &reg)
}

// reloadIncremental attempts an incremental reload using IncrementalCatalog.Changes().
// On success, updates the registered catalog entry with the new version.
func (c *memoryCatalog) reloadIncremental(ctx context.Context, name string, reg *registeredCatalog, ic IncrementalCatalog) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("provider does not support mutable operations")
	}

	changes, newVersion, err := ic.Changes(ctx, reg.version)
	if err != nil {
		return fmt.Errorf("get changes: %w", err)
	}

	compiled, err := c.compiler.CompileChanges(ctx, p, reg.source, changes, reg.source.CompileOptions())
	if err != nil {
		return fmt.Errorf("compile changes: %w", err)
	}

	if err := p.Update(ctx, compiled); err != nil {
		return fmt.Errorf("apply changes: %w", err)
	}

	reg.version = newVersion
	c.catalogs[name] = *reg
	slog.Info("catalog reloaded incrementally", "catalog", name, "version", newVersion)
	return nil
}

// reloadFull drops the existing catalog from the provider and recompiles from scratch.
func (c *memoryCatalog) reloadFull(ctx context.Context, name string, reg *registeredCatalog) error {
	// Suspend dependents before dropping.
	c.suspendDependents(ctx, name)

	_ = c.removeCatalog(ctx, name)

	deps, err := c.compileAndApply(ctx, name, reg.source)
	if err != nil {
		return fmt.Errorf("full recompilation: %w", err)
	}

	version, _ := reg.source.Version(ctx)
	reg.version = version
	reg.dependencies = deps
	reg.suspended = false
	c.catalogs[name] = *reg
	slog.Info("catalog reloaded (full recompilation)", "catalog", name, "version", version)

	c.reactivateSuspended(ctx)
	return nil
}

// compileAndApply compiles a catalog and applies it to the provider.
// Returns the list of dependencies extracted from the compiled output.
func (c *memoryCatalog) compileAndApply(ctx context.Context, name string, catalog Catalog) ([]string, error) {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return nil, errors.New("catalog provider does not support mutable operations, cannot update catalog")
	}
	compiled, err := c.compiler.Compile(ctx, p, catalog, catalog.CompileOptions())
	if err != nil {
		return nil, err
	}
	if err := p.Update(ctx, compiled); err != nil {
		return nil, err
	}

	// Extract dependencies if available.
	var deps []string
	if dc, ok := compiled.(base.DependentCompiledCatalog); ok {
		deps = dc.Dependencies()
	}
	return deps, nil
}

// suspendDependents finds all catalogs that depend on the given catalog name,
// drops them from the provider, and marks them as suspended.
func (c *memoryCatalog) suspendDependents(ctx context.Context, name string) {
	for depName, reg := range c.catalogs {
		if reg.suspended || !slices.Contains(reg.dependencies, name) {
			continue
		}
		slog.Info("suspending dependent catalog", "catalog", depName, "dependency", name, "deps", reg.dependencies)
		// Drop from provider (best-effort: types may already be partially gone).
		_ = c.removeCatalog(ctx, depName)
		reg.suspended = true
		c.catalogs[depName] = reg
	}
}

// reactivateSuspended re-compiles and re-applies any suspended catalogs
// whose dependencies are all present and non-suspended.
func (c *memoryCatalog) reactivateSuspended(ctx context.Context) {
	for name, reg := range c.catalogs {
		if !reg.suspended {
			continue
		}
		if !c.allDependenciesSatisfied(reg.dependencies) {
			slog.Info("skipping reactivation: dependencies not satisfied", "catalog", name, "deps", reg.dependencies)
			continue
		}
		slog.Info("reactivating suspended catalog", "catalog", name, "deps", reg.dependencies)
		deps, err := c.compileAndApply(ctx, name, reg.source)
		if err != nil {
			slog.Error("failed to reactivate catalog", "catalog", name, "error", err)
			continue // leave suspended on error
		}
		slog.Info("catalog reactivated successfully", "catalog", name, "deps", deps)
		reg.suspended = false
		reg.dependencies = deps
		c.catalogs[name] = reg
	}
}

// allDependenciesSatisfied returns true if all dependency catalogs
// exist in the map and are not themselves suspended.
func (c *memoryCatalog) allDependenciesSatisfied(deps []string) bool {
	for _, dep := range deps {
		reg, ok := c.catalogs[dep]
		if !ok || reg.suspended {
			return false
		}
	}
	return true
}

func (c *memoryCatalog) removeCatalog(ctx context.Context, name string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("catalog provider does not support mutable operations, cannot remove catalog")
	}
	return p.DropCatalog(ctx, name, true)
}

// Description delegation — proxies to the underlying MutableProvider.

func (c *memoryCatalog) SetDefinitionDescription(ctx context.Context, name, desc, longDesc string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("provider does not support mutable operations")
	}
	return p.SetDefinitionDescription(ctx, name, desc, longDesc)
}

func (c *memoryCatalog) SetFieldDescription(ctx context.Context, typeName, fieldName, desc, longDesc string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("provider does not support mutable operations")
	}
	return p.SetFieldDescription(ctx, typeName, fieldName, desc, longDesc)
}

func (c *memoryCatalog) SetModuleDescription(ctx context.Context, name, desc, longDesc string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("provider does not support mutable operations")
	}
	return p.SetModuleDescription(ctx, name, desc, longDesc)
}

func (c *memoryCatalog) SetCatalogDescription(ctx context.Context, name, desc, longDesc string) error {
	p, ok := c.provider.(base.MutableProvider)
	if !ok {
		return errors.New("provider does not support mutable operations")
	}
	return p.SetCatalogDescription(ctx, name, desc, longDesc)
}
