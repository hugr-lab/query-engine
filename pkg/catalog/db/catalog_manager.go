package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
)

// Compile-time check that Provider implements catalog.CatalogManager.
var _ catalog.CatalogManager = (*Provider)(nil)

// ErrNoCompiler is returned when CatalogManager methods are called on a
// Provider created without a compiler (use NewWithCompiler).
var ErrNoCompiler = errors.New("db provider: compiler not set (use NewWithCompiler)")

// catalogVersionWithOptions computes a composite version string that includes
// both the source version and a fingerprint of the compile options that affect
// the compiled schema output (engine type, prefix, readOnly, asModule, isExtension).
// This ensures recompilation when options change even if the source version is unchanged.
func catalogVersionWithOptions(sourceVersion string, opts base.Options) string {
	h := sha256.New()
	h.Write([]byte(opts.Name))
	h.Write([]byte(opts.EngineType))
	h.Write([]byte(opts.Prefix))
	if opts.ReadOnly {
		h.Write([]byte("ro"))
	}
	if opts.AsModule {
		h.Write([]byte("mod"))
	}
	if opts.IsExtension {
		h.Write([]byte("ext"))
	}
	return fmt.Sprintf("%s+%x", sourceVersion, h.Sum(nil)[:4])
}

// compileAndApply compiles a catalog source and persists the result to the DB.
// Update() handles the transaction, metadata reconciliation, and cache invalidation.
// Returns the dependency list extracted from the compiled output.
func (p *Provider) compileAndApply(ctx context.Context, name string, catalog sources.Catalog) ([]string, error) {
	if p.compiler == nil {
		return nil, ErrNoCompiler
	}

	opts := catalog.CompileOptions()
	compiled, err := p.compiler.Compile(ctx, p, catalog, opts)
	if err != nil {
		return nil, fmt.Errorf("compile catalog %q: %w", name, err)
	}

	// Use UpdateWithCatalogAndOptions to ensure catalog record is created even for
	// extension-only catalogs that don't produce definitions with @catalog,
	// and to persist source metadata (source_type, prefix, as_module, read_only).
	if err := p.UpdateWithCatalogAndOptions(ctx, compiled, name, &opts); err != nil {
		return nil, fmt.Errorf("persist catalog %q: %w", name, err)
	}

	var deps []string
	if dc, ok := compiled.(base.DependentCompiledCatalog); ok {
		deps = dc.Dependencies()
	}
	return deps, nil
}

// AddCatalog adds or updates a catalog with version-aware skip logic.
//
// Flow:
//   - Get version from source, check _schema_catalogs for existing record
//   - Version match → skip compilation, store source handle only
//   - Version mismatch → suspend dependents → drop schema objects → recompile
//   - New catalog → compile and persist
//   - After any change → reactivate suspended catalogs
func (p *Provider) AddCatalog(ctx context.Context, name string, catalog sources.Catalog) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	if p.compiler == nil {
		return ErrNoCompiler
	}

	// NOTE: We do NOT hold p.mu during compilation/persistence because
	// those operations call Update → InvalidateCatalog which also acquires p.mu.
	// The DB provides transactional consistency; p.mu only protects the catalogs map.

	srcVersion, err := catalog.Version(ctx)
	if err != nil {
		return fmt.Errorf("get catalog version: %w", err)
	}
	version := catalogVersionWithOptions(srcVersion, catalog.CompileOptions())

	rec, err := p.GetCatalog(ctx, name)
	if err != nil {
		return fmt.Errorf("check catalog: %w", err)
	}

	if rec != nil && !rec.Suspended && rec.Version == version {
		// Version match (including compile options) — skip compilation, just store source handle.
		slog.Debug("catalog version unchanged, skipping compilation",
			"catalog", name, "version", version)
		p.mu.Lock()
		p.catalogs[name] = catalog
		p.mu.Unlock()
		return nil
	}

	// Need (re)compilation: new, suspended, or version/options mismatch.
	if rec != nil && !rec.Suspended {
		// Version mismatch — suspend dependents and drop existing schema objects.
		slog.Info("catalog version changed, recompiling",
			"catalog", name, "old_version", rec.Version, "new_version", version)
		if err := p.suspendDependents(ctx, name); err != nil {
			slog.Error("failed to suspend dependents", "catalog", name, "error", err)
		}
		if err := p.dropCatalogSchemaObjects(ctx, name); err != nil {
			return fmt.Errorf("drop old schema objects: %w", err)
		}
	} else if rec != nil && rec.Suspended {
		// Reactivating a previously suspended catalog — clean stale objects.
		if err := p.dropCatalogSchemaObjects(ctx, name); err != nil {
			slog.Debug("drop suspended catalog objects (best-effort)",
				"catalog", name, "error", err)
		}
	}

	// Compile and persist.
	deps, err := p.compileAndApply(ctx, name, catalog)
	if err != nil {
		return err
	}

	// Update version with compile options fingerprint.
	if err := p.SetCatalogVersion(ctx, name, version); err != nil {
		return fmt.Errorf("set catalog version: %w", err)
	}
	// Ensure catalog is not marked suspended.
	if rec != nil && rec.Suspended {
		if err := p.SetCatalogSuspended(ctx, name, false); err != nil {
			return fmt.Errorf("unsuspend catalog: %w", err)
		}
	}

	p.mu.Lock()
	p.catalogs[name] = catalog
	p.mu.Unlock()

	slog.Info("catalog added", "catalog", name, "version", version, "dependencies", deps)

	// Reactivate suspended catalogs whose dependencies may now be satisfied.
	p.reactivateSuspended(ctx)

	return nil
}

// RemoveCatalog removes a catalog: suspends dependents, deletes all schema
// objects and the catalog record, and removes the source handle.
func (p *Provider) RemoveCatalog(ctx context.Context, name string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	if p.compiler == nil {
		return ErrNoCompiler
	}

	// Suspend dependents before removing (preserves their catalog records).
	if err := p.suspendDependents(ctx, name); err != nil {
		slog.Error("failed to suspend dependents during remove", "catalog", name, "error", err)
	}

	// Full delete: schema objects + catalog record + dependency metadata.
	if err := p.DropCatalog(ctx, name, true); err != nil {
		return fmt.Errorf("remove catalog %q: %w", name, err)
	}

	p.mu.Lock()
	delete(p.catalogs, name)
	p.mu.Unlock()

	slog.Info("catalog removed", "catalog", name)
	return nil
}

// ExistsCatalog checks whether a catalog exists, first in the in-memory
// source map, then falling back to the _schema_catalogs table.
func (p *Provider) ExistsCatalog(name string) bool {
	p.mu.RLock()
	_, ok := p.catalogs[name]
	p.mu.RUnlock()
	if ok {
		return true
	}

	// Fallback: check DB.
	rec, err := p.GetCatalog(context.Background(), name)
	if err != nil {
		return false
	}
	return rec != nil
}

// ReloadCatalog reloads a catalog by name. If the source supports incremental
// changes (IncrementalCatalog), only the delta is compiled and applied.
// Otherwise falls back to full recompilation (drop + recompile).
func (p *Provider) ReloadCatalog(ctx context.Context, name string) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	if p.compiler == nil {
		return ErrNoCompiler
	}

	p.mu.RLock()
	source, ok := p.catalogs[name]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("catalog %q not found", name)
	}

	// Refresh source data if the catalog supports it.
	if rc, ok := source.(sources.ReloadableCatalog); ok {
		if err := rc.Reload(ctx); err != nil {
			return fmt.Errorf("reload source: %w", err)
		}
	}

	// Check version (including compile options) — skip if unchanged.
	srcVersion, err := source.Version(ctx)
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	newVersion := catalogVersionWithOptions(srcVersion, source.CompileOptions())

	rec, err := p.GetCatalog(ctx, name)
	if err != nil {
		return fmt.Errorf("check catalog: %w", err)
	}
	if rec != nil && !rec.Suspended && rec.Version == newVersion {
		slog.Debug("catalog version unchanged, skipping reload",
			"catalog", name, "version", newVersion)
		return nil
	}

	// Try incremental compilation if supported.
	if ic, ok := source.(sources.IncrementalCatalog); ok && rec != nil {
		if err := p.reloadIncremental(ctx, name, rec.Version, ic); err != nil {
			slog.Warn("incremental reload failed, falling back to full recompilation",
				"catalog", name, "error", err)
		} else {
			return nil
		}
	}

	// Full recompilation fallback.
	return p.reloadFull(ctx, name, source)
}

// reloadIncremental attempts an incremental reload using IncrementalCatalog.Changes().
func (p *Provider) reloadIncremental(ctx context.Context, name, fromVersion string, ic sources.IncrementalCatalog) error {
	changes, newVersion, err := ic.Changes(ctx, fromVersion)
	if err != nil {
		return fmt.Errorf("get changes: %w", err)
	}

	compiled, err := p.compiler.CompileChanges(ctx, p, ic, changes, ic.CompileOptions())
	if err != nil {
		return fmt.Errorf("compile changes: %w", err)
	}

	if err := p.Update(ctx, compiled); err != nil {
		return fmt.Errorf("apply changes: %w", err)
	}

	compositeVersion := catalogVersionWithOptions(newVersion, ic.CompileOptions())
	if err := p.SetCatalogVersion(ctx, name, compositeVersion); err != nil {
		return fmt.Errorf("set version: %w", err)
	}

	slog.Info("catalog reloaded incrementally", "catalog", name, "version", compositeVersion)
	return nil
}

// reloadFull drops existing schema objects and recompiles from scratch.
func (p *Provider) reloadFull(ctx context.Context, name string, source sources.Catalog) error {
	// Suspend dependents before dropping.
	if err := p.suspendDependents(ctx, name); err != nil {
		slog.Error("failed to suspend dependents during reload", "catalog", name, "error", err)
	}

	if err := p.dropCatalogSchemaObjects(ctx, name); err != nil {
		return fmt.Errorf("drop old objects: %w", err)
	}

	deps, err := p.compileAndApply(ctx, name, source)
	if err != nil {
		return fmt.Errorf("full recompilation: %w", err)
	}

	srcVersion, _ := source.Version(ctx)
	compositeVersion := catalogVersionWithOptions(srcVersion, source.CompileOptions())
	if err := p.SetCatalogVersion(ctx, name, compositeVersion); err != nil {
		return fmt.Errorf("set version: %w", err)
	}

	slog.Info("catalog reloaded (full recompilation)",
		"catalog", name, "version", compositeVersion, "dependencies", deps)

	p.reactivateSuspended(ctx)
	return nil
}

// DisableCatalog sets the disabled flag on a catalog without removing data.
func (p *Provider) DisableCatalog(ctx context.Context, name string) error {
	if err := p.SetCatalogDisabled(ctx, name, true); err != nil {
		return fmt.Errorf("disable catalog: %w", err)
	}
	p.InvalidateCatalog(name)
	return nil
}

// EnableCatalog clears the disabled flag on a catalog.
func (p *Provider) EnableCatalog(ctx context.Context, name string) error {
	if err := p.SetCatalogDisabled(ctx, name, false); err != nil {
		return fmt.Errorf("enable catalog: %w", err)
	}
	p.InvalidateCatalog(name)
	return nil
}

// suspendDependents finds all catalogs that depend on the given name,
// removes their schema objects, and marks them as suspended.
//
// Dependency records in _schema_catalog_dependencies are preserved so
// reactivateSuspended can check whether all deps are satisfied.
func (p *Provider) suspendDependents(ctx context.Context, name string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("suspend dependents: %w", err)
	}
	defer conn.Close()

	// Find non-suspended catalogs that directly depend on `name`.
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT cd.catalog_name FROM %s cd
		 JOIN %s c ON cd.catalog_name = c.name
		 WHERE cd.depends_on = $1 AND c.suspended = false`,
		p.table("_schema_catalog_dependencies"), p.table("_schema_catalogs"),
	), name)
	if err != nil {
		return fmt.Errorf("suspend dependents query: %w", err)
	}

	var dependents []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			continue
		}
		dependents = append(dependents, dep)
	}
	rows.Close()

	for _, depName := range dependents {
		slog.Info("suspending dependent catalog",
			"catalog", depName, "dependency", name)

		// Recursively suspend catalogs that depend on this dependent.
		if err := p.suspendDependents(ctx, depName); err != nil {
			slog.Error("failed to recursively suspend dependents",
				"catalog", depName, "error", err)
		}

		// Drop schema objects (preserves catalog record + dependency metadata).
		if err := p.dropCatalogSchemaObjects(ctx, depName); err != nil {
			slog.Error("failed to drop dependent schema objects",
				"catalog", depName, "error", err)
		}

		// Mark as suspended in DB.
		if err := p.SetCatalogSuspended(ctx, depName, true); err != nil {
			slog.Error("failed to mark catalog as suspended",
				"catalog", depName, "error", err)
		}
	}

	return nil
}

// reactivateSuspended recompiles suspended catalogs whose dependencies
// are all present and active. Called after AddCatalog to restore
// catalogs that were suspended due to missing dependencies.
func (p *Provider) reactivateSuspended(ctx context.Context) {
	catalogs, err := p.ListCatalogs(ctx)
	if err != nil {
		slog.Error("failed to list catalogs for reactivation", "error", err)
		return
	}

	for _, rec := range catalogs {
		if !rec.Suspended {
			continue
		}

		p.mu.RLock()
		source, ok := p.catalogs[rec.Name]
		p.mu.RUnlock()
		if !ok {
			slog.Debug("skipping reactivation: no source handle",
				"catalog", rec.Name)
			continue
		}

		satisfied, err := p.allDependenciesSatisfied(ctx, rec.Name)
		if err != nil {
			slog.Error("failed to check dependencies for reactivation",
				"catalog", rec.Name, "error", err)
			continue
		}
		if !satisfied {
			slog.Info("skipping reactivation: dependencies not satisfied",
				"catalog", rec.Name)
			continue
		}

		slog.Info("reactivating suspended catalog", "catalog", rec.Name)

		deps, err := p.compileAndApply(ctx, rec.Name, source)
		if err != nil {
			slog.Error("failed to reactivate catalog",
				"catalog", rec.Name, "error", err)
			continue
		}

		srcVersion, _ := source.Version(ctx)
		compositeVersion := catalogVersionWithOptions(srcVersion, source.CompileOptions())
		_ = p.SetCatalogVersion(ctx, rec.Name, compositeVersion)
		_ = p.SetCatalogSuspended(ctx, rec.Name, false)

		p.mu.Lock()
		p.catalogs[rec.Name] = source // keep source handle after reactivation
		p.mu.Unlock()

		slog.Info("catalog reactivated",
			"catalog", rec.Name, "version", compositeVersion, "dependencies", deps)
	}
}

// allDependenciesSatisfied checks whether all catalogs that `name` depends on
// exist in _schema_catalogs and are not suspended.
func (p *Provider) allDependenciesSatisfied(ctx context.Context, name string) (bool, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	var unsatisfied int
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s cd
		 WHERE cd.catalog_name = $1
		   AND NOT EXISTS (
		     SELECT 1 FROM %s c
		     WHERE c.name = cd.depends_on AND c.suspended = false
		   )`,
		p.table("_schema_catalog_dependencies"), p.table("_schema_catalogs"),
	), name).Scan(&unsatisfied)
	if err != nil {
		return false, fmt.Errorf("check dependencies: %w", err)
	}

	return unsatisfied == 0, nil
}

// CleanOrphanedCatalogs detects catalogs in _schema_catalogs that have no
// corresponding record in data_sources and are not runtime catalogs, then
// removes them. This handles the case where a data source was deleted via
// GraphQL mutation while the engine was stopped — its schema objects would
// otherwise remain stale in _schema_* tables.
//
// Call after all runtime sources are registered via AddCatalog so that
// p.catalogs contains the full set of runtime catalog names to skip.
func (p *Provider) CleanOrphanedCatalogs(ctx context.Context) error {
	if p.isReadonly {
		return ErrReadOnly
	}
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("clean orphaned catalogs: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT c.name FROM %s c
		 LEFT JOIN %s ds ON ds.name = c.name
		 WHERE ds.name IS NULL AND c.name != '%s'`,
		p.table("_schema_catalogs"), p.table("data_sources"),
		SystemCatalogName,
	))
	if err != nil {
		return fmt.Errorf("clean orphaned catalogs query: %w", err)
	}

	var orphans []string
	p.mu.RLock()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if _, isRuntime := p.catalogs[name]; isRuntime {
			continue
		}
		orphans = append(orphans, name)
	}
	p.mu.RUnlock()
	rows.Close()

	for _, name := range orphans {
		slog.Info("removing orphaned catalog", "catalog", name)
		if err := p.DropCatalog(ctx, name, true); err != nil {
			slog.Error("failed to drop orphaned catalog", "catalog", name, "error", err)
		}
	}

	return nil
}

// dropCatalogSchemaObjects and deleteSchemaObjectsForCatalog are in drop.go.
