package catalog

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

type Provider base.Provider

// Aliases — canonical definitions live in pkg/catalog/sources/.
type Catalog = sources.Catalog
type ReloadableCatalog = sources.ReloadableCatalog
type ExtensionCatalog = sources.ExtensionCatalog

type CatalogManager interface {
	// Load/Unload catalogs (for dynamic schema updates)
	AddCatalog(ctx context.Context, name string, catalog Catalog) error
	RemoveCatalog(ctx context.Context, name string) error
	ExistsCatalog(name string) bool
	// ReloadCatalog re-reads the source and recompiles it in full. The
	// storage's version gate decides whether anything is actually rewritten.
	ReloadCatalog(ctx context.Context, name string) error
	// SuspendCatalog removes a catalog from the active schema without deleting
	// the registration. Used when a hugr-app becomes unreachable.
	SuspendCatalog(ctx context.Context, name string) error
	// ReactivateCatalog re-compiles and re-applies a previously suspended catalog.
	// The catalog source must be updated before calling this.
	ReactivateCatalog(ctx context.Context, name string, catalog Catalog) error
	// IsSuspended returns true if the named catalog exists but is suspended.
	IsSuspended(name string) bool
}

// ReplacingCatalogManager is optionally implemented by a CatalogManager whose
// AddCatalog REPLACES a catalog's stored schema in full: whatever the source
// stopped declaring is gone after the write, without a destructive pre-step.
//
// It decides how a RELOAD of a still-attached source is staged. Against such a
// manager the source can be detached softly — the catalog is suspended, its
// rows survive, and the re-add's version gate decides whether anything is
// rewritten at all, so reloading an unchanged source costs a flag flip instead
// of a full recompile (and, with an embedder, a full re-embedding).
//
// The compiled-schema provider is deliberately NOT one: its write path applies
// an incremental diff and its same-version path only clears the suspended
// flag, so a definition the source dropped would survive a soft reload. It
// must keep being dropped before it is re-added.
type ReplacingCatalogManager interface {
	ReplacesCatalogOnAdd() bool
}

// CatalogChecker reports whether a catalog currently has an active engine.
// It is the gate the hard-remove UDF gets from the engine (Service implements
// it), declared here so both catalog storages take the same type.
type CatalogChecker interface {
	ExistsCatalog(name string) bool
}

// VariableTransformer transforms query variables before parsing.
// The primary implementation is jqVariableTransformer (checks _jq key).
type VariableTransformer interface {
	TransformVariables(ctx context.Context, vars map[string]any) (map[string]any, error)
}

// Querier executes GraphQL queries.
// Used by VariableTransformer for the jq queryHugr function.
type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error)
}

type Manager interface {
	// Catalog lifecycle
	AddCatalog(ctx context.Context, name string, catalog Catalog) error
	RemoveCatalog(ctx context.Context, name string) error
	ExistsCatalog(name string) bool
	ReloadCatalog(ctx context.Context, name string) error
	Engine(name string) (engines.Engine, error)
	// RegisterEngine adds an engine for planner routing without compilation.
	// Used by cluster workers where schema is already compiled in CoreDB.
	RegisterEngine(name string, engine engines.Engine)
	// SuspendCatalog marks a catalog as suspended (unavailable).
	SuspendCatalog(ctx context.Context, name string) error
	// ReactivateCatalog reactivates a suspended catalog with a new source.
	ReactivateCatalog(ctx context.Context, name string, catalog Catalog) error
	// IsSuspended returns true if the catalog is suspended.
	IsSuspended(name string) bool
}
