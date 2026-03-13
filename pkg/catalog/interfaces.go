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
type IncrementalCatalog = sources.IncrementalCatalog

type CatalogManager interface {
	// Load/Unload catalogs (for dynamic schema updates)
	AddCatalog(ctx context.Context, name string, catalog Catalog) error
	RemoveCatalog(ctx context.Context, name string) error
	ExistsCatalog(name string) bool
	// ReloadCatalog reloads a catalog. If the source supports incremental
	// changes (IncrementalCatalog), only the delta is compiled and applied.
	// Otherwise falls back to full recompilation.
	ReloadCatalog(ctx context.Context, name string) error
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
}
