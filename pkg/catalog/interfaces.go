package catalog

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
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
	Engine(name string) (engines.Engine, error)
}
