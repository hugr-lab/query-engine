package schema

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Provider base.Provider

type Catalog interface {
	compiler.Catalog

	Name() string
	Description() string
}

type ReloadableCatalog interface {
	Catalog

	// Reload the catalog (e.g. after source changes). Returns an error if reloading fails.
	Reload(ctx context.Context) error
}

// CatalogChanger is a Catalog that can produce a new Catalog with changes since a given version.
type CatalogChanger interface {
	Catalog

	Version() string
	// Returns changes since the given version, or an error if the version is invalid.
	Changes(ctx context.Context, version string) (Catalog, error)
}

type ExtensionCatalog interface {
	Catalog

	base.ExtensionsSource
	Deps() []string // Names of other catalog extensions this extension depends on
}

type CatalogManager interface {
	// Load/Unload catalogs (for dynamic schema updates)
	AddCatalog(ctx context.Context, name string, catalog Catalog) error
	RemoveCatalog(ctx context.Context, name string) error
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
