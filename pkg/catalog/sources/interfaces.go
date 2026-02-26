package sources

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

type Catalog interface {
	compiler.Catalog

	Name() string
	Description() string
	// Version returns a version identifier for the catalog contents.
	// For file-based sources, this is a content hash (changes when files change).
	// For dynamic sources (URI, DB, HTTP), this is a timestamp (always recompiles).
	// For runtime sources, this is an embedded version constant.
	Version(ctx context.Context) (string, error)
	Engine() engines.Engine
}

type ReloadableCatalog interface {
	Catalog

	// Reload the catalog (e.g. after source changes). Returns an error if reloading fails.
	Reload(ctx context.Context) error
}

type ExtensionCatalog interface {
	Catalog

	base.ExtensionsSource
	Deps() []string // Names of other catalog extensions this extension depends on
}
