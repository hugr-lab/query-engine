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

// IncrementalCatalog is implemented by catalogs that support providing
// incremental schema changes instead of requiring full recompilation.
// This is the pull-based model: the catalog manager calls Changes() with
// the last known version and receives only the definitions that changed.
//
// Changes supports two granularities:
//   - Type-level: definitions with @drop (type removal) or no directive (type addition)
//   - Field-level: if the returned source also implements ExtensionsSource,
//     extensions provide field add/drop/replace and directive changes
//
// Use DiffSchemas to compute field-level changes between two schema snapshots.
type IncrementalCatalog interface {
	Catalog

	// Changes returns incremental DDL changes since fromVersion.
	// Returns:
	//   - changes: definitions with @drop or new additions;
	//     may also implement ExtensionsSource for field-level changes
	//   - newVersion: the version after these changes are applied
	//   - error: if changes cannot be computed (caller should fall back to full reload)
	Changes(ctx context.Context, fromVersion string) (changes base.DefinitionsSource, newVersion string, err error)
}
