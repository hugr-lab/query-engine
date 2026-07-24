package base

import (
	"context"
	"iter"

	"github.com/vektah/gqlparser/v2/ast"
)

// DefinitionsSource resolves types and directives with context support.
// A subset of Provider for functions that only need type resolution.
type DefinitionsSource interface {
	ForName(ctx context.Context, name string) *ast.Definition
	DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition

	// iteration
	Definitions(ctx context.Context) iter.Seq[*ast.Definition]
	DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition]
}

// ExtensionsSource extends DefinitionsSource with support for definition extensions.
type ExtensionsSource interface {
	DefinitionsSource

	// Extensions
	DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition]
	Extensions(ctx context.Context) iter.Seq[*ast.Definition]
}

// Provider is a read-only interface to a compiled schema.
// All methods accept context.Context for future storage-backed implementations.
type Provider interface {
	DefinitionsSource

	Description(ctx context.Context) string

	// Root operation types
	QueryType(ctx context.Context) *ast.Definition
	MutationType(ctx context.Context) *ast.Definition
	SubscriptionType(ctx context.Context) *ast.Definition

	// Type relationships (for validator: fragment spreading, interface checks)
	PossibleTypes(ctx context.Context, name string) iter.Seq[*ast.Definition]
	Implements(ctx context.Context, name string) iter.Seq[*ast.Definition]

	// types iteration (for introspection, meta-info)
	Types(ctx context.Context) iter.Seq2[string, *ast.Definition]
}

// MutableProvider is a mutable container for compiled schema definitions.
// It carries the compiler write path (per-source Update, DropCatalog) — the
// description-annotation surface is split out into SchemaAnnotator so a
// provider can curate descriptions without owning full compilation semantics.
type MutableProvider interface {
	Provider

	DropCatalog(ctx context.Context, name string, cascade bool) error
	Update(ctx context.Context, changes DefinitionsSource) error
}

// SchemaAnnotator curates human descriptions over an already-compiled schema.
// It is orthogonal to compilation: an annotator overrides the description /
// long-description of types, fields, arguments, modules and catalogs without
// re-running the compiler. The store applies these as an overlay at
// reconstruction time; the legacy DB provider persists them into its schema
// tables. Callers reach it via type assertion, like SuspendableProvider.
type SchemaAnnotator interface {
	// SetDefinitionDescription updates a type's description and long description.
	SetDefinitionDescription(ctx context.Context, name, desc, longDesc string) error
	// SetFieldDescription updates a field's description and long description.
	SetFieldDescription(ctx context.Context, typeName, fieldName, desc, longDesc string) error
	// SetArgumentDescription updates a field argument's description and long
	// description (typeName.fieldName identifies the field carrying the argument).
	SetArgumentDescription(ctx context.Context, typeName, fieldName, argName, desc, longDesc string) error
	// SetModuleDescription updates a module's description and long description.
	SetModuleDescription(ctx context.Context, name, desc, longDesc string) error
	// SetCatalogDescription updates a catalog's description and long description.
	SetCatalogDescription(ctx context.Context, name, desc, longDesc string) error
}

// SuspendableProvider is optionally implemented by providers that support
// suspending/resuming catalogs without physically removing schema data.
// The DB provider sets a suspended flag and invalidates the cache;
// the static provider falls back to DropCatalog.
type SuspendableProvider interface {
	SuspendCatalog(ctx context.Context, name string) error
	ResumeCatalog(ctx context.Context, name string) error
}
