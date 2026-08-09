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

// MutableProvider — the compiler write path (per-source Update + DropCatalog)
// — is gone with design-036: applying a compiled DDL feed to a schema container
// was what the GENERATE / ASSEMBLE pipeline produced, and the catalog storage
// persists a physical model instead. Curating descriptions was never part of it
// and lives on in LogicalAnnotator / GraphQLAnnotator below.

// LogicalAnnotator curates human descriptions over the LOGICAL model — the
// source-level entities the CoreDB core.catalog.* views expose: modules, data
// objects, residual source-defined types, object fields (including relation
// navigation fields), functions and data sources. The keys mirror exactly how
// those views select their overlay from catalog.annotations, so a curation
// written here also feeds the vector search MCP discovery runs over the same
// rows. The store applies these as a description overlay at reconstruction
// time; callers reach it via type assertion, like SuspendableProvider.
type LogicalAnnotator interface {
	// SetModuleDescription curates a module's description (entity_kind=module).
	SetModuleDescription(ctx context.Context, module, desc, longDesc string) error
	// SetDataObjectDescription curates a data object's description
	// (entity_kind=data_object, key = compiled object type name).
	SetDataObjectDescription(ctx context.Context, name, desc, longDesc string) error
	// SetSourceTypeDescription curates a residual source-defined type's
	// description (entity_kind=type).
	SetSourceTypeDescription(ctx context.Context, name, desc, longDesc string) error
	// SetObjectFieldDescription curates a data-object field's description,
	// including relation navigation fields (entity_kind=field, key=owner.field).
	SetObjectFieldDescription(ctx context.Context, owner, field, desc, longDesc string) error
	// SetFunctionDescription curates ONE function's description. The key is
	// module.name (or name when module is empty; parent=module) and the
	// entity_kind is the function's KIND — "function", "mutation" or
	// "subscription", the value catalog.functions.kind stores — because the same
	// name in two root namespaces is two operations. Empty means "function".
	SetFunctionDescription(ctx context.Context, module, name, kind, desc, longDesc string) error
	// SetDataSourceDescription curates a data source's description
	// (entity_kind=data_source).
	SetDataSourceDescription(ctx context.Context, name, desc, longDesc string) error
}

// GraphQLAnnotator curates human descriptions over the GENERATED GraphQL
// schema surface — the synthetic types (aggregations, filters, mutation
// inputs, module roots, shared query types), their fields and their arguments,
// which have no logical-model entity to hang a description on. Its keys live in
// a separate namespace (gql_type / gql_field / gql_argument) so they never
// collide with the logical annotations, and the store applies them AFTER the
// logical layer, so a GraphQL curation wins over a logical one.
type GraphQLAnnotator interface {
	// SetDefinitionDescription curates a generated type's description (gql_type).
	SetDefinitionDescription(ctx context.Context, typeName, desc, longDesc string) error
	// SetFieldDescription curates a field's description (gql_field, key=type.field).
	SetFieldDescription(ctx context.Context, typeName, fieldName, desc, longDesc string) error
	// SetArgumentDescription curates a field argument's description
	// (gql_argument, key=type.field.arg).
	SetArgumentDescription(ctx context.Context, typeName, fieldName, argName, desc, longDesc string) error
}

// SuspendableProvider described the two providers this seam existed for: the
// compiled-schema one, which set a flag and dropped its cache, and the static
// one, which fell back to DropCatalog. Neither exists any more — suspending a
// source is a catalog.CatalogManager operation on the storage, and the static
// provider is read-only.
