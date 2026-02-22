package schema

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/types"
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
	PossibleTypes(ctx context.Context, def *ast.Definition) iter.Seq[*ast.Definition]
	Implements(ctx context.Context, def *ast.Definition) iter.Seq[*ast.Definition]

	// types iteration (for introspection, meta-info)
	Types(ctx context.Context) iter.Seq2[string, *ast.Definition]
}

// MutableProvider is a mutable container for schema definitions.
type MutableProvider interface {
	Provider

	// Merge base types
	MergeFrom(ctx context.Context, other DefinitionsSource) error

	// Change the definition description
	SetDefinitionDescription(ctx context.Context, name, desc string) error
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
