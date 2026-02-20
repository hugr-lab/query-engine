package schema

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// DefinitionsSource resolves types and directives with context support.
// A subset of Provider for functions that only need type resolution.
type DefinitionsSource interface {
	ForName(ctx context.Context, name string) *ast.Definition
	DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition
}

// Source is a mutable container for schema definitions of a single data source.
type Source interface {
	DefinitionsSource

	// Iteration
	Definitions(yield func(def *ast.Definition) bool)
	Extensions(yield func(def *ast.Definition) bool)

	// Write
	AddDefinition(def *ast.Definition)
	RemoveDefinitions(filter func(def *ast.Definition) bool)
	AddExtension(ext *ast.Definition)
	ClearExtensions()

	// Directive definitions
	DirectiveDefinitions(yield func(dir *ast.DirectiveDefinition) bool)
	AddDirectiveDefinition(dir *ast.DirectiveDefinition)

	// Schema operation types (Query, Mutation root type names)
	OperationType(operation string) string
	SetOperationType(operation string, typeName string)

	// Merge base types
	MergeFrom(other *ast.SchemaDocument)

	// Read-only snapshot
	Provider(ctx context.Context) Provider
}

// Provider is a read-only interface to a compiled schema.
// All methods accept context.Context for future storage-backed implementations.
type Provider interface {
	DefinitionsSource

	// Root operation types
	QueryType(ctx context.Context) *ast.Definition
	MutationType(ctx context.Context) *ast.Definition
	SubscriptionType(ctx context.Context) *ast.Definition

	// Type relationships (for validator: fragment spreading, interface checks)
	PossibleTypes(ctx context.Context, def *ast.Definition) []*ast.Definition
	Implements(ctx context.Context, def *ast.Definition) []*ast.Definition

	// Enumeration (for introspection, meta-info)
	Types(ctx context.Context, yield func(name string, def *ast.Definition) bool)
	DirectiveDefinitions(ctx context.Context, yield func(name string, def *ast.DirectiveDefinition) bool)

	// Schema metadata
	Description(ctx context.Context) string
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
