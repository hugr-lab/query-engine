package validator

import (
	"context"
	"iter"

	"github.com/vektah/gqlparser/v2/ast"
)

// TypeResolver resolves types and directives from the schema.
// Matches a subset of schema.Provider methods but is defined here
// to avoid circular dependency between pkg/schema and pkg/schema/validator.
type TypeResolver interface {
	ForName(ctx context.Context, name string) *ast.Definition
	DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition
	QueryType(ctx context.Context) *ast.Definition
	MutationType(ctx context.Context) *ast.Definition
	SubscriptionType(ctx context.Context) *ast.Definition
	PossibleTypes(ctx context.Context, def *ast.Definition) iter.Seq[*ast.Definition]
	Implements(ctx context.Context, def *ast.Definition) iter.Seq[*ast.Definition]
}
