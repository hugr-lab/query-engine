package base

import (
	"context"

	"github.com/vektah/gqlparser/v2/ast"
)

// ContextDefsSource is a context-aware definitions source.
// schema.Provider satisfies this interface structurally (no import needed).
type ContextDefsSource interface {
	ForName(ctx context.Context, name string) *ast.Definition
	DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition
}

// DefsAdapter adapts a ContextDefsSource (e.g. schema.Provider) to a
// context-less DefinitionsSource used by the compiler.
// It captures a context at creation time.
type DefsAdapter struct {
	ctx context.Context
	src ContextDefsSource
}

// NewDefsAdapter creates an adapter that binds a context to a ContextDefsSource.
func NewDefsAdapter(ctx context.Context, src ContextDefsSource) *DefsAdapter {
	return &DefsAdapter{ctx: ctx, src: src}
}

func (a *DefsAdapter) ForName(name string) *ast.Definition {
	return a.src.ForName(a.ctx, name)
}

func (a *DefsAdapter) DirectiveForName(name string) *ast.DirectiveDefinition {
	return a.src.DirectiveForName(a.ctx, name)
}
