package sdl

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// defsAdapter bridges base.DefinitionsSource (context-aware) to the simpler
// Definitions interface used by sdl helpers.
type defsAdapter struct {
	ctx context.Context
	src base.DefinitionsSource
}

// NewDefsAdapter creates a Definitions adapter from a context-aware DefinitionsSource.
func NewDefsAdapter(ctx context.Context, src base.DefinitionsSource) Definitions {
	return &defsAdapter{ctx: ctx, src: src}
}

func (a *defsAdapter) ForName(name string) *ast.Definition {
	return a.src.ForName(a.ctx, name)
}

// schemaDefs wraps *ast.Schema as a Definitions implementation.
type schemaDefs struct {
	schema *ast.Schema
}

func (d schemaDefs) ForName(name string) *ast.Definition {
	return d.schema.Types[name]
}

// SchemaDefs returns a Definitions adapter backed by the given schema.
func SchemaDefs(schema *ast.Schema) Definitions {
	return schemaDefs{schema: schema}
}
