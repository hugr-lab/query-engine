package static

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time check that Provider implements schema.Provider.
var _ schema.Provider = (*Provider)(nil)

// Provider implements schema.Provider backed by *ast.Schema.
// All lookups are O(1) via maps.
type Provider struct {
	schema *ast.Schema
}

// New creates a Provider wrapping the given compiled schema.
func New(s *ast.Schema) *Provider {
	return &Provider{schema: s}
}

func (p *Provider) ForName(_ context.Context, name string) *ast.Definition {
	return p.schema.Types[name]
}

func (p *Provider) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return p.schema.Directives[name]
}

func (p *Provider) QueryType(_ context.Context) *ast.Definition {
	return p.schema.Query
}

func (p *Provider) MutationType(_ context.Context) *ast.Definition {
	return p.schema.Mutation
}

func (p *Provider) SubscriptionType(_ context.Context) *ast.Definition {
	return p.schema.Subscription
}

func (p *Provider) PossibleTypes(_ context.Context, def *ast.Definition) []*ast.Definition {
	return p.schema.GetPossibleTypes(def)
}

func (p *Provider) Implements(_ context.Context, def *ast.Definition) []*ast.Definition {
	return p.schema.GetImplements(def)
}

func (p *Provider) Types(_ context.Context, yield func(string, *ast.Definition) bool) {
	for name, def := range p.schema.Types {
		if !yield(name, def) {
			return
		}
	}
}

func (p *Provider) DirectiveDefinitions(_ context.Context, yield func(string, *ast.DirectiveDefinition) bool) {
	for name, dir := range p.schema.Directives {
		if !yield(name, dir) {
			return
		}
	}
}

func (p *Provider) Description(_ context.Context) string {
	return p.schema.Description
}
