package static

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time check that Provider implements schema.Provider.
var _ base.Provider = (*Provider)(nil)

// Provider is a READ-ONLY view of an *ast.Schema, all lookups O(1) via maps.
//
// In the engine there is exactly one: the system prelude the catalog storage
// layers its on-the-fly generation on top of (store.go). That prelude is a
// binary artifact — scalars, introspection, the @system SDL — so mutating it
// would be process-local state no restart and no second node would share.
// Descriptions in particular are NOT curated here: curation lives in
// catalog.annotations and is applied by the storage at reconstruction time.
//
// The mutable surface this type used to carry (Update, DropCatalog, the
// description setters) went with the compiler write path in design-036. It had
// stopped satisfying the annotator interfaces long before that — the setters
// were a 2-of-3 and a 1-of-6 match — so nothing could reach it even by
// assertion.
type Provider struct {
	schema *ast.Schema
}

// New builds the system prelude and CHECKS it. The prelude is parsed from SDL
// embedded in the binary, so a dangling type reference in it is a build-time
// mistake that would otherwise surface far away, as odd introspection output.
// Failing here names it.
func New() (*Provider, error) {
	schema, err := initSystemSchema()
	if err != nil {
		return nil, err
	}
	p := &Provider{schema: schema}
	if errs := p.ValidateSchema(); len(errs) > 0 {
		return nil, fmt.Errorf("system schema is not valid: %w", errors.Join(errs...))
	}
	return p, nil
}

// NewWithSchema creates a Provider wrapping the given compiled schema.
func NewWithSchema(s *ast.Schema) *Provider {
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

func (p *Provider) PossibleTypes(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, t := range p.schema.GetPossibleTypes(def) {
			if !yield(t) {
				return
			}
		}
	}
}

func (p *Provider) Implements(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, iface := range p.schema.GetImplements(def) {
			if !yield(iface) {
				return
			}
		}
	}
}

func (p *Provider) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, def := range p.schema.Types {
			if !yield(def) {
				return
			}
		}
	}
}

func (p *Provider) Types(_ context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for name, def := range p.schema.Types {
			if !yield(name, def) {
				return
			}
		}
	}
}

func (p *Provider) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for name, dir := range p.schema.Directives {
			if !yield(name, dir) {
				return
			}
		}
	}
}

func (p *Provider) Description(_ context.Context) string {
	return p.schema.Description
}

// Schema returns the underlying ast.Schema for direct access (e.g., comparison tests).
func (p *Provider) Schema() *ast.Schema {
	return p.schema
}
