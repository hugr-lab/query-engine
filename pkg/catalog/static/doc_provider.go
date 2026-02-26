package static

import (
	"context"
	"iter"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time check that docProvider implements catalog.Provider.
var _ base.Provider = (*docProvider)(nil)
var _ base.DefinitionsSource = (*docProvider)(nil)
var _ base.ExtensionsSource = (*docProvider)(nil)

type docProvider struct {
	doc *ast.SchemaDocument
}

// NewDocumentProvider creates a read-only Provider backed by an *ast.SchemaDocument.
// ForName is O(n) linear scan — suitable for compilation, not runtime.
func NewDocumentProvider(doc *ast.SchemaDocument) catalog.Provider {
	return &docProvider{doc: doc}
}

func (p *docProvider) ForName(_ context.Context, name string) *ast.Definition {
	return p.doc.Definitions.ForName(name)
}

func (p *docProvider) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return p.doc.Directives.ForName(name)
}

func (p *docProvider) QueryType(_ context.Context) *ast.Definition {
	name := "Query"
	if len(p.doc.Schema) > 0 {
		for _, op := range p.doc.Schema[0].OperationTypes {
			if op.Operation == ast.Query {
				name = op.Type
				break
			}
		}
	}
	return p.doc.Definitions.ForName(name)
}

func (p *docProvider) MutationType(_ context.Context) *ast.Definition {
	name := "Mutation"
	if len(p.doc.Schema) > 0 {
		for _, op := range p.doc.Schema[0].OperationTypes {
			if op.Operation == ast.Mutation {
				name = op.Type
				break
			}
		}
	}
	return p.doc.Definitions.ForName(name)
}

func (p *docProvider) SubscriptionType(_ context.Context) *ast.Definition {
	name := "Subscription"
	if len(p.doc.Schema) > 0 {
		for _, op := range p.doc.Schema[0].OperationTypes {
			if op.Operation == ast.Subscription {
				name = op.Type
				break
			}
		}
	}
	return p.doc.Definitions.ForName(name)
}

func (p *docProvider) PossibleTypes(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.doc.Definitions.ForName(name)
	if def == nil {
		return nil
	}
	switch def.Kind {
	case ast.Object:
		return func(yield func(*ast.Definition) bool) {
			if !yield(def) {
				return
			}
		}
	case ast.Interface:
		return func(yield func(*ast.Definition) bool) {
			for _, d := range p.doc.Definitions {
				if d.Kind != ast.Object {
					continue
				}
				if !slices.Contains(d.Interfaces, def.Name) {
					continue
				}
				if !yield(d) {
					return
				}
			}
		}
	case ast.Union:
		return func(yield func(*ast.Definition) bool) {
			for _, typeName := range def.Types {
				if d := p.doc.Definitions.ForName(typeName); d != nil {
					if !yield(d) {
						return
					}
				}
			}
		}
	}
	return nil
}

func (p *docProvider) Implements(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.doc.Definitions.ForName(name)
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, d := range p.doc.Definitions {
			if d.Kind != ast.Interface {
				continue
			}
			if def.Kind == ast.Object && slices.Contains(def.Interfaces, d.Name) {
				if !yield(d) {
					return
				}
			}
		}
	}
}

func (p *docProvider) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, def := range p.doc.Definitions {
			if !yield(def) {
				return
			}
		}
	}
}

func (p *docProvider) Types(_ context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for _, def := range p.doc.Definitions {
			if !yield(def.Name, def) {
				return
			}
		}
	}
}

func (p *docProvider) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for _, dir := range p.doc.Directives {
			if !yield(dir.Name, dir) {
				return
			}
		}
	}
}

func (p *docProvider) Description(_ context.Context) string {
	if len(p.doc.Schema) > 0 {
		return p.doc.Schema[0].Description
	}
	return ""
}

func (p *docProvider) Extensions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, def := range p.doc.Extensions {
			if !yield(def) {
				return
			}
		}
	}
}

func (p *docProvider) DefinitionExtensions(_ context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, ext := range p.doc.Extensions {
			if ext.Name == name {
				if !yield(ext) {
					return
				}
			}
		}
	}
}
