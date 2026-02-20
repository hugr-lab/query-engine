package static

import (
	"context"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time check that docProvider implements schema.Provider.
var _ schema.Provider = (*docProvider)(nil)

type docProvider struct {
	doc *ast.SchemaDocument
}

// NewDocumentProvider creates a read-only Provider backed by an *ast.SchemaDocument.
// ForName is O(n) linear scan — suitable for compilation, not runtime.
func NewDocumentProvider(doc *ast.SchemaDocument) schema.Provider {
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

func (p *docProvider) PossibleTypes(_ context.Context, def *ast.Definition) []*ast.Definition {
	if def == nil {
		return nil
	}
	switch def.Kind {
	case ast.Object:
		return []*ast.Definition{def}
	case ast.Interface:
		var result []*ast.Definition
		for _, d := range p.doc.Definitions {
			if d.Kind != ast.Object {
				continue
			}
			if slices.Contains(d.Interfaces, def.Name) {
				result = append(result, d)
			}
		}
		return result
	case ast.Union:
		var result []*ast.Definition
		for _, typeName := range def.Types {
			if d := p.doc.Definitions.ForName(typeName); d != nil {
				result = append(result, d)
			}
		}
		return result
	}
	return nil
}

func (p *docProvider) Implements(_ context.Context, def *ast.Definition) []*ast.Definition {
	if def == nil {
		return nil
	}
	var result []*ast.Definition
	for _, d := range p.doc.Definitions {
		if d.Kind != ast.Interface {
			continue
		}
		if def.Kind == ast.Object && slices.Contains(def.Interfaces, d.Name) {
			result = append(result, d)
		}
	}
	return result
}

func (p *docProvider) Types(_ context.Context, yield func(string, *ast.Definition) bool) {
	for _, def := range p.doc.Definitions {
		if !yield(def.Name, def) {
			return
		}
	}
}

func (p *docProvider) DirectiveDefinitions(_ context.Context, yield func(string, *ast.DirectiveDefinition) bool) {
	for _, dir := range p.doc.Directives {
		if !yield(dir.Name, dir) {
			return
		}
	}
}

func (p *docProvider) Description(_ context.Context) string {
	if len(p.doc.Schema) > 0 {
		return p.doc.Schema[0].Description
	}
	return ""
}
