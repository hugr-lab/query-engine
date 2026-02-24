package compiler

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.CompiledCatalog = (*compiledCatalog)(nil)
var _ base.DependentCompiledCatalog = (*compiledCatalog)(nil)

// compiledCatalog implements base.CompiledCatalog as a DDL feed
// consumable by static.Provider.Update().
type compiledCatalog struct {
	output *indexedOutput
}

func newCompiledCatalog(output *indexedOutput) *compiledCatalog {
	return &compiledCatalog{output: output}
}

func (c *compiledCatalog) ForName(_ context.Context, name string) *ast.Definition {
	return c.output.LookupDefinition(name)
}

func (c *compiledCatalog) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return c.output.LookupDirective(name)
}

func (c *compiledCatalog) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return c.output.Definitions()
}

func (c *compiledCatalog) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for _, d := range c.output.dirDefs {
			if !yield(d.Name, d) {
				return
			}
		}
	}
}

func (c *compiledCatalog) DefinitionExtensions(_ context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		if idx, ok := c.output.extIndex[name]; ok {
			yield(c.output.exts[idx])
		}
	}
}

func (c *compiledCatalog) Extensions(_ context.Context) iter.Seq[*ast.Definition] {
	return c.output.Extensions()
}

func (c *compiledCatalog) Dependencies() []string {
	return c.output.dependencies
}
