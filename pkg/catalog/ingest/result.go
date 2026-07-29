package ingest

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.CompiledCatalog = (*compiledCatalog)(nil)
var _ base.DependentCompiledCatalog = (*compiledCatalog)(nil)

// compiledCatalog is what the pipeline returns: the extensions it could not
// merge in place (cross-source `extend type`, module roots) and the dependency
// set. Its definitions half is empty — the source's own definitions are mutated
// in place and read from the source, not from here.
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
