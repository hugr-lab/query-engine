package compiler

import (
	"context"
	"iter"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.CompilationContext = (*compilationContext)(nil)

// compilationContext implements base.CompilationContext, providing rules
// access to source, target schema, output writing, and shared state.
type compilationContext struct {
	ctx    context.Context
	source base.DefinitionsSource
	schema base.Provider
	opts   base.Options
	output *indexedOutput

	// Shared state
	objects map[string]*base.ObjectInfo

	// Promoted definitions — added by PREPARE rules, dispatched in subsequent phases.
	promoted []*ast.Definition

	// Field collectors for ASSEMBLE phase
	queryFields         map[string][]*ast.FieldDefinition
	mutationFields      map[string][]*ast.FieldDefinition
	functionFields      []*ast.FieldDefinition
	functionMutFields   []*ast.FieldDefinition
}

func newCompilationContext(
	ctx context.Context,
	source base.DefinitionsSource,
	schema base.Provider,
	opts base.Options,
	output *indexedOutput,
) *compilationContext {
	return &compilationContext{
		ctx:            ctx,
		source:         source,
		schema:         schema,
		opts:           opts,
		output:         output,
		objects:        make(map[string]*base.ObjectInfo),
		queryFields:    make(map[string][]*ast.FieldDefinition),
		mutationFields: make(map[string][]*ast.FieldDefinition),
	}
}

func (c *compilationContext) Context() context.Context {
	return c.ctx
}

func (c *compilationContext) Source() base.DefinitionsSource {
	return c.source
}

func (c *compilationContext) CompileOptions() base.Options {
	return c.opts
}

func (c *compilationContext) ApplyPrefix(name string) string {
	return c.opts.ApplyPrefix(name)
}

func (c *compilationContext) ScalarLookup(name string) types.ScalarType {
	return types.Lookup(name)
}

func (c *compilationContext) IsScalar(name string) bool {
	return types.IsScalar(name)
}

func (c *compilationContext) AddDefinition(def *ast.Definition) {
	c.output.AddDefinition(def)
}

func (c *compilationContext) AddExtension(ext *ast.Definition) {
	if c.opts.IsExtension && c.opts.Name != "" {
		for _, f := range ext.Fields {
			f.Directives = append(f.Directives, dependencyDirective(c.opts.Name, f.Position))
		}
	}
	// Strip definition-level @dependency directives from extensions before
	// passing to Provider.Update(). These directives are compilation metadata
	// (what the extension source depends on), NOT what the target type depends on.
	// If propagated to the target type, DropCatalog would incorrectly drop the
	// target definition when a dependency catalog is reloaded.
	ext = stripExtensionDependencyDirectives(ext)
	c.output.AddExtension(ext)
}

// stripExtensionDependencyDirectives returns a copy of the definition with
// definition-level @dependency directives removed, or the original if none present.
func stripExtensionDependencyDirectives(ext *ast.Definition) *ast.Definition {
	hasDeps := false
	for _, d := range ext.Directives {
		if d.Name == base.DependencyDirectiveName {
			hasDeps = true
			break
		}
	}
	if !hasDeps {
		return ext
	}
	cp := *ext
	cp.Directives = slices.DeleteFunc(slices.Clone(ext.Directives), func(d *ast.Directive) bool {
		return d.Name == base.DependencyDirectiveName
	})
	return &cp
}

func dependencyDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: "dependency",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

func (c *compilationContext) AddDefinitionReplaceOrCreate(def *ast.Definition) {
	c.output.AddDefinitionReplaceOrCreate(def)
}

func (c *compilationContext) PromoteToSource(def *ast.Definition) {
	c.promoted = append(c.promoted, def)
}

func (c *compilationContext) PromotedDefinitions() []*ast.Definition {
	return c.promoted
}

// LookupType checks compilation output first, then target schema (FR-012).
func (c *compilationContext) LookupType(name string) *ast.Definition {
	if def := c.output.LookupDefinition(name); def != nil {
		return def
	}
	if c.schema != nil {
		return c.schema.ForName(c.ctx, name)
	}
	return nil
}

func (c *compilationContext) LookupExtension(name string) *ast.Definition {
	return c.output.LookupExtension(name)
}

func (c *compilationContext) LookupDirective(name string) *ast.DirectiveDefinition {
	if dir := c.output.LookupDirective(name); dir != nil {
		return dir
	}
	if c.schema != nil {
		return c.schema.DirectiveForName(c.ctx, name)
	}
	return nil
}

func (c *compilationContext) RegisterObject(name string, info *base.ObjectInfo) {
	c.objects[name] = info
}

func (c *compilationContext) GetObject(name string) *base.ObjectInfo {
	return c.objects[name]
}

func (c *compilationContext) RegisterQueryFields(objectName string, fields []*ast.FieldDefinition) {
	c.queryFields[objectName] = append(c.queryFields[objectName], fields...)
}

func (c *compilationContext) RegisterMutationFields(objectName string, fields []*ast.FieldDefinition) {
	c.mutationFields[objectName] = append(c.mutationFields[objectName], fields...)
}

func (c *compilationContext) RegisterFunctionFields(fields []*ast.FieldDefinition) {
	c.functionFields = append(c.functionFields, fields...)
}

func (c *compilationContext) RegisterFunctionMutationFields(fields []*ast.FieldDefinition) {
	c.functionMutFields = append(c.functionMutFields, fields...)
}

func (c *compilationContext) QueryFields() map[string][]*ast.FieldDefinition {
	return c.queryFields
}

func (c *compilationContext) MutationFields() map[string][]*ast.FieldDefinition {
	return c.mutationFields
}

func (c *compilationContext) FunctionFields() []*ast.FieldDefinition {
	return c.functionFields
}

func (c *compilationContext) FunctionMutationFields() []*ast.FieldDefinition {
	return c.functionMutFields
}

func (c *compilationContext) RegisterDependency(name string) {
	c.output.AddDependency(name)
}

func (c *compilationContext) Objects() iter.Seq2[string, *base.ObjectInfo] {
	return func(yield func(string, *base.ObjectInfo) bool) {
		for k, v := range c.objects {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (c *compilationContext) OutputDefinitions() iter.Seq[*ast.Definition] {
	return c.output.Definitions()
}

func (c *compilationContext) OutputExtensions() iter.Seq[*ast.Definition] {
	return c.output.Extensions()
}
