package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*FunctionRule)(nil)

type FunctionRule struct{}

func (r *FunctionRule) Name() string     { return "FunctionRule" }
func (r *FunctionRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *FunctionRule) Match(def *ast.Definition) bool {
	// Match Function and MutationFunction type definitions.
	// These come from "extend type Function { ... }" in user SDL,
	// merged into definitions during source extraction.
	return def.Name == "Function" || def.Name == "MutationFunction"
}

func (r *FunctionRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	opts := ctx.CompileOptions()
	pos := compiledPos("function")

	for _, field := range def.Fields {
		// Skip stub/placeholder fields
		if field.Name == "_stub" || field.Name == "_placeholder" {
			continue
		}

		funcDir := field.Directives.ForName("function")
		if funcDir == nil {
			continue
		}

		// Add @catalog directive
		if field.Directives.ForName("catalog") == nil {
			field.Directives = append(field.Directives, catalogDirective(opts.Name, opts.EngineType))
		}

		// Handle @module for AsModule option
		if opts.AsModule {
			if d := field.Directives.ForName("module"); d != nil {
				if a := d.Arguments.ForName("name"); a != nil {
					if a.Value.Raw == "" {
						a.Value.Raw = opts.Name
					} else {
						a.Value.Raw = opts.Name + "." + a.Value.Raw
					}
				}
			} else {
				field.Directives = append(field.Directives, &ast.Directive{
					Name: "module",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: opts.Name, Position: pos}, Position: pos},
					},
					Position: pos,
				})
			}
		}
	}

	// Add @system if not present
	if def.Directives.ForName("system") == nil {
		def.Directives = append(def.Directives, &ast.Directive{Name: "system", Position: pos})
	}

	// Emit the Function/MutationFunction definition to output.
	// Function fields stay inside the type (not registered as query fields).
	// The RootTypeAssembler adds a "function" gateway field on Query/Mutation.
	ctx.AddDefinition(def)

	return nil
}
