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
	return def.Directives.ForName("function") != nil
}

func (r *FunctionRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	// Stub: function generation is complex and will be fleshed out
	// incrementally. The full implementation will:
	//   - Determine if this is a query function or mutation function
	//   - Generate the appropriate return type wrapper
	//   - Build argument lists from function parameters
	//   - Register fields via ctx.RegisterFunctionFields or
	//     ctx.RegisterFunctionMutationFields
	//
	// For now, add the definition to output so downstream rules can
	// reference it.
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}

	addDef := ctx.AddDefinition
	if info.IsReplace {
		addDef = ctx.AddDefinitionReplaceOrCreate
	}
	addDef(def)

	return nil
}
