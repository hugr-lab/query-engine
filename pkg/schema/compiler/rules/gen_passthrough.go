package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*PassthroughRule)(nil)

// PassthroughRule adds source definitions that are not handled by other
// Generate-phase rules (e.g. structural Object types, InputObject, Interface,
// Union, Enum) directly to the compiler output so they appear in the final schema.
type PassthroughRule struct{}

func (r *PassthroughRule) Name() string     { return "PassthroughRule" }
func (r *PassthroughRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *PassthroughRule) Match(def *ast.Definition) bool {
	// Skip definitions handled by TableRule, ViewRule
	if def.Directives.ForName("table") != nil {
		return false
	}
	if def.Directives.ForName("view") != nil {
		return false
	}
	if def.Directives.ForName("function") != nil {
		return false
	}
	// Skip Function/MutationFunction — handled by FunctionRule
	if def.Name == "Function" || def.Name == "MutationFunction" {
		return false
	}
	// Match structural types that need to be passed through
	switch def.Kind {
	case ast.Object, ast.InputObject, ast.Interface, ast.Union, ast.Enum:
		return true
	}
	return false
}

func (r *PassthroughRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	// Skip types that already exist in the target schema (system types).
	// These will be extended rather than re-added.
	if ctx.LookupType(def.Name) != nil {
		return nil
	}
	ctx.AddDefinition(def)
	return nil
}
