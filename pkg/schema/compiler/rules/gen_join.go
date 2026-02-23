package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*JoinRule)(nil)

type JoinRule struct{}

func (r *JoinRule) Name() string     { return "JoinRule" }
func (r *JoinRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *JoinRule) Match(def *ast.Definition) bool {
	for _, f := range def.Fields {
		if f.Directives.ForName("join") != nil {
			return true
		}
	}
	return false
}

func (r *JoinRule) Process(_ base.CompilationContext, _ *ast.Definition) error {
	// Stub: join field processing will be fleshed out incrementally.
	// The @join directive on fields indicates a computed join relationship
	// between objects. Full implementation will:
	//   - Look up the referenced object for each @join field
	//   - Validate join conditions
	//   - Generate appropriate join query fields
	return nil
}
