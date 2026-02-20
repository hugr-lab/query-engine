package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueDirectivesPerLocation checks that non-repeatable directives are unique per location.
type UniqueDirectivesPerLocation struct{}

func (r *UniqueDirectivesPerLocation) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	checkDirectives := func(directives []*ast.Directive) {
		seen := make(map[string]bool)
		for _, dir := range directives {
			if dir.Definition != nil && dir.Definition.IsRepeatable {
				continue
			}
			if seen[dir.Name] {
				errs = append(errs, gqlerror.ErrorPosf(dir.Position,
					`The directive "@%s" can only be used once at this location.`, dir.Name))
			}
			seen[dir.Name] = true
		}
	}
	for _, op := range doc.Operations {
		checkDirectives(op.Directives)
		WalkDirectivesInSelections(op.SelectionSet, checkDirectives)
		for _, varDef := range op.VariableDefinitions {
			checkDirectives(varDef.Directives)
		}
	}
	for _, frag := range doc.Fragments {
		checkDirectives(frag.Directives)
		WalkDirectivesInSelections(frag.SelectionSet, checkDirectives)
	}
	return errs
}
