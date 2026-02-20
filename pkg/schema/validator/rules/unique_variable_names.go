package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueVariableNames checks that variable names are unique within an operation.
type UniqueVariableNames struct{}

func (r *UniqueVariableNames) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		seen := make(map[string]bool)
		for _, varDef := range op.VariableDefinitions {
			if seen[varDef.Variable] {
				errs = append(errs, gqlerror.ErrorPosf(varDef.Position,
					`There can be only one variable named "$%s".`, varDef.Variable))
			}
			seen[varDef.Variable] = true
		}
	}
	return errs
}
