package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// VariablesAreInputTypes checks that variables are declared with input types.
type VariablesAreInputTypes struct{}

func (r *VariablesAreInputTypes) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		for _, varDef := range op.VariableDefinitions {
			if varDef.Definition == nil {
				continue
			}
			if !varDef.Definition.IsInputType() {
				errs = append(errs, gqlerror.ErrorPosf(varDef.Position,
					`Variable "$%s" cannot be non-input type "%s".`,
					varDef.Variable, varDef.Type.String()))
			}
		}
	}
	return errs
}
