package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// NoUndefinedVariables checks that all used variables are defined in the operation.
type NoUndefinedVariables struct{}

func (r *NoUndefinedVariables) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		defined := make(map[string]bool)
		for _, varDef := range op.VariableDefinitions {
			defined[varDef.Variable] = true
		}
		var checkValue func(value *ast.Value)
		checkValue = func(value *ast.Value) {
			if value == nil {
				return
			}
			if value.Kind == ast.Variable {
				if !defined[value.Raw] {
					opName := op.Name
					if opName == "" {
						opName = "anonymous operation"
					}
					errs = append(errs, gqlerror.ErrorPosf(value.Position,
						`Variable "$%s" is not defined by operation "%s".`, value.Raw, opName))
				}
				return
			}
			for _, child := range value.Children {
				checkValue(child.Value)
			}
		}
		WalkAllValuesInOperation(op, checkValue)
	}
	return errs
}
