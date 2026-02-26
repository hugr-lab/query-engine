package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// VariablesInAllowedPosition checks that variable types are compatible with their usage.
type VariablesInAllowedPosition struct{}

func (r *VariablesInAllowedPosition) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	var checkValue func(value *ast.Value, op *ast.OperationDefinition)
	checkValue = func(value *ast.Value, op *ast.OperationDefinition) {
		if value == nil {
			return
		}
		if value.Kind == ast.Variable && value.ExpectedType != nil && value.VariableDefinition != nil {
			tmp := *value.ExpectedType
			if value.VariableDefinition.DefaultValue != nil && value.VariableDefinition.DefaultValue.Kind != ast.NullValue {
				if value.ExpectedType.NonNull {
					tmp.NonNull = false
				}
			}
			if value.ExpectedTypeHasDefault {
				tmp.NonNull = false
			}
			if !value.VariableDefinition.Type.IsCompatible(&tmp) {
				errs = append(errs, gqlerror.ErrorPosf(value.Position,
					`Variable "$%s" of type "%s" used in position expecting type "%s".`,
					value.Raw, value.VariableDefinition.Type.String(), value.ExpectedType.String()))
			}
			return
		}
		for _, child := range value.Children {
			checkValue(child.Value, op)
		}
	}
	for _, op := range doc.Operations {
		WalkAllValuesInOperation(op, func(value *ast.Value) {
			checkValue(value, op)
		})
	}
	return errs
}
