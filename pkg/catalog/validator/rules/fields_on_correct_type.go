package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// FieldsOnCorrectType checks that a queried field exists on the parent type.
type FieldsOnCorrectType struct{ BaseInlineRule }

func (r *FieldsOnCorrectType) EnterField(_ *validator.WalkContext, parentDef *ast.Definition, field *ast.Field) gqlerror.List {
	if field.Name == "__typename" {
		return nil
	}
	if field.Definition == nil && parentDef != nil {
		return gqlerror.List{gqlerror.ErrorPosf(field.Position,
			`Cannot query field "%s" on type "%s".`, field.Name, parentDef.Name)}
	}
	return nil
}
