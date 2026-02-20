package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueInputFieldNames checks that input object fields are unique.
type UniqueInputFieldNames struct{}

func (r *UniqueInputFieldNames) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	var checkValue func(value *ast.Value)
	checkValue = func(value *ast.Value) {
		if value == nil || value.Kind != ast.ObjectValue {
			return
		}
		seen := make(map[string]bool)
		for _, child := range value.Children {
			if seen[child.Name] {
				errs = append(errs, gqlerror.ErrorPosf(child.Position,
					`There can be only one input field named "%s".`, child.Name))
			}
			seen[child.Name] = true
			checkValue(child.Value)
		}
	}
	WalkAllValues(doc, checkValue)
	return errs
}
