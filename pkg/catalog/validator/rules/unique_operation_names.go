package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueOperationNames checks that operation names are unique.
type UniqueOperationNames struct{}

func (r *UniqueOperationNames) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	seen := make(map[string]bool)
	var errs gqlerror.List
	for _, op := range doc.Operations {
		if op.Name == "" {
			continue
		}
		if seen[op.Name] {
			errs = append(errs, gqlerror.ErrorPosf(op.Position,
				`There can be only one operation named "%s".`, op.Name))
		}
		seen[op.Name] = true
	}
	return errs
}
