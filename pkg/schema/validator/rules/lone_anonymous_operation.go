package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// LoneAnonymousOperation checks that anonymous operations are the only defined operation.
type LoneAnonymousOperation struct{}

func (r *LoneAnonymousOperation) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	if len(doc.Operations) <= 1 {
		return nil
	}
	var errs gqlerror.List
	for _, op := range doc.Operations {
		if op.Name == "" {
			errs = append(errs, gqlerror.ErrorPosf(op.Position,
				`This anonymous operation must be the only defined operation.`))
		}
	}
	return errs
}
