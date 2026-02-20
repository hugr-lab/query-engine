package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// SingleFieldSubscriptions checks that subscriptions select only one top-level field.
type SingleFieldSubscriptions struct{}

func (r *SingleFieldSubscriptions) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		if op.Operation != ast.Subscription {
			continue
		}
		fields := countRootFields(op.SelectionSet)
		if fields > 1 {
			name := op.Name
			if name == "" {
				name = "Anonymous Subscription"
			}
			errs = append(errs, gqlerror.ErrorPosf(op.Position,
				`Subscription "%s" must select only one top level field.`, name))
		}
	}
	return errs
}

func countRootFields(selections ast.SelectionSet) int {
	count := 0
	for _, sel := range selections {
		switch sel := sel.(type) {
		case *ast.Field:
			if sel.Name == "__typename" {
				continue
			}
			count++
		case *ast.InlineFragment:
			count += countRootFields(sel.SelectionSet)
		case *ast.FragmentSpread:
			if sel.Definition != nil {
				count += countRootFields(sel.Definition.SelectionSet)
			}
		}
	}
	return count
}
