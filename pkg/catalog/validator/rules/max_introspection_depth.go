package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const maxIntrospectionDepth = 20

// MaxIntrospectionDepth prevents excessively deep queries.
type MaxIntrospectionDepth struct{}

func (r *MaxIntrospectionDepth) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	var errs gqlerror.List
	for _, op := range doc.Operations {
		visited := make(map[string]bool)
		depth := selectionSetDepth(op.SelectionSet, 0, visited)
		if depth > maxIntrospectionDepth {
			errs = append(errs, gqlerror.ErrorPosf(op.Position,
				`Operation has depth %d, which exceeds the limit of %d.`, depth, maxIntrospectionDepth))
		}
	}
	return errs
}

func selectionSetDepth(ss ast.SelectionSet, current int, visited map[string]bool) int {
	maxDepth := current
	for _, sel := range ss {
		var childSS ast.SelectionSet
		switch sel := sel.(type) {
		case *ast.Field:
			childSS = sel.SelectionSet
		case *ast.InlineFragment:
			childSS = sel.SelectionSet
		case *ast.FragmentSpread:
			if sel.Definition != nil && !visited[sel.Name] {
				visited[sel.Name] = true
				childSS = sel.Definition.SelectionSet
			}
		}
		if len(childSS) > 0 {
			d := selectionSetDepth(childSS, current+1, visited)
			if d > maxDepth {
				maxDepth = d
			}
		}
	}
	return maxDepth
}
