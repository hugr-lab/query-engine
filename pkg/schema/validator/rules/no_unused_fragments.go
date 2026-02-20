package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// NoUnusedFragments checks that all defined fragments are used.
type NoUnusedFragments struct{}

func (r *NoUnusedFragments) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	used := make(map[string]bool)
	for _, op := range doc.Operations {
		collectUsedFragments(doc, op.SelectionSet, used)
	}
	var errs gqlerror.List
	for _, frag := range doc.Fragments {
		if !used[frag.Name] {
			errs = append(errs, gqlerror.ErrorPosf(frag.Position,
				`Fragment "%s" is never used.`, frag.Name))
		}
	}
	return errs
}

func collectUsedFragments(doc *ast.QueryDocument, selections ast.SelectionSet, used map[string]bool) {
	for _, sel := range selections {
		switch sel := sel.(type) {
		case *ast.Field:
			collectUsedFragments(doc, sel.SelectionSet, used)
		case *ast.InlineFragment:
			collectUsedFragments(doc, sel.SelectionSet, used)
		case *ast.FragmentSpread:
			if !used[sel.Name] {
				used[sel.Name] = true
				if frag := doc.Fragments.ForName(sel.Name); frag != nil {
					collectUsedFragments(doc, frag.SelectionSet, used)
				}
			}
		}
	}
}
