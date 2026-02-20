package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// UniqueFragmentNames checks that fragment names are unique.
type UniqueFragmentNames struct{}

func (r *UniqueFragmentNames) Validate(_ *validator.WalkContext, doc *ast.QueryDocument) gqlerror.List {
	seen := make(map[string]bool)
	var errs gqlerror.List
	for _, frag := range doc.Fragments {
		if seen[frag.Name] {
			errs = append(errs, gqlerror.ErrorPosf(frag.Position,
				`There can be only one fragment named "%s".`, frag.Name))
		}
		seen[frag.Name] = true
	}
	return errs
}
