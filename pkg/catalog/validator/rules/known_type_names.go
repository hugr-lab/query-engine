package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// KnownTypeNames checks that type conditions in fragments reference known schema types.
type KnownTypeNames struct{ BaseInlineRule }

func (r *KnownTypeNames) EnterFragment(ctx *validator.WalkContext, _ *ast.Definition, frag ast.Selection) gqlerror.List {
	switch frag := frag.(type) {
	case *ast.InlineFragment:
		if frag.TypeCondition == "" {
			return nil
		}
		typDef := ctx.Provider.ForName(ctx.Context, frag.TypeCondition)
		if typDef == nil {
			return gqlerror.List{gqlerror.ErrorPosf(frag.Position,
				`Unknown type "%s".`, frag.TypeCondition)}
		}
	case *ast.FragmentSpread:
		// Fragment type conditions are checked via the fragment definition
	}
	return nil
}
