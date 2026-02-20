package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// FragmentsOnCompositeTypes checks that fragments are only defined on composite types.
type FragmentsOnCompositeTypes struct{ BaseInlineRule }

func (r *FragmentsOnCompositeTypes) EnterFragment(ctx *validator.WalkContext, _ *ast.Definition, frag ast.Selection) gqlerror.List {
	switch frag := frag.(type) {
	case *ast.InlineFragment:
		if frag.TypeCondition == "" {
			return nil
		}
		typDef := ctx.Provider.ForName(ctx.Context, frag.TypeCondition)
		if typDef != nil && !typDef.IsCompositeType() {
			return gqlerror.List{gqlerror.ErrorPosf(frag.Position,
				`Fragment cannot condition on non composite type "%s".`, frag.TypeCondition)}
		}
	case *ast.FragmentSpread:
		if frag.Definition == nil {
			return nil
		}
		typDef := ctx.Provider.ForName(ctx.Context, frag.Definition.TypeCondition)
		if typDef != nil && !typDef.IsCompositeType() {
			return gqlerror.List{gqlerror.ErrorPosf(frag.Position,
				`Fragment "%s" cannot condition on non composite type "%s".`,
				frag.Name, frag.Definition.TypeCondition)}
		}
	}
	return nil
}
