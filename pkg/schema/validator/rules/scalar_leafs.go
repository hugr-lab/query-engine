package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ScalarLeafs checks that leaf types have no selections and composite types have selections.
type ScalarLeafs struct{ BaseInlineRule }

func (r *ScalarLeafs) EnterField(ctx *validator.WalkContext, _ *ast.Definition, field *ast.Field) gqlerror.List {
	if field.Definition == nil {
		return nil
	}
	typeDef := ctx.Provider.ForName(ctx.Context, field.Definition.Type.Name())
	if typeDef == nil {
		return nil
	}
	if typeDef.IsLeafType() && len(field.SelectionSet) > 0 {
		return gqlerror.List{gqlerror.ErrorPosf(field.Position,
			`Field "%s" must not have a selection since type "%s" has no subfields.`,
			field.Name, field.Definition.Type.Name())}
	}
	if typeDef.IsCompositeType() && len(field.SelectionSet) == 0 {
		return gqlerror.List{gqlerror.ErrorPosf(field.Position,
			`Field "%s" of type "%s" must have a selection of subfields. Did you mean "%s { ... }"?`,
			field.Name, field.Definition.Type.Name(), field.Name)}
	}
	return nil
}
