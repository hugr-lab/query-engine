package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// KnownArgumentNames checks that arguments exist on the field or directive definition.
type KnownArgumentNames struct{ BaseInlineRule }

func (r *KnownArgumentNames) EnterArgument(_ *validator.WalkContext, argDef *ast.ArgumentDefinition, arg *ast.Argument) gqlerror.List {
	if argDef == nil {
		return gqlerror.List{gqlerror.ErrorPosf(arg.Position,
			`Unknown argument "%s".`, arg.Name)}
	}
	return nil
}
