package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// KnownDirectives checks that directives are defined and used at correct locations.
type KnownDirectives struct{ BaseInlineRule }

func (r *KnownDirectives) EnterDirective(_ *validator.WalkContext, _ *ast.Definition, dir *ast.Directive) gqlerror.List {
	if dir.Definition == nil {
		return gqlerror.List{gqlerror.ErrorPosf(dir.Position,
			`Unknown directive "@%s".`, dir.Name)}
	}
	hasLocation := false
	for _, loc := range dir.Definition.Locations {
		if loc == dir.Location {
			hasLocation = true
			break
		}
	}
	if !hasLocation {
		return gqlerror.List{gqlerror.ErrorPosf(dir.Position,
			`Directive "@%s" may not be used on %s.`, dir.Name, dir.Location)}
	}
	return nil
}
