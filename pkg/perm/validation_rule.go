package perm

import (
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/schema/validator"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// PermissionFieldRule is an InlineRule that checks field access permissions
// using RolePermissions from the context.
type PermissionFieldRule struct{}

func (r *PermissionFieldRule) EnterField(ctx *validator.WalkContext, parentDef *ast.Definition, field *ast.Field) gqlerror.List {
	checker := PermissionsFromCtx(ctx.Context)
	if checker == nil || parentDef == nil {
		return nil
	}
	_, ok := checker.Enabled(parentDef.Name, field.Name)
	if !ok {
		return gqlerror.List{gqlerror.WrapIfUnwrapped(auth.ErrForbidden)}
	}
	return nil
}

func (r *PermissionFieldRule) EnterFragment(_ *validator.WalkContext, _ *ast.Definition, _ ast.Selection) gqlerror.List {
	return nil
}

func (r *PermissionFieldRule) EnterDirective(_ *validator.WalkContext, _ *ast.Definition, _ *ast.Directive) gqlerror.List {
	return nil
}

func (r *PermissionFieldRule) EnterArgument(_ *validator.WalkContext, _ *ast.ArgumentDefinition, _ *ast.Argument) gqlerror.List {
	return nil
}
