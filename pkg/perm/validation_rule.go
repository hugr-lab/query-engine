package perm

import (
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
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
	// Deny fields returning a disabled data object (table-level rule). This is
	// a fast-path for plain fields; aggregation and mutation paths are enforced
	// in the planner, where the target object is resolved. Scalar return types
	// can never name a data object, so skip the permission scan for them (most
	// selected fields are scalars).
	if field.Definition != nil {
		if rt := field.Definition.Type.Name(); !sdl.IsScalarType(rt) && checker.DataObjectDisabled(rt, OpQuery) {
			return gqlerror.List{gqlerror.WrapIfUnwrapped(auth.ErrForbidden)}
		}
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
