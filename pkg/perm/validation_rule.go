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
	if auth.IsFullAccess(ctx.Context) {
		return nil
	}
	checker := PermissionsFromCtx(ctx.Context)
	if checker == nil || parentDef == nil {
		return nil
	}
	_, ok := checker.Enabled(parentDef.Name, field.Name)
	if !ok {
		return gqlerror.List{gqlerror.WrapIfUnwrapped(auth.ErrForbidden)}
	}
	// Deny fields returning a disabled data object (table-level rule). This is
	// a fast-path for plain relation/reference fields; aggregation and mutation
	// paths are enforced in the planner, where the target object is resolved
	// (an aggregation field's return type is a synthetic *_aggregation type, so
	// it is not caught here).
	//
	// Order matters: the cheap in-memory permission scan gates the (possibly
	// DB-hitting) type lookup. Only a rule match triggers ForName, and a rule
	// only fires for an actual data object — the IsDataObject check rejects a
	// scalar/struct return type a wildcard rule would otherwise match. If the
	// type cannot be resolved (transient error / suspended catalog), fail
	// closed and deny rather than leak the field.
	if field.Definition != nil {
		if rt := field.Definition.Type.Name(); !sdl.IsScalarType(rt) && checker.DataObjectDisabled(rt, OpQuery) {
			if def := ctx.Provider.ForName(ctx.Context, rt); def == nil || sdl.IsDataObject(def) {
				return gqlerror.List{gqlerror.WrapIfUnwrapped(auth.ErrForbidden)}
			}
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
