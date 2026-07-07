package perm

import (
	"context"
	"fmt"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
)

type RolePermissions struct {
	Name           string       `json:"name"`
	Disabled       bool         `json:"disabled"`
	CanImpersonate bool         `json:"can_impersonate"`
	Permissions    []Permission `json:"permissions"`
}

type Permission struct {
	Object   string         `json:"type_name"`
	Field    string         `json:"field_name"`
	Hidden   bool           `json:"hidden"`
	Disabled bool           `json:"disabled"`
	Filter   map[string]any `json:"filter"`
	Data     map[string]any `json:"data"`
}

func (r *RolePermissions) Enabled(object, field string) (*Permission, bool) {
	return r.checkObjectField(object, field, false)
}

func (r *RolePermissions) Visible(object, field string) (*Permission, bool) {
	return r.checkObjectField(object, field, true)
}

func (r *RolePermissions) CheckQuery(query *ast.Field) error {
	if r.Disabled {
		return auth.ErrForbidden
	}
	_, ok := r.Enabled(query.ObjectDefinition.Name, query.Name)
	if !ok {
		return auth.ErrForbidden
	}
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		if err := r.CheckQuery(f.Field); err != nil {
			return err
		}
	}

	return nil
}


func (r *RolePermissions) CheckMutationInput(ctx context.Context, defs base.DefinitionsSource, inputName string, data map[string]any) error {
	if r.Disabled {
		return auth.ErrForbidden
	}
	if sdl.IsScalarType(inputName) {
		return nil
	}
	input := defs.ForName(ctx, inputName)
	if input == nil {
		return fmt.Errorf("input type %s not found", inputName)
	}
	for fn, fv := range data {
		if _, ok := r.Enabled(inputName, fn); !ok {
			return auth.ErrForbidden
		}
		if data, ok := fv.(map[string]any); ok {
			fd := input.Fields.ForName(fn)
			if fd == nil {
				return fmt.Errorf("field %s not found in input type %s", fn, inputName)
			}
			if err := r.CheckMutationInput(ctx, defs, fd.Type.Name(), data); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RolePermissions) FilterArgument(ctx context.Context, object, field string) map[string]any {
	if r.Disabled {
		return nil
	}
	if p := r.bestMatch(object, field); p != nil {
		return applyContextVariable(ctx, p.Filter, nil)
	}
	return nil
}

func (r *RolePermissions) DataArgument(ctx context.Context, object, field string) map[string]any {
	if r.Disabled {
		return nil
	}
	if p := r.bestMatch(object, field); p != nil {
		return applyContextVariable(ctx, p.Data, nil)
	}
	return nil
}

// matchRank reports the specificity with which the permission row matches
// (object, field): 3 = exact (object, field), 2 = (object, *),
// 1 = (*, field), 0 = (*, *), -1 = no match.
func (p *Permission) matchRank(object, field string) int {
	switch {
	case p.Object == object && p.Field == field:
		return 3
	case p.Object == object && p.Field == "*":
		return 2
	case p.Object == "*" && p.Field == field:
		return 1
	case p.Object == "*" && p.Field == "*":
		return 0
	}
	return -1
}

// bestMatch returns the most specific permission row matching (object, field):
// exact (object, field) > (object, *) > (*, field) > (*, *). Rows of equal
// rank keep the first one in the role's permission list. The winning row
// decides all of its attributes (disabled/hidden/filter/data) — lower-ranked
// rows are not consulted.
func (r *RolePermissions) bestMatch(object, field string) *Permission {
	best := -1
	var match *Permission
	for i := range r.Permissions {
		rank := r.Permissions[i].matchRank(object, field)
		if rank > best {
			best = rank
			match = &r.Permissions[i]
		}
	}
	return match
}

func (r *RolePermissions) checkObjectField(object, field string, toVisible bool) (*Permission, bool) {
	if r.Disabled {
		return nil, false
	}
	p := r.bestMatch(object, field)
	if p == nil {
		// open by default: without a matching row access is allowed
		return nil, true
	}
	if toVisible {
		return p, !p.Hidden
	}
	return p, !p.Disabled
}

// applyContextVariable rebuilds data with `[$auth.*]` placeholder strings
// substituted from vars (AuthVars when nil). Every other leaf — literal
// strings, booleans, numbers, nulls, arrays — is preserved as-is, so the
// output is identical to the input except for substituted placeholders.
// The input is never mutated.
func applyContextVariable(ctx context.Context, data map[string]any, vars map[string]any) map[string]any {
	if len(data) == 0 {
		return nil
	}
	if vars == nil {
		vars = AuthVars(ctx)
		if len(vars) == 0 {
			return data
		}
	}
	res := make(map[string]any, len(data))
	for k, v := range data {
		res[k] = applyContextVariableValue(ctx, v, vars)
	}

	return res
}

func applyContextVariableValue(ctx context.Context, v any, vars map[string]any) any {
	switch v := v.(type) {
	case map[string]any:
		return applyContextVariable(ctx, v, vars)
	case []any:
		res := make([]any, len(v))
		for i, vv := range v {
			res[i] = applyContextVariableValue(ctx, vv, vars)
		}
		return res
	case string:
		if val, ok := vars[v]; ok {
			return val
		}
		return v
	default:
		return v
	}
}

func AuthVars(ctx context.Context) map[string]any {
	ai := auth.AuthInfoFromContext(ctx)
	if ai == nil {
		return nil
	}

	userIdInt, _ := strconv.Atoi(ai.UserId)

	vars := make(map[string]any, len(ai.Claims)+9)
	// Custom token claims first, so the built-in placeholders below always win
	// on a name collision (a token claim named "role" cannot shadow the
	// resolved role).
	for k, v := range ai.Claims {
		vars["[$auth."+k+"]"] = v
	}
	vars["[$auth.user_name]"] = ai.UserName
	vars["[$auth.user_id]"] = ai.UserId
	vars["[$auth.user_id_int]"] = userIdInt
	vars["[$auth.role]"] = ai.Role
	vars["[$auth.auth_type]"] = ai.AuthType
	vars["[$auth.provider]"] = ai.AuthProvider
	if ai.ImpersonatedBy != nil {
		vars["[$auth.impersonated_by_role]"] = ai.ImpersonatedBy.Role
		vars["[$auth.impersonated_by_user_id]"] = ai.ImpersonatedBy.UserId
		vars["[$auth.impersonated_by_user_name]"] = ai.ImpersonatedBy.UserName
	}
	return vars
}
