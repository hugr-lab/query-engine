package perm

import (
	"context"
	"fmt"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

type RolePermissions struct {
	Name        string       `json:"name"`
	Disabled    bool         `json:"disabled"`
	Permissions []Permission `json:"permissions"`
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

type Definitions interface {
	ForName(name string) *ast.Definition
}

func (r *RolePermissions) CheckMutationInput(defs Definitions, inputName string, data map[string]any) error {
	if r.Disabled {
		return auth.ErrForbidden
	}
	if compiler.IsScalarType(inputName) {
		return nil
	}
	input := defs.ForName(inputName)
	if input == nil {
		return fmt.Errorf("input type %s not found", inputName)
	}
	for fn, fv := range data {
		if _, ok := r.Enabled(inputName, fn); !ok {
			return auth.ErrForbidden
		}
		if data, ok := fv.(map[string]any); ok {
			if err := r.CheckMutationInput(defs, fn, data); err != nil {
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
	for _, p := range r.Permissions {
		if p.Object == object && p.Field == field {
			return applyContextVariable(ctx, p.Filter, nil)
		}
	}
	return nil
}

func (r *RolePermissions) DataArgument(ctx context.Context, object, field string) map[string]any {
	if r.Disabled {
		return nil
	}
	for _, p := range r.Permissions {
		if p.Object == object && p.Field == field {
			return applyContextVariable(ctx, p.Data, nil)
		}
	}
	return nil
}

func (r *RolePermissions) checkObjectField(object, field string, toVisible bool) (*Permission, bool) {
	if r.Disabled {
		return nil, false
	}
	allObjects := true
	allFields := true
	for _, p := range r.Permissions {
		out := p.Disabled
		if toVisible {
			out = p.Hidden
		}
		switch {
		case p.Object == "*" && p.Field == "*":
			allObjects = !out
			allFields = !out
		case p.Object == "*" && p.Field == field:
			allFields = !out
		case p.Object == object && p.Field == field:
			return &p, !out
		}
	}
	return nil, allFields && allObjects
}

func applyContextVariable(ctx context.Context, data map[string]any, vars map[string]any) map[string]any {
	if len(data) == 0 {
		return nil
	}
	if vars == nil {
		vars = authVars(ctx)
		if len(vars) == 0 {
			return data
		}
	}
	res := make(map[string]any, len(data))
	for k, v := range data {
		switch v := v.(type) {
		case map[string]any:
			res[k] = applyContextVariable(ctx, v, vars)
		case []any:
			for i, vv := range v {
				switch vv := vv.(type) {
				case map[string]any:
					v[i] = applyContextVariable(ctx, vv, vars)
				}
			}
		case string:
			if val, ok := vars[v]; ok {
				res[k] = val
				continue
			}
		}
	}

	return res
}

func authVars(ctx context.Context) map[string]any {
	ai := auth.AuthInfoFromContext(ctx)
	if ai == nil {
		return nil
	}

	userIdInt, _ := strconv.Atoi(ai.UserId)

	return map[string]any{
		"[$auth.user_name]":   ai.UserName,
		"[$auth.user_id]":     ai.UserId,
		"[$auth.user_id_int]": userIdInt,
		"[$auth.role]":        ai.Role,
		"[$auth.auth_type]":   ai.AuthType,
		"[$auth.provider]":    ai.AuthProvider,
	}
}
