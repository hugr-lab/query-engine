package planner

import (
	"context"
	"maps"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

func permissionFilterNode(ctx context.Context, defs base.DefinitionsSource, info *sdl.Object, query *ast.Field, prefix string, byAlias bool, op perm.Op) (*QueryPlanNode, error) {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return nil, nil
	}
	if p.Disabled {
		return nil, auth.ErrForbidden
	}
	// Table-level (data-object) permissions are keyed on the resolved target
	// object, so they apply on every path the object is materialised through —
	// top-level fields, _by_pk, relations, _join, aggregations, delete/update.
	objType := info.TypeName()
	if p.DataObjectDisabled(objType, op) {
		return nil, auth.ErrForbidden
	}
	arg := p.FilterArgument(ctx, query.ObjectDefinition.Name, query.Name)
	objArg := p.DataObjectFilter(ctx, objType, op)
	switch {
	case len(arg) == 0 && len(objArg) == 0:
		return nil, nil
	case len(arg) == 0:
		arg = objArg
	case len(objArg) != 0:
		// both present: each term only narrows, the object filter is a hard
		// floor a field-level rule cannot widen
		arg = map[string]any{"_and": []any{arg, objArg}}
	}
	ftn := info.InputFilterName()
	if ftn == "" {
		return nil, nil
	}
	data, err := sdl.ParseDataAsInputObject(ctx, defs, &ast.Type{
		NamedType: ftn,
		Position:  base.CompiledPos("permissionFilterNode"),
	}, arg, false)
	if err != nil {
		return nil, err
	}
	node, err := whereNode(ctx, defs, info, data.(map[string]any), prefix, byAlias, true)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	return &QueryPlanNode{
		Name:  "permission_filter",
		Query: query,
		Nodes: QueryPlanNodes{node},
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return children.FirstResult().Result, params, nil
		},
	}, nil
}

func checkMutationData(ctx context.Context, defs base.DefinitionsSource, query *ast.Field, inputType *ast.Type, m *sdl.Mutation, data map[string]any) (map[string]any, error) {
	// check permission
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return data, nil
	}

	inputTypeName := inputType.Name()
	if err := p.CheckMutationInput(ctx, defs, inputTypeName, data); err != nil {
		return nil, err
	}

	// field-level data rule for the mutation field: injected over the client
	// data (used to be skipped by an inverted early return)
	if arg := p.DataArgument(ctx, query.ObjectDefinition.Name, query.Name); len(arg) != 0 {
		values, err := sdl.ParseDataAsInputObject(ctx, defs, inputType, arg, false)
		if err != nil {
			return nil, err
		}
		if vm, ok := values.(map[string]any); ok {
			maps.Copy(data, vm)
		}
	}

	// table-level (data-object) rules: applied to the target object and every
	// nested reference object in the data, after the field-level rule so the
	// object-level values are the force-stamp floor
	if m != nil {
		op := perm.OpInsert
		if m.Type == sdl.MutationTypeUpdate {
			op = perm.OpUpdate
		}
		if err := applyDataObjectMutationRules(ctx, defs, p, m, inputType, op, data); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// applyDataObjectMutationRules enforces data-object:insert / data-object:update
// permission rows on the mutation data: the operation must not be disabled for
// the target object, and the row's data values overwrite the client's
// (force-stamp). Nested reference objects (nested inserts) are enforced
// recursively, so a table-level rule cannot be bypassed by inserting through a
// parent object.
func applyDataObjectMutationRules(ctx context.Context, defs base.DefinitionsSource, p *perm.RolePermissions, m *sdl.Mutation, inputType *ast.Type, op perm.Op, data map[string]any) error {
	if inputType.Elem != nil {
		inputType = inputType.Elem
	}
	if m.ObjectDefinition != nil {
		objType := m.ObjectDefinition.Name
		if p.DataObjectDisabled(objType, op) {
			return auth.ErrForbidden
		}
		if objArg := p.DataObjectData(ctx, objType, op); len(objArg) != 0 {
			values, err := sdl.ParseDataAsInputObject(ctx, defs, inputType, objArg, false)
			if err != nil {
				return err
			}
			if vm, ok := values.(map[string]any); ok {
				maps.Copy(data, vm)
			}
		}
	}
	inputDef := defs.ForName(ctx, inputType.Name())
	if inputDef == nil {
		return nil
	}
	refs := m.ReferencesFields()
	if len(refs) == 0 {
		return nil
	}
	for f, v := range data {
		// only reference fields carry nested inserts; struct/JSON object
		// values are plain data of the current object
		if !slices.Contains(refs, f) {
			continue
		}
		subMut := m.ReferencesMutation(f)
		if subMut == nil {
			continue
		}
		fd := inputDef.Fields.ForName(f)
		if fd == nil {
			continue
		}
		switch v := v.(type) {
		case map[string]any:
			if err := applyDataObjectMutationRules(ctx, defs, p, subMut, fd.Type, op, v); err != nil {
				return err
			}
		case []any:
			for _, item := range v {
				im, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if err := applyDataObjectMutationRules(ctx, defs, p, subMut, fd.Type, op, im); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
