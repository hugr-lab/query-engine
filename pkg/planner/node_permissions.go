package planner

import (
	"context"
	"maps"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

// permissionFilterNodeName is the plan-node name for the row-level-security
// WHERE fragment. It is looked up by name wherever the filter must survive
// node relocation (join pushdown in node_select.go, WHERE assembly in
// node_select_params.go, delete/update in node_delete.go/node_update.go), so
// it must stay a single shared constant — a missed literal silently drops the
// filter with no compile error.
const permissionFilterNodeName = "permission_filter"

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
	isDataObject := info.IsDataObject()
	if isDataObject && p.DataObjectDisabled(objType, op) {
		return nil, auth.ErrForbidden
	}
	arg := p.FilterArgument(ctx, query.ObjectDefinition.Name, query.Name)
	var objArg map[string]any
	if isDataObject {
		objArg = p.DataObjectFilter(ctx, objType, op)
	}
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
		Name:  permissionFilterNodeName,
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
	// m carries the mutation target; both call sites resolve it before calling,
	// so a nil here is a planner bug — fail closed rather than skip enforcement.
	if m == nil {
		return nil, ErrInternalPlanner
	}

	inputTypeName := inputType.Name()
	if err := p.CheckMutationInput(ctx, defs, inputTypeName, data); err != nil {
		return nil, err
	}

	// field-level data rule for the mutation field: injected over the client
	// data (used to be skipped by an inverted early return)
	if arg := p.DataArgument(ctx, query.ObjectDefinition.Name, query.Name); len(arg) != 0 {
		if err := stampPermissionData(ctx, defs, inputType, arg, data); err != nil {
			return nil, err
		}
	}

	// Table-level (data-object) enforcement. Insert (and its nested reference
	// objects, incl. m2m junctions) is enforced during the insert walk in
	// insertDataObjectNode, where every materialised object is visited exactly
	// once — so here we only enforce the update target, which has no nested
	// traversal. The object-level stamp runs after the field-level one so it is
	// the force-stamp floor.
	if m.Type == sdl.MutationTypeUpdate {
		if err := enforceDataObjectMutation(ctx, defs, m.ObjectDefinition, inputType, perm.OpUpdate, data); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// enforceDataObjectMutation applies the data-object:insert/update rules for a
// single object: it denies the operation when the object is disabled and
// force-stamps the rule's data values over the client's. It does NOT recurse —
// nested inserts are enforced by the insert walk visiting each object.
func enforceDataObjectMutation(ctx context.Context, defs base.DefinitionsSource, objDef *ast.Definition, inputType *ast.Type, op perm.Op, data map[string]any) error {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil || objDef == nil {
		return nil
	}
	if p.DataObjectDisabled(objDef.Name, op) {
		return auth.ErrForbidden
	}
	return stampPermissionData(ctx, defs, inputType, p.DataObjectData(ctx, objDef.Name, op), data)
}

// checkDataObjectInsertDisabled denies an insert into a data object whose
// data-object:insert (or :query) rule is disabled. Used by the insert walk for
// materialised objects that have no stampable data of their own — notably m2m
// junction rows, whose values are the two foreign keys.
func checkDataObjectInsertDisabled(ctx context.Context, objType string) error {
	p := perm.PermissionsFromCtx(ctx)
	if p != nil && p.DataObjectDisabled(objType, perm.OpInsert) {
		return auth.ErrForbidden
	}
	return nil
}

// stampPermissionData overlays a permission rule's data values (field-level or
// data-object) onto the mutation data, coercing them to the object's field
// types via the input definition. Overwriting is the force-stamp guarantee.
func stampPermissionData(ctx context.Context, defs base.DefinitionsSource, inputType *ast.Type, stamp map[string]any, data map[string]any) error {
	if inputType == nil || len(stamp) == 0 {
		return nil
	}
	if inputType.Elem != nil {
		inputType = inputType.Elem
	}
	values, err := sdl.ParseDataAsInputObject(ctx, defs, inputType, stamp, false)
	if err != nil {
		return err
	}
	if vm, ok := values.(map[string]any); ok {
		maps.Copy(data, vm)
	}
	return nil
}
