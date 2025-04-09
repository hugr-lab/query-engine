package planner

import (
	"context"
	"maps"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

func permissionFilterNode(ctx context.Context, defs compiler.DefinitionsSource, info *compiler.Object, query *ast.Field, prefix string, byAlias bool) (*QueryPlanNode, error) {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return nil, nil
	}
	if p.Disabled {
		return nil, auth.ErrForbidden
	}
	arg := p.FilterArgument(ctx, query.ObjectDefinition.Name, query.Name)
	if arg == nil {
		return nil, nil
	}
	ftn := info.InputFilterName()
	if ftn == "" {
		return nil, nil
	}
	data, err := compiler.ParseDataAsInputObject(defs, &ast.Type{
		NamedType: ftn,
		Position:  compiler.CompiledPosName("permissionFilterNode"),
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

func checkMutationData(ctx context.Context, defs compiler.DefinitionsSource, query *ast.Field, inputType *ast.Type, data map[string]any) (map[string]any, error) {
	// check permission
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return data, nil
	}

	inputTypeName := inputType.Name()
	if err := p.CheckMutationInput(defs, inputTypeName, data); err != nil {
		return nil, err
	}

	arg := p.DataArgument(ctx, query.ObjectDefinition.Name, query.Name)
	if arg != nil {
		return data, nil
	}

	values, err := compiler.ParseDataAsInputObject(defs, inputType, arg, false)
	if err != nil {
		return nil, err
	}
	if values == nil {
		return data, nil
	}

	maps.Copy(data, values.(map[string]any))

	return data, nil
}
