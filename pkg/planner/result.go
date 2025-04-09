package planner

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

func finalResultNode(_ context.Context, schema *ast.Schema, planner Catalog, field *ast.Field, node *QueryPlanNode, transformTypes bool) *QueryPlanNode {
	node = applyAllParametersNode(node)
	node.engines = planner
	node.schema = schema

	return &QueryPlanNode{
		Name:    field.Name,
		Query:   field,
		Nodes:   QueryPlanNodes{node},
		Comment: "final result",
		CollectFunc: func(node *QueryPlanNode, children Results, params []interface{}) (string, []interface{}, error) {
			if len(children) == 0 {
				return "", nil, ErrInternalPlanner
			}
			res := children.FirstResult()
			if res == nil {
				return "", nil, ErrInternalPlanner
			}
			sql, n, err := engines.ApplyQueryParams(defaultEngine, res.Result, params)
			if err != nil {
				return "", nil, err
			}
			params = params[:n]
			if !transformTypes {
				return sql, params, nil
			}

			if compiler.IsScalarType(node.Query.Definition.Type.Name()) {
				if node.Query.Definition.Type.Name() != compiler.GeometryTypeName {
					return sql, params, nil
				}
				if node.Query.Definition.Type.NamedType != "" {
					return "SELECT ST_AsGeoJSON(" + sql + ") AS " + engines.Ident(node.Query.Alias), params, nil
				}
				return "SELECT ST_AsGeoJSON(_geom) AS " + engines.Ident(node.Query.Alias) +
					" FROM (" + sql + ") AS _geom", params, nil
			}
			// transform fields to output format
			var fields []string
			for _, f := range engines.SelectedFields(node.Query.SelectionSet) {
				if f.Field.Definition.Type.Name() == compiler.GeometryTypeName {
					fields = append(fields, "ST_AsGeoJSON("+engines.Ident(f.Field.Alias)+") AS "+engines.Ident(f.Field.Alias))
					continue
				}
				fields = append(fields, engines.Ident(f.Field.Alias))
			}
			ff := strings.Join(fields, ",")
			if ff == "" {
				ff = "*"
			}
			return "SELECT " + ff + " FROM (" + sql + ")", params, nil
		},
	}
}
