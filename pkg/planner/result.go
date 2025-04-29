package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

func finalResultNode(ctx context.Context, schema *ast.Schema, planner Catalog, field *ast.Field, node *QueryPlanNode, transformTypes bool) *QueryPlanNode {
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
			if !transformTypes && !IsRawResultsQuery(ctx, field) {
				return sql, params, nil
			}
			geomTransformFunc := "ST_AsGeoJSON"
			if IsRawResultsQuery(ctx, field) {
				geomTransformFunc = "ST_AsWKB"
			}
			if compiler.IsScalarType(node.Query.Definition.Type.Name()) {
				if node.Query.Definition.Type.Name() != compiler.GeometryTypeName {
					return sql, params, nil
				}
				if node.Query.Definition.Type.NamedType != "" {
					return fmt.Sprintf("SELECT %s(%s) AS %s", geomTransformFunc, sql, engines.Ident(node.Query.Alias)), params, nil
				}
				return fmt.Sprintf("SELECT %s(_geom) AS %s FROM (%s) AS _geom",
					geomTransformFunc, engines.Ident(node.Query.Alias), sql,
				), params, nil
			}
			// transform fields to output format
			var fields []string
			for _, f := range engines.SelectedFields(node.Query.SelectionSet) {
				if f.Field.Definition.Type.Name() == compiler.GeometryTypeName {
					fields = append(fields, geomTransformFunc+"("+engines.Ident(f.Field.Alias)+") AS "+engines.Ident(f.Field.Alias))
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
