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
			isRaw := IsRawResultsQuery(ctx, field)
			if !transformTypes && !isRaw {
				return sql, params, nil
			}
			if compiler.IsScalarType(node.Query.Definition.Type.Name()) {
				st, ok := compiler.ScalarTypes[node.Query.Definition.Type.Name()]
				if !ok {
					return "", nil, fmt.Errorf("unknown scalar type %s", node.Query.Definition.Type.Name())
				}
				if st.ToOutputTypeSQL == nil {
					return sql, params, nil
				}
				return fmt.Sprintf("SELECT %s AS %s FROM (%s) AS _raw",
					st.ToOutputTypeSQL(engines.Ident(node.Query.Alias), isRaw),
					engines.Ident(node.Query.Alias), sql,
				), params, nil
			}
			// transform fields to output format
			var fields []string
			for _, f := range engines.SelectedFields(node.Query.SelectionSet) {
				if st, ok := compiler.ScalarTypes[f.Field.Definition.Type.Name()]; ok && st.ToOutputTypeSQL != nil {
					fields = append(fields,
						st.ToOutputTypeSQL(
							engines.Ident(f.Field.Alias),
							isRaw,
						)+" AS "+engines.Ident(f.Field.Alias))
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
