package planner

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

// create cast result node to translate query result from TypeCaster QueryEngine to the original duckdb types.
// Objects casts to JSON and scalars casts to natural duckdb types trough intermediate representation.
// receive node that should generate a valid SQL query and a TypeCaster QueryEngine.
func castResultsNode(_ context.Context, caster engines.EngineTypeCaster, node *QueryPlanNode, toOutput, withRowNum bool) (*QueryPlanNode, error) {
	return &QueryPlanNode{
		Name:    node.Name,
		Query:   node.Query,
		Nodes:   QueryPlanNodes{node},
		Comment: "transfer results from the data source",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 0 {
				return "", nil, ErrInternalPlanner
			}
			res := children.FirstResult()
			if res == nil {
				return "", nil, ErrInternalPlanner
			}
			sql, n, err := engines.ApplyQueryParams(caster, res.Result, params)
			if err != nil {
				return "", nil, err
			}
			params = params[:n]
			if strings.HasPrefix(sql, "SELECT") || strings.HasPrefix(sql, "WITH") {
				sql = "(" + sql + ")"
			}
			var intermediateSelection, castingSelection []string
			refFields, err := referencesFields(node.TypeDefs(), node.Query)
			if err != nil {
				return "", nil, err
			}
			ss := engines.SelectedFields(node.Query.SelectionSet)
			// add references fields to type caster
			for _, f := range refFields {
				if ss.ForName(f.Name) != nil {
					continue
				}
				ss = append(ss, engines.SelectedField{
					Field: f,
				})
			}
			for _, f := range ss {
				v, err := caster.ToIntermediateType(f.Field)
				if err != nil {
					return "", nil, err
				}
				if v == f.Field.Alias {
					intermediateSelection = append(intermediateSelection, engines.Ident(f.Field.Alias))
				}
				if v != f.Field.Alias {
					intermediateSelection = append(intermediateSelection, v+" AS "+engines.Ident(f.Field.Alias))
				}
				c, err := caster.CastFromIntermediateType(f.Field, toOutput)
				if err != nil {
					return "", nil, err
				}
				if c == f.Field.Alias {
					castingSelection = append(castingSelection, engines.Ident(f.Field.Alias))
				}
				if c != f.Field.Alias {
					castingSelection = append(castingSelection, c+" AS "+engines.Ident(f.Field.Alias))
				}
			}
			if len(intermediateSelection) == 0 {
				intermediateSelection = append(intermediateSelection, "1")
			}
			if withRowNum {
				castingSelection = append(castingSelection, "row_number() OVER () AS _row_num")
			}
			if len(castingSelection) == 0 {
				castingSelection = append(castingSelection, "1")
			}
			sql = "(SELECT " + strings.Join(intermediateSelection, ",") + " FROM " + sql + " AS _objects)"
			if s, ok := caster.(engines.EngineQueryScanner); ok {
				sql = s.WarpScann(
					base.FieldCatalogName(node.Query.Definition),
					sql,
				)
			}
			return "SELECT " + strings.Join(castingSelection, ",") + " FROM " + sql + " AS _objects", params, nil
		},
	}, nil
}

// accept a node that should generate a valid SQL as scalar value (if array it will be unnested and aggregate after if needed) and a TypeCaster QueryEngine.
func castScalarResultsNode(_ context.Context, caster engines.EngineTypeCaster, node *QueryPlanNode, aggArray, toOutput bool) (*QueryPlanNode, error) {
	return &QueryPlanNode{
		Name:    node.Name,
		Query:   node.Query,
		Nodes:   QueryPlanNodes{node},
		Comment: "transfer scalar results from the data source",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 0 {
				return "", nil, ErrInternalPlanner
			}
			res := children.ForName(node.Name)
			if res == nil {
				return "", nil, ErrInternalPlanner
			}
			sql, n, err := engines.ApplyQueryParams(caster, res.Result, params)
			if err != nil {
				return "", nil, err
			}
			params = params[:n]

			v, err := caster.ToIntermediateType(node.Query)
			if err != nil {
				return "", nil, err
			}
			if v != node.Query.Alias {
				v = v + " AS " + engines.Ident(node.Query.Alias)
			}
			if node.Query.Definition.Type.NamedType == "" {
				sql = "unnest(" + sql + ")"
			}
			sql = "(SELECT " + v + " FROM (SELECT " + sql + " AS " + engines.Ident(node.Query.Alias) + "))"
			if s, ok := caster.(engines.EngineQueryScanner); ok {
				sql = s.WarpScann(
					base.FieldCatalogName(node.Query.Definition),
					sql,
				)
			}

			c, err := caster.CastFromIntermediateType(node.Query, toOutput)
			if err != nil {
				return "", nil, err
			}
			if c != node.Query.Alias {
				c = c + " AS " + engines.Ident(node.Query.Alias)
			}
			if aggArray && node.Query.Definition.Type.NamedType == "" {
				sql = "SELECT array_agg(" + c + ") FROM " + sql
			}
			return "SELECT " + c + " FROM " + sql, params, nil
		},
	}, nil
}

func applyAllParametersNode(child *QueryPlanNode) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    child.Name,
		Query:   child.Query,
		Nodes:   QueryPlanNodes{child},
		Comment: "apply all parameters to query",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 0 {
				return "", nil, ErrInternalPlanner
			}
			res := children.ForName(node.Name)
			if res == nil {
				return "", nil, ErrInternalPlanner
			}
			sql, n, err := engines.ApplyQueryParams(defaultEngine, res.Result, params)
			if err != nil {
				return "", nil, err
			}
			return sql, params[:n], nil
		},
	}
}
