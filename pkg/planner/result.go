package planner

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/vektah/gqlparser/v2/ast"
)

func finalResultNode(provider catalog.Provider, planner Catalog, field *ast.Field, node *QueryPlanNode, transformTypes bool) *QueryPlanNode {
	node = applyAllParametersNode(node)
	node.engines = planner
	node.provider = provider

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
			// List-typed queries (both list<Object> → QueryArrowTable and
			// [scalar] → QueryJsonScalarArray) flow through the native-
			// Arrow path; RecordToJSON decodes geometry / temporal /
			// decimal on the Go side, so SQL-level ST_AsGeoJSON must be
			// skipped. Scalar / named-type queries flow through
			// QueryJsonRow's wrapJSON and need ST_AsGeoJSON emitted.
			isListField := field.Definition.Type.NamedType == ""
			if !transformTypes && !isListField {
				return sql, params, nil
			}
			if sdl.IsScalarType(node.Query.Definition.Type.Name()) {
				typeName := node.Query.Definition.Type.Name()
				transformed := sdl.ToOutputSQL(typeName, engines.Ident(node.Query.Alias), isListField)
				if transformed == engines.Ident(node.Query.Alias) {
					return sql, params, nil
				}
				return fmt.Sprintf("SELECT %s AS %s FROM (%s) AS _raw",
					transformed,
					engines.Ident(node.Query.Alias), sql,
				), params, nil
			}
			// transform fields to output format
			var fields []string
			for _, f := range engines.SelectedFields(node.Query.SelectionSet) {
				typeName := f.Field.Definition.Type.Name()
				transformed := sdl.ToOutputSQL(typeName, engines.Ident(f.Field.Alias), isListField)
				if transformed != engines.Ident(f.Field.Alias) {
					fields = append(fields, transformed+" AS "+engines.Ident(f.Field.Alias))
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
