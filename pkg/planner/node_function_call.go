package planner

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// functionCallRootNode is a root node for function call query.
func functionCallRootNode(ctx context.Context, schema *ast.Schema, planer Catalog, query *ast.Field, vars map[string]interface{}) (*QueryPlanNode, error) {
	catalog := base.FieldCatalogName(query.Definition)
	defs := compiler.SchemaDefs(schema)
	node, err := functionCallNode(ctx, defs, planer, "", query, vars)
	if err != nil {
		return nil, err
	}

	e, err := planer.Engine(catalog)
	if err != nil {
		return nil, err
	}
	tc, isTypeCaster := e.(engines.EngineTypeCaster)
	if !isTypeCaster {
		return finalResultNode(ctx,
			schema, planer, query,
			selectFromFunctionCallNode(ctx, defs, node),
			true,
		), nil
	}
	if isTypeCaster {
		node, err = castFunctionResultsNode(ctx, defs, tc, node, !IsRawResultsQuery(ctx, query))
		if err != nil {
			return nil, err
		}
	}
	return selectFromFunctionCallNode(ctx, defs, node), nil
}

func functionCallNode(_ context.Context, defs compiler.DefinitionsSource, planner Catalog, prefix string, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	call := compiler.FunctionCallInfo(query)
	if call == nil {
		return nil, ErrInternalPlanner
	}
	info, err := call.FunctionInfo(defs)
	if err != nil {
		return nil, err
	}
	var nodes QueryPlanNodes
	e, err := planner.Engine(info.Catalog)
	if err != nil {
		return nil, err
	}
	if info.ReturnsTable {
		ff := allSelectedFields(query)
		refFields, err := referencesFields(defs, query)
		if err != nil {
			return nil, err
		}
		for _, f := range refFields {
			found := false
			for _, ff := range ff {
				if f.Alias == ff.Alias {
					found = true
					break
				}
			}
			if !found {
				ff = append(ff, f)
			}
		}
		nodes = append(nodes,
			fieldsNode(query, funcFieldsNodes(e, prefix, ff, vars)),
		)
	}
	if !info.ReturnsTable && len(query.SelectionSet) != 0 {
		nodes = append(nodes, repackObjectNode(e, query, "_value"))
	}
	return &QueryPlanNode{
		Name:    query.Name,
		Query:   query,
		Nodes:   nodes,
		Comment: "function call",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			sql, params, err := functionCallSQL(node.TypeDefs(), e, node.Query, vars, params)
			if err != nil {
				return "", nil, err
			}
			if len(compiler.ExtractFieldsFromSQL(sql)) != 0 {
				return "", nil, compiler.ErrorPosf(node.Query.Position, "function call %s required arguments", info.Name)
			}
			if len(children) == 0 {
				return sql, params, nil
			}
			if fields := children.ForName("fields"); fields != nil {
				if fields.Result == "" {
					return "SELECT * FROM " + sql, params, nil
				}
				return "SELECT " + fields.Result + " " +
					"FROM " + sql + " AS _objects", params, nil
			}
			if repack := children.ForName("repack"); repack != nil {
				return "SELECT " + repack.Result + " " +
					"FROM (SELECT " + sql + " AS _value)", params, nil
			}

			return "", nil, ErrInternalPlanner
		},
	}, nil
}

// generates select statement from function call node.
// if func returns scalar value (non array): SELECT func() AS alias
// if func returns array of scalar: SELECT unnest(func()) AS alias
// if func returns object: SELECT unpack(value) AS alias FROM (SELECT func() AS _value)
// if func is table function: SELECT fields FROM func(), where fields is repack values
func selectFromFunctionCallNode(_ context.Context, defs compiler.DefinitionsSource, node *QueryPlanNode) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    node.Name,
		Query:   node.Query,
		Nodes:   QueryPlanNodes{node},
		Comment: "select from function call",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 0 {
				return "", nil, ErrInternalPlanner
			}
			res := children.ForName(node.Name)
			if res == nil {
				return "", nil, ErrInternalPlanner
			}
			call := compiler.FunctionCallInfo(node.Query)
			if call == nil {
				return "", nil, ErrInternalPlanner
			}
			info, err := call.FunctionInfo(defs)
			if err != nil {
				return "", nil, err
			}
			funcCallSQL := res.Result
			if info.ReturnsTable {
				return funcCallSQL, params, nil
			}
			if compiler.IsScalarType(node.Query.Definition.Type.Name()) {
				if node.Query.Definition.Type.NamedType == "" {
					return "SELECT unnest(" + funcCallSQL + ") AS " + engines.Ident(node.Query.Alias), params, nil
				}
				return "SELECT " + funcCallSQL + " AS " + engines.Ident(node.Query.Alias), params, nil
			}

			return "SELECT " + unpackObject("_value") + " " +
				"FROM (SELECT (" + funcCallSQL + ") AS _value)", params, nil
		},
	}
}

func castFunctionResultsNode(ctx context.Context, defs compiler.DefinitionsSource, caster engines.EngineTypeCaster, child *QueryPlanNode, toJSON bool) (*QueryPlanNode, error) {
	call := compiler.FunctionCallInfo(child.Query)
	if call == nil {
		return nil, ErrInternalPlanner
	}
	info, err := call.FunctionInfo(defs)
	if err != nil {
		return nil, err
	}
	if info.ReturnsTable {
		return castResultsNode(ctx, caster, child, toJSON, false)
	}

	return castScalarResultsNode(ctx, caster, child, true, toJSON)
}

func repackObjectNode(e engines.Engine, query *ast.Field, sqlName string) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    "repack",
		Query:   query,
		Comment: "repack object " + sqlName,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return e.RepackObject(sqlName, query) + " AS " + engines.Ident(query.Alias), params, nil
		},
	}
}

func unpackObject(sqlName string) string {
	return sqlName + ".*"
}

func funcFieldsNodes(e engines.Engine, prefix string, fields []*ast.Field, vars map[string]any) QueryPlanNodes {
	var nodes QueryPlanNodes
	for _, field := range fields {
		nodes = append(nodes, &QueryPlanNode{
			Name:    field.Alias,
			Query:   field,
			Comment: fmt.Sprintf("field %s (%s)", field.Name, field.Alias),
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				if node.Query.Name == "__typename" {
					return fmt.Sprintf("'%s' AS %s", node.Query.ObjectDefinition.Name, engines.Ident(node.Query.Alias)), params, nil
				}
				info := compiler.FieldInfo(node.Query)
				sql := info.SQL(prefix)
				typeName := info.Definition().Type.NamedType
				if len(node.Query.Arguments) != 0 &&
					compiler.IsScalarType(typeName) &&
					(compiler.ScalarTypes[typeName].Arguments != nil || compiler.IsExtraField(info.Definition())) {
					args, err := compiler.ArgumentValues(node.TypeDefs(), field, vars, false)
					if err != nil {
						return "", nil, err
					}
					sql = e.ApplyFieldTransforms(sql, field, args)
				}
				if len(field.SelectionSet) != 0 {
					sql = e.RepackObject(sql, field)
				}
				if sql == field.Alias {
					return sql, params, nil
				}
				return sql + " AS " + engines.Ident(field.Alias), params, nil
			},
		})
	}
	return nodes
}

func functionCallSQL(defs compiler.Definitions, e engines.Engine, field *ast.Field, vars map[string]any, params []any) (string, []any, error) {
	call := compiler.FunctionCallInfo(field)
	if call == nil {
		return "", nil, ErrInternalPlanner
	}
	info, err := call.FunctionInfo(defs)
	if err != nil {
		return "", nil, err
	}
	sql := info.SQL()
	_, isScanner := e.(engines.EngineQueryScanner)
	// json cast work only in duckdb base engines (with out scanner)
	if info.JsonCast && !isScanner {
		if len(field.SelectionSet) != 0 {
			sql = "(SELECT " + engines.JsonToStruct(field, "", true, true) +
				" FROM (SELECT " + sql + " AS " + engines.Ident(field.Alias) + "))"
		}
		if len(field.SelectionSet) == 0 {
			sql = "(SELECT " + sql + "::JSON)"
		}
	}

	for k, v := range call.ArgumentMap() {
		sql = strings.ReplaceAll(sql, "["+k+"]", "["+v+"]")
	}
	queryArg, err := call.ArgumentValues(defs, vars)
	if err != nil {
		return "", nil, err
	}
	for _, a := range queryArg {
		switch {
		case a.Value == nil && info.SkipNullArg:
			sql = strings.ReplaceAll(sql, "["+a.Name+"]", "")
		case a.Value == nil:
			sql = strings.ReplaceAll(sql, "["+a.Name+"]", "NULL")
		default:
			params = append(params, a.Value)
			sql = strings.ReplaceAll(sql, "["+a.Name+"]", "$"+strconv.Itoa(len(params)))
		}
	}

	return sql, params, nil
}
