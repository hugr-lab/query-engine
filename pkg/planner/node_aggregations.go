package planner

import (
	"context"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"gopkg.in/errgo.v2/fmt/errors"
)

func aggregateRootNode(ctx context.Context, schema *ast.Schema, planner Catalog, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	node, inGeneral, err := aggregateDataNode(ctx, compiler.SchemaDefs(schema), planner, false, query, vars)
	if err != nil {
		return nil, err
	}
	// catalog for the query
	catalog := base.FieldCatalogName(query.Definition)
	e, err := planner.Engine(catalog)
	if err != nil {
		return nil, err
	}
	if caster, ok := e.(engines.EngineTypeCaster); ok && !inGeneral {
		node, err = castResultsNode(ctx, caster, node, !IsRawResultsQuery(ctx, query), false)
		if err != nil {
			return nil, err
		}
	}

	return finalResultNode(ctx, schema, planner, query, node, inGeneral), nil
}

// returns nodes: fields, from, where, group by (if bucket aggregation)
func joinAggregateNodes(_ context.Context, defs compiler.DefinitionsSource, planner Catalog, inGeneral bool, right *ast.Field, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	nodes.Add(&QueryPlanNode{
		Name:  "fields",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			catalog := base.FieldCatalogName(right.Definition)
			e, err := node.Engine(catalog)
			if err != nil {
				return "", nil, err
			}
			if inGeneral {
				e = defaultEngine
			}
			sql := e.PackFieldsToObject(rAlias, right)
			return rAlias + "._root_row_num, " + sql + " AS _selection", params, nil
		},
	})
	nodes.Add(&QueryPlanNode{
		Name:  "from",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return rAlias, params, nil
		},
	})
	nodes.Add(&QueryPlanNode{
		Name:  "where",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return "_objects._row_num = " + rAlias + "._root_row_num", params, nil
		},
	})

	if compiler.IsBucketAggregateQuery(right) {
		nodes = append(nodes, &QueryPlanNode{
			Name:  "fields_agg",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_root_row_num, array_agg( _selection) AS _selection", params, nil
			},
		})
	}
	if right.Directives.ForName(base.UnnestDirective) == nil {
		nodes.Add(&QueryPlanNode{
			Name:  "groupBy",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_root_row_num", params, nil
			},
		})
	}

	return nodes, nil
}

func aggregateDataNode(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, inGeneral bool, query *ast.Field, vars map[string]any) (*QueryPlanNode, bool, error) {
	// 1. get references data object
	aggregated := compiler.AggregatedQueryDef(query)
	// 2. create references object query (if table function than pass parameters and add selection set to perform aggregation)
	var keyFields, aggFields fieldList
	// split by aggregate and bucket key fields
	if !compiler.IsBucketAggregateQuery(query) {
		var err error
		aggFields, err = aggSelectedFieldsForAggregation(defs, query)
		if err != nil {
			return nil, false, err
		}
	}
	if compiler.IsBucketAggregateQuery(query) {
		for _, f := range engines.SelectedFields(query.SelectionSet) {
			// if not bucket aggregation add selected fields for aggregate
			if f.Field.Name == compiler.AggregateKeyFieldName {
				keyFields = append(keyFields, f.Field)
			}
			if f.Field.Name == compiler.AggregateFieldName {
				aggFields = append(aggFields, f.Field)
			}
		}
	}
	// if keys is not empty create group by fields (merge selected fields)
	refFields, err := referencesFields(defs, &ast.Field{
		Alias:            query.Alias,
		Name:             aggregated.Name,
		Arguments:        query.Arguments,
		Definition:       aggregated,
		ObjectDefinition: query.ObjectDefinition,
		Directives:       query.Directives,
		Position:         query.Position,
		Comment:          query.Comment,
	})
	if err != nil {
		return nil, false, err
	}
	var groupByFields fieldList
	var groupByNodes QueryPlanNodes
	var ri *compiler.References

	groupByFieldsMap := map[string]map[string]string{}
	for _, f := range keyFields {
		fm := map[string]string{}
		for _, keySelected := range engines.SelectedFields(f.SelectionSet) {
			alias := "_key_" + f.Alias + "_" + keySelected.Field.Alias
			fm[keySelected.Field.Alias] = alias
			groupByFields = append(groupByFields, &ast.Field{
				Alias:            alias,
				Name:             keySelected.Field.Name,
				Arguments:        keySelected.Field.Arguments,
				Directives:       keySelected.Field.Directives,
				SelectionSet:     keySelected.Field.SelectionSet,
				Definition:       keySelected.Field.Definition,
				ObjectDefinition: keySelected.Field.ObjectDefinition,
				Position:         keySelected.Field.Position,
				Comment:          keySelected.Field.Comment,
			})
			groupByNodes.Add(&QueryPlanNode{
				Name: keySelected.Field.Alias,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return alias, params, nil
				},
			})
		}
		groupByFieldsMap[f.Alias] = fm
	}
	// create base queries for each aggregations fields (with out sub query fields)
	queries := map[string]*ast.Field{}
	queryNodes := map[string]*QueryPlanNode{}
	def := defs.ForName(query.Definition.Type.Name())
	def = compiler.AggregatedObjectDef(defs, def)
	catalog := ""
	if compiler.IsDataObject(def) {
		info := compiler.DataObjectInfo(def)
		if info == nil {
			return nil, false, errors.New("data object info not found")
		}
		catalog = info.Catalog
	}
	if catalog == "" {
		catalog = base.FieldCatalogName(query.Definition)
	}
	if catalog == "" {
		return nil, false, errors.New("catalog not found")
	}
	e, err := planner.Engine(catalog)
	if err != nil {
		return nil, false, err
	}
	caster, isCaster := e.(engines.EngineTypeCaster)
	isInCatalog := true
	if !compiler.IsBucketAggregateQuery(query) {
		baseQuery := &ast.Field{
			Alias:            "_" + query.Alias,
			Name:             aggregated.Name,
			Arguments:        query.Arguments,
			Definition:       aggregated,
			ObjectDefinition: query.ObjectDefinition,
			Directives:       query.Directives,
			SelectionSet:     append(groupByFields.AsSelectionSet(), aggFields.AsSelectionSet()...),
		}
		queries["aggregation"] = baseQuery

		var (
			node           *QueryPlanNode
			queryInGeneral bool
		)
		switch {
		case compiler.IsDataObject(def):
			node, queryInGeneral, err = selectDataObjectNode(ctx, defs, planner, baseQuery, vars)
		case compiler.IsFunctionCall(aggregated), compiler.IsFunction(aggregated):
			node, err = functionCallNode(ctx, defs, planner, "", baseQuery, vars)
		default:
			return nil, false, errors.New("unsupported aggregated query")
		}
		if err != nil {
			return nil, false, err
		}
		_, ok := e.(engines.EngineAggregator)
		// cast to general if needed
		// query not casted (!queryInGeneral), inCatalog or query run in general or engine is not aggregation and engine is caseter
		if !queryInGeneral && (!ok || inGeneral) && isCaster {
			queryInGeneral = true
			node, err = castResultsNode(ctx, caster, node, false, false)
			if err != nil {
				return nil, false, err
			}
		}
		if queryInGeneral {
			isInCatalog = false
		}
		queryNodes["aggregation"] = node
	}
	if compiler.IsBucketAggregateQuery(query) {
		for _, f := range aggFields {
			baseQuery := &ast.Field{
				Alias:            "_" + query.Alias + "_" + f.Alias,
				Name:             aggregated.Name,
				Definition:       aggregated,
				ObjectDefinition: query.ObjectDefinition,
				Directives:       query.Directives,
				SelectionSet:     groupByFields.AsSelectionSet(),
			}
			ff, err := aggSelectedFieldsForAggregation(defs, f)
			if err != nil {
				return nil, false, err
			}
			baseQuery.SelectionSet = append(baseQuery.SelectionSet, ff.AsSelectionSet()...)
			// merge filter arguments
			// order by arguments
			if a := f.Arguments.ForName("order_by"); a != nil {
				baseQuery.Arguments = append(baseQuery.Arguments, a)
			}
			// args for parameterized view
			if a := query.Arguments.ForName("args"); a != nil {
				baseQuery.Arguments = append(baseQuery.Arguments, a)
			}
			// filter arguments
			if qa := query.Arguments.ForName("filter"); qa != nil {
				if a := f.Arguments.ForName("filter"); a != nil {
					baseQuery.Arguments = append(baseQuery.Arguments, qa)
					if and := qa.Value.Children.ForName("_and"); and != nil {
						and.Children = append(and.Children,
							&ast.ChildValue{Value: a.Value, Position: a.Position, Comment: a.Comment},
						)
					} else {
						qa.Value.Children = append(qa.Value.Children, &ast.ChildValue{
							Name: "_and",
							Value: &ast.Value{
								Kind: ast.ListValue,
								Children: ast.ChildValueList{
									{Value: a.Value, Position: a.Position, Comment: a.Comment},
								},
							},
						})
					}
				}
				if f.Arguments.ForName("filter") == nil {
					baseQuery.Arguments = append(baseQuery.Arguments, qa)
				}
			}
			if baseQuery.Arguments.ForName("filter") == nil {
				aggArg := f.Arguments.ForName("filter")
				if aggArg != nil {
					baseQuery.Arguments = append(baseQuery.Arguments, aggArg)
				}
			}

			queries[f.Alias] = baseQuery
			var (
				node           *QueryPlanNode
				queryInGeneral bool
			)
			switch {
			case compiler.IsDataObject(def):
				node, queryInGeneral, err = selectDataObjectNode(ctx, defs, planner, baseQuery, vars)
			case compiler.IsFunctionCall(aggregated), compiler.IsFunction(aggregated):
				node, err = functionCallNode(ctx, defs, planner, "", baseQuery, vars)
			default:
				return nil, false, errors.New("unsupported aggregated query")
			}
			if err != nil {
				return nil, false, err
			}
			_, ok := e.(engines.EngineAggregator)
			// cast to general if needed
			if !queryInGeneral && (inGeneral || !ok || !isInCatalog) && isCaster {
				queryInGeneral = true
				node, err = castResultsNode(ctx, caster, node, false, false)
				if err != nil {
					return nil, false, err
				}
			}
			// cast all revisions nodes to general if needed
			if queryInGeneral && isInCatalog && isCaster {
				// transform all revisions nodes to general
				for alias, n := range queryNodes {
					queryNodes[alias], err = castResultsNode(ctx, caster, n, false, false)
					if err != nil {
						return nil, false, err
					}
				}
			}
			if isInCatalog && queryInGeneral {
				isInCatalog = false
			}
			queryNodes[f.Alias] = node
		}
	}
	aggregator, ok := e.(engines.EngineAggregator)
	if !ok || !isInCatalog {
		aggregator = defaultEngine
	}
	var unions QueryPlanNodes

	for alias, node := range queryNodes {
		// create aggregation node
		node.Name = "_" + alias
		// 1. fields
		var fieldNodes QueryPlanNodes
		// 1.1. references fields to join with base query add row number from object
		if len(refFields) != 0 {
			fieldNodes.Add(&QueryPlanNode{
				Name: "_row_num",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_root_objects._row_num AS _root_row_num", params, nil
				},
			})
		}
		// 1.2. add group by fields (key fields)
		for _, key := range keyFields {
			fieldNodes.Add(&QueryPlanNode{
				Name: key.Alias,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					fm := groupByFieldsMap[key.Alias]
					return aggregator.MakeObject(fm) + " AS " + node.Name, params, nil
				},
			})
		}
		// 1.3. add aggregation fields
		// 1.3.1. add aggregation fields for non bucket aggregation (as is with out sub query fields)
		if !compiler.IsBucketAggregateQuery(query) {
			ff, err := aggAggregationFieldNodes(aggregator, defs, query, vars, node.Name, "")
			if err != nil {
				return nil, false, err
			}
			for _, f := range ff {
				fieldNodes.Add(&QueryPlanNode{
					Name:  f.Name,
					Query: f.Query,
					Nodes: QueryPlanNodes{f},
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						sql := children.FirstResult().Result
						return sql + " AS " + node.Name, params, nil
					},
				})
			}
		}
		// 1.3.2. add aggregation fields for bucket aggregation (with sub query fields)
		if compiler.IsBucketAggregateQuery(query) {
			f := engines.SelectedFields(query.SelectionSet).ForAlias(alias)
			if f == nil {
				return nil, false, compiler.ErrorPosf(query.Position, "field %s not found in query", alias)
			}
			ff, err := aggAggregationFieldNodes(aggregator, defs, f.Field, vars, node.Name, "")
			if err != nil {
				return nil, false, err
			}
			// add aggregation object field
			fieldNodes = append(fieldNodes, &QueryPlanNode{
				Name:  alias,
				Query: f.Field,
				Nodes: ff,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					fm := map[string]string{}
					for _, v := range children {
						fm[v.Name] = v.Result
					}
					return aggregator.MakeObject(fm) + " AS " + node.Name, params, nil
				},
			})
			// add aggregation fields for other aggregations objects
			for f := range queries {
				if f == alias {
					continue
				}
				fieldNodes.Add(&QueryPlanNode{
					Name: f,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return aggregator.JSONTypeCast("NULL") + " AS " + f, params, nil
					},
				})
			}
		}

		fromNode := &QueryPlanNode{
			Name: "from",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_" + alias, params, nil
			},
		}
		nodes := QueryPlanNodes{
			withNode(QueryPlanNodes{node}),
			fieldsNode(query, fieldNodes),
			fromNode,
		}
		if len(refFields) != 0 {
			var joinNodes QueryPlanNodes
			if ri != nil && ri.IsM2M {
				joinNodes.Add(&QueryPlanNode{
					Name: "m2m",
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						m2m := node.TypeDefs().ForName(ri.M2MName)
						m2mInfo := compiler.DataObjectInfo(m2m)
						refInfo := m2mInfo.M2MReferencesQueryInfo(defs, ri.Name)
						jc, err := refInfo.JoinConditions(node.TypeDefs(), "_join_m2m", "_"+alias, true, false)
						if err != nil {
							return "", nil, err
						}
						db := m2mInfo.Catalog
						if isInCatalog {
							db = ""
						}
						return "INNER JOIN " + m2mInfo.SQL(ctx, db) + " AS _join_m2m ON " + jc, params, nil
					},
				})
			}
			// add join to base object
			rAlias := "_" + alias
			if ri != nil && ri.IsM2M {
				rAlias = "_join_m2m"
			}
			joinConditionNode := aggregationWhereJoinNode(defs, query, "_root_objects", rAlias)
			joinNodes.Add(&QueryPlanNode{
				Name:  "join",
				Nodes: QueryPlanNodes{joinConditionNode},
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					sql := children.FirstResult().Result
					return "INNER JOIN _objects AS _root_objects ON " + sql, params, nil
				},
			})
			if len(joinNodes) != 0 {
				nodes.Add(joinsNode(joinNodes))
			}
		}
		if len(groupByNodes) != 0 || len(refFields) != 0 {
			nodes.Add(&QueryPlanNode{
				Name:  "groupBy",
				Nodes: groupByNodes,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					var sql []string
					for _, v := range children {
						sql = append(sql, v.Result)
					}
					if len(refFields) != 0 {
						sql = append(sql, "_root_objects._row_num")
					}
					return strings.Join(sql, ", "), params, nil
				},
			})
		}
		node = selectStatementNode(query, nodes, "_"+alias, false)
		if !compiler.IsBucketAggregateQuery(query) {
			return node, !isInCatalog, nil
		}
		unions.Add(node)
	}
	/*if len(unions) == 1 {
		return unions[0], !isInCatalog, nil
	}*/

	var finalFieldList, fieldList, groupByFieldList []string
	if len(refFields) != 0 {
		fieldList = append(fieldList, "_root_row_num")
	}
	for _, f := range keyFields {
		fieldList = append(fieldList, engines.Ident(f.Alias))
	}
	finalFieldList = append(finalFieldList, fieldList...)
	groupByFieldList = append(groupByFieldList, fieldList...)
	for _, f := range aggFields {
		fieldList = append(fieldList, engines.Ident(f.Alias))
		sql := aggregator.AggregateFuncAny(engines.Ident(f.Alias))
		finalFieldList = append(finalFieldList, sql+" AS "+engines.Ident(f.Alias))
	}
	nodes := QueryPlanNodes{
		{
			Name: "fields",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return strings.Join(finalFieldList, ", "), params, nil
			},
		},
		{
			Name:  "from",
			Nodes: unions,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				var unionSQL []string
				for _, v := range children {
					unionSQL = append(unionSQL,
						"SELECT "+
							strings.Join(fieldList, ",")+
							" FROM ("+v.Result+") AS _objects",
					)
				}
				sql := strings.Join(unionSQL, " UNION ")
				return "(" + sql + ") AS _objects", params, nil
			},
		},
		{
			Name: "groupBy",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return strings.Join(groupByFieldList, ","), params, nil
			},
		},
	}
	node := selectStatementNode(query, nodes, "", false)
	qe := e
	if !isInCatalog {
		qe = defaultEngine
	}
	queryArg, err := compiler.ArgumentValues(defs, query, vars, false)
	if err != nil {
		return nil, false, err
	}
	paramNodes, err := selectQueryParamsNodes(ctx, defs, qe, nil, "", query, queryArg, true)
	if err != nil {
		return nil, false, err
	}
	paramNodes = slices.DeleteFunc(paramNodes, func(n *QueryPlanNode) bool {
		return n.Name == "where"
	})
	if len(paramNodes) == 0 {
		return node, !isInCatalog, nil
	}
	extraNodes := QueryPlanNodes{
		{
			Name: "fields",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "*", params, nil
			},
		},
		{
			Name:  "from",
			Nodes: QueryPlanNodes{node},
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				sql := children.FirstResult().Result
				return "(" + sql + ") AS _objects", params, nil
			},
		},
	}
	extraNodes = append(extraNodes, paramNodes...)
	return selectStatementNode(query, extraNodes, "", false), !isInCatalog, nil
}

func aggSelectedFieldsForAggregation(defs compiler.Definitions, query *ast.Field) (fieldList, error) {
	var fields fieldList
	def := defs.ForName(query.Definition.Type.Name())
	def = compiler.AggregatedObjectDef(defs, def)
	if def == nil {
		return nil, compiler.ErrorPosf(query.Definition.Position, "aggregated object %s not found", query.Definition.Type.Name())
	}
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		if f.Field.Name == "__typename" {
			continue
		}
		if f.Field.Name == compiler.AggRowsCountFieldName {
			continue
		}
		fd := def.Fields.ForName(f.Field.Name)
		if fd == nil {
			return nil, compiler.ErrorPosf(f.Field.Position, "field %s not found in aggregated object %s", f.Field.Alias, def.Name)
		}
		fields = append(fields, &ast.Field{
			Alias:            f.Field.Alias,
			Name:             f.Field.Name,
			Arguments:        f.Field.Arguments,
			Directives:       f.Field.Directives,
			Position:         f.Field.Position,
			Comment:          f.Field.Comment,
			Definition:       fd,
			ObjectDefinition: def,
		})
		if compiler.IsScalarType(fd.Type.Name()) {
			continue
		}
		if len(f.Field.SelectionSet) == 0 {
			return nil, compiler.ErrorPosf(f.Field.Position, "field %s must have selection set", f.Field.Alias)
		}
		if fd.Type.NamedType == "" {
			fields[len(fields)-1].Directives = append(fields[len(fields)-1].Directives, &ast.Directive{
				Name: base.UnnestDirective,
			})
		}
		ff, err := aggSelectedFieldsForAggregation(defs, f.Field)
		if err != nil {
			return nil, err
		}
		fields[len(fields)-1].SelectionSet = ff.AsSelectionSet()
	}
	return fields, nil
}

func aggAggregationFieldNodes(e engines.EngineAggregator, defs compiler.Definitions, query *ast.Field, vars map[string]any, prefix, path string) (QueryPlanNodes, error) {
	def := defs.ForName(query.Definition.Type.Name())
	def = compiler.AggregatedObjectDef(defs, def)
	if def == nil {
		return nil, compiler.ErrorPosf(query.Definition.Position, "aggregated object %s not found", query.Definition.Type.Name())
	}
	// add selected fields recursively
	var nodes QueryPlanNodes
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		if f.Field.Name == "__typename" {
			nodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "'" + query.Definition.Type.Name() + "'", params, nil
				},
			})
			continue
		}
		if f.Field.Name == compiler.AggRowsCountFieldName {
			nodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return e.AggregateFuncSQL("count", "", "", f.Field, f.Field.ArgumentMap(vars), params)
				},
			})
			continue
		}

		// find field definition
		fd := def.Fields.ForName(f.Field.Name)
		if fd == nil {
			return nil, compiler.ErrorPosf(f.Field.Position, "field %s not found in aggregated object %s", f.Field.Alias, def.Name)
		}
		// if it is scalar type than add selected aggregation to the field (as object)
		if compiler.IsScalarType(fd.Type.Name()) {
			// add aggregations for fields
			var subNodes QueryPlanNodes
			// loop over selected aggregation fields
			for _, ac := range engines.SelectedFields(f.Field.SelectionSet) {
				subNodes.Add(&QueryPlanNode{
					Name:  ac.Field.Alias,
					Query: ac.Field,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						if ac.Field.Name == "__typename" {
							return "'" + ac.Field.ObjectDefinition.Name + "'", params, nil
						}
						// path
						sql := engines.Ident(f.Field.Alias)
						if path == "" && prefix != "" {
							sql = prefix + "." + sql
						}
						sp := path
						if path != "" {
							sp += "." + f.Field.Alias
							pp := strings.SplitN(sp, ".", 2)
							if prefix != "" {
								sql = prefix + "." + pp[0]
							}
							sp = pp[1]
						}
						sql, params, err := e.AggregateFuncSQL(ac.Field.Name, sql, sp, f.Field, ac.Field.ArgumentMap(vars), params)
						if err != nil {
							return "", nil, err
						}
						return sql, params, nil
					},
				})
			}
			nodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				Nodes: subNodes,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					m := map[string]string{}
					for _, v := range children {
						m[v.Name] = v.Result
					}
					return e.MakeObject(m), params, nil
				},
			})
			continue
		}
		if len(f.Field.SelectionSet) == 0 {
			return nil, compiler.ErrorPosf(f.Field.Position, "field %s must have selection set", f.Field.Alias)
		}
		subPath := path
		if subPath != "" {
			subPath += "."
		}
		subPath += f.Field.Alias
		ff, err := aggAggregationFieldNodes(e, defs, f.Field, vars, prefix, subPath)
		if err != nil {
			return nil, err
		}
		nodes.Add(&QueryPlanNode{
			Name:  f.Field.Alias,
			Query: f.Field,
			Nodes: ff,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				m := map[string]string{}
				for _, v := range children {
					m[v.Name] = v.Result
				}
				return e.MakeObject(m), params, nil
			},
		})
	}
	return nodes, nil
}

func aggregationWhereJoinNode(defs compiler.DefinitionsSource, right *ast.Field, prefix, rAlias string) *QueryPlanNode {
	aggregated := compiler.AggregatedQueryDef(right)
	aggregatedQuery := &ast.Field{
		Alias:            right.Alias,
		Name:             aggregated.Name,
		Arguments:        right.Arguments,
		Definition:       aggregated,
		Directives:       right.Directives,
		ObjectDefinition: right.ObjectDefinition,
		Position:         right.Position,
		Comment:          right.Comment,
	}
	return &QueryPlanNode{
		Name:  "where",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			switch {
			case compiler.IsJoinSubquery(aggregatedQuery):
				ji := compiler.JoinInfo(aggregatedQuery)
				if ji == nil {
					return "", nil, errors.New("join info not found")
				}
				sql := ji.JoinConditionsTemplate()
				if prefix != "" {
					prefix += "."
				}
				leftFields, err := ji.SourceFields()
				if err != nil {
					return "", nil, err
				}
				for _, fn := range leftFields {
					sql = strings.ReplaceAll(sql, "["+compiler.JoinSourceFieldPrefix+"."+fn+"]", prefix+fn)
				}
				rightFields, err := ji.ReferencesFields()
				if err != nil {
					return "", nil, err
				}
				for _, fn := range rightFields {
					sql = strings.ReplaceAll(sql, "["+compiler.JoinRefFieldPrefix+"."+fn+"]", rAlias+"."+fn)
				}
				return sql, params, nil
			case compiler.IsReferencesSubquery(aggregated):
				info := compiler.DataObjectInfo(right.ObjectDefinition)
				ri := info.ReferencesQueryInfo(defs, aggregated.Name)
				if ri == nil {
					return "", nil, errors.New("references query info not found")
				}

				fields := ri.ReferencesFields()
				if ri.IsM2M {
					m2m := defs.ForName(ri.M2MName)
					m2mInfo := compiler.DataObjectInfo(m2m)
					ri = m2mInfo.ReferencesQueryInfoByName(defs, ri.Name)
					if ri == nil {
						return "", nil, errors.New("references query info not found")
					}
					fields = ri.ReferencesFields()
				}
				jc, err := ri.JoinConditions(defs, prefix, rAlias, false, false)
				if err != nil {
					return "", nil, err
				}
				// replace right fields in join conditions
				for _, f := range fields {
					jc = strings.ReplaceAll(jc, rAlias+"."+f, rAlias+"."+f)
				}
				return jc, params, nil
			case compiler.IsTableFuncJoinSubquery(aggregatedQuery):
				call := compiler.FunctionCallInfo(aggregatedQuery)
				if call == nil {
					return "", nil, errors.New("function call info not found")
				}
				sql := call.JoinConditionsTemplate()
				leftFields, err := call.SourceFields()
				if err != nil {
					return "", nil, err
				}
				if prefix != "" {
					prefix += "."
				}
				for _, fn := range leftFields {
					sql = strings.ReplaceAll(sql, "["+compiler.JoinSourceFieldPrefix+"."+fn+"]", prefix+fn)
				}
				rightFields := call.ReferencesFields()
				for _, fn := range rightFields {
					sql = strings.ReplaceAll(sql, "["+compiler.JoinRefFieldPrefix+"."+fn+"]", rAlias+"."+fn)
				}
				return sql, params, nil
			}
			return "", nil, compiler.ErrorPosf(right.Position, "unsupported join aggregated subquery")
		},
	}
}
