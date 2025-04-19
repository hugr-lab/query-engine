package planner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

func selectDataObjectRootNode(ctx context.Context, schema *ast.Schema, planner Catalog, query *ast.Field, vars map[string]interface{}) (*QueryPlanNode, error) {
	node, inGeneral, err := selectDataObjectNode(ctx, compiler.SchemaDefs(schema), planner, query, vars)
	if err != nil {
		return nil, err
	}

	dataObject, ok := schema.Types[query.Definition.Type.Name()]
	if !ok || !compiler.IsDataObject(dataObject) {
		return nil, errors.New("data object for query not found")
	}
	info := compiler.DataObjectInfo(dataObject)
	if info == nil {
		return nil, errors.New("data object info not found")
	}
	e, err := planner.Engine(info.Catalog)
	if err != nil {
		return nil, err
	}
	caster, isTypeCast := e.(engines.EngineTypeCaster)
	if isTypeCast && !inGeneral {
		node, err = castResultsNode(ctx, caster, node, true, false)
		if err != nil {
			return nil, err
		}
	}

	return finalResultNode(ctx, schema, planner, query, node, inGeneral || !isTypeCast), nil
}

// selectDataObjectNode creates a select statement node for data object query
// The SQL query looks like this:
// [WITH _subquery_sub_node AS (
//
//	  subquery with extra fields to join with main query
//	}]
//
// SELECT [DISTINCT ON ([distinct on arguments])] row_number() OVER() AS _row_num,
//
//	*[fields from data object table (including selected fields from nested objects)],
//	[fields to calculate aggregates],
//	[fields from call scalar functions],
//	[fields from join with subqueries],
//	[fields from call table functions],
//
// FROM [data object table]
// [,LATERAL (
//
//		[with _pre_aggregate_objects_node AS (
//			-- aggregate node
//			SELECT [array_agg(struct pack (fields from subquery))]
//			FROM _subquery_sub_node
//			WHERE [join conditions]
//		)]
//		SELECT [array_agg(struct pack (fields from subquery))]
//		FROM _subquery_sub_node WHERE [join conditions]
//	) AS _subquery_sub_node_final]
//
// [WHERE [argument filters for data object and references]]
// [ORDER BY [order by fields]]
// [LIMIT [limit] OFFSET [offset]] -- for select one limit is set to 1
func selectDataObjectNode(ctx context.Context, defs compiler.DefinitionsSource, planer Catalog, query *ast.Field, vars map[string]interface{}) (*QueryPlanNode, bool, error) {
	dataObject := defs.ForName(query.Definition.Type.Name())
	if dataObject == nil || !compiler.IsDataObject(dataObject) {
		return nil, false, errors.New("data object for query not found")
	}
	info := compiler.DataObjectInfo(dataObject)
	if info == nil {
		return nil, false, errors.New("data object info not found")
	}
	e, err := planer.Engine(info.Catalog)
	if err != nil {
		return nil, false, err
	}

	// split query on the parts
	qp, err := splitByQueryParts(ctx, defs, query, vars)
	if err != nil {
		return nil, false, err
	}

	var ff QueryPlanNodes
	caster, isCaster := e.(engines.EngineTypeCaster)
	_, catQuery := e.(engines.EngineQueryScanner)
	var withCatalogNodes, withGeneralNodes QueryPlanNodes
	var joinCatalogNodes, joinGeneralNodes QueryPlanNodes
	var innerCatalog, innerGeneral []string
	var generalSourceFields fieldList
	qJoinsCatalogFields := map[string]map[string]string{}
	qJoinsGeneralFields := map[string]map[string]string{}
	for _, sq := range qp.subQueries {
		rAlias := "_" + sq.Alias + "_sub_node"
		isInner := sq.Arguments.ForName("inner")
		node, isGeneral, err := subDataQueryNode(ctx, defs, planer, info, !catQuery, sq, rAlias, vars)
		if err != nil {
			return nil, false, err
		}
		qe := e
		if isGeneral {
			qe = defaultEngine
		}
		joinNode, err := joinSubQueryNode(ctx,
			defs,
			planer,
			qe,
			info,
			!catQuery || isGeneral,
			query,
			sq,
			vars,
			"_objects", rAlias,
		)
		if err != nil {
			return nil, false, err
		}
		if isGeneral && isCaster {
			if node != nil {
				withGeneralNodes = append(withGeneralNodes, node)
			}
			joinGeneralNodes = append(joinGeneralNodes, joinNode)
			if isInner != nil && isInner.Value != nil && isInner.Value.Raw == "true" {
				innerGeneral = append(innerGeneral, joinNode.Name+"._selection IS NOT NULL")
			}
			ff, err := sourceFields(defs, dataObject, sq)
			if err != nil {
				return nil, false, err
			}
			for _, f := range ff {
				if generalSourceFields.ForName(f.Alias) == nil {
					generalSourceFields = append(generalSourceFields, f)
				}
			}
			qJoinsGeneralFields[sq.Alias] = nil
		}
		if !isGeneral || !isCaster {
			if node != nil {
				withCatalogNodes = append(withCatalogNodes, node)
			}
			joinCatalogNodes = append(joinCatalogNodes, joinNode)
			if isInner != nil && isInner.Value != nil && isInner.Value.Raw == "true" {
				innerCatalog = append(innerCatalog, joinNode.Name+"._selection IS NOT NULL")
			}
			ff = append(ff, &QueryPlanNode{
				Name:  joinNode.Query.Alias,
				Query: joinNode.Query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return joinNode.Name + "._selection AS " + engines.Ident(node.Query.Alias), params, nil
				},
			})
		}
	}

	for _, qj := range qp.queryTimeJoins {
		catalogFields := map[string]string{}
		generalFields := map[string]string{}
		for _, selected := range engines.SelectedFields(qj.SelectionSet) {
			sq := selected.Field
			rAlias := "_" + sq.Alias + "_join_sub_node"
			isInner := sq.Arguments.ForName("inner")
			node, isGeneral, err := subDataQueryNode(ctx, defs, planer, info, !catQuery, sq, rAlias, vars)
			if err != nil {
				return nil, false, err
			}
			qe := e
			if isGeneral {
				qe = defaultEngine
			}
			joinNode, err := joinSubQueryNode(ctx,
				defs,
				planer,
				qe,
				info,
				!catQuery || isGeneral,
				query,
				sq,
				vars,
				"_objects", rAlias,
			)
			if err != nil {
				return nil, false, err
			}
			if isGeneral && isCaster {
				if node != nil {
					withGeneralNodes = append(withGeneralNodes, node)
				}
				joinGeneralNodes = append(joinGeneralNodes, joinNode)
				if isInner != nil && isInner.Value != nil && isInner.Value.Raw == "true" {
					innerGeneral = append(innerGeneral, joinNode.Name+"._selection IS NOT NULL")
				}
				generalFields[sq.Alias] = joinNode.Name + "._selection"
			}
			if !isGeneral || !isCaster {
				if node != nil {
					withCatalogNodes = append(withCatalogNodes, node)
				}
				joinCatalogNodes = append(joinCatalogNodes, joinNode)
				if isInner != nil && isInner.Value != nil && isInner.Value.Raw == "true" {
					innerCatalog = append(innerCatalog, joinNode.Name+"._selection IS NOT NULL")
				}
				catalogFields[sq.Alias] = joinNode.Name + "._selection"
			}
		}
		if len(catalogFields) != 0 {
			qJoinsCatalogFields[qj.Alias] = catalogFields
			ff = append(ff, &QueryPlanNode{
				Name:  qj.Alias,
				Query: qj,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return e.MakeObject(catalogFields) + " AS " + qj.Alias, params, nil
				},
			})
		}
		if len(generalFields) != 0 {
			qJoinsGeneralFields[qj.Alias] = generalFields
			ff, err := sourceFields(defs, dataObject, qj)
			if err != nil {
				return nil, false, err
			}
			for _, f := range ff {
				if generalSourceFields.ForName(f.Alias) == nil {
					generalSourceFields = append(generalSourceFields, f)
				}
			}
		}
	}
	queryArg, err := compiler.ArgumentValues(defs, query, vars, true)
	if err != nil {
		return nil, false, err
	}
	if info.HasArguments() {
		arg := queryArg.ForName("args")
		am, _ := arg.Value.(map[string]any)
		err = info.ApplyArguments(defs, am, e)
		if err != nil {
			return nil, false, err
		}
	}

	fieldNodes := fieldsNodes(e, info, "_objects",
		append(qp.fields, qp.extraSourceFields...), // add selected fields and extra fields that are required for joins
		vars,
		false,
	)
	nodes := QueryPlanNodes{
		fieldsNode(
			query,
			fieldNodes,
		),
		fromDataObjectNode(ctx, info),
	}
	if info.IsCube { // add group by if needed
		node, err := cubeGroupByNode(info, query, append(qp.fields, qp.extraSourceFields...))
		if err != nil {
			return nil, false, err
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	var paramNodes QueryPlanNodes
	if compiler.IsSelectOneQuery(query) {
		// return query with limit 1 and filtered by unique fields
		paramNodes, err = selectOneQueryParamsNodes(ctx, info, query, queryArg, "_objects")
		if err != nil {
			return nil, false, err
		}
	}
	if !compiler.IsSelectOneQuery(query) {
		// return query with limit, offset, filter, order by, distinct on, field filters
		paramNodes, err = selectQueryParamsNodes(ctx, defs, e, info, "_objects", query, queryArg, false)
		if err != nil {
			return nil, false, err
		}
	}
	pn, err := permissionFilterNode(ctx, defs, info, query, "_objects", false)
	if err != nil {
		return nil, false, err
	}
	if pn != nil {
		paramNodes = append(paramNodes, pn)
	}
	if len(joinCatalogNodes) == 0 && len(joinGeneralNodes) == 0 {
		nodes = append(nodes, paramNodes...)
	} else {
		whereNode := paramNodes.ForName("where")
		if whereNode != nil {
			nodes = append(nodes, whereNode)
		}
	}

	// add group by nodes if it is cube data object

	baseData := selectStatementNode(query, nodes, "_objects", qp.withRowNum)

	if len(joinCatalogNodes) != 0 {
		baseData.Name = "_objects"
		withCatalogNodes = append(append(QueryPlanNodes{}, baseData), withCatalogNodes...)
		nodes = QueryPlanNodes{
			withNode(withCatalogNodes),
			fieldsNode(query,
				append(
					fieldsNodes(e, info, "_objects",
						append(
							append(qp.fields, qp.extraSourceFields...), // add selected fields and extra fields that are required for joins
							generalSourceFields...,                     // add general fields that are required for joins in general catalog query
						),
						vars,
						true,
					),
					ff...,
				),
			),
			&QueryPlanNode{
				Name:  "from",
				Query: query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects", params, nil
				},
			},
			joinsNode(joinCatalogNodes),
		}
		if len(joinGeneralNodes) == 0 && !compiler.IsSelectOneQuery(query) {
			paramNodes, err = selectQueryParamsNodes(ctx, defs, e, info, "_objects", query, queryArg, true)
			if err != nil {
				return nil, false, err
			}
			paramNodes = slices.DeleteFunc(paramNodes, func(n *QueryPlanNode) bool {
				return n.Name == "where"
			})
			nodes = append(nodes, paramNodes...)
		}
		if len(innerCatalog) != 0 {
			nodes = append(nodes, &QueryPlanNode{
				Name:  "where",
				Query: query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return strings.Join(innerCatalog, " AND "), params, nil
				},
			})
		}
		baseData = selectStatementNode(query, nodes, "_objects", qp.withRowNum)
	}
	if len(joinGeneralNodes) == 0 {
		return baseData, false, nil
	}

	// change cast fields list (only needed fields)
	baseData.Name = "_objects"
	// prepare for type casting
	// create fields list for type casting
	baseData.Query = queryWithExtraFields(ctx, query, generalSourceFields, qJoinsGeneralFields)
	cast, err := castResultsNode(ctx, caster, baseData, false, qp.withRowNum)
	if err != nil {
		return nil, false, err
	}
	cast.Name = "_objects"

	ff = fieldsNodes(e, info, "_objects", qp.fields, vars, true)
	for _, j := range joinCatalogNodes {
		ff = append(ff, &QueryPlanNode{
			Name:  j.Query.Alias,
			Query: j.Query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return engines.Ident(node.Query.Alias), params, nil
			},
		})
	}
	for _, j := range joinGeneralNodes {
		ff = append(ff, &QueryPlanNode{
			Name:  j.Query.Alias,
			Query: j.Query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return j.Name + "._selection AS " + engines.Ident(node.Query.Alias), params, nil
			},
		})
	}
	for fn, sub := range qJoinsGeneralFields {
		if _, ok := qJoinsCatalogFields[fn]; ok {
			// append fields from general catalog query
			ff = append(ff, &QueryPlanNode{
				Name:  fn,
				Query: query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return defaultEngine.AddObjectFields("_objects."+fn, sub) + " AS " + fn, params, nil
				},
			})
			continue
		}
		if len(sub) == 0 {
			continue
		}
		ff = append(ff, &QueryPlanNode{
			Name:  fn,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return defaultEngine.MakeObject(sub) + " AS " + fn, params, nil
			},
		})
	}

	nodes = QueryPlanNodes{
		withNode(
			append(append(QueryPlanNodes{}, cast), withGeneralNodes...),
		),
		fieldsNode(query, ff),
		&QueryPlanNode{
			Name:  "from",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects", params, nil
			},
		},
		joinsNode(joinGeneralNodes),
		{
			Name:  "from",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects", params, nil
			},
		},
	}
	if !compiler.IsSelectOneQuery(query) {
		paramNodes, err = selectQueryParamsNodes(ctx, defs, defaultEngine, info, "_objects", query, queryArg, true)
		if err != nil {
			return nil, false, err
		}
		paramNodes = slices.DeleteFunc(paramNodes, func(n *QueryPlanNode) bool {
			return n.Name == "where"
		})
		nodes = append(nodes, paramNodes...)
	}

	if len(innerGeneral) != 0 {
		nodes = append(nodes, &QueryPlanNode{
			Name:  "where",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return strings.Join(innerGeneral, " AND "), params, nil
			},
		})
	}

	return selectStatementNode(query, nodes, "_objects", false), true, nil
}

func cubeGroupByNode(info *compiler.Object, query *ast.Field, fieldList fieldList) (*QueryPlanNode, error) {
	if !info.IsCube {
		return nil, fmt.Errorf("object %s is not cube", info.Name)
	}
	var fields []string
	for _, f := range fieldList {
		if f.Definition.Directives.ForName(compiler.FieldMeasurementDirectiveName) != nil &&
			f.Arguments.ForName(compiler.FieldMeasurementFuncArgName) != nil {
			continue
		}
		fields = append(fields, engines.Ident(f.Alias))
	}
	if len(fields) == 0 || len(fields) == len(fieldList) {
		return nil, nil
	}
	return &QueryPlanNode{
		Name:  "groupBy",
		Query: query,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return strings.Join(fields, ", "), params, nil
		},
	}, nil
}

func withNode(nodes QueryPlanNodes) *QueryPlanNode {
	return &QueryPlanNode{
		Name:  "with",
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, n := range children {
				ff = append(ff, n.Name+" AS ("+n.Result+")")
			}
			return "WITH " + strings.Join(ff, ","), params, nil
		},
	}
}

func joinsNode(nodes QueryPlanNodes) *QueryPlanNode {
	return &QueryPlanNode{
		Name:  "joins",
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, n := range children {
				ff = append(ff, n.Result)
			}
			return strings.Join(ff, " "), params, nil
		},
	}
}

func subDataQueryNode(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, info *compiler.Object, inGeneral bool, field *ast.Field, rAlias string, vars map[string]interface{}) (node *QueryPlanNode, isGeneral bool, err error) {
	var e engines.Engine
	var qCatalog string
	def := defs.ForName(field.Definition.Type.Name())
	switch {
	case compiler.IsDataObject(def):
		node, isGeneral, err = selectDataObjectNode(ctx, defs, planner, field, vars)
		if err != nil {
			return nil, false, err
		}
		qCatalog = compiler.DataObjectInfo(def).Catalog
		e, err = planner.Engine(qCatalog)
		if err != nil {
			return nil, false, err
		}
	case compiler.IsTableFuncJoinSubquery(field),
		compiler.IsFunctionCallSubquery(field):
		qCatalog = base.FieldCatalogName(field.Definition)
		e, err = planner.Engine(qCatalog)
		if err != nil {
			return nil, false, err
		}
		// check if func call use some fields from the main query than it will call from join
		fInfo := compiler.FunctionCallInfo(field)
		if fInfo == nil {
			return nil, false, errors.New("function call info not found")
		}
		if len(fInfo.ArgumentMap()) != 0 && qCatalog == info.Catalog {
			return nil, false, nil
		}
		_, ok := e.(engines.EngineQueryScanner)
		if ok && len(fInfo.ArgumentMap()) != 0 {
			return nil, false, errors.New("pass arguments to function call (arguments mapping) is not supported by query engine")
		}
		prefix := qCatalog
		if ok {
			prefix = ""
		}
		node, err = functionCallNode(ctx, defs, planner, prefix, field, vars)
		if err != nil {
			return nil, false, err
		}
	case compiler.IsAggregateQuery(field), compiler.IsBucketAggregateQuery(field):
		qCatalog = base.FieldCatalogName(field.Definition)
		node, isGeneral, err = aggregateDataNode(ctx, defs, planner, inGeneral || qCatalog != info.Catalog, field, vars)
		if err != nil {
			return nil, false, err
		}
		node.Name = rAlias
		// cast to general performs in the node
		return node, inGeneral || isGeneral, nil
	}

	if e, ok := e.(engines.EngineTypeCaster); ok &&
		(inGeneral && !isGeneral || qCatalog != info.Catalog) {
		node, err = castResultsNode(ctx, e, node, false, false)
		if err != nil {
			return nil, false, err
		}
		node.Name = rAlias
		return node, true, nil
	}
	if node != nil {
		node.Name = rAlias
	}
	return node, isGeneral, nil
}

// node create sql :
// ,LATERAL (
//
//	SELECT [array_agg](struct pack (fields from subquery))
//	FROM _subquery_sub_node (or function call sql)
//	WHERE [join conditions]
//
// ) AS _subquery_sub_node_final
func joinSubQueryNode(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, e engines.Engine, info *compiler.Object, inGeneral bool, left, right *ast.Field, vars map[string]any, prefix, rAlias string) (node *QueryPlanNode, err error) {
	var nodes QueryPlanNodes
	switch {
	case compiler.IsAggregateQuery(right), compiler.IsBucketAggregateQuery(right):
		nodes, err = joinAggregateNodes(ctx, defs, planner, inGeneral, right, prefix, rAlias)
	case compiler.IsReferencesSubquery(right.Definition):
		nodes, err = joinReferencesQueryNodes(ctx, defs, info, inGeneral, right, prefix, rAlias)
	case compiler.IsJoinSubquery(right):
		nodes, err = joinQueryNodes(ctx, defs, inGeneral, right, prefix, rAlias)
	case compiler.IsFunctionCallSubquery(right) || compiler.IsTableFuncJoinSubquery(right):
		nodes, err = joinFunctionCallNodes(ctx, defs, inGeneral, left, right, vars, prefix, rAlias)
	default:
		return nil, errors.New("unsupported subquery type")
	}
	if err != nil {
		return nil, err
	}

	if len(right.Arguments) != 0 {
		am := right.ArgumentMap(vars)
		nestedOrderBy, ok := am["nested_order_by"]
		if ok {
			node, err := orderByNode(e, info, right, rAlias, nestedOrderBy, nil, true)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
		limit, ok := am["nested_limit"]
		if ok {
			if l, ok := limit.(int64); ok && l == 0 {
				nodes = append(nodes, &QueryPlanNode{
					Name: "nested_limit",
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return fmt.Sprintf("%d", l), params, nil
					},
				})
			}
		}
		offset, ok := am["nested_offset"]
		if ok {
			if o, ok := offset.(int64); ok && o == 0 {
				nodes = append(nodes, &QueryPlanNode{
					Name: "nested_offset",
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return fmt.Sprintf("%d", 0), params, nil
					},
				})
			}
		}
	}

	return &QueryPlanNode{
		Name:  rAlias + "_final",
		Query: right,
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			fields := children.ForName("fields").Result
			sql := "SELECT " + fields + " FROM " + children.ForName("from").Result
			where := children.ForName("where")
			if where != nil && node.Query.Directives.ForName("unnest") == nil && where.Result != "" {
				sql += " WHERE " + where.Result
			}
			orderBy := children.ForName("nested_order_by")
			if orderBy != nil {
				sql += " ORDER BY " + orderBy.Result
			}
			limit := children.ForName("nested_limit")
			if limit != nil {
				sql += " LIMIT " + limit.Result
			}
			offset := children.ForName("nested_offset")
			if offset != nil && limit != nil {
				sql += " OFFSET " + offset.Result
			}
			fieldsAgg := children.ForName("fields_agg")
			if fieldsAgg != nil && node.Query.Directives.ForName("unnest") == nil {
				sql = "SELECT " + fieldsAgg.Result + " FROM (" + sql + ") AS _subquery_sub_node"
				// check args for subquery (nested_order_by, nested_limit, nested_offset)
			}

			if node.Query.Directives.ForName("unnest") != nil {
				sql = " LEFT JOIN (" + sql + ") AS " + node.Name + " ON "
				if where == nil {
					return sql + "true", params, nil
				}
				jc := strings.ReplaceAll(where.Result, rAlias+".", node.Name+".")
				return sql + jc, params, nil
			}
			je := e
			if inGeneral {
				je = defaultEngine
			}
			return je.LateralJoin(sql, node.Name), params, nil
		},
	}, nil
}

func joinReferencesQueryNodes(ctx context.Context, defs compiler.DefinitionsSource, info *compiler.Object, inGeneral bool, field *ast.Field, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	ri := info.ReferencesQueryInfo(defs, field.Name)
	if ri == nil {
		return nil, errors.New("references query info not found")
	}
	refObject := defs.ForName(field.Definition.Type.Name())
	refObjectInfo := compiler.DataObjectInfo(refObject)
	nodes = QueryPlanNodes{
		{
			Name:  "from",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				sql := rAlias
				if ri.IsM2M {
					jc, err := ri.FromM2MJoinConditions(defs, "_join_m2m", sql, true, false)
					if err != nil {
						return "", nil, err
					}
					m2m := defs.ForName(ri.M2MName)
					m2mInfo := compiler.DataObjectInfo(m2m)
					db := m2mInfo.Catalog
					if !inGeneral {
						db = ""
					}
					sql += " INNER JOIN " + m2mInfo.SQL(ctx, db) + " AS _join_m2m ON " + jc
				}
				return sql, params, nil
			},
		},
		{
			Name:  "where",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				if ri.IsM2M {
					sql, err := ri.ToM2MJoinConditions(defs, prefix, "_join_m2m", false, true)
					return sql, params, err
				}
				sql, err := ri.JoinConditions(defs, prefix, rAlias, false, false)
				return sql, params, err
			},
		}, {
			Name:  "fields",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				e, err := node.Engine(refObjectInfo.Catalog)
				if err != nil {
					return "", nil, err
				}
				if inGeneral {
					e = defaultEngine
				}
				sql := e.PackFieldsToObject(rAlias, field) + " AS _selection"
				if node.Query.Directives.ForName("unnest") != nil {
					for _, f := range ri.ReferencesFields() {
						if ri.IsM2M {
							sql += ", _join_m2m." + f
							continue
						}
						sql += ", " + rAlias + "." + f
					}
				}
				return sql, params, nil
			},
		},
	}
	if field.Definition.Type.NamedType == "" {
		nodes = append(nodes, &QueryPlanNode{
			Name:  "fields_agg",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "array_agg( _selection) AS _selection", params, nil
			},
		})
	}
	return nodes, nil
}

func joinQueryNodes(_ context.Context, defs compiler.DefinitionsSource, inGeneral bool, right *ast.Field, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	ji := compiler.JoinInfo(right)
	if ji == nil {
		return nil, errors.New("join info not found")
	}
	refObject := defs.ForName(right.Definition.Type.Name())
	if refObject == nil {
		return nil, errors.New("reference object not found")
	}
	catalog := compiler.DataObjectInfo(refObject).Catalog

	nodes = QueryPlanNodes{
		{
			Name:  "from",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return rAlias, params, nil
			},
		},
		{
			Name:  "where",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				sql := ji.JoinConditionsTemplate()
				leftFields, err := ji.SourceFields()
				if err != nil {
					return "", nil, err
				}
				if prefix != "" {
					prefix += "."
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
			},
		}, {
			Name:  "fields",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				e, err := node.Engine(catalog)
				if err != nil {
					return "", nil, err
				}
				if inGeneral {
					e = defaultEngine
				}
				sql := e.PackFieldsToObject(rAlias, right) + " AS _selection"
				if len(right.SelectionSet) == 0 {
					sql = "1 AS _selection"
				}
				if node.Query.Directives.ForName("unnest") != nil {
					rightFields, err := ji.ReferencesFields()
					if err != nil {
						return "", nil, err
					}
					for _, fn := range rightFields {
						sql += ", " + rAlias + "." + fn
					}
				}
				return sql, params, nil
			},
		},
	}
	if right.Definition.Type.NamedType == "" {
		nodes = append(nodes, &QueryPlanNode{
			Name:  "fields_agg",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "array_agg( _selection) AS _selection", params, nil
			},
		})
	}
	return nodes, nil
}

func joinFunctionCallNodes(_ context.Context, defs compiler.DefinitionsSource, inGeneral bool, left, right *ast.Field, vars map[string]interface{}, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	call := compiler.FunctionCallInfo(right)
	if call == nil {
		return nil, errors.New("function call info not found")
	}
	info, err := call.FunctionInfo(defs)
	if err != nil {
		return nil, err
	}
	fromNode := &QueryPlanNode{
		Name:  "from",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			call := compiler.FunctionCallInfo(node.Query)
			if call == nil {
				return "", nil, ErrInternalPlanner
			}
			if len(call.ArgumentMap()) == 0 {
				return rAlias, params, nil
			}
			e, err := node.Engine(info.Catalog)
			if err != nil {
				return "", nil, err
			}
			sql, params, err := functionCallSQL(node.TypeDefs(), e, node.Query, vars, params)
			if err != nil {
				return "", nil, err
			}
			// replace fields
			def := defs.ForName(left.Definition.Type.Name())
			info := compiler.DataObjectInfo(def)
			for _, f := range compiler.ExtractFieldsFromSQL(sql) {
				fieldInfo := info.FieldForName(f)
				if fieldInfo == nil {
					return "", nil, fmt.Errorf("field %s not found in data object", f)
				}
				fSQL := fieldInfo.SQL(prefix)
				if inGeneral {
					ff := filterFields(left.SelectionSet, func(field *ast.Field) bool {
						return field.Alias == f
					}, false)
					if len(ff) != 0 {
						fSQL = ff[0].Alias
					}
				}
				sql = strings.ReplaceAll(sql, "["+f+"]", fSQL)
			}
			if len(compiler.ExtractFieldsFromSQL(sql)) != 0 {
				return "", nil, fmt.Errorf("not all function %s arguments is defined %s",
					node.Query.Name,
					strings.Join(compiler.ExtractFieldsFromSQL(sql), ","))
			}
			fi, err := call.FunctionInfo(defs)
			if err != nil {
				return "", nil, err
			}
			if fi.ReturnsTable {
				return sql + " AS " + rAlias, params, nil
			}
			return "(SELECT " + sql + " AS _value) AS " + rAlias, params, nil
		},
	}
	if info.ReturnsTable {
		nodes = QueryPlanNodes{fromNode, {
			Name:  "fields",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				e, err := node.Engine(info.Catalog)
				if err != nil {
					return "", nil, err
				}
				if inGeneral {
					e = defaultEngine
				}
				sql := e.PackFieldsToObject(rAlias, right) + " AS _selection"
				if node.Query.Directives.ForName("unnest") != nil {
					rightFields := call.ReferencesFields()
					for _, fn := range rightFields {
						sql += ", " + rAlias + "." + fn
					}
				}
				return sql, params, nil
			},
		}, {
			Name:  "where",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
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
			},
		}}
		if right.Definition.Type.NamedType == "" {
			nodes = append(nodes, &QueryPlanNode{
				Name:  "fields_agg",
				Query: right,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "array_agg( _selection) AS _selection", params, nil
				},
			})
		}
		return nodes, nil
	}

	if len(right.SelectionSet) == 0 {
		return QueryPlanNodes{fromNode, {
			Name:  "fields",
			Query: right,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return rAlias + "._value AS _selection", params, nil
			},
		}}, nil
	}

	return QueryPlanNodes{fromNode, {
		Name:  "fields",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			e, err := node.Engine(info.Catalog)
			if err != nil {
				return "", nil, err
			}
			if inGeneral {
				e = defaultEngine
			}
			return e.RepackObject("_value", right) + " AS _selection", params, nil
		},
	}}, nil
}

func fromDataObjectNode(ctx context.Context, info *compiler.Object) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    "from",
		Comment: "from data object",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			prefix := info.Catalog
			e, err := node.Engine(info.Catalog)
			if err != nil {
				return "", nil, err
			}
			if _, ok := e.(engines.EngineQueryScanner); ok {
				prefix = ""
			}
			return info.SQL(ctx, prefix), params, nil
		},
	}
}

func fieldsNode(query *ast.Field, fields QueryPlanNodes) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    "fields",
		Query:   query,
		Comment: "fields",
		Nodes:   fields,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, field := range children {
				ff = append(ff, field.Result)
			}
			return strings.Join(ff, ","), params, nil
		},
	}
}

func fieldsNodes(e engines.Engine, info *compiler.Object, prefix string, fields []*ast.Field, vars map[string]any, aliases bool) QueryPlanNodes {
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
				if aliases {
					if prefix != "" {
						return prefix + "." + engines.Ident(field.Alias), params, nil
					}
					return engines.Ident(field.Alias), params, nil
				}
				fi := info.FieldForName(node.Query.Name)
				sql := fi.SQL(prefix)
				typeName := fi.Definition().Type.NamedType
				if len(node.Query.Arguments) != 0 &&
					compiler.IsScalarType(typeName) {
					args, err := compiler.ArgumentValues(node.TypeDefs(), field, vars, true)
					if err != nil {
						return "", nil, err
					}
					if compiler.ScalarTypes[typeName].Arguments != nil ||
						compiler.IsExtraField(fi.Definition()) {
						sql = e.ApplyFieldTransforms(sql, field, args)
					}
					if info.IsCube &&
						fi.Definition().Directives.ForName(compiler.FieldMeasurementDirectiveName) != nil &&
						args.ForName(compiler.FieldMeasurementFuncArgName) != nil {
						// aggregate measurements
						mf, ok := args.ForName(compiler.FieldMeasurementFuncArgName).Value.(string)
						if !ok || compiler.MeasurementAggregations[mf] == "" {
							return "", nil, errors.New("measurement function not found")
						}
						aggregator, ok := e.(engines.EngineAggregator)
						if !ok {
							return "", nil, fmt.Errorf("cube %s engine does not support aggregation", info.Name)
						}
						sql, params, err = aggregator.AggregateFuncSQL(compiler.MeasurementAggregations[mf], sql, "", field, nil, params)
						if err != nil {
							return "", nil, err
						}
					}
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

func allSelectedFields(query *ast.Field) []*ast.Field {
	var fields []*ast.Field
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		fields = append(fields, f.Field)
	}
	return fields
}

type queryPart struct {
	withRowNum        bool
	fields            fieldList
	extraSourceFields fieldList
	subQueries        fieldList
	queryTimeJoins    fieldList
}

func splitByQueryParts(_ context.Context, defs compiler.DefinitionsSource, query *ast.Field, vars map[string]any) (*queryPart, error) {
	qp := queryPart{}
	var queryTimeJoins, queryTimeSpatial []*ast.Field
	for _, field := range engines.SelectedFields(query.SelectionSet) {
		switch {
		case compiler.IsReferencesSubquery(field.Field.Definition):
			qp.subQueries = append(qp.subQueries, field.Field)
		case compiler.IsFunctionCallSubquery(field.Field),
			compiler.IsTableFuncJoinSubquery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
		case compiler.IsJoinSubquery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
		case field.Field.Name == compiler.QueryTimeJoinFieldName:
			queryTimeJoins = append(queryTimeJoins, field.Field)
		case field.Field.Name == compiler.QueryTimeSpatialFieldName:
			queryTimeSpatial = append(queryTimeSpatial, field.Field)
		case compiler.IsAggregateQuery(field.Field), compiler.IsBucketAggregateQuery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
			qp.withRowNum = true
		default:
			qp.fields = append(qp.fields, field.Field)
		}
	}
	// add join directives for query time join
	for _, f := range queryTimeJoins {
		join, withAgg, err := castJoinDirectiveToJoin(defs, query, f, vars)
		if err != nil {
			return nil, err
		}
		qp.queryTimeJoins = append(qp.queryTimeJoins, join)
		if withAgg {
			qp.withRowNum = true
		}
	}
	for _, f := range queryTimeSpatial {
		join, withAgg, err := castSpatialQueryToJoin(defs, query, f, vars)
		if err != nil {
			return nil, err
		}
		qp.queryTimeJoins = append(qp.queryTimeJoins, join)
		if withAgg {
			qp.withRowNum = true
		}
	}
	// add references fields to selected fields (to make joins possible)
	refFields, err := referencesFields(defs, query)
	if err != nil {
		return nil, err
	}
	for _, f := range refFields {
		ef := qp.fields.ForAlias(f.Alias)
		if ef == nil {
			qp.fields = append(qp.fields, f)
			continue
		}
		if ef.Name != f.Name {
			return nil, fmt.Errorf("can not query references %s, because there is define field with alias %s ", query.Alias, f.Alias)
		}
	}
	// add source fields for subqueries and function calls
	for _, f := range append(qp.subQueries, append(queryTimeJoins, queryTimeSpatial...)...) {
		sff, err := sourceFields(defs, f.ObjectDefinition, f)
		if err != nil {
			return nil, err
		}
		for _, sf := range sff {
			found := false
			for _, ff := range append(qp.fields, qp.extraSourceFields...) {
				if ff.Alias == sf.Alias {
					if ff.Name != sf.Name {
						return nil, fmt.Errorf("can not query references %s, because there is define field with alias %s ", query.Alias, sf.Alias)
					}
					found = true
					break
				}
			}
			if !found {
				qp.extraSourceFields = append(qp.extraSourceFields, sf)
			}
		}
	}
	// add source fields for order by arguments to order by non selected fields (will be used in aggregation)
	slices.SortFunc(qp.subQueries, func(f1, f2 *ast.Field) int {
		u1 := f1.Directives.ForName("unnest") != nil
		u2 := f2.Directives.ForName("unnest") != nil
		if u1 && u2 || !u1 && !u2 {
			return 0
		}
		if u1 && !u2 {
			return -1
		}
		return 1
	})

	return &qp, nil
}

func castJoinDirectiveToJoin(defs compiler.DefinitionsSource, query, joinQuery *ast.Field, vars map[string]any) (*ast.Field, bool, error) {
	if joinQuery.Name != compiler.QueryTimeJoinFieldName {
		return nil, false, errors.New("field is not join")
	}
	// 1. create copy of join field
	new := &ast.Field{
		Alias:            joinQuery.Alias,
		Name:             joinQuery.Name,
		Arguments:        joinQuery.Arguments,
		Definition:       joinQuery.Definition,
		ObjectDefinition: joinQuery.ObjectDefinition,
		Position:         joinQuery.Position,
		Comment:          joinQuery.Comment,
	}
	// 2. get source field name
	argMap := joinQuery.ArgumentMap(vars)
	v, ok := argMap["fields"]
	if !ok {
		return nil, false, compiler.ErrorPosf(joinQuery.Position, "fields argument is required")
	}
	sourceFields, ok := v.([]any)
	if !ok {
		return nil, false, compiler.ErrorPosf(joinQuery.Position, "fields argument must be an array")
	}
	if len(sourceFields) == 0 {
		return nil, false, compiler.ErrorPosf(joinQuery.Position, "fields argument must have at least one field")
	}
	// 3. add query fields with @join directive
	withAgg := false
	for _, sq := range engines.SelectedFields(joinQuery.SelectionSet) {
		if compiler.IsAggregateQuery(sq.Field) || compiler.IsBucketAggregateQuery(sq.Field) {
			withAgg = true
		}
		// 3.1 get references fields name
		argMap := sq.Field.ArgumentMap(vars)
		v, ok := argMap["fields"]
		if !ok {
			return nil, false, compiler.ErrorPosf(sq.Field.Position, "fields argument is required")
		}
		refFields, ok := v.([]any)
		if !ok {
			return nil, false, compiler.ErrorPosf(sq.Field.Position, "fields argument must be an array")
		}
		if len(refFields) == 0 {
			return nil, false, compiler.ErrorPosf(sq.Field.Position, "fields argument must have at least one field")
		}
		// 3.2 create sql for @join directive
		if len(sourceFields) != len(refFields) {
			return nil, false, compiler.ErrorPosf(sq.Field.Position, "fields and references fields must have the same length")
		}
		var sql string
		for i, f := range sourceFields {
			sourceFieldName, ok := f.(string)
			if !ok {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "field must be a string")
			}
			// check fields and if the field is not in sub query (join, function call, etc) add it to source fields
			if strings.Contains(sourceFieldName, ".") {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "join field could not be in a nested object")
			}
			var sourceFieldDef *ast.FieldDefinition
			if s := engines.SelectedFields(query.SelectionSet).ForName(sourceFieldName); s != nil {
				sourceFieldDef = s.Field.Definition
			}
			if sourceFieldDef == nil &&
				query.ObjectDefinition.Fields.ForName(sourceFieldName) != nil {
				sourceFieldDef = query.ObjectDefinition.Fields.ForName(sourceFieldName)
			}
			if sourceFieldDef == nil {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "left object field %s not found", sourceFieldName)
			}
			sourceFieldName = "[" + compiler.JoinSourceFieldPrefix + "." + sourceFieldName + "]"
			refFieldName, ok := refFields[i].(string)
			if !ok {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "field must be a string")
			}
			if strings.Contains(refFieldName, ".") {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "join field could not be in a nested object")
			}
			rightDef := defs.ForName(sq.Field.Definition.Type.Name())
			if rightDef == nil {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "right object %s not found", sq.Field.Definition.Type.Name())
			}
			var refFieldDef *ast.FieldDefinition
			if s := engines.SelectedFields(sq.Field.SelectionSet).ForName(refFieldName); s != nil {
				refFieldDef = s.Field.Definition
			}
			if refFieldDef == nil &&
				rightDef.Fields.ForName(refFieldName) != nil {
				refFieldDef = rightDef.Fields.ForName(refFieldName)
			}
			if refFieldDef == nil {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "right object field %s not found", sourceFieldName)
			}
			if sourceFieldDef.Type.NamedType == "" ||
				!compiler.IsScalarType(sourceFieldDef.Type.NamedType) ||
				sourceFieldDef.Type.NamedType != refFieldDef.Type.NamedType {
				return nil, false, compiler.ErrorPosf(sq.Field.Position, "fields must be scalar and have the same type")
			}
			refFieldName = "[" + compiler.JoinRefFieldPrefix + "." + refFieldName + "]"
			if sql != "" {
				sql += " AND "
			}
			sql += sourceFieldName + " = " + refFieldName
		}
		// 3.3 add @join directive to subquery field
		sq.Field.Directives = append(sq.Field.Directives, &ast.Directive{
			Name: compiler.JoinDirectiveName,
			Arguments: []*ast.Argument{
				{
					Name: "sql",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  sql,
					},
					Position: sq.Field.Position,
				},
			},
		})
		new.SelectionSet = append(new.SelectionSet, sq.Field)
	}

	return new, withAgg, nil
}

func castSpatialQueryToJoin(defs compiler.DefinitionsSource, query *ast.Field, field *ast.Field, vars map[string]any) (*ast.Field, bool, error) {
	if field.Name != compiler.QueryTimeSpatialFieldName {
		return nil, false, errors.New("field is not spatial query")
	}
	def := defs.ForName(query.Definition.Type.Name())
	joinField := def.Fields.ForName(compiler.QueryTimeJoinFieldName)
	if joinField == nil {
		return nil, false, errors.New("join field not found")
	}

	joinObject := defs.ForName(compiler.QueryTimeJoinObjectName)
	if joinObject == nil {
		return nil, false, errors.New("join object not found")
	}
	joinDirectiveDef := defs.DirectiveForName(compiler.JoinDirectiveName)
	if joinDirectiveDef == nil {
		return nil, false, errors.New("join directive definition not found")
	}

	new := &ast.Field{
		Alias:            field.Alias,
		Name:             joinField.Name,
		Definition:       joinField,
		Position:         field.Position,
		ObjectDefinition: joinObject,
	}
	var aliasName, operation string
	var buffer float64
	argMap := field.ArgumentMap(vars)

	if v, ok := argMap["field"]; ok {
		aliasName = v.(string)
		new.Arguments = append(new.Arguments, &ast.Argument{
			Name: "fields",
			Value: &ast.Value{
				Kind: ast.ListValue,
				Children: ast.ChildValueList{
					{
						Value: &ast.Value{
							Kind:     ast.StringValue,
							Raw:      aliasName,
							Position: field.Position,
						},
						Position: field.Position,
					},
				},
			},
			Position: field.Position,
		})
	}
	if v, ok := argMap["type"]; ok {
		operation = v.(string)
	}
	if v, ok := argMap["buffer"]; ok {
		buffer = float64(v.(int64))
	}
	if aliasName == "" {
		return nil, false, errors.New("field or alias argument for spatial query is required")
	}
	if operation == "" {
		return nil, false, errors.New("type argument for spatial query is required")
	}
	sqlTemplate := "ST_[operation]([field1], [field2])"
	if buffer != 0 {
		if buffer > 5000 {
			return nil, false, errors.New("buffer value can not be more than 5000 meters")
		}
		// convert buffer from meters to degrees (1 degree = 111 km)
		buffer = buffer / 111111
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[field1]",
			fmt.Sprintf("ST_Buffer([%s.%s], %f)",
				compiler.JoinSourceFieldPrefix, aliasName, buffer),
		)
	}
	if buffer == 0 {
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[field1]", "["+compiler.JoinSourceFieldPrefix+"."+aliasName+"]")
	}
	switch operation {
	case "INTERSECTS":
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[operation]", "Intersects")
	case "WITHIN":
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[operation]", "Within")
	case "CONTAINS":
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[operation]", "Contains")
	case "DISJOIN":
		sqlTemplate = "NOT(" + strings.ReplaceAll(sqlTemplate, "[operation]", "Intersects") + ")"
	}

	withAgg := false
	for _, f := range engines.SelectedFields(field.SelectionSet) {
		if compiler.IsAggregateQuery(f.Field) || compiler.IsBucketAggregateQuery(f.Field) {
			withAgg = true
		}
		sql := sqlTemplate
		argMap = f.Field.ArgumentMap(vars)
		v, ok := argMap["field"]
		if !ok {
			return nil, false, errors.New("field argument for spatial query is required")
		}
		sql = strings.ReplaceAll(sql, "[field2]", "["+compiler.JoinRefFieldPrefix+"."+v.(string)+"]")

		joinDirective := &ast.Directive{
			Name: compiler.JoinDirectiveName,
			Arguments: []*ast.Argument{
				{
					Name: "sql",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  sql,
					},
					Position: f.Field.Position,
				},
			},
			Definition:       joinDirectiveDef,
			ParentDefinition: joinObject,
			Location:         ast.LocationField,
		}
		joinObjectField := joinObject.Fields.ForName(f.Field.Name)
		if joinObjectField == nil {
			return nil, false, fmt.Errorf("field %s not found in join object", f.Field.Name)
		}

		var args ast.ArgumentList
		for _, arg := range f.Field.Arguments {
			if arg.Name == "field" || arg.Name == "alias" {
				continue
			}
			args = append(args, arg)
		}

		newField := &ast.Field{
			Alias:            f.Field.Alias,
			Name:             joinObjectField.Name,
			Definition:       joinObjectField,
			ObjectDefinition: f.Field.ObjectDefinition,
			Position:         f.Field.Position,
			SelectionSet:     f.Field.SelectionSet,
			Arguments:        args,
			Directives:       []*ast.Directive{joinDirective},
		}
		new.SelectionSet = append(new.SelectionSet, newField)
	}

	return new, withAgg, nil

}

func referencesFields(defs compiler.DefinitionsSource, query *ast.Field) (fieldList, error) {
	var refFields []string
	switch {
	case compiler.IsReferencesSubquery(query.Definition):
		info := compiler.DataObjectInfo(query.ObjectDefinition)
		if info == nil {
			return nil, nil
		}
		ri := info.ReferencesQueryInfo(defs, query.Name)
		if ri == nil {
			return nil, errors.New("sub query reference info not found")
		}
		refFields = ri.ReferencesFields()
		if ri.IsM2M {
			m2m := defs.ForName(ri.M2MName)
			refObjectInfo := compiler.DataObjectInfo(m2m)
			ri = refObjectInfo.M2MReferencesQueryInfo(defs, ri.Name)
			if ri == nil {
				return nil, errors.New("references query info not found")
			}
			refFields = ri.ReferencesFields()
		}
	case compiler.IsJoinSubquery(query):
		ji := compiler.JoinInfo(query)
		if ji == nil {
			return nil, errors.New("join info not found")
		}
		ff, err := ji.ReferencesFields()
		if err != nil {
			return nil, err
		}
		refFields = ff
	case compiler.IsTableFuncJoinSubquery(query):
		fc := compiler.FunctionCallInfo(query)
		if fc == nil {
			return nil, errors.New("function call info not found")
		}
		refFields = fc.ReferencesFields()
	case query.ObjectDefinition.Name == compiler.QueryTimeJoinObjectName:
		a := query.Arguments.ForName("fields")
		if a == nil {
			return nil, errors.New("fields argument is required")
		}
		fa, err := a.Value.Value(nil)
		if err != nil {
			return nil, err
		}
		fields, ok := fa.([]any)
		if !ok {
			return nil, errors.New("fields argument must be an array")
		}
		for _, f := range fields {
			ff, ok := f.(string)
			if !ok {
				return nil, errors.New("field must be a string")
			}
			refFields = append(refFields, ff)
		}
	}
	var fields fieldList
	def := defs.ForName(query.Definition.Type.Name())
	for _, f := range refFields {
		fd := def.Fields.ForName(f)
		if fd == nil {
			return nil, fmt.Errorf("field %s not found in object definition", f)
		}
		fields = append(fields, &ast.Field{
			Alias:            f,
			Name:             f,
			Definition:       fd,
			ObjectDefinition: query.ObjectDefinition,
			Position:         query.Position,
		})
	}
	return fields, nil
}

func sourceFields(defs compiler.DefinitionsSource, def *ast.Definition, query *ast.Field) (fieldList, error) {
	var fields []*ast.Field
	var fieldNames []string
	byAliases := false
	info := compiler.DataObjectInfo(def)
	if info == nil {
		return nil, nil
	}
	switch {
	case compiler.IsAggregateQuery(query),
		compiler.IsBucketAggregateQuery(query):
		aggregated := compiler.AggregatedQueryDef(query)
		if aggregated == nil {
			return nil, errors.New("aggregated field not found")
		}
		return sourceFields(defs, def, &ast.Field{
			Alias:            query.Alias,
			Name:             aggregated.Name,
			Definition:       aggregated,
			Arguments:        query.Arguments,
			ObjectDefinition: query.ObjectDefinition,
			Position:         query.Position,
			Comment:          query.Comment,
		})
	case compiler.IsJoinSubquery(query):
		ji := compiler.JoinInfo(query)
		if ji == nil {
			return nil, errors.New("join info not found")
		}
		ff, err := ji.SourceFields()
		if err != nil {
			return nil, err
		}
		byAliases = ji.IsQueryTime
		fieldNames = ff
	case compiler.IsTableFuncJoinSubquery(query),
		compiler.IsFunctionCallSubquery(query):
		fc := compiler.FunctionCallInfo(query)
		if fc == nil {
			return nil, errors.New("function call info not found")
		}
		ff, err := fc.SourceFields()
		if err != nil {
			return nil, err
		}
		fieldNames = ff
	case compiler.IsReferencesSubquery(query.Definition):
		ri := info.ReferencesQueryInfo(defs, query.Name)
		if ri == nil {
			return nil, errors.New("references query info not found")
		}
		ff := ri.SourceFields()
		fieldNames = ff
	case query.Name == compiler.QueryTimeJoinFieldName:
		a := query.Arguments.ForName("fields")
		if a == nil {
			return nil, errors.New("fields argument is required")
		}
		fa, err := a.Value.Value(nil)
		if err != nil {
			return nil, err
		}
		fields, ok := fa.([]any)
		if !ok {
			return nil, errors.New("fields argument must be an array")
		}
		for _, f := range fields {
			ff, ok := f.(string)
			if !ok {
				return nil, errors.New("field must be a string")
			}
			fieldNames = append(fieldNames, ff)
		}
	case query.Name == compiler.QueryTimeSpatialFieldName:
		a := query.Arguments.ForName("field")
		if a == nil {
			return nil, errors.New("field argument is required")
		}
		fa, err := a.Value.Value(nil)
		if err != nil {
			return nil, err
		}
		field, ok := fa.(string)
		if !ok {
			return nil, errors.New("field must be a string")
		}
		fieldNames = append(fieldNames, field)
	}

	for _, f := range fieldNames {
		if byAliases {
			fd := filterFields(query.SelectionSet, func(field *ast.Field) bool {
				return field.Alias == f
			}, false)
			if len(fd) == 0 {
				return nil, fmt.Errorf("field %s not found in object definition", f)
			}
			continue
		}
		fd := info.Definition().Fields.ForName(f)
		if fd == nil {
			return nil, fmt.Errorf("field %s not found in object definition", f)
		}
		fields = append(fields, &ast.Field{
			Alias:            f,
			Name:             f,
			Definition:       fd,
			ObjectDefinition: query.ObjectDefinition,
			Position:         query.Position,
		})
	}

	return fields, nil
}

// create new query field with extra selected fields
func queryWithExtraFields(ctx context.Context, query *ast.Field, extra fieldList, filter map[string]map[string]string) *ast.Field {
	newField := &ast.Field{
		Alias:            query.Alias,
		Name:             query.Name,
		Arguments:        query.Arguments,
		Directives:       query.Directives,
		Position:         query.Position,
		Comment:          query.Comment,
		Definition:       query.Definition,
		ObjectDefinition: query.ObjectDefinition,
	}
	var fields fieldList
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		filtered, ok := filter[f.Field.Alias]
		if !ok {
			fields = append(fields, f.Field)
			continue
		}
		if len(filtered) == 0 {
			continue
		}
		newFilter := make(map[string]map[string]string)
		for k := range filtered {
			newFilter[k] = nil
		}
		subField := queryWithExtraFields(ctx, f.Field, nil, newFilter)
		if len(subField.SelectionSet) != 0 {
			fields = append(fields, subField)
		}
	}
	for _, f := range extra {
		if fields.ForAlias(f.Alias) == nil {
			fields = append(fields, f)
		}
	}
	for _, f := range fields {
		newField.SelectionSet = append(newField.SelectionSet, f)
	}
	return newField
}
