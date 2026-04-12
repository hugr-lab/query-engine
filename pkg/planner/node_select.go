package planner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

// measurementAggregations maps measurement function names (user-facing) to
// engine-level aggregation function names.
var measurementAggregations = map[string]string{
	"MIN": "min",
	"MAX": "max",
	"SUM": "sum",
	"AVG": "avg",
	"ANY": "any",
}

func selectDataObjectRootNode(ctx context.Context, provider catalog.Provider, planner Catalog, query *ast.Field, vars map[string]interface{}) (*QueryPlanNode, error) {
	node, inGeneral, err := selectDataObjectNode(ctx, provider, planner, query, vars)
	if err != nil {
		return nil, err
	}

	dataObject := provider.ForName(ctx, query.Definition.Type.Name())
	if dataObject == nil || !sdl.IsDataObject(dataObject) {
		return nil, errors.New("data object for query not found")
	}
	info := sdl.DataObjectInfo(dataObject)
	if info == nil {
		return nil, errors.New("data object info not found")
	}
	e, err := planner.Engine(info.Catalog)
	if err != nil {
		return nil, err
	}
	caster, isTypeCast := e.(engines.EngineTypeCaster)
	if isTypeCast && !inGeneral {
		node, err = castResultsNode(ctx, caster, node, !IsRawResultsQuery(ctx, query), false)
		if err != nil {
			return nil, err
		}
	}
	return finalResultNode(ctx, provider, planner, query, node, inGeneral || !isTypeCast), nil
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
func selectDataObjectNode(ctx context.Context, defs base.DefinitionsSource, planer Catalog, query *ast.Field, vars map[string]interface{}) (*QueryPlanNode, bool, error) {
	dataObject := defs.ForName(ctx, query.Definition.Type.Name())
	if dataObject == nil || !sdl.IsDataObject(dataObject) {
		return nil, false, errors.New("data object for query not found")
	}
	info := sdl.DataObjectInfo(dataObject)
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
		isInner := false
		if !sdl.IsFunctionCall(sq.Definition) {
			am := sq.ArgumentMap(vars)
			if iv, ok := am["inner"]; ok {
				isInner, _ = iv.(bool)
			}
		}
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
			if isInner {
				innerGeneral = append(innerGeneral, joinNode.Name+"._selection IS NOT NULL")
			}
			ff, err := sourceFields(ctx, defs, dataObject, sq)
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
			if isInner {
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
	qJoinNodeNames := map[string]struct{}{}
	for _, qj := range qp.queryTimeJoins {
		catalogFields := map[string]string{}
		generalFields := map[string]string{}
		for _, selected := range engines.SelectedFields(qj.SelectionSet) {
			sq := selected.Field
			rAlias := "_" + qj.Alias + "_" + sq.Alias + "_join_sub_node"
			isInner := false
			if !sdl.IsFunctionCall(sq.Definition) {
				am := sq.ArgumentMap(vars)
				if iv, ok := am["inner"]; ok {
					isInner, _ = iv.(bool)
				}
			}
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
				if isInner {
					innerGeneral = append(innerGeneral, joinNode.Name+"._selection IS NOT NULL")
				}
				generalFields[sq.Alias] = joinNode.Name + "._selection"
				qJoinNodeNames[joinNode.Name] = struct{}{}
			}
			if !isGeneral || !isCaster {
				if node != nil {
					withCatalogNodes = append(withCatalogNodes, node)
				}
				joinCatalogNodes = append(joinCatalogNodes, joinNode)
				if isInner {
					innerCatalog = append(innerCatalog, joinNode.Name+"._selection IS NOT NULL")
				}
				catalogFields[sq.Alias] = joinNode.Name + "._selection"
				qJoinNodeNames[joinNode.Name] = struct{}{}
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
			ff, err := sourceFields(ctx, defs, dataObject, qj)
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
	// sort join nodes by unnsested first than by name
	slices.SortFunc(joinCatalogNodes, func(a, b *QueryPlanNode) int {
		au := a.Query.Directives.ForName(base.UnnestDirectiveName)
		bu := b.Query.Directives.ForName(base.UnnestDirectiveName)
		if au != nil && bu == nil {
			return -1 // a is unnest, b is not
		}
		if au == nil && bu != nil {
			return 1 // b is unnest, a is not
		}
		return strings.Compare(a.Name, b.Name) // both are unnest or both are not
	})
	// sort join nodes by unnsested first than by name
	slices.SortFunc(joinGeneralNodes, func(a, b *QueryPlanNode) int {
		au := a.Query.Directives.ForName(base.UnnestDirectiveName)
		bu := b.Query.Directives.ForName(base.UnnestDirectiveName)
		if au != nil && bu == nil {
			return -1 // a is unnest, b is not
		}
		if au == nil && bu != nil {
			return 1 // b is unnest, a is not
		}
		return strings.Compare(a.Name, b.Name) // both are unnest or both are not
	})
	queryArg, err := sdl.ArgumentValues(ctx, defs, query, vars, true)
	if err != nil {
		return nil, false, err
	}
	if info.HasArguments() {
		arg := queryArg.ForName("args")
		var am map[string]any
		if arg != nil {
			am = arg.Value.(map[string]any)
		}
		if am == nil {
			am = map[string]any{}
		}
		// perm.AuthVars(ctx) provides [$auth.*] and similar context placeholders
		// for resolving @arg_default input fields and embedded placeholders in
		// @view(sql:) templates.
		err = info.ApplyArguments(ctx, defs, am, e, perm.AuthVars(ctx))
		if err != nil {
			return nil, false, err
		}
	}

	atInfo, err := resolveAtInfo(query, vars)
	if err != nil {
		return nil, false, err
	}

	fieldNodes := fieldsNodes(ctx, e, info, "_objects",
		append(qp.fields, qp.extraSourceFields...), // add selected fields and extra fields that are required for joins
		vars,
		false,
	)
	nodes := QueryPlanNodes{
		fieldsNode(
			query,
			fieldNodes,
		),
		fromDataObjectNode(ctx, info, atInfo),
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

	// params nodes (limit, offset, filter, order by, distinct on)
	var paramNodes QueryPlanNodes
	if sdl.IsSelectOneQuery(query) {
		// return query with limit 1 and filtered by unique fields
		paramNodes, err = selectOneQueryParamsNodes(ctx, info, query, queryArg, "_objects")
		if err != nil {
			return nil, false, err
		}
	}
	if !sdl.IsSelectOneQuery(query) {
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
		// if there are no joins, we push down all params nodes
		nodes = append(nodes, paramNodes...)
	}
	if len(joinCatalogNodes) != 0 || len(joinGeneralNodes) != 0 {
		// if there are joins, we push down only where and vector search nodes
		whereNode := paramNodes.ForName("where")
		if whereNode != nil {
			nodes = append(nodes, whereNode)
		}
		vectorSearchNode := paramNodes.ForName(vectorDistanceNodeName)
		if vectorSearchNode != nil {
			nodes = append(nodes, vectorSearchNode)
		}
		vectorLimitNode := paramNodes.ForName(vectorSearchLimitNodeName)
		if vectorLimitNode != nil {
			nodes = append(nodes, vectorLimitNode)
		}
	}

	baseData := selectStatementNode(query, nodes, "_objects", qp.withRowNum)
	// if there are no joins, we can return the base query
	if len(joinGeneralNodes) == 0 &&
		len(joinCatalogNodes) == 0 &&
		qp.h3 == nil &&
		!sdl.IsNoJoinPushdown(query) && paramNodes.ForName(vectorSearchLimitNodeName) == nil {
		return baseData, false, nil
	}

	// get parameters nodes by aliases
	paramNodes, err = selectQueryParamsNodes(ctx, defs, e, info, "_objects", query, queryArg, true)
	if err != nil {
		return nil, false, err
	}

	distinctNode := paramNodes.ForName("distinct")
	orderByNode := paramNodes.ForName("orderBy")
	limitNode := paramNodes.ForName("limit")
	offsetNode := paramNodes.ForName("offset")

	canOrderByPushDown := len(joinGeneralNodes) == 0

	// check if we can push down order by fields
	// look at order by fields (nested node names in order by node) and check if they are all in catalog query
	if orderByNode != nil && !canOrderByPushDown {
		canOrderByPushDown = true
		for _, orderByFieldNode := range orderByNode.Nodes {
			if !strings.Contains(orderByFieldNode.Name, ".") {
				// if order by field is not a nested node, we can push down order by
				continue
			}
			pp := strings.SplitN(orderByFieldNode.Name, ".", 2)
			// check join nodes for general catalog query
			if slices.ContainsFunc(joinGeneralNodes, func(n *QueryPlanNode) bool {
				return n.Query.Alias == pp[0]
			}) {
				canOrderByPushDown = false
				break
			}

			// check join queries nodes for general catalog query
			jm, ok := qJoinsGeneralFields[pp[0]]
			if !ok {
				continue
			}

			if _, ok := jm[pp[1]]; ok {
				canOrderByPushDown = false
				break
			}
		}
	}

	canLimitPushDown := len(joinGeneralNodes) == 0 || len(innerGeneral) == 0 && (canOrderByPushDown || orderByNode == nil)

	if len(joinCatalogNodes) != 0 {
		baseData.Name = "_objects"
		withCatalogNodes = append(append(QueryPlanNodes{}, baseData), withCatalogNodes...)
		nodes = QueryPlanNodes{
			withNode(withCatalogNodes),
			fieldsNode(query,
				append(
					fieldsNodes(ctx, e, info, "_objects",
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
		if len(joinGeneralNodes) == 0 && !sdl.IsSelectOneQuery(query) && distinctNode != nil {
			// only distinct on params can be pushed down, because the distinct fields always are in the base data object
			nodes = append(nodes, distinctNode)
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

	if canLimitPushDown || canOrderByPushDown {
		// create the top query based on the base query with items
		baseData.Name = "_objects"
		nodes := QueryPlanNodes{
			withNode(QueryPlanNodes{baseData}),
			&QueryPlanNode{
				Name:  "fields",
				Query: query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects.*", params, nil
				},
			},
			&QueryPlanNode{
				Name:  "from",
				Query: query,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects", params, nil
				},
			},
		}
		if canLimitPushDown && limitNode != nil {
			nodes = append(nodes, limitNode)
		}
		if canLimitPushDown && offsetNode != nil {
			nodes = append(nodes, offsetNode)
		}
		if canOrderByPushDown && orderByNode != nil {
			nodes = append(nodes, orderByNode)
		}
		baseData = selectStatementNode(query, nodes, "_objects", false)
	}

	if len(joinGeneralNodes) == 0 &&
		qp.h3 == nil &&
		!sdl.IsNoJoinPushdown(query) {
		// if there are no general joins, we can return the base query
		return baseData, false, nil
	}

	if qp.h3 != nil {
		// if there is h3 query, we need to add geometry source fields to the base query
		generalSourceFields = append(generalSourceFields, qp.extraSourceFields.ForAlias("_h3_base_field"))
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

	ff = fieldsNodes(ctx, e, info, "_objects", qp.fields, vars, true)
	for _, j := range joinCatalogNodes {
		if _, ok := qJoinNodeNames[j.Name]; ok {
			continue
		}
		ff = append(ff, &QueryPlanNode{
			Name:  j.Query.Alias,
			Query: j.Query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return engines.Ident(node.Query.Alias), params, nil
			},
		})
	}
	for _, j := range joinGeneralNodes {
		if _, ok := qJoinNodeNames[j.Name]; ok {
			continue
		}
		ff = append(ff, &QueryPlanNode{
			Name:  j.Query.Alias,
			Query: j.Query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return j.Name + "._selection AS " + engines.Ident(node.Query.Alias), params, nil
			},
		})
	}
	for fn := range qJoinsCatalogFields {
		if _, ok := qJoinsGeneralFields[fn]; ok {
			continue
		}
		ff = append(ff, &QueryPlanNode{
			Name:  fn,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects." + fn, params, nil
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
	// add h3 field if needed
	if qp.h3 != nil {
		ff = append(ff, &QueryPlanNode{
			Name:  "_h3_base_field",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects._h3_base_field", params, nil
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
	if !sdl.IsSelectOneQuery(query) {
		paramNodes, err = selectQueryParamsNodes(ctx, defs, defaultEngine, info, "_objects", query, queryArg, true)
		if err != nil {
			return nil, false, err
		}
		orderByNode = paramNodes.ForName("orderBy")
		limitNode = paramNodes.ForName("limit")
		offsetNode = paramNodes.ForName("offset")
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
	if orderByNode == nil && limitNode == nil && qp.h3 == nil {
		return selectStatementNode(query, nodes, "_objects", qp.withRowNum), true, nil
	}

	// create the top query based on the last one with order by and limits
	if qp.h3 != nil {
		qp.withRowNum = true
	}
	baseData = selectStatementNode(query, nodes, "_objects", qp.withRowNum)
	baseData.Name = "_objects"
	nodes = QueryPlanNodes{
		withNode(QueryPlanNodes{baseData}),
		&QueryPlanNode{
			Name:  "fields",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				if qp.h3 == nil && !qp.withRowNum {
					return "_objects.*", params, nil
				}
				if qp.h3 == nil && qp.withRowNum {
					return "_objects.* EXCLUDE(_row_num)", params, nil
				}
				sql := "_h3_base_field"
				if qp.withRowNum {
					sql += ",_row_num"
				}
				sql = "_objects.* EXCLUDE(" + sql + ")"
				h3Field := "_objects._h3_base_field"
				if qp.h3.extractFromGeom {
					h3Field = "_h3_cells._list"
				}
				if qp.h3.unnest {
					sql += ", unnest(" + h3Field + ") AS _h3_cell"
					sql += ", len(" + h3Field + ") AS _h3_cells_count"
				} else {
					sql += ", " + h3Field + " AS _h3_cell"
					sql += ", 1 AS _h3_cells_count"
				}
				return sql, params, nil
			},
		},
		&QueryPlanNode{
			Name:  "from",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects", params, nil
			},
		},
	}
	if qp.h3 != nil && qp.h3.extractFromGeom {
		nodes.Add(&QueryPlanNode{
			Name:  "joins",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				h3SQL := "_objects._h3_base_field"
				if qp.h3.extractFromGeom {
					if qp.h3.transformFrom != 0 {
						h3SQL = fmt.Sprintf("ST_Transform(%s, 'EPSG:%d', 'EPSG:4326')", h3SQL, qp.h3.transformFrom)
					}
					if qp.h3.buffer > 0 {
						h3SQL = fmt.Sprintf("ST_Buffer(%s, %f)", h3SQL, qp.h3.buffer)
					}
					h3SQL = fmt.Sprintf("h3_geom_to_cells(%s, %d, %v)", h3SQL, qp.h3.res, qp.h3.simplify)
				}
				return defaultEngine.LateralJoin("SELECT "+h3SQL+" as _list", "_h3_cells"), params, nil
			},
		})
	}
	if limitNode != nil {
		nodes = append(nodes, limitNode)
	}
	if offsetNode != nil {
		nodes = append(nodes, offsetNode)
	}
	if orderByNode != nil {
		nodes = append(nodes, orderByNode)
	}
	return selectStatementNode(query, nodes, "_objects", qp.withRowNum), true, nil
}

func cubeGroupByNode(info *sdl.Object, query *ast.Field, fieldList fieldList) (*QueryPlanNode, error) {
	if !info.IsCube {
		return nil, fmt.Errorf("object %s is not cube", info.Name)
	}
	var fields []string
	for _, f := range fieldList {
		if f.Definition.Directives.ForName(base.FieldMeasurementDirectiveName) != nil &&
			f.Arguments.ForName(base.FieldMeasurementFuncArgName) != nil {
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

func subDataQueryNode(ctx context.Context, defs base.DefinitionsSource, planner Catalog, info *sdl.Object, inGeneral bool, field *ast.Field, rAlias string, vars map[string]interface{}) (node *QueryPlanNode, isGeneral bool, err error) {
	var e engines.Engine
	var qCatalog string
	def := defs.ForName(ctx, field.Definition.Type.Name())
	if def == nil {
		return nil, false, fmt.Errorf("subquery %q: type %q not found", field.Name, field.Definition.Type.Name())
	}
	switch {
	case sdl.IsDataObject(def):
		node, isGeneral, err = selectDataObjectNode(ctx, defs, planner, field, vars)
		if err != nil {
			return nil, false, err
		}
		qCatalog = sdl.DataObjectInfo(def).Catalog
		e, err = planner.Engine(qCatalog)
		if err != nil {
			return nil, false, err
		}
	case sdl.IsTableFuncJoinSubquery(field),
		sdl.IsFunctionCallSubquery(field):
		qCatalog = base.FieldDefCatalog(field.Definition)
		e, err = planner.Engine(qCatalog)
		if err != nil {
			return nil, false, err
		}
		// check if func call use some fields from the main query than it will call from join
		fInfo := sdl.FunctionCallInfo(field)
		if fInfo == nil {
			return nil, false, errors.New("function call info not found")
		}
		if len(fInfo.ArgumentMap()) != 0 && qCatalog == info.Catalog {
			return nil, false, nil
		}
		if len(fInfo.ArgumentMap()) != 0 && qCatalog != info.Catalog {
			return nil, true, nil
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
	case sdl.IsAggregateQuery(field), sdl.IsBucketAggregateQuery(field):
		qCatalog = base.FieldDefCatalog(field.Definition)
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
	// If catalogs differ, mark as general even if the inner engine doesn't
	// need type casting (e.g., DuckDB is the default engine). This ensures
	// the subquery runs outside the outer engine's query wrapper (e.g.,
	// not inside postgres_query()).
	return node, isGeneral || qCatalog != info.Catalog, nil
}

// node create sql :
// ,LATERAL (
//
//	SELECT [array_agg](struct pack (fields from subquery))
//	FROM _subquery_sub_node (or function call sql)
//	WHERE [join conditions]
//
// ) AS _subquery_sub_node_final
func joinSubQueryNode(ctx context.Context, defs base.DefinitionsSource, planner Catalog, e engines.Engine, info *sdl.Object, inGeneral bool, left, right *ast.Field, vars map[string]any, prefix, rAlias string) (node *QueryPlanNode, err error) {
	var nodes QueryPlanNodes
	switch {
	case sdl.IsAggregateQuery(right), sdl.IsBucketAggregateQuery(right):
		nodes, err = joinAggregateNodes(ctx, defs, planner, inGeneral, right, prefix, rAlias)
	case sdl.IsReferencesSubquery(right.Definition):
		nodes, err = joinReferencesQueryNodes(ctx, defs, info, inGeneral, right, prefix, rAlias)
	case sdl.IsJoinSubquery(right):
		nodes, err = joinQueryNodes(ctx, defs, inGeneral, right, prefix, rAlias)
	case sdl.IsFunctionCallSubquery(right) || sdl.IsTableFuncJoinSubquery(right):
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
			node.Name = "nested_order_by"
			nodes = append(nodes, node)
		}
		limit, ok := am["nested_limit"]
		if ok {
			if l, ok := limit.(int64); ok && l != 0 {
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
			if o, ok := offset.(int64); ok && o != 0 {
				nodes = append(nodes, &QueryPlanNode{
					Name: "nested_offset",
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return fmt.Sprintf("%d", o), params, nil
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
			if where != nil && node.Query.Directives.ForName(base.UnnestDirectiveName) == nil && where.Result != "" {
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
			if fieldsAgg != nil && node.Query.Directives.ForName(base.UnnestDirectiveName) == nil {
				sql = "SELECT " + fieldsAgg.Result + " FROM (" + sql + ") AS _subquery_sub_node"
				if groupBy := children.ForName("groupBy"); groupBy != nil {
					sql += " GROUP BY " + groupBy.Result
				}
				// check args for subquery (nested_order_by, nested_limit, nested_offset)
			}

			if node.Query.Directives.ForName(base.UnnestDirectiveName) != nil {
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

func joinReferencesQueryNodes(ctx context.Context, defs base.DefinitionsSource, info *sdl.Object, inGeneral bool, field *ast.Field, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	ri := info.ReferencesQueryInfo(ctx, defs, field.Name)
	if ri == nil {
		return nil, errors.New("references query info not found")
	}
	refObject := defs.ForName(ctx, field.Definition.Type.Name())
	refObjectInfo := sdl.DataObjectInfo(refObject)
	nodes = QueryPlanNodes{
		{
			Name:  "from",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				sql := rAlias
				if ri.IsM2M {
					jc, err := ri.FromM2MJoinConditions(ctx, defs, "_join_m2m", sql, true, false)
					if err != nil {
						return "", nil, err
					}
					m2m := defs.ForName(ctx, ri.M2MName)
					m2mInfo := sdl.DataObjectInfo(m2m)
					db := m2mInfo.Catalog
					if !inGeneral {
						db = ""
					}
					sql += " INNER JOIN " + m2mInfo.SQL(ctx, engines.Ident(db)) + " AS _join_m2m ON " + jc
				}
				return sql, params, nil
			},
		},
		{
			Name:  "where",
			Query: field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				if ri.IsM2M {
					sql, err := ri.ToM2MJoinConditions(ctx, defs, prefix, "_join_m2m", false, true)
					return sql, params, err
				}
				sql, err := ri.JoinConditions(ctx, defs, prefix, rAlias, false, false)
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
				if node.Query.Directives.ForName(base.UnnestDirectiveName) != nil {
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

func joinQueryNodes(ctx context.Context, defs base.DefinitionsSource, inGeneral bool, right *ast.Field, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	ji := sdl.JoinInfo(right)
	if ji == nil {
		return nil, errors.New("join info not found")
	}
	refObject := defs.ForName(ctx, right.Definition.Type.Name())
	if refObject == nil {
		return nil, errors.New("reference object not found")
	}
	catalog := sdl.DataObjectInfo(refObject).Catalog

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
					sql = strings.ReplaceAll(sql, "["+base.JoinSourceFieldPrefix+"."+fn+"]", prefix+fn)
				}
				rightFields, err := ji.ReferencesFields()
				if err != nil {
					return "", nil, err
				}
				for _, fn := range rightFields {
					sql = strings.ReplaceAll(sql, "["+base.JoinRefFieldPrefix+"."+fn+"]", rAlias+"."+fn)
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
				if node.Query.Directives.ForName(base.UnnestDirectiveName) != nil {
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

func joinFunctionCallNodes(ctx context.Context, defs base.DefinitionsSource, inGeneral bool, left, right *ast.Field, vars map[string]interface{}, prefix, rAlias string) (nodes QueryPlanNodes, err error) {
	call := sdl.FunctionCallInfo(right)
	if call == nil {
		return nil, errors.New("function call info not found")
	}
	info, err := call.FunctionInfo(ctx, defs)
	if err != nil {
		return nil, err
	}
	fromNode := &QueryPlanNode{
		Name:  "from",
		Query: right,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			call := sdl.FunctionCallInfo(node.Query)
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
			sql, params, err := functionCallSQL(ctx, node.TypeDefs(), e, node.Query, vars, params)
			if err != nil {
				return "", nil, err
			}
			// replace fields
			def := defs.ForName(ctx, left.Definition.Type.Name())
			info := sdl.DataObjectInfo(def)
			for _, f := range sdl.ExtractFieldsFromSQL(sql) {
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
			if len(sdl.ExtractFieldsFromSQL(sql)) != 0 {
				return "", nil, fmt.Errorf("not all function %s arguments is defined %s",
					node.Query.Name,
					strings.Join(sdl.ExtractFieldsFromSQL(sql), ","))
			}
			fi, err := call.FunctionInfo(ctx, defs)
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
				if node.Query.Directives.ForName(base.UnnestDirectiveName) != nil {
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
					sql = strings.ReplaceAll(sql, "["+base.JoinSourceFieldPrefix+"."+fn+"]", prefix+fn)
				}
				rightFields := call.ReferencesFields()
				for _, fn := range rightFields {
					sql = strings.ReplaceAll(sql, "["+base.JoinRefFieldPrefix+"."+fn+"]", rAlias+"."+fn)
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

func fromDataObjectNode(ctx context.Context, info *sdl.Object, atInfo *AtInfo) *QueryPlanNode {
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
			sql := info.SQL(ctx, engines.Ident(prefix))
			at, err := atClause(atInfo)
			if err != nil {
				return "", nil, err
			}
			if at != "" {
				// Wrap in subquery: DuckDB requires AT clause before any alias,
				// but the caller appends "AS _objects". Use a subquery to isolate.
				sql = "(SELECT * FROM " + sql + at + ")"
			}
			return sql, params, nil
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

func fieldsNodes(ctx context.Context, e engines.Engine, info *sdl.Object, prefix string, fields []*ast.Field, vars map[string]any, aliases bool) QueryPlanNodes {
	var nodes QueryPlanNodes
	for _, field := range fields {
		if fn := nodes.ForName(field.Alias); fn != nil {
			if fn.Query.Name == field.Name {
				// skip duplicate fields
				// TODO FIXME: here can be a problem if user give the same alias to the additional field
				continue
			}
		}
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
					sdl.IsScalarType(typeName) {
					args, err := sdl.ArgumentValues(ctx, node.TypeDefs(), field, vars, true)
					if err != nil {
						return "", nil, err
					}
					if len(fi.Definition().Arguments) != 0 ||
						sdl.IsExtraField(fi.Definition()) {
						sql, params, err = e.ApplyFieldTransforms(ctx, node.Querier(), sql, field, args, params)
						if err != nil {
							return "", nil, err
						}
					}
					if info.IsCube &&
						fi.Definition().Directives.ForName(base.FieldMeasurementDirectiveName) != nil &&
						args.ForName(base.FieldMeasurementFuncArgName) != nil {
						// aggregate measurements
						mf, ok := args.ForName(base.FieldMeasurementFuncArgName).Value.(string)
						if !ok || measurementAggregations[mf] == "" {
							return "", nil, errors.New("measurement function not found")
						}
						aggregator, ok := e.(engines.EngineAggregator)
						if !ok {
							return "", nil, fmt.Errorf("cube %s engine does not support aggregation", info.Name)
						}
						sql, params, err = aggregator.AggregateFuncSQL(measurementAggregations[mf], sql, "", "", field.Definition, sdl.IsHyperTable(info.Definition()), nil, params)
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

	// h3, if non nil, the special H3 field will be added to the query
	// the field will have the name "_h3_cell", cells will be unnested
	// The conditions - @add_h3(field: "geom", res: 9) - will be added to the query
	h3 *h3SubQuery
}

type h3SubQuery struct {
	res             int     // resolution of the H3 index
	baseGeom        string  // name of the geometry or H3 field to get H3 indexes
	unnest          bool    // if true, the H3 cells will be unnested
	extractFromGeom bool    // if true, the H3 cells will be extracted from the geometry
	simplify        bool    // if true, the geometry will be simplified before extracting H3 cells
	transformFrom   int     // if non zero, the H3 cells will be transformed from this resolution to the `res` resolution
	buffer          float64 // if non zero, the H3 cells will be buffered by this value
}

func splitByQueryParts(ctx context.Context, defs base.DefinitionsSource, query *ast.Field, vars map[string]any) (*queryPart, error) {
	qp := queryPart{}
	var queryTimeJoins, queryTimeSpatial []*ast.Field
	for _, field := range engines.SelectedFields(query.SelectionSet) {
		switch {
		case sdl.IsReferencesSubquery(field.Field.Definition):
			qp.subQueries = append(qp.subQueries, field.Field)
		case sdl.IsFunctionCallSubquery(field.Field),
			sdl.IsTableFuncJoinSubquery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
		case sdl.IsJoinSubquery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
		case field.Field.Name == base.QueryTimeJoinsFieldName:
			queryTimeJoins = append(queryTimeJoins, field.Field)
		case field.Field.Name == base.QueryTimeSpatialFieldName:
			queryTimeSpatial = append(queryTimeSpatial, field.Field)
		case sdl.IsAggregateQuery(field.Field), sdl.IsBucketAggregateQuery(field.Field):
			qp.subQueries = append(qp.subQueries, field.Field)
			qp.withRowNum = true
		default:
			qp.fields = append(qp.fields, field.Field)
		}
	}
	// add join directives for query time join
	for _, f := range queryTimeJoins {
		join, withAgg, err := castJoinDirectiveToJoin(ctx, defs, query, f, vars)
		if err != nil {
			return nil, err
		}
		qp.queryTimeJoins = append(qp.queryTimeJoins, join)
		if withAgg {
			qp.withRowNum = true
		}
	}
	for _, f := range queryTimeSpatial {
		join, withAgg, err := castSpatialQueryToJoin(ctx, defs, query, f, vars)
		if err != nil {
			return nil, err
		}
		qp.queryTimeJoins = append(qp.queryTimeJoins, join)
		if withAgg {
			qp.withRowNum = true
		}
	}
	// add references fields to selected fields (to make joins possible)
	refFields, err := referencesFields(ctx, defs, query)
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
		sff, err := sourceFields(ctx, defs, f.ObjectDefinition, f)
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
	// add H3 base field if needed
	if d := query.Directives.ForName(base.AddH3DirectiveName); d != nil {
		res, err := strconv.Atoi(sdl.DirectiveArgValue(d, "res", vars))
		if err != nil {
			return nil, sdl.ErrorPosf(d.Position, "invalid H3 resolution: %s", err.Error())
		}
		if res < 0 || res > 15 {
			return nil, sdl.ErrorPosf(d.Position, "H3 resolution must be in range 0-15, got %d", res)
		}
		qp.h3 = &h3SubQuery{
			res:      res,
			baseGeom: sdl.DirectiveArgValue(d, "field", vars),
		}
		def := defs.ForName(ctx, query.Definition.Type.Name())
		if def == nil {
			return nil, sdl.ErrorPosf(d.Position, "object %s not found", query.Definition.Type.Name())
		}
		h3Base := def.Fields.ForName(qp.h3.baseGeom)
		if h3Base == nil {
			return nil, sdl.ErrorPosf(d.Position, "H3 base field %s not found in object %s", qp.h3.baseGeom, query.ObjectDefinition.Name)
		}
		if h3Base.Type.NamedType != base.GeometryTypeName &&
			h3Base.Type.Name() != base.H3CellTypeName {
			return nil, sdl.ErrorPosf(d.Position, "H3 base field %s must be of type %s or %s, got %s",
				qp.h3.baseGeom,
				base.GeometryTypeName,
				base.H3CellTypeName,
				h3Base.Type.NamedType)
		}
		qp.h3.transformFrom, _ = strconv.Atoi(sdl.DirectiveArgValue(d, "transform_from", vars))
		if d := h3Base.Directives.ForName(base.FieldGeometryInfoDirectiveName); d != nil && qp.h3.transformFrom == 0 {
			qp.h3.transformFrom, _ = strconv.Atoi(sdl.DirectiveArgValue(d, "srid", vars))
			if qp.h3.transformFrom == 4326 {
				qp.h3.transformFrom = 0 // 4326 is the default SRID, no need to transform
			}
		}
		qp.h3.buffer, _ = strconv.ParseFloat(sdl.DirectiveArgValue(d, "buffer", vars), 64)
		if qp.h3.buffer != 0 {
			if qp.h3.buffer > 5000 {
				return nil, sdl.ErrorPosf(d.Position, "H3 buffer must be in range 0-5000 meters, got %f", qp.h3.buffer)
			}
			qp.h3.buffer /= 111111 // convert meters to degrees
		}
		qp.h3.extractFromGeom = h3Base.Type.NamedType == base.GeometryTypeName
		qp.h3.unnest = h3Base.Type.Name() != base.H3CellTypeName || h3Base.Type.NamedType != ""
		qp.h3.simplify = sdl.DirectiveArgValue(d, "simplify", vars) == "true"
		qp.extraSourceFields = append(qp.extraSourceFields, &ast.Field{
			Alias:            "_h3_base_field",
			Name:             qp.h3.baseGeom,
			Definition:       h3Base,
			ObjectDefinition: def,
			Position:         h3Base.Position,
		})
	}
	// add source fields for order by arguments to order by non selected fields (will be used in aggregation)
	slices.SortFunc(qp.subQueries, func(f1, f2 *ast.Field) int {
		u1 := f1.Directives.ForName(base.UnnestDirectiveName) != nil
		u2 := f2.Directives.ForName(base.UnnestDirectiveName) != nil
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

func castJoinDirectiveToJoin(ctx context.Context, defs base.DefinitionsSource, query, joinQuery *ast.Field, vars map[string]any) (*ast.Field, bool, error) {
	if joinQuery.Name != base.QueryTimeJoinsFieldName {
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
		return nil, false, sdl.ErrorPosf(joinQuery.Position, "fields argument is required")
	}
	sourceFields, ok := v.([]any)
	if !ok {
		return nil, false, sdl.ErrorPosf(joinQuery.Position, "fields argument must be an array")
	}
	if len(sourceFields) == 0 {
		return nil, false, sdl.ErrorPosf(joinQuery.Position, "fields argument must have at least one field")
	}
	// 3. add query fields with @join directive
	withAgg := false
	for _, sq := range engines.SelectedFields(joinQuery.SelectionSet) {
		if sdl.IsAggregateQuery(sq.Field) || sdl.IsBucketAggregateQuery(sq.Field) {
			withAgg = true
		}
		// 3.1 get references fields name
		argMap := sq.Field.ArgumentMap(vars)
		v, ok := argMap["fields"]
		if !ok {
			return nil, false, sdl.ErrorPosf(sq.Field.Position, "fields argument is required")
		}
		refFields, ok := v.([]any)
		if !ok {
			return nil, false, sdl.ErrorPosf(sq.Field.Position, "fields argument must be an array")
		}
		if len(refFields) == 0 {
			return nil, false, sdl.ErrorPosf(sq.Field.Position, "fields argument must have at least one field")
		}
		// 3.2 create sql for @join directive
		if len(sourceFields) != len(refFields) {
			return nil, false, sdl.ErrorPosf(sq.Field.Position, "fields and references fields must have the same length")
		}
		sourceDef := defs.ForName(ctx, query.Definition.Type.Name())
		if sourceDef == nil {
			return nil, false, sdl.ErrorPosf(sq.Field.Position, "left object %s not found", query.Definition.Type.Name())
		}
		var sql string
		for i, f := range sourceFields {
			sourceFieldName, ok := f.(string)
			if !ok {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "field must be a string")
			}
			// check fields and if the field is not in sub query (join, function call, etc) add it to source fields
			if strings.Contains(sourceFieldName, ".") {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "join field could not be in a nested object")
			}
			var sourceFieldDef *ast.FieldDefinition
			if s := engines.SelectedFields(query.SelectionSet).ForName(sourceFieldName); s != nil {
				sourceFieldDef = s.Field.Definition
			}
			if sourceFieldDef == nil && sourceDef.Fields.ForName(sourceFieldName) != nil {
				sourceFieldDef = sourceDef.Fields.ForName(sourceFieldName)
			}
			if sourceFieldDef == nil {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "left object field %s not found", sourceFieldName)
			}
			sourceFieldName = "[" + base.JoinSourceFieldPrefix + "." + sourceFieldName + "]"
			refFieldName, ok := refFields[i].(string)
			if !ok {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "field must be a string")
			}
			if strings.Contains(refFieldName, ".") {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "join field could not be in a nested object")
			}
			rightDef := defs.ForName(ctx, sq.Field.Definition.Type.Name())
			if rightDef == nil {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "right object %s not found", sq.Field.Definition.Type.Name())
			}
			var refFieldDef *ast.FieldDefinition
			if s := engines.SelectedFields(sq.Field.SelectionSet).ForName(refFieldName); s != nil {
				refFieldDef = s.Field.Definition
			}
			if refFieldDef == nil && rightDef.Fields.ForName(refFieldName) != nil {
				refFieldDef = rightDef.Fields.ForName(refFieldName)
			}
			if refFieldDef == nil {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "right object field %s not found", refFieldName)
			}
			if sourceFieldDef.Type.NamedType == "" ||
				!sdl.IsScalarType(sourceFieldDef.Type.NamedType) ||
				sourceFieldDef.Type.NamedType != refFieldDef.Type.NamedType {
				return nil, false, sdl.ErrorPosf(sq.Field.Position, "fields must be scalar and have the same type")
			}
			refFieldName = "[" + base.JoinRefFieldPrefix + "." + refFieldName + "]"
			if sql != "" {
				sql += " AND "
			}
			sql += sourceFieldName + " = " + refFieldName
		}
		// 3.3 add @join directive to subquery field
		sq.Field.Directives = append(sq.Field.Directives, &ast.Directive{
			Name: base.JoinDirectiveName,
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

func castSpatialQueryToJoin(ctx context.Context, defs base.DefinitionsSource, query *ast.Field, field *ast.Field, vars map[string]any) (*ast.Field, bool, error) {
	if field.Name != base.QueryTimeSpatialFieldName {
		return nil, false, errors.New("field is not spatial query")
	}
	def := defs.ForName(ctx, query.Definition.Type.Name())
	joinField := def.Fields.ForName(base.QueryTimeJoinsFieldName)
	if joinField == nil {
		return nil, false, errors.New("join field not found")
	}

	joinObject := defs.ForName(ctx, base.QueryTimeJoinsTypeName)
	if joinObject == nil {
		return nil, false, errors.New("join object not found")
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
	case "DWITHIN":
		sqlTemplate = fmt.Sprintf("ST_DWithin([field1], [field2], %f)", buffer)
		buffer = 0
	}
	if buffer != 0 {
		sqlTemplate = strings.ReplaceAll(sqlTemplate, "[field1]",
			fmt.Sprintf("ST_Buffer([field1], %f)", buffer),
		)
	}
	sqlTemplate = strings.ReplaceAll(sqlTemplate, "[field1]", "["+base.JoinSourceFieldPrefix+"."+aliasName+"]")

	withAgg := false
	for _, f := range engines.SelectedFields(field.SelectionSet) {
		if sdl.IsAggregateQuery(f.Field) || sdl.IsBucketAggregateQuery(f.Field) {
			withAgg = true
		}
		sql := sqlTemplate
		argMap = f.Field.ArgumentMap(vars)
		v, ok := argMap["field"]
		if !ok {
			return nil, false, errors.New("field argument for spatial query is required")
		}
		fieldSQL := v.(string)

		sql = strings.ReplaceAll(sql, "[field2]", "["+base.JoinRefFieldPrefix+"."+fieldSQL+"]")

		joinDirective := &ast.Directive{
			Name: base.JoinDirectiveName,
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
			Definition:       nil,
			ParentDefinition: joinObject,
			Location:         ast.LocationField,
		}
		joinObjectField := joinObject.Fields.ForName(f.Field.Name)
		if joinObjectField == nil {
			return nil, false, fmt.Errorf("field %s not found in join object", f.Field.Name)
		}

		var args ast.ArgumentList
		for _, arg := range f.Field.Arguments {
			if arg.Name == "field" {
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
		if f.Field.Directives.ForName(base.UnnestDirectiveName) != nil {
			newField.Directives = append(newField.Directives,
				f.Field.Directives.ForName(base.UnnestDirectiveName),
			)
		}
		if f.Field.Directives.ForName(base.AddH3DirectiveName) != nil {
			newField.Directives = append(newField.Directives,
				f.Field.Directives.ForName(base.AddH3DirectiveName),
			)
		}
		if sdl.IsNoJoinPushdown(query) ||
			sdl.IsNoJoinPushdown(f.Field) {
			newField.Directives = append(newField.Directives,
				f.Field.Directives.ForName(base.NoPushdownDirectiveName),
			)
		}
		new.SelectionSet = append(new.SelectionSet, newField)
	}

	return new, withAgg, nil

}

func referencesFields(ctx context.Context, defs base.DefinitionsSource, query *ast.Field) (fieldList, error) {
	var refFields []string
	switch {
	case sdl.IsReferencesSubquery(query.Definition):
		info := sdl.DataObjectInfo(query.ObjectDefinition)
		if info == nil {
			return nil, nil
		}
		ri := info.ReferencesQueryInfo(ctx, defs, query.Name)
		if ri == nil {
			return nil, errors.New("sub query reference info not found")
		}
		refFields = ri.ReferencesFields()
		if ri.IsM2M {
			m2m := defs.ForName(ctx, ri.M2MName)
			refObjectInfo := sdl.DataObjectInfo(m2m)
			ri = refObjectInfo.M2MReferencesQueryInfo(ctx, defs, ri.Name)
			if ri == nil {
				return nil, errors.New("references query info not found")
			}
			refFields = ri.ReferencesFields()
		}
	case sdl.IsJoinSubquery(query):
		ji := sdl.JoinInfo(query)
		if ji == nil {
			return nil, errors.New("join info not found")
		}
		ff, err := ji.ReferencesFields()
		if err != nil {
			return nil, err
		}
		refFields = ff
	case sdl.IsTableFuncJoinSubquery(query):
		fc := sdl.FunctionCallInfo(query)
		if fc == nil {
			return nil, errors.New("function call info not found")
		}
		refFields = fc.ReferencesFields()
	case query.ObjectDefinition.Name == base.QueryTimeJoinsTypeName:
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
	def := defs.ForName(ctx, query.Definition.Type.Name())
	for _, f := range refFields {
		fd := def.Fields.ForName(f)
		if fd == nil {
			return nil, fmt.Errorf("field %s not found in object definition", f)
		}
		fields = append(fields, &ast.Field{
			Alias:            f,
			Name:             f,
			Definition:       fd,
			ObjectDefinition: def,
			Position:         query.Position,
		})
	}
	return fields, nil
}

func sourceFields(ctx context.Context, defs base.DefinitionsSource, def *ast.Definition, query *ast.Field) (fieldList, error) {
	var fields []*ast.Field
	var fieldNames []string
	byAliases := false
	info := sdl.DataObjectInfo(def)
	if info == nil {
		return nil, nil
	}
	switch {
	case sdl.IsAggregateQuery(query),
		sdl.IsBucketAggregateQuery(query):
		aggregated := sdl.AggregatedQueryDef(query)
		if aggregated == nil {
			return nil, errors.New("aggregated field not found")
		}
		return sourceFields(ctx, defs, def, &ast.Field{
			Alias:            query.Alias,
			Name:             aggregated.Name,
			Definition:       aggregated,
			Arguments:        query.Arguments,
			ObjectDefinition: query.ObjectDefinition,
			Position:         query.Position,
			Comment:          query.Comment,
		})
	case sdl.IsJoinSubquery(query):
		ji := sdl.JoinInfo(query)
		if ji == nil {
			return nil, errors.New("join info not found")
		}
		ff, err := ji.SourceFields()
		if err != nil {
			return nil, err
		}
		byAliases = ji.IsQueryTime
		fieldNames = ff
	case sdl.IsTableFuncJoinSubquery(query),
		sdl.IsFunctionCallSubquery(query):
		fc := sdl.FunctionCallInfo(query)
		if fc == nil {
			return nil, errors.New("function call info not found")
		}
		ff, err := fc.SourceFields()
		if err != nil {
			return nil, err
		}
		fieldNames = ff
	case sdl.IsReferencesSubquery(query.Definition):
		ri := info.ReferencesQueryInfo(ctx, defs, query.Name)
		if ri == nil {
			return nil, errors.New("references query info not found")
		}
		ff := ri.SourceFields()
		fieldNames = ff
	case query.Name == base.QueryTimeJoinsFieldName:
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
	case query.Name == base.QueryTimeSpatialFieldName:
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
