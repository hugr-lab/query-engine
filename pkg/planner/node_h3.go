package planner

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func h3RootNode(ctx context.Context, schema *ast.Schema, planner Catalog, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	node, err := h3DataNode(ctx, compiler.SchemaDefs(schema), planner, query, vars)
	if err != nil {
		return nil, err
	}

	return finalResultNode(ctx, schema, planner, query, node, true), nil
}

// Create a node that provides SQL query for h3 analytical query.
// This node will use the parent query to create sub queries for each data field with H3 cell.
func h3DataNode(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	subCount, err := validateH3Query(query, vars)
	if err != nil {
		return nil, err
	}

	res := query.ArgumentMap(vars)
	resolution, ok := res["resolution"].(int64)
	if !ok || resolution < 1 || resolution > 15 {
		return nil, compiler.ErrorPosf(query.Position, "H3 query 'resolution' argument must be an integer between 1 and 15")
	}

	var withNodes, fieldNodes, unions QueryPlanNodes
	for _, f := range engines.SelectedFields(query.SelectionSet) {
		switch f.Field.Name {
		case "cell":
			fieldNodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects._h3_cell AS " + engines.Ident(node.Name), params, nil
				},
			})
		case "resolution":
			fieldNodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return strconv.Itoa(int(resolution)) + " AS " + engines.Ident(node.Name), params, nil
				},
			})
		case "geom":
			fieldNodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "ST_GeomFromText(h3_cell_to_boundary_wkt(_objects._h3_cell)) AS " + engines.Ident(node.Name), params, nil
				},
			})
		case base.H3DataFieldName:
			// create sub query nodes for each data field with H3 cell (split or/and aggregate query)
			sq, innerQueries, err := h3DataSubQueryNodes(ctx, defs, planner, f.Field, int(resolution), vars)
			if err != nil {
				return nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' sub query: %v", query.Alias, err)
			}
			withNodes = append(withNodes, sq...)
			// add final data node that will return results and NULL for other data and distribution fields
			var nodes, fields QueryPlanNodes
			fields.Add(&QueryPlanNode{
				Name: "_h3_cell",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects._h3_cell", params, nil
				},
			})
			for _, sub := range engines.SelectedFields(query.SelectionSet) {
				if sub.Field.Name != base.H3DataFieldName &&
					sub.Field.Name != base.DistributionFieldName &&
					sub.Field.Name != base.BucketDistributionFieldName {
					continue
				}
				if sub.Field.Alias == f.Field.Alias {
					fields.Add(&QueryPlanNode{
						Name: f.Field.Alias,
						CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
							return "_objects." + engines.Ident(node.Name), params, nil
						},
					})
					continue
				}
				fields.Add(&QueryPlanNode{
					Name:  sub.Field.Alias,
					Query: sub.Field,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return "NULL AS " + engines.Ident(node.Name), params, nil
					},
				})
			}
			nodes.Add(fieldsNode(f.Field, fields))
			nodes.Add(&QueryPlanNode{
				Name: "from",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_" + f.Field.Alias + "_data", params, nil
				},
			})
			if len(innerQueries) != 0 {
				nodes.Add(&QueryPlanNode{
					Name:  "where",
					Query: f.Field,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						var inners []string
						for _, inner := range innerQueries {
							inners = append(inners, "_objects."+engines.Ident(f.Field.Alias)+"['"+inner+"'] IS NOT NULL")
						}
						return strings.Join(inners, " AND "), params, nil
					},
				})
			}
			final := selectStatementNode(f.Field, nodes, "_objects", false)
			final.Name = "_" + f.Field.Alias + "_data_final"
			withNodes.Add(final)
			unions.Add(&QueryPlanNode{
				Name:  "_" + f.Field.Alias + "_data_final",
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_" + f.Field.Alias + "_data_final", params, nil
				},
			})
			fieldNodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					if subCount == 1 {
						return "_objects." + engines.Ident(node.Name), params, nil
					}
					return "ANY_VALUE(_objects." + node.Name + ") AS " + engines.Ident(node.Name), params, nil
				},
			})
		case base.DistributionFieldName, base.BucketDistributionFieldName:
			// create sub query nodes for distribution by
			dn, err := h3distributionByNode(ctx, defs, planner, query, f.Field, vars)
			if err != nil {
				return nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field '%s' distribution by: %v", query.Alias, f.Field.Alias, err)
			}
			dn.Name = "_" + f.Field.Alias + "_data"
			withNodes.Add(dn)
			// add final data node that will return struct and NULL for other fields
			var nodes, fields QueryPlanNodes
			fields.Add(&QueryPlanNode{
				Name: "_h3_cell",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects._h3_cell", params, nil
				},
			})
			for _, sub := range engines.SelectedFields(query.SelectionSet) {
				if sub.Field.Name != base.H3DataFieldName &&
					sub.Field.Name != base.DistributionFieldName &&
					sub.Field.Name != base.BucketDistributionFieldName {
					continue
				}
				if sub.Field.Alias == f.Field.Alias {
					fields.Add(&QueryPlanNode{
						Name: f.Field.Alias,
						CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
							object := map[string]string{}
							for _, sf := range engines.SelectedFields(f.Field.SelectionSet) {
								object[sf.Field.Alias] = "_objects." + engines.Ident(sf.Field.Name)
							}
							if f.Field.Definition.Type.NamedType != "" {
								return defaultEngine.MakeObject(object) + " AS " + engines.Ident(node.Name), params, nil
							}
							return "LIST(" + defaultEngine.MakeObject(object) + ") AS " + engines.Ident(node.Name), params, nil
						},
					})
					continue
				}
				fields.Add(&QueryPlanNode{
					Name:  sub.Field.Alias,
					Query: sub.Field,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return "NULL AS " + engines.Ident(node.Name), params, nil
					},
				})
			}
			nodes.Add(fieldsNode(f.Field, fields))
			nodes.Add(&QueryPlanNode{
				Name: "from",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_" + f.Field.Alias + "_data", params, nil
				},
			})
			if f.Field.Definition.Type.NamedType == "" {
				nodes.Add(&QueryPlanNode{
					Name:  "groupBy",
					Query: f.Field,
					CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
						return "_objects._h3_cell", params, nil
					},
				})
			}
			final := selectStatementNode(f.Field, nodes, "_objects", false)
			final.Name = "_" + f.Field.Alias + "_data_final"
			withNodes.Add(final)
			unions.Add(&QueryPlanNode{
				Name:  "_" + f.Field.Alias + "_data_final",
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_" + f.Field.Alias + "_data_final", params, nil
				},
			})
			fieldNodes.Add(&QueryPlanNode{
				Name:  f.Field.Alias,
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					if subCount == 1 {
						return "_objects." + node.Name, params, nil
					}
					return "ANY_VALUE(_objects." + node.Name + ") AS " + engines.Ident(node.Name), params, nil
				},
			})
		}
	}

	// 3. collect results
	var nodes QueryPlanNodes
	// with nodes
	nodes.Add(withNode(withNodes))
	nodes.Add(fieldsNode(query, fieldNodes))
	nodes.Add(&QueryPlanNode{
		Name:  "from",
		Query: query,
		Nodes: unions,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 1 {
				return children[0].Result, params, nil
			}
			var unions []string
			for _, union := range children {
				unions = append(unions, "FROM "+union.Result)
			}
			sql := strings.Join(unions, " UNION ")
			return "(" + sql + ")", params, nil
		},
	})
	if len(unions) > 1 {
		nodes.Add(&QueryPlanNode{
			Name:  "groupBy",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects._h3_cell", params, nil
			},
		})
	}
	return selectStatementNode(query, nodes, "_objects", false), nil
}

// h3DataSubQueryNodes creates sub query nodes for H3 data queries.
// create query nodes to use in WITH clause for H3 data queries:
// - "_" + query.Alias + "_" + f.Field.Alias + "_data" - for each data query
// - "_" + query.Alias + "_" + f.Field.Alias + "_data_final" - for the final result of each data query
// - "_" + query.Alias + "_data" - for the final result for the data field
func h3DataSubQueryNodes(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, query *ast.Field, res int, vars map[string]any) (out QueryPlanNodes, inner []string, err error) {
	// 1. create sub query nodes for each data field with H3 cell (split or/and aggregate query)
	if query == nil || len(query.SelectionSet) == 0 {
		return nil, nil, ErrInternalPlanner
	}

	var unions, fields QueryPlanNodes
	ss := engines.SelectedFields(query.SelectionSet)
	for _, f := range ss {
		am := f.Field.ArgumentMap(vars)
		if len(am) == 0 {
			return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' must contain field arguments", query.Alias)
		}
		v, ok := am["field"]
		if !ok {
			return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' must contain 'field' argument", query.Alias)
		}
		baseFieldName, ok := v.(string)
		if !ok || baseFieldName == "" {
			return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' argument 'field' must be a string", query.Alias)
		}
		v, ok = am["divide_values"]
		if !ok {
			v = false
		}
		divideVals, _ := v.(bool)
		v, ok = am["transform_from"]
		if !ok {
			v = int64(0)
		}
		transformFrom, _ := v.(int64)
		v, ok = am["buffer"]
		if !ok {
			v = float64(0)
		}
		buffer, _ := v.(float64)
		v, ok = am["simplify"]
		if !ok {
			v = false
		}
		simplify, _ := v.(bool)

		f.Field.Directives = append(f.Field.Directives, base.AddH3Directive(baseFieldName, res, int(transformFrom), buffer, divideVals, simplify, f.Field.Position))
		var baseNode *QueryPlanNode
		switch {
		case compiler.IsAggregateQuery(f.Field) || compiler.IsBucketAggregateQuery(f.Field):
			baseNode, _, err = aggregateDataNode(ctx, defs, planner, true, f.Field, vars)
			if err != nil {
				return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' aggregate sub query: %v", query.Alias, err)
			}
		case compiler.IsSelectQuery(f.Field) || compiler.IsSelectOneQuery(f.Field):
			baseNode, _, err = selectDataObjectNode(ctx, defs, planner, f.Field, vars)
			if err != nil {
				return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' field 'data' select sub query: %v", query.Alias, err)
			}
		default:
			return nil, nil, compiler.ErrorPosf(f.Field.Position, "H3 query '%s' unsupported query type for 'data' field: %s", query.Alias, f.Field.Name)
		}
		baseNode.Name = "_" + query.Alias + "_" + f.Field.Alias + "_data"
		out.Add(baseNode)
		// create final node
		var nodes QueryPlanNodes
		// fields
		nodes.Add(&QueryPlanNode{
			Name:  "fields",
			Query: f.Field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				var fields []string
				// 1. h3 cell
				fields = append(fields, "_objects._h3_cell")
				// 2. query fields packed into struct
				selectedField := map[string]string{}
				for _, sf := range engines.SelectedFields(f.Field.SelectionSet) {
					selectedField[sf.Field.Alias] = "_objects." + engines.Ident(sf.Field.Alias)
				}
				sql := defaultEngine.MakeObject(selectedField)
				// 3. add other subquery results with NULL values to combine with UNION ALL
				for _, sub := range engines.SelectedFields(query.SelectionSet) {
					if sub.Field.Alias == f.Field.Alias {
						if f.Field.Definition.Type.NamedType != "" {
							fields = append(fields, sql+" AS "+engines.Ident(f.Field.Alias))
						}
						if f.Field.Definition.Type.NamedType == "" {
							fields = append(fields, "LIST("+sql+") AS "+engines.Ident(f.Field.Alias))
						}
						continue
					}
					fields = append(fields, "NULL AS "+engines.Ident(sub.Field.Alias))
				}
				return strings.Join(fields, ", "), params, nil
			},
		})

		nodes.Add(&QueryPlanNode{
			Name:  "from",
			Query: f.Field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return baseNode.Name, params, nil
			},
		})

		if f.Field.Definition.Type.NamedType == "" {
			nodes.Add(&QueryPlanNode{
				Name:  "groupBy",
				Query: f.Field,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "_objects._h3_cell", params, nil
				},
			})
		}
		final := selectStatementNode(f.Field, nodes, "_objects", false)
		final.Name = baseNode.Name + "_final"
		out.Add(final)
		unions.Add(&QueryPlanNode{
			Name:  baseNode.Name + "_final",
			Query: f.Field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return baseNode.Name + "_final", params, nil
			},
		})
		if isInner, ok := am["inner"].(bool); ok && isInner {
			inner = append(inner, f.Field.Alias)
		}
		fields.Add(&QueryPlanNode{
			Name:  f.Field.Alias,
			Query: f.Field,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				if len(ss) == 1 {
					return "_objects." + engines.Ident(node.Query.Alias), params, nil
				}
				return "ANY_VALUE(_objects." + engines.Ident(node.Query.Alias) + ")", params, nil
			},
		})
	}
	// 2. create sub query node that combine base nodes into final result
	// this will union across all data fields
	var nodes QueryPlanNodes
	nodes.Add(&QueryPlanNode{
		Name:  "fields",
		Query: query,
		Nodes: fields,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var fields []string
			fields = append(fields, "_objects._h3_cell")
			selected := map[string]string{}
			for _, child := range children {
				selected[child.Name] = child.Result
			}
			fields = append(fields, defaultEngine.MakeObject(selected)+" AS "+node.Query.Alias)
			return strings.Join(fields, ", "), params, nil
		},
	})
	nodes.Add(&QueryPlanNode{
		Name:  "from",
		Query: query,
		Nodes: unions,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if len(children) == 1 {
				return children[0].Result, params, nil
			}
			var unions []string
			for _, union := range children {
				unions = append(unions, "FROM "+union.Result)
			}
			sql := strings.Join(unions, " UNION ")
			return "(" + sql + ")", params, nil
		},
	})
	if len(unions) > 1 {
		nodes.Add(&QueryPlanNode{
			Name:  "groupBy",
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "_objects._h3_cell", params, nil
			},
		})
	}

	final := selectStatementNode(query, nodes, "_objects", false)
	final.Name = "_" + query.Alias + "_data"
	out.Add(final)

	return out, inner, nil
}

// h3distributionByNode creates a sub query nodes for H3 distribution by query.
func h3distributionByNode(ctx context.Context, defs compiler.DefinitionsSource, planner Catalog, h3Query, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	if h3Query == nil || query == nil {
		return nil, ErrInternalPlanner
	}

	dp, err := h3distributionByParams(h3Query, query, vars)
	if err != nil {
		return nil, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' distribution by parameters: %v", h3Query.Alias, query.Alias, err)
	}
	if dp.numeratorQuery == nil || dp.denominatorQuery == nil {
		return nil, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' distribution by numerator and denominator queries must be specified", h3Query.Alias, query.Alias)
	}

	numeratorAlias := "_" + strings.ReplaceAll(dp.numeratorQueryPath, ".", "_") + "_data"
	denominatorAlias := "_" + strings.ReplaceAll(dp.denominatorQueryPath, ".", "_") + "_data"

	// fields nodes for distribution by
	var fields QueryPlanNodes
	fields.Add(&QueryPlanNode{
		Name: "_h3_cell",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return "_denominator._h3_cell", params, nil
		},
	})
	calcFields := []string{"numerator", "denominator", "numerator_key", "denominator_key", "denominator_total", "ratio", "value"}
	if !dp.isNumeratorBucket {
		calcFields = slices.DeleteFunc(calcFields, func(fn string) bool {
			return fn == "numerator_key"
		})
	}
	if !dp.isDenominatorBucket {
		calcFields = slices.DeleteFunc(calcFields, func(fn string) bool {
			return fn == "denominator_key"
		})
	}
	for _, fn := range calcFields {
		fields.Add(&QueryPlanNode{
			Name: fn,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				switch fn {
				case "numerator":
					return defaultEngine.ExtractNestedTypedValue("_numerator."+dp.numeratorField, dp.numeratorValuePath, "number") + " AS numerator", params, nil
				case "numerator_key":
					return "_numerator." + dp.numeratorKey + " AS numerator_key", params, nil
				case "denominator_key":
					return "_denominator." + dp.denominatorKey + " AS denominator_key", params, nil
				case "denominator":
					return defaultEngine.ExtractNestedTypedValue("_denominator."+dp.denominatorField, dp.denominatorValuePath, "number") + " AS denominator", params, nil
				case "denominator_total":
					var partitions []string
					if dp.isDenominatorBucket {
						partitions = append(partitions, "denominator_key")
					}
					if dp.isNumeratorBucket {
						partitions = append(partitions, "numerator_key")
					}
					if len(partitions) != 0 {
						return "SUM(denominator) OVER (PARTITION BY " + strings.Join(partitions, ", ") + ") AS denominator_total", params, nil
					}
					return "SUM(denominator) OVER () AS denominator_total", params, nil
				case "ratio":
					return "denominator / denominator_total AS ratio", params, nil
				case "value":
					return "numerator * ratio AS value", params, nil
				}
				return "_denominator._selected", params, nil
			},
		})
	}

	var nodes QueryPlanNodes
	// fields node
	nodes.Add(fieldsNode(query, fields))
	nodes.Add(&QueryPlanNode{
		Name:  "from",
		Query: query,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			return denominatorAlias, params, nil
		},
	})
	nodes.Add(&QueryPlanNode{
		Name:  "joins",
		Query: query,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			if dp.numeratorInner {
				return "INNER JOIN " + numeratorAlias + " AS _numerator ON _denominator._h3_cell = _numerator._h3_cell", params, nil
			}
			return "LEFT JOIN " + numeratorAlias + " AS _numerator ON _denominator._h3_cell = _numerator._h3_cell", params, nil
		},
	})
	return selectStatementNode(query, nodes, "_denominator", false), nil
}

type h3DistributionByParams struct {
	numerator            string     // path to numerator aggregation value
	denominator          string     // path to denominator aggregation value
	numeratorQuery       *ast.Field // query for numerator aggregation
	numeratorQueryPath   string     // path to numerator aggregation query in the H3 query
	numeratorField       string
	numeratorValuePath   string     // path to numerator value in the numerator aggregation
	numeratorInner       bool       // true if numerator is an inner aggregation (e.g. for H3 cell)
	denominatorQuery     *ast.Field // query for denominator aggregation
	denominatorQueryPath string     // path to denominator aggregation query in the H3 query
	denominatorField     string
	denominatorValuePath string // path to denominator value in the denominator aggregation

	isNumeratorBucket   bool // true if numerator is a bucket aggregation
	isDenominatorBucket bool // true if denominator is a bucket aggregation
	// NumeratorKey is the key in the numerator bucket aggregation, if numerator is a bucket aggregation.
	numeratorKey string // path to numerator key in the numerator bucket aggregation
	// DenominatorKey is the key in the denominator bucket aggregation, if denominator is a bucket aggregation.
	denominatorKey string // path to denominator key in the denominator bucket aggregation
}

func h3distributionByParams(h3Query, query *ast.Field, vars map[string]any) (params h3DistributionByParams, err error) {
	if h3Query == nil || query == nil {
		return params, ErrInternalPlanner
	}

	// define parameters for distribution by query
	am := query.ArgumentMap(vars)
	if len(am) == 0 {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' must contain arguments", h3Query.Alias, query.Alias)
	}
	v, ok := am["numerator"]
	if !ok {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' must contain 'numerator' argument", h3Query.Alias, query.Alias)
	}
	numerator, ok := v.(string)
	if !ok || numerator == "" {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'numerator' must be a string", h3Query.Alias, query.Alias)
	}
	v, ok = am["denominator"]
	if !ok {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' must contain 'denominator' argument", h3Query.Alias, query.Alias)
	}
	denominator, ok := v.(string)
	if !ok || denominator == "" {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'denominator' must be a string", h3Query.Alias, query.Alias)
	}

	// find numerator and denominator aggregation queries
	ss := engines.SelectedFields(h3Query.SelectionSet)
	numeratorQuery, numeratorPath := pathToAggregationQuery(ss, numerator, []string{base.H3DataFieldName})
	if numeratorQuery == nil ||
		!compiler.IsAggregateQuery(numeratorQuery.Field) &&
			!compiler.IsBucketAggregateQuery(numeratorQuery.Field) {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'numerator' must point to the aggregation value in the 'data' field", h3Query.Alias, query.Alias)
	}
	na := numeratorQuery.Field.ArgumentMap(vars)
	params.numeratorInner = len(na) != 0 && na["inner"] != nil && na["inner"].(bool)
	params.numeratorField = strings.TrimPrefix(numerator, numeratorPath+".")
	if pp := strings.SplitN(params.numeratorField, ".", 2); len(pp) > 1 {
		params.numeratorField = pp[0]
		params.numeratorValuePath = pp[1]
	}
	params.numeratorQuery = numeratorQuery.Field
	params.numeratorQueryPath = numeratorPath

	denominatorQuery, denominatorPath := pathToAggregationQuery(ss, denominator, []string{base.H3DataFieldName})
	if denominatorQuery == nil ||
		!compiler.IsAggregateQuery(denominatorQuery.Field) &&
			!compiler.IsBucketAggregateQuery(denominatorQuery.Field) {
		return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'denominator' must point to the aggregation value in the 'data' field", h3Query.Alias, query.Alias)
	}
	params.denominatorField = strings.TrimPrefix(denominator, denominatorPath+".")
	if pp := strings.SplitN(params.denominatorField, ".", 2); len(pp) > 1 {
		params.denominatorField = pp[0]
		params.denominatorValuePath = pp[1]
	}
	params.denominatorQuery = denominatorQuery.Field
	params.denominatorQueryPath = denominatorPath

	// check if numerator and denominator are bucket aggregations
	params.isNumeratorBucket = compiler.IsBucketAggregateQuery(numeratorQuery.Field)
	if params.isNumeratorBucket {
		// get key path for numerator bucket aggregation
		v, ok := am["numerator_key"]
		if !ok {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' must contain 'numerator_key' argument for bucket aggregation", h3Query.Alias, query.Alias)
		}
		params.numeratorKey, ok = v.(string)
		if !ok || params.numeratorKey == "" {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'numerator_key' must be a string", h3Query.Alias, query.Alias)
		}
		params.numeratorKey = strings.TrimPrefix(params.numeratorKey, numeratorPath+".")
		if params.numeratorKey == "" || strings.Contains(params.numeratorKey, ".") {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'numerator_key' must be a non-empty string", h3Query.Alias, query.Alias)
		}
	}
	params.isDenominatorBucket = compiler.IsBucketAggregateQuery(denominatorQuery.Field)
	if params.isDenominatorBucket {
		// get key path for denominator bucket aggregation
		v, ok := am["denominator_key"]
		if !ok {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' must contain 'denominator_key' argument for bucket aggregation", h3Query.Alias, query.Alias)
		}
		params.denominatorKey, ok = v.(string)
		if !ok || params.denominatorKey == "" {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'denominator_key' must be a string", h3Query.Alias, query.Alias)
		}
		params.denominatorKey = strings.TrimPrefix(params.denominatorKey, denominatorPath+".")
		if params.denominatorKey == "" || strings.Contains(params.denominatorKey, ".") {
			return params, compiler.ErrorPosf(query.Position, "H3 query '%s' field '%s' argument 'denominator_key' must be a non-empty string", h3Query.Alias, query.Alias)
		}
	}
	return params, nil
}

// validateH3Query validates the H3 query.
// It checks that the query has a resolution argument, at least one data field,
// and validates the distribution by and distribution by bucket fields if present.
func validateH3Query(query *ast.Field, vars map[string]any) (count int, err error) {
	if query == nil {
		return 0, ErrInternalPlanner
	}
	// check resolution argument
	am := query.ArgumentMap(vars)
	if len(am) == 0 {
		return 0, compiler.ErrorPosf(query.Position, "H3 query must contain 'resolution' argument")
	}
	ri, ok := am["resolution"]
	if !ok {
		return 0, compiler.ErrorPosf(query.Position, "H3 query must contain 'resolution' argument")
	}
	res, ok := ri.(int64)
	if !ok || res < 0 || res > 15 {
		return 0, compiler.ErrorPosf(query.Position, "H3 query 'resolution' argument must be an integer between 0 and 15")
	}

	// check at least one data field
	ss := engines.SelectedFields(query.SelectionSet)
	if ss.ForName(base.H3DataFieldName) == nil {
		return 0, compiler.ErrorPosf(query.Position, "H3 query must contain at least one 'data' field")
	}
	count = len(ss)

	// check distribution by and distribution by bucket fields
	var errs gqlerror.List
	for _, f := range ss {
		switch f.Field.Name {
		case base.DistributionFieldName:
			err = validateDistributionByField(query, f.Field, []string{base.H3DataFieldName}, vars)
		case base.BucketDistributionFieldName:
			err = validateDistributionByBucketField(query, f.Field, []string{base.H3DataFieldName}, vars)
		case base.H3DataFieldName:
		default:
			count--
		}
		if err != nil {
			errs = append(errs, gqlerror.Wrap(err))
			err = nil
		}
	}
	if len(errs) > 0 {
		return 0, errs
	}
	return count, nil
}

// validateDistributionByField validates the distribution by field in an H3 query.
// It checks that the field is of type "_distribution_by" and that the arguments are
// correctly specified. It also validates the numerator and denominator paths against the source fields.
// It returns an error if the validation fails.
func validateDistributionByField(query, field *ast.Field, sourceFields []string, vars map[string]any) error {
	if query == nil {
		return ErrInternalPlanner
	}
	if field == nil ||
		field.Definition.Type.Name() != base.DistributionTypeName {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' type must be '%s'", field.Alias, query.Alias, base.DistributionTypeName)
	}
	am := field.ArgumentMap(vars)
	if len(am) == 0 {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain arguments", field.Alias, query.Alias)
	}
	v, ok := am["numerator"]
	if !ok {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'numerator' argument", field.Alias, query.Alias)
	}
	numerator, ok := v.(string)
	if !ok || numerator == "" {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument must be a string", field.Alias, query.Alias)
	}
	ss := engines.SelectedFields(query.SelectionSet)
	nq, np := pathToAggregationQuery(ss, numerator, sourceFields)
	if nq == nil || !compiler.IsAggregateQuery(nq.Field) {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument path '%s' must point to the aggregation value in the '%v' field", field.Alias, query.Alias, numerator, sourceFields)
	}
	numeratorPath := strings.TrimPrefix(numerator, np+".")
	err := checkDistributedByPath(numeratorPath, "", nq.Field)
	if err != nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument path '%s': %v", field.Alias, query.Alias, numeratorPath, err)
	}

	v, ok = am["denominator"]
	if !ok {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'denominator' argument", field.Alias, query.Alias)
	}
	denominator, ok := v.(string)
	if !ok || denominator == "" {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument must be a string", field.Alias, query.Alias)
	}
	dq, dp := pathToAggregationQuery(ss, denominator, sourceFields)
	if dq == nil || !compiler.IsAggregateQuery(dq.Field) {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument path '%s' must point to the aggregation value in the '%v' field", field.Alias, query.Alias, denominator, sourceFields)
	}
	denominatorPath := strings.TrimPrefix(denominator, dp+".")

	err = checkDistributedByPath(denominatorPath, "", dq.Field)
	if err != nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument path '%s': %v", field.Alias, query.Alias, denominatorPath, err)
	}

	if numeratorPath == denominatorPath {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' and 'denominator' argument paths must not be the same", field.Alias, query.Alias)
	}
	if !compiler.IsAggregateQuery(nq.Field) || !compiler.IsAggregateQuery(dq.Field) {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' or 'denominator' argument paths must point to aggregate queries", field.Alias, query.Alias)
	}
	return nil
}

// distributionByBucketField validates the distribution by bucket field in an H3 query.
// It checks that the field is of type "_distribution_by_bucket" and that the arguments are
// correctly specified. It also validates the numerator and denominator paths against the source fields.
//
// It returns an error if the validation fails.
//  1. Numerator and/or denominator must point to a aggregated value in the bucket aggregation query.
//  2. If numerator points to a bucket aggregation, numerator_key must point to the key in the numerator bucket aggregation.
//  3. If denominator points to a bucket aggregation, denominator_key must point to the key
//     in the denominator bucket aggregation.
//  4. Numerator and denominator must be valid paths in the source fields.
//  5. Numerator and denominator must be of type Float or Int.
func validateDistributionByBucketField(query, field *ast.Field, sourceFields []string, vars map[string]any) error {
	if query == nil {
		return ErrInternalPlanner
	}
	if field == nil ||
		field.Definition.Type.Name() != base.BucketDistributionTypeName {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' type must be '%s'", field.Alias, query.Alias, base.BucketDistributionTypeName)
	}
	am := field.ArgumentMap(vars)
	if len(am) == 0 {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain arguments", field.Alias, query.Alias)
	}
	// 1. numerator
	v, ok := am["numerator"]
	if !ok {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'numerator' argument", field.Alias, query.Alias)
	}
	numerator, ok := v.(string)
	if !ok || numerator == "" {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument must be a string", field.Alias, query.Alias)
	}
	// check numerator path
	ss := engines.SelectedFields(query.SelectionSet)
	nq, np := pathToAggregationQuery(ss, numerator, sourceFields)
	if nq == nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument path '%s' must point to the aggregation value in the '%v' field", field.Alias, query.Alias, numerator, sourceFields)
	}
	numeratorPath := strings.TrimPrefix(numerator, np+".")
	numeratorKeyPath := ""

	if compiler.IsBucketAggregateQuery(nq.Field) {
		v = am["numerator_key"]
		if v == nil {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'numerator_key' argument", field.Alias, query.Alias)
		}
		numeratorKey, ok := v.(string)
		if !ok || numeratorKey == "" {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator_key' argument must be a string", field.Alias, query.Alias)
		}
		if !strings.HasPrefix(numeratorKey, np+".") {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator_key' argument path '%s' must point to the key in the numerator bucket aggregation", field.Alias, query.Alias, numeratorKey)
		}
		numeratorKeyPath = strings.TrimPrefix(numeratorKey, np+".")
	}
	err := checkDistributedByPath(numeratorPath, numeratorKeyPath, nq.Field)
	if err != nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' argument path '%s': %v", field.Alias, query.Alias, numeratorPath, err)
	}

	// 2. denominator
	v, ok = am["denominator"]
	if !ok {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'denominator' argument", field.Alias, query.Alias)
	}
	denominator, ok := v.(string)
	if !ok || denominator == "" {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument must be a string", field.Alias, query.Alias)
	}
	// check denominator path
	dq, dp := pathToAggregationQuery(ss, denominator, sourceFields)
	if dq == nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument path '%s' must point to the aggregation value in the '%v' field", field.Alias, query.Alias, denominator, sourceFields)
	}
	denominatorPath := strings.TrimPrefix(denominator, dp+".")
	denominatorKeyPath := ""
	if compiler.IsBucketAggregateQuery(dq.Field) {
		v = am["denominator_key"]
		if v == nil {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field must contain 'denominator_key' argument", field.Alias, query.Alias)
		}
		denominatorKey, ok := v.(string)
		if !ok || denominatorKey == "" {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator_key' argument must be a string", field.Alias, query.Alias)
		}
		if !strings.HasPrefix(denominatorKey, dp+".") {
			return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator_key' argument path '%s' must point to the key in the denominator bucket aggregation",
				field.Alias, query.Alias, denominatorKey)
		}
		denominatorKeyPath = strings.TrimPrefix(denominatorKey, dp+".")
	}
	err = checkDistributedByPath(denominatorPath, denominatorKeyPath, dq.Field)
	if err != nil {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'denominator' argument path '%s': %v", field.Alias, query.Alias, denominatorPath, err)
	}
	// check numerator and denominator paths are not the same
	if numeratorPath == denominatorPath && numeratorKeyPath == denominatorKeyPath {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' and 'denominator' argument paths must not be the same", field.Alias, query.Alias)
	}
	if !compiler.IsBucketAggregateQuery(nq.Field) &&
		!compiler.IsBucketAggregateQuery(dq.Field) {
		return compiler.ErrorPosf(field.Position, "%s H3 query '%s' field 'numerator' or 'denominator' argument paths must point to bucket aggregations", field.Alias, query.Alias)
	}

	return nil
}

// checkDistributedByPath checks if the given value and key paths are valid in the context of the query.
// 1. If bucket aggregation is used, the key must be present in the bucket aggregation.
// 2. Value must point to an aggregated value in the aggregation query or bucket aggregation.
func checkDistributedByPath(value, key string, query *ast.Field) error {
	if value == "" {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path must not be empty", query.Alias)
	}
	if compiler.IsAggregateQuery(query) {
		if key != "" {
			return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must not contain a key, only value", query.Alias, value)
		}
		vf := engines.SelectedFields(query.SelectionSet).ForPath(value)
		if vf == nil {
			return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' not found in the aggregation query", query.Alias, value)
		}
		if len(vf.Field.SelectionSet) != 0 {
			return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must point to an aggregated value, not a selection set", query.Alias, value)
		}
		if vf.Field.Definition.Type.NamedType == "" ||
			vf.Field.Definition.Type.NamedType != "Float" &&
				vf.Field.Definition.Type.NamedType != "Int" &&
				vf.Field.Definition.Type.NamedType != "BigInt" {
			return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must be a Float, BigInt or Int", query.Alias, value)
		}
		return nil
	}

	if !compiler.IsBucketAggregateQuery(query) {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must point to an aggregate query or bucket aggregation", query.Alias, value)
	}
	vf := engines.SelectedFields(query.SelectionSet).ForPath(value)
	if vf == nil {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' not found in the bucket aggregation", query.Alias, value)
	}
	if vf.Field.Definition.Type.NamedType == "" ||
		vf.Field.Definition.Type.NamedType != "Float" &&
			vf.Field.Definition.Type.NamedType != "Int" &&
			vf.Field.Definition.Type.NamedType != "BigInt" {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must be a Float, BigInt or Int", query.Alias, value)
	}
	vf = engines.SelectedFields(query.SelectionSet).ForName(strings.SplitN(value, ".", 2)[0])
	if vf == nil || vf.Field.Definition.Name != compiler.AggregateFieldName {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must point to an aggregated value in the bucket aggregation", query.Alias, value)
	}
	if key == "" || strings.Contains(key, ".") {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' must contain a key in the bucket aggregation", query.Alias, value)
	}
	kf := engines.SelectedFields(query.SelectionSet).ForPath(key)
	if kf == nil || kf.Field.Name != compiler.AggregateKeyFieldName {
		return compiler.ErrorPosf(query.Position, "H3 query '%s' field 'numerator' or 'denominator' argument path '%s' key not found in the bucket aggregation", query.Alias, key)
	}

	return nil
}

// pathToAggregationQuery finds the aggregation query in the selection set by the given path.
// It returns the query and the path to the query in the selection set.
func pathToAggregationQuery(ss engines.SelectionSet, path string, sourceFields []string) (query *engines.SelectedField, queryPath string) {
	if len(ss) == 0 || path == "" {
		return nil, ""
	}
	pp := strings.SplitN(path, ".", 2)
	if len(sourceFields) != 0 && !slices.Contains(sourceFields, pp[0]) {
		return nil, ""
	}
	query = ss.ForPath(pp[0])
	if query == nil {
		return nil, ""
	}

	if compiler.IsAggregateQuery(query.Field) || compiler.IsBucketAggregateQuery(query.Field) {
		return query, pp[0]
	}

	if len(pp) == 1 || len(query.Field.SelectionSet) == 0 {
		return nil, ""
	}

	qp, path := pathToAggregationQuery(engines.SelectedFields(query.Field.SelectionSet), pp[1], nil)
	if qp == nil {
		return nil, ""
	}
	return qp, pp[0] + "." + path
}
