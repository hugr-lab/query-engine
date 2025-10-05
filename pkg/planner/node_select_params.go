package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"gopkg.in/errgo.v2/fmt/errors"
)

func selectQueryParamsNodes(ctx context.Context, defs compiler.DefinitionsSource, e engines.Engine, info *compiler.Object, prefix string, query *ast.Field, args compiler.FieldQueryArguments, byAlias bool) (nodes QueryPlanNodes, err error) {
	filter := args.ForName("filter")
	limit := args.ForName("limit")
	offset := args.ForName("offset")
	distinctOn := args.ForName("distinct_on")
	orderBy := args.ForName("order_by")
	similarity := args.ForName(base.SimilaritySearchArgumentName)
	semantic := args.ForName(base.SemanticSearchArgumentName)
	if info != nil {
		selectDeleted := info.SoftDelete && query.Directives.ForName(base.WithDeletedDirectiveName) != nil
		if filter == nil && !selectDeleted && info.SoftDelete {
			nodes = append(nodes, &QueryPlanNode{
				Name: "where",
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return info.SoftDeleteCondition(prefix), params, nil
				},
			})
		}
		if filter != nil {
			filterVals := filter.Value.(map[string]any)
			whereNode, err := whereNode(ctx, defs, info, filterVals, prefix, byAlias, selectDeleted)
			if err != nil {
				return nil, err
			}
			if whereNode != nil {
				nodes = append(nodes, whereNode)
			}
		}
	}
	if limit != nil {
		nodes = append(nodes, &QueryPlanNode{
			Name: "limit",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				l := limit.Value.(int64)
				return fmt.Sprintf("%d", l), params, nil
			},
		})
	}
	if offset != nil {
		nodes = append(nodes, &QueryPlanNode{
			Name: "offset",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				l := offset.Value.(int64)
				return fmt.Sprintf("%d", l), params, nil
			},
		})
	}
	if distinctOn != nil {
		node, err := distinctOnNode(info, query, prefix, distinctOn.Value, byAlias)
		if err != nil {
			return nil, err
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	if orderBy != nil {
		var dv any
		if distinctOn != nil {
			dv = distinctOn.Value
		}
		node, err := orderByNode(e, info, query, prefix, orderBy.Value, dv, byAlias)
		if err != nil {
			return nil, err
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	if semantic != nil && similarity != nil {
		return nil, compiler.ErrorPosf(query.Position, "only one of semantic or similarity search can be specified")
	}
	// add vector search nodes (vector limit and vector order)
	if similarity != nil {
		simNodes, err := vectorSearchNodes(e, info, query, prefix, similarity.Value)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, simNodes...)
	}
	// add semantic search nodes (vector limit and vector order)
	if semantic != nil {
		semNodes, err := semanticSearchNodes(e, info, query, prefix, semantic.Value)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, semNodes...)
	}
	return nodes, nil
}

func selectOneQueryParamsNodes(ctx context.Context, info *compiler.Object, query *ast.Field, args compiler.FieldQueryArguments, prefix string) (QueryPlanNodes, error) {
	nodes := QueryPlanNodes{
		{
			Name:    "limit",
			Comment: "limit 1",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return "1", params, nil
			},
		},
	}
	selectDeleted := info.SoftDelete && query.Directives.ForName(base.WithDeletedDirectiveName) != nil
	where, err := whereUniqueNode(ctx, info, args, prefix, selectDeleted)
	if err != nil {
		return nil, err
	}
	if where != nil {
		nodes = append(nodes, where)
	}
	return nodes, nil
}

// selectStatementNode creates a select statement node
// accepts children nodes: distinctOn, fields, from, joins, where, groupBy, orderBy, limit, offset, with
func selectStatementNode(query *ast.Field, nodes QueryPlanNodes, alias string, addRowNum bool) *QueryPlanNode {
	return &QueryPlanNode{
		Name:    query.Name,
		Query:   query,
		Comment: "select data object",
		Nodes:   nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			sql := "SELECT "
			distinctOn := children.ForName("distinctOn")
			if distinctOn != nil {
				sql += "DISTINCT ON (" + distinctOn.Result + ") "
			}
			if addRowNum {
				sql += "row_number() OVER () AS _row_num, "
			}
			fields := children.ForName("fields")
			if fields == nil {
				return "", nil, errors.New("fields definition is required")
			}
			simOrderBy := children.ForName(vectorDistanceNodeName)
			if fields.Result != "" {
				sql += fields.Result
			}
			if fields.Result == "" {
				sql += "1"
			}
			from := children.ForName("from")
			if from == nil {
				return "", nil, errors.New("from definition is required")
			}
			sql += " FROM " + from.Result
			if alias != "" {
				sql += " AS " + alias
			}
			joins := children.ForName("joins")
			if joins != nil {
				sql += " " + joins.Result
			}
			where := children.ForName("where")
			perm := children.ForName("permission_filter")
			whereSQL := ""
			if where != nil {
				whereSQL = where.Result
			}
			if perm != nil {
				if whereSQL != "" {
					whereSQL += " AND "
				}
				whereSQL += perm.Result
			}
			if whereSQL != "" {
				sql += " WHERE " + whereSQL
			}
			groupBy := children.ForName("groupBy")
			if groupBy != nil {
				sql += " GROUP BY " + groupBy.Result
			}
			if simOrderBy != nil {
				sql += " ORDER BY " + simOrderBy.Result
			}
			// use vector order by to add as a first element
			orderBy := children.ForName("orderBy")
			if orderBy != nil && simOrderBy == nil {
				sql += " ORDER BY " + orderBy.Result
			}
			// use vector limit instead of regular limit if exists
			simVectorLimit := children.ForName(vectorSearchLimitNodeName)
			if simVectorLimit != nil {
				sql += " LIMIT " + simVectorLimit.Result
			}
			limit := children.ForName("limit")
			if limit != nil && simVectorLimit == nil {
				sql += " LIMIT " + limit.Result
			}
			offset := children.ForName("offset")
			if offset != nil && limit != nil && simVectorLimit == nil {
				sql += " OFFSET " + offset.Result
			}
			with := children.ForName("with")
			if with != nil {
				sql = with.Result + " " + sql
			}
			return sql, params, nil
		},
	}
}

type orderByField struct {
	field string
	desc  bool
}

func parseOrderBy(param any) (orderByField, error) {
	var item orderByField
	vm, ok := param.(map[string]any)
	if !ok {
		return item, errors.New("invalid orderBy value")
	}
	item.field, ok = vm["field"].(string)
	if !ok {
		return item, errors.New("invalid orderBy field")
	}
	if d := vm["direction"]; d != nil {
		item.desc = d.(string) == "DESC"
	}
	return item, nil
}

func parseOrderByArray(param any) ([]orderByField, error) {
	if param == nil {
		return nil, nil
	}
	var orderBy []orderByField
	vv, ok := param.([]any)
	if !ok {
		return nil, errors.New("invalid orderBy value")
	}
	for _, v := range vv {
		item, err := parseOrderBy(v)
		if err != nil {
			return nil, err
		}
		orderBy = append(orderBy, item)
	}
	return orderBy, nil
}

func orderByNode(e engines.Engine, info *compiler.Object, query *ast.Field, prefix string, param, distinctParam any, byAlias bool) (*QueryPlanNode, error) {
	if param == nil {
		return nil, nil
	}
	orderByFields, err := parseOrderByArray(param)
	if err != nil {
		return nil, err
	}

	var nodes QueryPlanNodes
	for _, o := range orderByFields {
		nodes = append(nodes, orderByFieldNode(e, info, query, prefix, o, byAlias))
	}
	if len(nodes) == 0 {
		return nil, nil
	}

	return &QueryPlanNode{
		Name:  "orderBy",
		Query: query,
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, f := range children {
				if f.Result != "" {
					ff = append(ff, f.Result)
				}
			}
			return strings.Join(ff, ", "), params, nil
		},
	}, nil
}

func orderByFieldNode(e engines.Engine, info *compiler.Object, query *ast.Field, prefix string, field orderByField, byAlias bool) *QueryPlanNode {
	return &QueryPlanNode{
		Name:  field.field,
		Query: query,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var fieldName string
			var fieldInfo *compiler.Field
			fieldName = field.field
			sf := engines.SelectedFields(node.Query.SelectionSet)
			queryField := sf.ScalarForPath(fieldName)
			if queryField == nil {
				return "", nil, fmt.Errorf("field %s not found or not sortable", field.field)
			}
			// if nested field, extract real value for ordering
			if strings.Contains(fieldName, ".") {
				// extract real value for ordering
				pp := strings.SplitN(fieldName, ".", 2)
				if prefix != "" {
					pp[0] = prefix + "." + engines.Ident(pp[0])
				}
				t, ok := compiler.FieldJSONTypes[queryField.Field.Definition.Type.Name()]
				if !ok {
					return "", nil, fmt.Errorf("invalid orderBy field %s", fieldName)
				}
				fieldName = e.ExtractNestedTypedValue(pp[0], pp[1], t)
			}
			if !strings.Contains(fieldName, ".") {
				if prefix != "" {
					fieldName = prefix + "." + engines.Ident(fieldName)
				}
				if !byAlias && info != nil {
					if compiler.IsExtraField(queryField.Field.Definition) {
						fieldName = queryField.Field.Alias
					}
					if !compiler.IsExtraField(queryField.Field.Definition) {
						fieldInfo = info.FieldForName(queryField.Field.Name)
						if fieldInfo == nil {
							return "", nil, fmt.Errorf("field %s not found", queryField.Field.Name)
						}
						fieldName = fieldInfo.SQL(prefix)
					}
				}
				if !byAlias && info == nil {
					return "", params, nil
				}
			}
			if !compiler.IsScalarType(queryField.Field.Definition.Type.NamedType) {
				return "", nil, fmt.Errorf("invalid orderBy field %s", fieldName)
			}

			if field.desc {
				return fieldName + " DESC", params, nil
			}
			return fieldName, params, nil
		},
	}
}

func distinctOnNode(info *compiler.Object, query *ast.Field, prefix string, param any, byAlias bool) (*QueryPlanNode, error) {
	if param == nil {
		return nil, nil
	}
	distinctOn, ok := param.([]string)
	if !ok {
		return nil, errors.New("invalid distinctOn value")
	}
	if len(distinctOn) == 0 {
		return nil, nil
	}

	return &QueryPlanNode{
		Name:  "distinctOn",
		Query: query,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			ss := engines.SelectedFields(node.Query.SelectionSet)
			for _, fn := range distinctOn {
				field := ss.ForAlias(fn)
				if field == nil {
					return "", nil, fmt.Errorf("field %s not found", fn)
				}
				if !compiler.IsScalarType(field.Field.Definition.Type.Name()) || field.Field.Definition.Type.NamedType == "" {
					return "", nil, errors.New("invalid distinct on field")
				}
				if byAlias {
					if prefix != "" {
						prefix += "."
					}
					ff = append(ff, prefix+engines.Ident(field.Field.Alias))
					continue
				}
				if info == nil {
					return "", nil, ErrInternalPlanner
				}
				fi := info.FieldForName(field.Field.Name)
				if fi == nil {
					return "", nil, ErrInternalPlanner
				}
				ff = append(ff, fi.SQL(prefix))
			}
			return strings.Join(ff, ", "), params, nil
		},
	}, nil
}

func whereUniqueNode(_ context.Context, info *compiler.Object, filter compiler.FieldQueryArguments, prefix string, selectDeleted bool) (*QueryPlanNode, error) {
	var nodes QueryPlanNodes
	for _, arg := range filter {
		field := info.FieldForName(arg.Name)
		if field == nil {
			return nil, errors.New("field not found in data object")
		}

		nodes = append(nodes, &QueryPlanNode{
			Name: field.Name,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				params = append(params, arg.Value)
				return fmt.Sprintf("%s = $%d", field.SQL(prefix), len(params)), params, nil
			},
		})
	}
	if info.SoftDelete && !selectDeleted {
		nodes = append(nodes, &QueryPlanNode{
			Name: "_soft_delete",
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				return info.SoftDeleteCondition(prefix), params, nil
			},
		})
	}

	return &QueryPlanNode{
		Name:  "where",
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, field := range children {
				ff = append(ff, "("+field.Result+")")
			}
			return strings.Join(ff, " AND "), params, nil
		},
	}, nil
}

func whereNode(ctx context.Context, defs compiler.DefinitionsSource, info *compiler.Object, filter map[string]any, prefix string, byAlias, selectDeleted bool) (*QueryPlanNode, error) {
	if len(filter) == 0 {
		return nil, nil
	}
	// find input definition
	input := defs.ForName(info.InputFilterName())
	if input == nil {
		return nil, errors.New("input filter definition not found")
	}
	var nodes QueryPlanNodes
	// create nodes for input filter
	for fn, fv := range filter {
		if fv == nil {
			continue
		}
		// find field in input filter
		inputField := input.Fields.ForName(fn)
		if inputField == nil {
			return nil, errors.New("field not found in input filter")
		}
		switch {
		case fn == "_not":
			node, err := whereNode(ctx, defs, info, fv.(map[string]any), prefix, byAlias, selectDeleted)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, &QueryPlanNode{
				Name:  fn,
				Nodes: QueryPlanNodes{node},
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					return "NOT (" + children.FirstResult().Result + ")", params, nil
				},
			})
		case fn == "_and" || fn == "_or":
			children, ok := fv.([]any)
			if !ok {
				return nil, fmt.Errorf("invalid value for %s field", fn)
			}
			var andNodes QueryPlanNodes
			for _, v := range children {
				node, err := whereNode(ctx, defs, info, v.(map[string]any), prefix, byAlias, selectDeleted)
				if err != nil {
					return nil, err
				}
				andNodes = append(andNodes, node)
			}
			if len(andNodes) == 0 {
				continue
			}
			nodes = append(nodes, &QueryPlanNode{
				Name:  fn,
				Nodes: andNodes,
				CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
					var ff []string
					for _, and := range children {
						ff = append(ff, "("+and.Result+")")
					}
					if fn == "_and" {
						return strings.Join(ff, " AND "), params, nil
					}
					return strings.Join(ff, " OR "), params, nil
				},
			})
		case compiler.IsReferencesSubquery(inputField):
			node, err := whereReferencesObjectNode(ctx, defs, info, "", prefix, fn, fv.(map[string]any), byAlias, selectDeleted)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		default:
			if info.FieldForName(fn) == nil {
				return nil, errors.New("field not found in input filter")
			}
			node, err := whereFieldNode(info, prefix, fn, fv, byAlias)
			if err != nil {
				return nil, err
			}
			if node != nil {
				nodes = append(nodes, node)
			}
		}
	}

	return &QueryPlanNode{
		Name:    "where",
		Nodes:   nodes,
		Comment: "where",
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			var ff []string
			for _, field := range children {
				ff = append(ff, "("+field.Result+")")
			}
			if info.SoftDelete && !selectDeleted {
				ff = append(ff, info.SoftDeleteCondition(prefix))
			}
			return strings.Join(ff, " AND "), params, nil
		},
	}, nil
}

func whereReferencesObjectNode(ctx context.Context, defs compiler.DefinitionsSource, info *compiler.Object, op, prefix, name string, value map[string]any, byAlias, selectDeleted bool) (*QueryPlanNode, error) {
	if len(value) == 0 {
		return nil, nil
	}
	field := info.FieldForName(name)
	if field == nil {
		return nil, errors.New("field not found in data object")
	}
	if op == "" && field.Definition().Type.NamedType == "" {
		var nodes QueryPlanNodes
		for fn, fv := range value {
			if fv == nil {
				continue
			}
			value, ok := fv.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid value for %s field", fn)
			}
			node, err := whereReferencesObjectNode(ctx, defs, info, fn, prefix, name, value, byAlias, selectDeleted)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
		return &QueryPlanNode{
			Name:  name,
			Nodes: nodes,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				var sql []string
				for _, n := range children {
					sql = append(sql, "("+n.Result+")")
				}
				return strings.Join(sql, " AND "), params, nil
			},
		}, nil
	}
	if op == "" {
		op = "any_of"
	}

	var nodes QueryPlanNodes
	// from node
	def := defs.ForName(field.Definition().Type.Name())
	if def == nil {
		return nil, fmt.Errorf("references object %s not found", field.Definition().Type.Name())
	}
	refObjectInfo := compiler.DataObjectInfo(def)
	if refObjectInfo == nil {
		return nil, ErrInternalPlanner
	}
	nodes = append(nodes, fromDataObjectNode(ctx, refObjectInfo))
	refInfo := info.ReferencesQueryInfo(defs, name)
	if refInfo == nil {
		return nil, fmt.Errorf("references query info for %s not found", name)
	}
	sqlObjectAlias := "_where_" + prefix + "_" + field.Name
	joinObjectAlias := sqlObjectAlias
	if refInfo.IsM2M {
		// add from and join nodes for m2m references
		m2mDef := defs.ForName(refInfo.M2MName)
		if m2mDef == nil {
			return nil, fmt.Errorf("m2m object %s not found", refInfo.M2MName)
		}
		m2mInfo := compiler.DataObjectInfo(m2mDef)
		joinObjectAlias = "_join_" + prefix + "_" + field.Name
		nodes.Add(&QueryPlanNode{
			Name:  "m2m",
			Nodes: QueryPlanNodes{fromDataObjectNode(ctx, m2mInfo)},
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				sql := children.ForName("from").Result
				sql += " AS " + joinObjectAlias
				on, err := refInfo.FromM2MJoinConditions(node.TypeDefs(), joinObjectAlias, sqlObjectAlias, true, true)
				if err != nil {
					return "", nil, err
				}
				sql += " ON " + on

				return sql, params, nil
			},
		})
	}
	// where node
	whereNode, err := whereNode(ctx, defs, refObjectInfo, value, sqlObjectAlias, byAlias, selectDeleted)
	if err != nil {
		return nil, err
	}
	if whereNode != nil {
		nodes = append(nodes, whereNode)
	}

	return &QueryPlanNode{
		Name:  name,
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			from := children.ForName("from")
			if from == nil {
				return "", nil, errors.New("from definition is required")
			}
			fromSQL := from.Result + " AS " + sqlObjectAlias
			if refInfo.IsM2M {
				fromSQL += " INNER JOIN " + children.ForName("m2m").Result
			}

			where := children.ForName("where")
			if where == nil {
				return "", nil, errors.New("where definition is required")
			}
			whereSQL := where.Result
			if op == "all_of" && whereSQL != "" {
				whereSQL = "NOT(" + whereSQL + ")"
			}
			if whereSQL != "" {
				whereSQL += " AND "
			}
			// join
			var addSQL string
			if !refInfo.IsM2M {
				addSQL, err = refInfo.JoinConditions(node.TypeDefs(), prefix, joinObjectAlias, true, true)
			}
			if refInfo.IsM2M {
				addSQL, err = refInfo.ToM2MJoinConditions(node.TypeDefs(), prefix, joinObjectAlias, true, true)
			}
			if err != nil {
				return "", nil, err
			}
			whereSQL += addSQL
			switch op {
			case "any_of":
				return fmt.Sprintf("EXISTS (SELECT 1 FROM %s WHERE %s)", fromSQL, whereSQL), params, nil
			case "all_of":
				return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %[1]s WHERE %[2]s) AND EXISTS(SELECT 1 FROM %[1]s WHERE %[3]s)", fromSQL, whereSQL, addSQL), params, nil
			case "none_of":
				return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s WHERE %s)", fromSQL, whereSQL), params, nil
			default:
				return "", nil, fmt.Errorf("invalid operation %s", op)
			}
		},
	}, nil
}

func whereFieldNode(info *compiler.Object, prefix, name string, value any, byAlias bool) (*QueryPlanNode, error) {
	vals := value.(map[string]any)
	if len(vals) == 0 {
		return nil, nil
	}

	return &QueryPlanNode{
		Name: name,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			e, err := node.Engine(info.Catalog)
			if err != nil {
				return "", nil, err
			}
			field := info.FieldForName(name)
			if field == nil {
				return "", nil, errors.New("field not found in data object")
			}
			sqlName := field.SQL(prefix)
			if byAlias {
				sqlName = engines.Ident(name)
				if prefix != "" {
					sqlName = prefix + "." + sqlName
				}
			}
			return filterSQLValue(e, node.TypeDefs(), field.Definition(), sqlName, "", vals, params)
		},
	}, nil
}

func filterSQLValue(e engines.Engine, defs compiler.Definitions, field *ast.FieldDefinition, sqlName, path string, value map[string]any, params []any) (string, []any, error) {
	if !compiler.IsScalarType(field.Type.Name()) {
		def := defs.ForName(field.Type.Name())
		if def == nil {
			return "", nil, fmt.Errorf("type %s not found", field.Type.Name())
		}
		if field.Type.NamedType == "" {
			// ops - any_of, all_of, none_of arrays of objects
			return "", nil, errors.New("filtering by non scalar arrays are not supported yet")
		}
		return nestedFieldFilterSQLValue(e, defs, def, sqlName, path, value, params)
	}
	var filters []string
	for op, v := range value {
		filter, p, err := e.FilterOperationSQLValue(sqlName, path, op, v, params)
		if err != nil {
			return "", nil, err
		}
		filters = append(filters, "("+filter+")")
		params = p
	}
	if len(filters) == 1 {
		return strings.TrimPrefix(strings.TrimSuffix(filters[0], ")"), "("), params, nil
	}
	return strings.Join(filters, " AND "), params, nil
}

func nestedFieldFilterSQLValue(e engines.Engine, defs compiler.Definitions, def *ast.Definition, sqlName, path string, value map[string]any, params []any) (string, []any, error) {
	var nestedFilters []string
	for fieldName, fieldValue := range value {
		field := def.Fields.ForName(fieldName)
		if field == nil {
			return "", nil, fmt.Errorf("field %s not found in %s", fieldName, def.Name)
		}
		v, ok := fieldValue.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("nested filter fields value must be an object")
		}
		var q string
		var err error
		p := fieldName
		if path != "" {
			p = path + "." + fieldName
		}
		q, params, err = filterSQLValue(e, defs, field, sqlName, p, v, params)
		if err != nil {
			return "", nil, err
		}
		nestedFilters = append(nestedFilters, "("+q+")")
		continue

	}
	return strings.Join(nestedFilters, " AND "), params, nil
}
