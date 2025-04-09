package planner

import (
	"context"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

func deleteRootNode(ctx context.Context, schema *ast.Schema, planner Catalog, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	catalog := base.FieldCatalogName(query.Definition)
	e, err := planner.Engine(catalog)
	if err != nil {
		return nil, err
	}
	m := compiler.MutationInfo(compiler.SchemaDefs(schema), query.Definition)
	if m == nil {
		return nil, ErrInternalPlanner
	}
	if m.Type != compiler.MutationTypeDelete {
		return nil, compiler.ErrorPosf(query.Position, "mutation type is not delete")
	}
	def := schema.Types[m.ObjectName]
	if def == nil {
		return nil, ErrInternalPlanner
	}
	info := compiler.DataObjectInfo(def)
	if info == nil {
		return nil, ErrInternalPlanner
	}
	if info.Type != compiler.TableDataObject {
		return nil, compiler.ErrorPosf(query.Position, "unsupported data object type %s", info.Type)
	}
	queryArg, err := compiler.ArgumentValues(compiler.SchemaDefs(schema), query, vars, true)
	if err != nil {
		return nil, err
	}
	prefix := ""
	if _, ok := e.(engines.EngineQueryScanner); !ok {
		prefix = catalog
	}
	dbObject := info.SQL(ctx, prefix)

	var nodes QueryPlanNodes
	// where node
	filter := queryArg.ForName("filter")
	if filter != nil {
		v, ok := filter.Value.(map[string]interface{})
		if !ok {
			return nil, compiler.ErrorPosf(query.Position, "invalid filter argument type")
		}
		whereNode, err := whereNode(ctx, compiler.SchemaDefs(schema), info, v, "_object", false, false)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, whereNode)
	}

	pf, err := permissionFilterNode(ctx, compiler.SchemaDefs(schema), info, query, "_object", false)
	if err != nil {
		return nil, err
	}
	if pf != nil {
		nodes = append(nodes, pf)
	}

	return &QueryPlanNode{
		Name:  "update",
		Query: query,
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			sql := "DELETE FROM " + dbObject + " AS _object"
			if info.SoftDelete {
				sql = "UPDATE " + dbObject + " AS _object SET " + info.SoftDeleteSet(dbObject)
			}
			whereSQL := ""
			where := children.ForName("where")
			if where != nil {
				whereSQL += where.Result
			}
			pf := children.ForName("permission_filter")
			if pf != nil {
				if whereSQL != "" {
					whereSQL += " AND "
				}
				whereSQL += pf.Result
			}
			if whereSQL != "" {
				sql += " WHERE " + whereSQL
			}
			if s, ok := e.(engines.EngineQueryScanner); ok {
				var err error
				var n int
				sql, n, err = engines.ApplyQueryParams(e, sql, params)
				if err != nil {
					return "", nil, err
				}
				params = params[:n]
				sql = "CALL " + s.WrapExec(catalog, sql)
			}
			return sql, params, nil
		},
		Before: func(ctx context.Context, db *db.Pool, node *QueryPlanNode) error {
			res, err := db.Exec(ctx, node.plan.CompiledQuery, node.plan.Params...)
			if err != nil {
				return err
			}
			var resFields []string
			for _, s := range engines.SelectedFields(query.SelectionSet) {
				switch s.Field.Name {
				case "success":
					resFields = append(resFields, "true AS "+engines.Ident(s.Field.Alias))
				case "affected_rows":
					r, _ := res.RowsAffected()
					resFields = append(resFields, strconv.Itoa(int(r))+" AS "+engines.Ident(s.Field.Alias))
				case "last_id":
					r, _ := res.LastInsertId()
					resFields = append(resFields, strconv.Itoa(int(r))+" AS "+engines.Ident(s.Field.Alias))
				case "message":
					resFields = append(resFields, "'success' AS "+engines.Ident(s.Field.Alias))
				}
			}
			node.plan.CompiledQuery = "SELECT " + strings.Join(resFields, ",")
			node.plan.Params = nil
			return nil
		},
	}, nil
}
