package planner

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type seqValue struct {
	fieldName    string
	sequenceName string
	name         string
	path         string
}

type seqValues []seqValue

func (sv seqValues) ForPath(name, path string) (seqValue, bool) {
	for _, s := range sv {
		if s.path == path && s.fieldName == name {
			return s, true
		}
	}
	return seqValue{}, false
}

func (sv seqValues) IsExists(seqName string) bool {
	for _, s := range sv {
		if s.sequenceName == seqName {
			return true
		}
	}
	return false
}

func insertRootNode(ctx context.Context, schema *ast.Schema, planner Catalog, query *ast.Field, vars map[string]any) (*QueryPlanNode, error) {
	// define request sequences values
	var sv []seqValue

	// get values from variables
	queryArg, err := compiler.ArgumentValues(compiler.SchemaDefs(schema), query, vars, false)
	if err != nil {
		return nil, err
	}
	if len(queryArg) == 0 {
		return nil, compiler.ErrorPosf(query.Position, "no arguments provided for mutation")
	}
	d := queryArg.ForName("data")
	if d == nil {
		return nil, compiler.ErrorPosf(query.Position, "data argument is not provided for mutation")
	}
	data, ok := d.Value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil, compiler.ErrorPosf(query.Position, "data argument should be an object")
	}

	data, err = checkMutationData(ctx, compiler.SchemaDefs(schema), query, d.Type, data)
	if err != nil {
		return nil, err
	}

	catalog := base.FieldCatalogName(query.Definition)
	if catalog == "" {
		return nil, compiler.ErrorPosf(query.Position, "catalog directive is not defined for mutation")
	}
	e, err := planner.Engine(catalog)
	if err != nil {
		return nil, err
	}

	m := compiler.MutationInfo(compiler.SchemaDefs(schema), query.Definition)
	if m == nil {
		return nil, compiler.ErrorPosf(query.Position, "mutation %s is not defined", query.Alias)
	}
	if m.Type != compiler.MutationTypeInsert {
		return nil, compiler.ErrorPosf(query.Position, "mutation %s type is not insert", query.Alias)
	}

	node, sv, err := insertDataObjectNode(ctx, schema, e, m, data, "", sv, map[string]string{})
	if err != nil {
		return nil, err
	}

	if s, ok := e.(engines.EngineQueryScanner); ok {
		node = &QueryPlanNode{
			Name:  "exec",
			Nodes: QueryPlanNodes{node},
			CollectFunc: func(node *QueryPlanNode, children Results, params []interface{}) (string, []interface{}, error) {
				sql := children.FirstResult().Result
				return "CALL " + s.WrapExec(catalog, sql), params, nil
			},
		}
	}

	node.Before, err = insertBeforeExec(e, m, query, data, sv)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func insertBeforeExec(e engines.Engine, m *compiler.Mutation, query *ast.Field, data map[string]any, sv seqValues) (NodeBeforeExecFunc, error) {
	seqSQL := []string{}
	for _, s := range sv {
		seqSQL = append(seqSQL, fmt.Sprintf(
			"SELECT '%s' as name, nextval('%s') AS id",
			s.name,
			s.sequenceName,
		))
	}

	// select node
	sq := m.SelectByPKQuery(query)
	sqVars := map[string]any{}
	sequenceVars := map[string]string{}
	if sq != nil {
		for _, a := range sq.Definition.Arguments {
			sq.Arguments = append(sq.Arguments, &ast.Argument{
				Name: a.Name,
				Value: &ast.Value{
					Raw:  a.Name,
					Kind: ast.Variable,
					VariableDefinition: &ast.VariableDefinition{
						Variable: a.Name,
						Type:     a.Type,
						Used:     true,
					},
					ExpectedType: a.Type,
				},
				Position: query.Position,
			})
			if v, ok := data[a.Name]; ok {
				sqVars[a.Name] = v
				continue
			}
			s, ok := sv.ForPath(a.Name, "")
			if !ok {
				return nil, compiler.ErrorPosf(query.Position, "field %s is required", a.Name)
			}
			sequenceVars[s.name] = a.Name
		}
		if len(sq.Arguments) != len(sq.Definition.Arguments) {
			return nil, compiler.ErrorPosf(query.Position, "not all arguments provided for select query")
		}
	}
	if sq == nil && query.Definition.Type.NamedType != compiler.OperationResultTypeName {
		return nil, compiler.ErrorPosf(query.Position, "no query by primary key for object %s", m.ObjectName)
	}

	return func(ctx context.Context, db *db.Pool, node *QueryPlanNode) error {
		// 1. get sequences values
		type seqVal struct {
			Name string `json:"name"`
			ID   int    `json:"id"`
		}
		var vals []seqVal
		if len(seqSQL) != 0 {
			sql := "SELECT name, id FROM (" + strings.Join(seqSQL, " UNION ALL ") + ") as seq"
			if s, ok := e.(engines.EngineQueryScanner); ok {
				sql = "FROM " + s.WarpScann(base.FieldCatalogName(query.Definition), sql)
			}
			err := db.QueryTableToSlice(ctx, &vals, sql)
			if err != nil {
				return err
			}
		}
		// 2. replace sequences values in insert query
		sql := node.plan.CompiledQuery
		for _, v := range vals {
			sql = strings.ReplaceAll(sql, "["+v.Name+"]", strconv.Itoa(v.ID))
			if s, ok := sequenceVars[v.Name]; ok {
				sqVars[s] = v.ID
			}
		}
		// 3. execute insert query
		res, err := db.Exec(ctx, sql)
		if err != nil {
			return err
		}

		// 4. returning select statement
		if sq != nil {
			sn, err := selectDataObjectRootNode(ctx, node.schema, node.engines, sq, sqVars)
			if err != nil {
				return err
			}
			sn.schema = node.schema
			sn.engines = node.engines
			r, err := sn.Compile(sn, nil)
			if err != nil {
				return err
			}
			node.plan.CompiledQuery = r.Result
			node.plan.Params = r.Params
			node.Before = nil
			return nil
		}

		// return operation result if no primary key
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
	}, nil
}

func insertDataObjectNode(ctx context.Context, schema *ast.Schema, e engines.Engine, m *compiler.Mutation, data map[string]any, path string, sv seqValues, parentSeqVal map[string]string) (*QueryPlanNode, seqValues, error) {
	refs := m.ReferencesFields()
	m2mRefs := m.M2MReferencesFields()
	// define queries nodes of that current query is depended (references data that should be inserted before)
	var nodes QueryPlanNodes
	depends := map[string]string{}
	for f, v := range data {
		if !slices.Contains(refs, f) {
			continue
		}
		fd := m.FieldDefinition(f)
		if fd == nil {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s is not defined in object %s", f, m.ObjectDefinition.Name)
		}
		if fd.Type.NamedType == "" || slices.Contains(m2mRefs, f) {
			continue
		}
		refData, ok := v.(map[string]any)
		if !ok {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s should be an object", path, f)
		}
		sp := path
		if sp != "" {
			sp += "."
		}
		sp += f
		subMut := m.ReferencesMutation(f)
		if subMut == nil {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "references mutation for field %s.%s is not defined", path, f)
		}
		node, ssv, err := insertDataObjectNode(ctx, schema, e, subMut, refData, sp, sv, map[string]string{})
		if err != nil {
			return nil, nil, err
		}
		sourceFields := m.ReferencesFieldsSource(f)
		refFields := m.ReferencesFieldsReferences(f)
		for i, sf := range sourceFields {
			rf := refFields[i]
			v, ok := refData[rf]
			if !ok {
				// find sequence field
				s, ok := ssv.ForPath(rf, sp)
				if !ok {
					return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s is not defined in object %s", path, refFields[i], m.ObjectDefinition.Name)
				}
				depends[sf] = "[" + s.name + "]"
				continue
			}
			depends[sf], err = e.SQLValue(v)
			if err != nil {
				return nil, nil, err
			}
		}
		sv = ssv
		nodes = append(nodes, node)
	}
	// define values for insert query
	fv := map[string]string{}
	var err error
	for _, fieldInfo := range m.Fields() {
		value, hasValue := data[fieldInfo.Name]
		if hasValue {
			if slices.Contains(refs, fieldInfo.Name) {
				continue
			}
			fv[fieldInfo.Name], err = e.SQLValue(value)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		if v, ok := depends[fieldInfo.Name]; ok {
			fv[fieldInfo.Name] = v
			continue
		}
		if v, ok := parentSeqVal[fieldInfo.Name]; ok {
			fv[fieldInfo.Name] = v
			continue
		}
		if !fieldInfo.IsRequired() {
			continue
		}
		// check sequence
		sn := fieldInfo.SequenceName()
		if sn == "" {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s is required", path, fieldInfo.Name)
		}
		sv = append(sv, seqValue{
			fieldName:    fieldInfo.Name,
			sequenceName: sn,
			name:         sn + strconv.Itoa(len(sv)+1),
			path:         path,
		})
		fv[fieldInfo.Name] = "[" + sn + strconv.Itoa(len(sv)) + "]"
	}
	if len(fv) == 0 {
		return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "no values provided for insert")
	}
	info := compiler.DataObjectInfo(m.ObjectDefinition)
	if info == nil {
		return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "object %s is not defined", m.ObjectDefinition.Name)
	}
	nodes = append(nodes,
		insertNode(ctx, info, fv),
	)
	// define queries that depend on current query (references data that should be inserted after)
	for f, v := range data {
		if !slices.Contains(refs, f) {
			continue
		}
		fd := m.FieldDefinition(f)
		if fd == nil {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s is not defined in object %s", path, f, m.ObjectDefinition.Name)
		}
		if fd.Type.NamedType != "" {
			continue
		}
		refDataList, ok := v.([]any)
		if !ok {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s should be an array of objects", path, f)
		}
		subMut := m.ReferencesMutation(f)
		if subMut == nil {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "references mutation for field %s,%s is not defined", path, f)
		}
		sp := path
		if sp != "" {
			sp += "."
		}
		sp += f
		refFields := m.ReferencesFieldsReferences(f)
		sourceFields := m.ReferencesFieldsSource(f)
		ref := info.ReferencesQueryInfo(compiler.SchemaDefs(schema), f)
		if ref == nil {
			return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "references query for field %s.%s is not defined", path, f)
		}
		for i, v := range refDataList {
			refData, ok := v.(map[string]any)
			if !ok {
				return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s should be an array of objects", path, f)
			}
			psv := map[string]string{}
			if !ref.IsM2M {
				for j, rf := range refFields {
					sf := sourceFields[j]
					if _, ok := refData[rf]; ok {
						continue
					}
					if s, ok := sv.ForPath(sf, path); ok {
						psv[rf] = "[" + s.name + "]"
						continue
					}
					refData[rf], ok = data[sf]
					if !ok {
						return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "field %s.%s is required", path, sf)
					}
				}
			}

			node, ssv, err := insertDataObjectNode(ctx, schema, e, subMut, refData, sp+"["+strconv.Itoa(i)+"]", sv, psv)
			if err != nil {
				return nil, nil, err
			}
			sv = ssv
			nodes = append(nodes, node)

			if !ref.IsM2M {
				continue
			}
			// add m2m links
			m2m := schema.Types[ref.M2MName]
			m2mInfo := compiler.DataObjectInfo(m2m)
			if m2mInfo == nil {
				return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "object %s is not defined", ref.M2MName)
			}
			m2mRef := m2mInfo.M2MReferencesQueryInfo(compiler.SchemaDefs(schema), ref.Name)
			m2mSourceFields := m2mRef.SourceFields()
			m2mRefFields := m2mRef.ReferencesFields()
			m2mData := map[string]string{}
			for j, rf := range m2mRefFields {
				sf := m2mSourceFields[j]
				v, ok := refData[rf]
				if ok {
					m2mData[sf], err = e.SQLValue(v)
					if err != nil {
						return nil, nil, err
					}
					continue
				}
				s, ok := ssv.ForPath(rf, sp+"["+strconv.Itoa(i)+"]")
				if !ok {
					return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "To m2m link %s and %s field %[2]s.%s is required", m.ObjectName, sp+"["+strconv.Itoa(i)+"]", rf)
				}
				m2mData[sf] = "[" + s.name + "]"
			}
			for j, rf := range refFields {
				sf := sourceFields[j]
				v, ok := fv[sf]
				if !ok {
					return nil, nil, compiler.ErrorPosf(m.ObjectDefinition.Position, "To m2m link %s and %s field %[1]s.%s is required", m.ObjectName, path, sf)
				}
				m2mData[rf] = v
				if err != nil {
					return nil, nil, err
				}
			}
			nodes = append(nodes,
				insertNode(ctx, m2mInfo, m2mData),
			)
		}
	}
	return &QueryPlanNode{
		Name:  m.ObjectName,
		Nodes: nodes,
		CollectFunc: func(node *QueryPlanNode, children Results, params []interface{}) (string, []interface{}, error) {
			var sql []string
			for _, res := range children {
				sql = append(sql, res.Result)
			}
			return strings.Join(sql, ";\n"), params, nil
		},
	}, sv, nil
}

func insertNode(ctx context.Context, info *compiler.Object, fieldValues map[string]string) *QueryPlanNode {
	return &QueryPlanNode{
		Name: info.Name,
		CollectFunc: func(node *QueryPlanNode, children Results, params []interface{}) (string, []interface{}, error) {
			e, err := node.Engine(info.Catalog)
			if err != nil {
				return "", nil, err
			}
			prefix := ""
			if _, ok := e.(engines.EngineQueryScanner); !ok {
				prefix = info.Catalog
			}
			sql := "INSERT INTO " + info.SQL(ctx, engines.Ident(prefix))
			var fields, values []string
			for fn, fv := range fieldValues {
				fi := info.FieldForName(fn)
				if fi == nil {
					return "", nil, compiler.ErrorPosf(node.Query.Position, "field %s is not defined in object %s", fn, info.Name)
				}
				fields = append(fields, fi.SQL(""))
				values = append(values, fv)
			}
			sql += "(" + strings.Join(fields, ",") + ") VALUES(" + strings.Join(values, ",") + ")"
			return sql, params, nil
		},
	}
}
