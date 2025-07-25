package compiler

import (
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

func IsDataObjectFieldDefinition(def *ast.FieldDefinition) bool {
	if def == nil {
		return false
	}
	return !IsAggregateQueryDefinition(def) &&
		!IsBucketAggregateQueryDefinition(def) &&
		!IsJoinSubqueryDefinition(def) &&
		!IsFunctionCallSubqueryDefinition(def) &&
		!IsTableFuncJoinSubqueryDefinition(def) &&
		!IsReferencesSubquery(def) &&
		!IsSelectQueryDefinition(def) &&
		!IsSelectOneQueryDefinition(def)
}

func IsAggregateQuery(field *ast.Field) bool {
	return IsAggregateQueryDefinition(field.Definition)
}

func IsAggregateQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(fieldAggregationQueryDirectiveName)
	return d != nil && directiveArgValue(d, "is_bucket") != "true"
}

func IsBucketAggregateQuery(field *ast.Field) bool {
	return IsBucketAggregateQueryDefinition(field.Definition)
}

func IsBucketAggregateQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(fieldAggregationQueryDirectiveName)
	return d != nil && directiveArgValue(d, "is_bucket") == "true"
}

func IsH3Query(field *ast.Field) bool {
	return field.Definition.Type.Name() == base.H3QueryTypeName &&
		field.Definition.Type.NamedType == ""
}

func IsSelectQuery(field *ast.Field) bool {
	return IsSelectQueryDefinition(field.Definition)
}

func IsSelectQueryDefinition(field *ast.FieldDefinition) bool {
	d := field.Directives.ForName(queryDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == queryTypeTextSelect
}

func IsSelectOneQuery(field *ast.Field) bool {
	return IsSelectOneQueryDefinition(field.Definition)
}

func IsSelectOneQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(queryDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == queryTypeTextSelectOne
}

func IsFunctionCallQuery(field *ast.Field) bool {
	return IsFunction(field.Definition)
}

func IsInsertQuery(field *ast.Field) bool {
	return IsInsertQueryDefinition(field.Definition)
}

func IsInsertQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(mutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == mutationTypeTextInsert
}

func IsUpdateQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(mutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == mutationTypeTextUpdate
}

func IsUpdateQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(mutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == mutationTypeTextUpdate
}

func IsDeleteQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(mutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == mutationTypeTextDelete
}

func IsDeleteQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(mutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == mutationTypeTextDelete
}

func IsReferencesSubquery(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(fieldReferencesQueryDirectiveName) != nil
}

func IsJoinSubquery(field *ast.Field) bool {
	return field.Definition.Directives.ForName(JoinDirectiveName) != nil ||
		field.Directives.ForName(JoinDirectiveName) != nil
}

func IsJoinSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(JoinDirectiveName) != nil
}

func IsFunctionCallSubquery(field *ast.Field) bool {
	return IsFunctionCallSubqueryDefinition(field.Definition)
}

func IsFunctionCallSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(functionCallDirectiveName) != nil
}

func IsTableFuncJoinSubquery(field *ast.Field) bool {
	return IsTableFuncJoinSubqueryDefinition(field.Definition)
}

func IsTableFuncJoinSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(functionCallTableJoinDirectiveName) != nil
}

func ObjectSQL(def *ast.Definition) string {
	name := objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "name")
	if name != "" {
		return name
	}

	name = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "name")
	sql := objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "sql")
	if sql != "" {
		return fmt.Sprintf("(%s) AS %s", sql, name)
	}
	return name
}

func FieldReferencesInfo(defs Definitions, def *ast.Definition, field *ast.FieldDefinition) *References {
	fieldRefName := fieldDirectiveArgValue(field, fieldReferencesQueryDirectiveName, "name")
	for _, d := range def.Directives.ForNames(referencesDirectiveName) {
		name := directiveArgValue(d, "name")
		if name == fieldRefName {
			ref := ReferencesInfo(d)
			ref.sourceName = def.Name
			if ref.Query != field.Name && def.Name == ref.ReferencesName { // recursive reference
				ref.isBackRef = true
			}
			return ref
		}
	}
	// check in corresponding object
	def = defs.ForName(field.Type.Name())
	if def == nil {
		return nil
	}
	for _, d := range def.Directives.ForNames(referencesDirectiveName) {
		name := directiveArgValue(d, "name")
		if name == fieldRefName {
			ref := ReferencesInfo(d)
			ref.sourceName = def.Name
			// flip source and references
			ref.isBackRef = true
			return ref
		}
	}
	return nil
}

func IsNoJoinPushdown(field *ast.Field) bool {
	if field == nil {
		return false
	}
	if field.Directives.ForName(base.NoPushdownDirectiveName) != nil {
		return true
	}
	return IsNoJoinPushdownDefinition(field.Definition)
}

func IsNoJoinPushdownDefinition(def *ast.FieldDefinition) bool {
	if def == nil {
		return false
	}
	return def.Directives.ForName(base.NoPushdownDirectiveName) != nil
}
