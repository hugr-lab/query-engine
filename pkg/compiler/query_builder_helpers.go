package compiler

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
)

func IsAggregateQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(fieldAggregationQueryDirectiveName)
	return d != nil && directiveArgValue(d, "is_bucket") != "true"
}

func IsBucketAggregateQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(fieldAggregationQueryDirectiveName)
	return d != nil && directiveArgValue(d, "is_bucket") == "true"
}

func IsSelectQuery(field *ast.Field) bool {
	return isSelectQueryDefinition(field.Definition)
}

func IsSelectOneQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(queryDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName("type")
	if t == nil {
		return false
	}
	return t.Value.Raw == queryTypeTextSelectOne
}

func isSelectQueryDefinition(field *ast.FieldDefinition) bool {
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

func IsFunctionCallQuery(field *ast.Field) bool {
	return IsFunction(field.Definition)
}

func IsInsertQuery(field *ast.Field) bool {
	d := field.Definition.Directives.ForName(mutationDirectiveName)
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

func IsReferencesSubquery(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(fieldReferencesQueryDirectiveName) != nil
}

func IsJoinSubquery(field *ast.Field) bool {
	return field.Definition.Directives.ForName(JoinDirectiveName) != nil ||
		field.Directives.ForName(JoinDirectiveName) != nil
}

func IsFunctionCallSubquery(field *ast.Field) bool {
	return field.Definition.Directives.ForName(functionCallDirectiveName) != nil
}

func IsTableFuncJoinSubquery(field *ast.Field) bool {
	return field.Definition.Directives.ForName(functionCallTableJoinDirectiveName) != nil
}

func ObjectSQL(def *ast.Definition) string {
	name := objectDirectiveArgValue(def, objectTableDirectiveName, "name")
	if name != "" {
		return name
	}

	name = objectDirectiveArgValue(def, objectViewDirectiveName, "name")
	sql := objectDirectiveArgValue(def, objectViewDirectiveName, "sql")
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
