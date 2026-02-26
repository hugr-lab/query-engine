package sdl

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var systemTypeByName = map[string]struct{}{
	"Query":        {},
	"Mutation":     {},
	"Subscription": {},
}

func IsSystemType(def *ast.Definition) bool {
	if def.Kind == ast.Scalar || strings.HasPrefix(def.Name, "__") {
		return true
	}
	if _, ok := systemTypeByName[def.Name]; ok {
		return true
	}
	return HasSystemDirective(def.Directives)
}

func HasSystemDirective(def ast.DirectiveList) bool {
	return def.ForName(base.SystemDirective.Name) != nil
}

func IsDataObject(def *ast.Definition) bool {
	if def.Kind != ast.Object {
		return false
	}
	if IsSystemType(def) {
		return false
	}
	if d := def.Directives.ForName(base.ObjectTableDirectiveName); d != nil {
		return true
	}
	if d := def.Directives.ForName(base.ObjectViewDirectiveName); d != nil {
		return true
	}
	return false
}

func IsHyperTable(def *ast.Definition) bool {
	if def.Kind != ast.Object || IsSystemType(def) {
		return false
	}
	return def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil
}

func DataObjectType(def *ast.Definition) string {
	if d := def.Directives.ForName(base.TableDataObject); d != nil {
		return base.TableDataObject
	}
	if d := def.Directives.ForName(base.ViewDataObject); d != nil {
		return base.ViewDataObject
	}
	return ""
}

func IsVectorSearchable(def *ast.Definition) bool {
	if def == nil || !IsDataObject(def) {
		return false
	}
	for _, f := range def.Fields {
		if f.Type.NamedType == base.VectorTypeName {
			return true
		}
	}
	return false
}

func IsEmbeddedObject(def *ast.Definition) bool {
	if def == nil {
		return false
	}
	return def.Directives.ForName(base.EmbeddingsDirectiveName) != nil
}

func IsEqualTypes(a, b *ast.Type) bool {
	if a.Name() != b.Name() {
		return false
	}
	if (a.Elem == nil) != (b.Elem == nil) {
		return false
	}
	if a.Elem != nil && b.Elem != nil {
		return IsEqualTypes(a.Elem, b.Elem)
	}
	return true
}

// --- Query/Mutation classification predicates ---

func IsSelectQuery(field *ast.Field) bool {
	return IsSelectQueryDefinition(field.Definition)
}

func IsSelectQueryDefinition(field *ast.FieldDefinition) bool {
	d := field.Directives.ForName(base.QueryDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName(base.ArgType)
	if t == nil {
		return false
	}
	return t.Value.Raw == base.QueryTypeTextSelect
}

func IsSelectOneQuery(field *ast.Field) bool {
	return IsSelectOneQueryDefinition(field.Definition)
}

func IsSelectOneQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(base.QueryDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName(base.ArgType)
	if t == nil {
		return false
	}
	return t.Value.Raw == base.QueryTypeTextSelectOne
}

func IsAggregateQuery(field *ast.Field) bool {
	return IsAggregateQueryDefinition(field.Definition)
}

func IsAggregateQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(base.FieldAggregationQueryDirectiveName)
	return d != nil && base.DirectiveArgString(d, base.ArgIsBucket) != "true"
}

func IsBucketAggregateQuery(field *ast.Field) bool {
	return IsBucketAggregateQueryDefinition(field.Definition)
}

func IsBucketAggregateQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(base.FieldAggregationQueryDirectiveName)
	return d != nil && base.DirectiveArgString(d, base.ArgIsBucket) == "true"
}

func IsH3Query(field *ast.Field) bool {
	return field.Definition.Type.Name() == base.H3QueryTypeName &&
		field.Definition.Type.NamedType == ""
}

func IsInsertQuery(field *ast.Field) bool {
	return IsInsertQueryDefinition(field.Definition)
}

func IsInsertQueryDefinition(def *ast.FieldDefinition) bool {
	d := def.Directives.ForName(base.MutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName(base.ArgType)
	if t == nil {
		return false
	}
	return t.Value.Raw == base.MutationTypeTextInsert
}

func IsUpdateQuery(field *ast.Field) bool {
	return isFieldMutationType(field, base.MutationTypeTextUpdate)
}

func IsUpdateQueryDefinition(def *ast.FieldDefinition) bool {
	return isFieldDefMutationType(def, base.MutationTypeTextUpdate)
}

func IsDeleteQuery(field *ast.Field) bool {
	return isFieldMutationType(field, base.MutationTypeTextDelete)
}

func IsDeleteQueryDefinition(def *ast.FieldDefinition) bool {
	return isFieldDefMutationType(def, base.MutationTypeTextDelete)
}

func isFieldMutationType(field *ast.Field, mutType string) bool {
	return isFieldDefMutationType(field.Definition, mutType)
}

func isFieldDefMutationType(def *ast.FieldDefinition, mutType string) bool {
	d := def.Directives.ForName(base.MutationDirectiveName)
	if d == nil {
		return false
	}
	t := d.Arguments.ForName(base.ArgType)
	if t == nil {
		return false
	}
	return t.Value.Raw == mutType
}

func IsSubQuery(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(base.JoinDirectiveName) != nil ||
		field.Directives.ForName(base.FieldReferencesQueryDirectiveName) != nil
}

func IsReferencesSubquery(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(base.FieldReferencesQueryDirectiveName) != nil
}

func IsJoinSubquery(field *ast.Field) bool {
	return field.Definition.Directives.ForName(base.JoinDirectiveName) != nil ||
		field.Directives.ForName(base.JoinDirectiveName) != nil
}

func IsJoinSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(base.JoinDirectiveName) != nil
}

func IsFunctionCallSubquery(field *ast.Field) bool {
	return IsFunctionCallSubqueryDefinition(field.Definition)
}

func IsFunctionCallSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(base.FunctionCallDirectiveName) != nil
}

func IsTableFuncJoinSubquery(field *ast.Field) bool {
	return IsTableFuncJoinSubqueryDefinition(field.Definition)
}

func IsTableFuncJoinSubqueryDefinition(def *ast.FieldDefinition) bool {
	return def.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil
}

func IsFunctionCallQuery(field *ast.Field) bool {
	return IsFunction(field.Definition)
}

func IsFunction(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(base.FunctionDirectiveName) != nil
}

func IsFunctionCall(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(base.FunctionCallDirectiveName) != nil ||
		field.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil
}

func IsTableFuncJoin(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil
}

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
