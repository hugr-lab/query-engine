package sdl

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

func AggregatedQueryDef(field *ast.Field) *ast.FieldDefinition {
	refField := fieldDirectiveArgValue(field.Definition, base.FieldAggregationQueryDirectiveName, "name")
	if refField == "" {
		return nil
	}
	return field.ObjectDefinition.Fields.ForName(refField)
}

func AggregatedQueryFieldName(def *ast.FieldDefinition) (string, bool) {
	if def == nil {
		return "", false
	}
	return fieldDirectiveArgValue(def, base.FieldAggregationQueryDirectiveName, "name"),
		fieldDirectiveArgValue(def, base.FieldAggregationQueryDirectiveName, "is_bucket") == "true"
}

func AggregatedObjectDef(defs Definitions, def *ast.Definition) *ast.Definition {
	if def == nil {
		return nil
	}
	// Check if this is a scalar sub-aggregation type (e.g. FloatSubAggregation → FloatAggregation)
	if parentAgg := types.AggregationTypeFromSub(def.Name); parentAgg != "" {
		return defs.ForName(parentAgg)
	}
	refName := objectDirectiveArgValue(def, base.ObjectAggregationDirectiveName, "name")
	if refName == "" {
		return nil
	}
	return defs.ForName(refName)
}

func buildObjectAggregationTypeName(name string, isSub, isBucket bool) string {
	typeName := "_" + name + base.AggregationSuffix
	if isSub {
		return typeName + "_sub_aggregation"
	}
	if isBucket {
		typeName += "_bucket"
	}
	return typeName
}
