package sdl

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

func AggregatedQueryDef(field *ast.Field) *ast.FieldDefinition {
	refField := base.FieldDefDirectiveArgString(field.Definition, base.FieldAggregationQueryDirectiveName, base.ArgName)
	if refField == "" {
		return nil
	}
	return field.ObjectDefinition.Fields.ForName(refField)
}

func AggregatedQueryFieldName(def *ast.FieldDefinition) (string, bool) {
	if def == nil {
		return "", false
	}
	return base.FieldDefDirectiveArgString(def, base.FieldAggregationQueryDirectiveName, base.ArgName),
		base.FieldDefDirectiveArgString(def, base.FieldAggregationQueryDirectiveName, base.ArgIsBucket) == "true"
}

func AggregatedObjectDef(ctx context.Context, defs base.DefinitionsSource, def *ast.Definition) *ast.Definition {
	if def == nil {
		return nil
	}
	// Check if this is a scalar sub-aggregation type (e.g. FloatSubAggregation → FloatAggregation)
	if parentAgg := types.AggregationTypeFromSub(def.Name); parentAgg != "" {
		return defs.ForName(ctx, parentAgg)
	}
	refName := base.DefinitionDirectiveArgString(def, base.ObjectAggregationDirectiveName, base.ArgName)
	if refName == "" {
		return nil
	}
	return defs.ForName(ctx, refName)
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
