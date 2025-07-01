package compiler

import (
	"fmt"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	fieldAggregationQueryDirectiveName = "aggregation_query"
	objectAggregationDirectiveName     = "aggregation"
	objectFieldAggregationDirective    = "field_aggregation"
	AggregateKeyFieldName              = "key"
	AggregateFieldName                 = "aggregations"
	AggRowsCountFieldName              = "_rows_count"
	maxAggLevel                        = 2
	AggregationSuffix                  = "_aggregation"
	BucketAggregationSuffix            = "_bucket_aggregation"
)

func addAggregationQuery(schema *ast.SchemaDocument, def *ast.Definition, opt *Options) {
	for _, field := range def.Fields {
		addAggregationQueryField(schema, def, field, opt)
	}
}

func addAggregationQueryField(schema *ast.SchemaDocument, def *ast.Definition, field *ast.FieldDefinition, opt *Options) {
	if IsScalarType(field.Type.Name()) {
		return
	}
	if field.Type.NamedType != "" {
		return
	}
	ft := schema.Definitions.ForName(field.Type.Name())
	if ft.Kind != ast.Object {
		return
	}
	// add only data objects or table functions or joins (spatial and join)
	if !IsDataObject(ft) && !IsFunctionCall(field) && !IsFunction(field) &&
		field.Name != base.QueryTimeJoinsFieldName && field.Name != base.QueryTimeSpatialFieldName {
		return
	}
	// add only functions call with out arguments
	if d := field.Directives.ForName(functionCallTableJoinDirectiveName); d != nil {
		if d.Arguments.ForName("args") != nil ||
			d.Arguments.ForName("arg") != nil {
			return
		}
	}

	typeName := objectAggregationTypeName(schema, opt, ft, false)
	def.Fields = append(def.Fields, &ast.FieldDefinition{
		Name:        field.Name + AggregationSuffix,
		Type:        ast.NamedType(typeName, CompiledPosName("add_aggs")),
		Arguments:   field.Arguments,
		Description: "The aggregation for " + field.Name,
		Directives:  ast.DirectiveList{aggQueryDirective(field, false), opt.catalog},
		Position:    compiledPos(),
	})
	typeName = objectAggregationTypeName(schema, opt, ft, true)
	def.Fields = append(def.Fields, &ast.FieldDefinition{
		Name:        field.Name + BucketAggregationSuffix,
		Type:        ast.ListType(ast.NamedType(typeName, CompiledPosName("add_aggs")), CompiledPosName("add_aggs")),
		Arguments:   field.Arguments,
		Description: "The aggregation for " + field.Name,
		Directives:  ast.DirectiveList{aggQueryDirective(field, true), opt.catalog},
		Position:    compiledPos(),
	})
}

func aggQueryDirective(def *ast.FieldDefinition, isBucket bool) *ast.Directive {
	return &ast.Directive{
		Name: fieldAggregationQueryDirectiveName,
		Arguments: ast.ArgumentList{
			{
				Name: "name",
				Value: &ast.Value{
					Raw:      def.Name,
					Kind:     ast.StringValue,
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
			{
				Name: "is_bucket",
				Value: &ast.Value{
					Raw:      fmt.Sprint(isBucket),
					Kind:     ast.BooleanValue,
					Position: compiledPos(),
				},
			},
		},
		Position: compiledPos(),
	}
}

func aggObjectDirective(def *ast.Definition, isBucket bool, level int) *ast.Directive {
	return &ast.Directive{
		Name: objectAggregationDirectiveName,
		Arguments: ast.ArgumentList{
			{
				Name: "name",
				Value: &ast.Value{
					Raw:      def.Name,
					Kind:     ast.StringValue,
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
			{
				Name: "is_bucket",
				Value: &ast.Value{
					Raw:      fmt.Sprint(isBucket),
					Kind:     ast.BooleanValue,
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
			{
				Name: "level",
				Value: &ast.Value{
					Raw:      fmt.Sprint(level),
					Kind:     ast.IntValue,
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
		},
		Position: compiledPos(),
	}
}

func aggObjectLevel(def *ast.Definition) int {
	d := def.Directives.ForName(objectAggregationDirectiveName)
	if d == nil {
		return 0
	}
	levelStr := directiveArgValue(d, "level")
	if l, err := strconv.Atoi(levelStr); err == nil {
		return l
	}
	return 1
}

func aggObjectFieldAggregationDirective(def *ast.FieldDefinition) *ast.Directive {
	return &ast.Directive{
		Name: objectFieldAggregationDirective,
		Arguments: ast.ArgumentList{
			{
				Name: "name",
				Value: &ast.Value{
					Raw:      def.Name,
					Kind:     ast.StringValue,
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
		},
		Position: compiledPos(),
	}
}

func AggregatedQueryDef(field *ast.Field) *ast.FieldDefinition {
	refField := fieldDirectiveArgValue(field.Definition, fieldAggregationQueryDirectiveName, "name")
	if refField == "" {
		return nil
	}
	return field.ObjectDefinition.Fields.ForName(refField)
}

func AggregatedObjectDef(defs Definitions, def *ast.Definition) *ast.Definition {
	if def == nil {
		return nil
	}
	if _, ok := subAggregationTypes[def.Name]; ok {
		for at, sat := range subAggregationTypes {
			if sat == def.Name {
				return defs.ForName(at)
			}
		}
		return nil
	}

	refName := objectDirectiveArgValue(def, objectAggregationDirectiveName, "name")
	if refName == "" {
		return nil
	}
	return defs.ForName(refName)
}

func objectAggregationTypeName(schema *ast.SchemaDocument, opt *Options, def *ast.Definition, isBucket bool) string {
	typeName := "_" + def.Name + AggregationSuffix
	level := 1
	if def.Directives.ForName(objectAggregationDirectiveName) != nil {
		typeName = def.Name + "_sub_aggregation"
		level = aggObjectLevel(def) + 1
	}
	if isBucket {
		typeName += "_bucket"
	}
	if def.Name == base.QueryTimeJoinsTypeName {
		typeName = base.QueryTimeJoinsTypeName + AggregationSuffix
	}
	if def.Name == base.QueryTimeSpatialTypeName {
		typeName = base.QueryTimeSpatialTypeName + AggregationSuffix
	}
	if def.Name == base.QueryTimeJoinsTypeName+AggregationSuffix ||
		def.Name == base.QueryTimeSpatialTypeName+AggregationSuffix {
		return ""
	}
	aggType := schema.Definitions.ForName(typeName)
	if aggType != nil {
		return typeName
	}
	aggType = &ast.Definition{
		Kind:        ast.Object,
		Name:        typeName,
		Description: "Aggregation for " + def.Name,
		Position:    compiledPos(),
		Directives:  ast.DirectiveList{aggObjectDirective(def, isBucket, level)},
	}
	if isBucket {
		aggType.Description = "Bucket aggregation for " + def.Name
	}
	schema.Definitions = append(schema.Definitions, aggType)

	if isBucket {
		aggName := objectAggregationTypeName(schema, opt, def, false)
		aggType.Fields = append(aggType.Fields, &ast.FieldDefinition{
			Name:        AggregateKeyFieldName,
			Type:        ast.NamedType(def.Name, CompiledPosName("add_aggs")),
			Description: "The key of the bucket",
			Position:    compiledPos(),
		}, &ast.FieldDefinition{
			Name: AggregateFieldName,
			Type: ast.NamedType(aggName, CompiledPosName("add_aggs")),
			Arguments: []*ast.ArgumentDefinition{
				{
					Name: "filter",
					Type: ast.NamedType(inputObjectFilterName(schema, def, false), CompiledPosName("add_aggs")),
				},
				{
					Name: "order_by",
					Type: ast.ListType(ast.NamedType("OrderByField", compiledPos()), CompiledPosName("add_aggs")),
				},
			},
			Description: "The aggregations of the bucket",
		},
		)
		return typeName
	}
	if def.Name != base.QueryTimeJoinsTypeName && def.Name != base.QueryTimeSpatialTypeName &&
		def.Directives.ForName(objectAggregationDirectiveName) == nil {
		aggType.Fields = append(aggType.Fields, &ast.FieldDefinition{
			Name:        AggRowsCountFieldName,
			Type:        ast.NamedType("BigInt", compiledPos()),
			Description: "The count of items " + def.Description,
			Position:    compiledPos(),
		})
	}
	// nested objects aggregations
	for _, field := range def.Fields {
		if IsScalarType(field.Type.Name()) {
			fieldAggObjectName := ScalarTypes[field.Type.Name()].AggType
			if fieldAggObjectName == "" || field.Type.NamedType == "" {
				continue
			}
			aggType.Fields = append(aggType.Fields, &ast.FieldDefinition{
				Name:        field.Name,
				Type:        ast.NamedType(fieldAggObjectName, CompiledPosName("add_aggs")),
				Arguments:   field.Arguments,
				Description: "The aggregation for " + field.Name,
				Directives:  ast.DirectiveList{aggObjectFieldAggregationDirective(field)},
				Position:    compiledPos(),
			})
			continue
		}
		ft := schema.Definitions.ForName(field.Type.Name())
		if ft.Kind != ast.Object ||
			(field.Type.NamedType == "" && !IsDataObject(ft) && !IsFunctionCall(field) && !IsFunction(field)) {
			continue
		}
		aggFieldDirectives := ast.DirectiveList{aggObjectFieldAggregationDirective(field)}
		if def.Name == base.QueryTimeJoinsTypeName || def.Name == base.QueryTimeSpatialTypeName {
			aggFieldDirectives = append(aggFieldDirectives, aggQueryDirective(field, false), opt.catalog)
		}
		fieldName := field.Name
		aggTypeName, ok := subAggregationTypes[ft.Name]
		if !ok && level > maxAggLevel {
			continue
		}
		if !ok {
			aggTypeName = objectAggregationTypeName(schema, opt, ft, false)
		}
		if aggTypeName == "" {
			continue
		}
		addAggs := field.Type.NamedType != "" || !IsFunctionCall(field)
		if IsFunctionCall(field) {
			if d := field.Directives.ForName(functionCallTableJoinDirectiveName); d != nil &&
				d.Arguments.ForName("args") == nil && d.Arguments.ForName("arg") == nil {
				addAggs = true
			}
		}
		if addAggs {
			args := make([]*ast.ArgumentDefinition, 0, len(field.Arguments))
			for _, arg := range field.Arguments {
				if IsDataObject(ft) && (arg.Name == "limit" || arg.Name == "offset") {
					continue
				}
				args = append(args, arg)
			}
			aggType.Fields = append(aggType.Fields, &ast.FieldDefinition{
				Name:        fieldName,
				Type:        ast.NamedType(aggTypeName, CompiledPosName("add_aggs")),
				Arguments:   args,
				Description: "The aggregation for " + field.Name,
				Directives:  aggFieldDirectives,
				Position:    compiledPos(),
			})
		}
		// add sub aggregations
		if field.Type.NamedType == "" &&
			// add only if it is not join and not spatial object aggregation and not scalar function call
			def.Name != base.QueryTimeJoinsTypeName && def.Name != base.QueryTimeSpatialTypeName &&
			!(IsFunctionCall(field) && !IsTableFuncJoin(field)) {
			ft := schema.Definitions.ForName(aggTypeName)
			if ft == nil {
				continue
			}
			aggTypeName = objectAggregationTypeName(schema, opt, ft, false)
			fieldName = field.Name + "_aggregation"
			// add arguments to subquery aggregations
			aggType.Fields = append(aggType.Fields, &ast.FieldDefinition{
				Name:        fieldName,
				Type:        ast.NamedType(aggTypeName, CompiledPosName("add_aggs")),
				Arguments:   field.Arguments,
				Description: "The aggregation for " + fieldName,
				Directives:  aggFieldDirectives,
				Position:    compiledPos(),
			})
		}
	}
	return typeName
}
