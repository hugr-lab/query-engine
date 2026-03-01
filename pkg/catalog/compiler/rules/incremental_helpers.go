package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// --- Shared emit functions for incremental and full compilation ---
//
// These functions abstract the emit logic so both the incremental compiler
// (which uses output.AddExtension) and full compilation rules (which use
// ctx.AddExtension) can share the same code.

// EmitScalarFieldDerivedTypes generates extensions for all derived types
// (filter, mutation, aggregation) when a scalar field is added.
// Used by both full compilation (gen_table) and incremental compilation.
//
// The emit callback abstracts over output.AddExtension (incremental) vs
// ctx.AddExtension (full compilation).
func EmitScalarFieldDerivedTypes(
	emit func(*ast.Definition),
	typeName string,
	field *ast.FieldDefinition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
) {
	// Virtual fields are excluded from all derived types (filter, mutation, aggregation).
	if isVirtualField(field) {
		return
	}

	pos := base.CompiledPos("incremental-field-add")
	fieldName := field.Name
	scalarName := field.Type.Name()
	isList := field.Type.NamedType == ""

	s := scalarLookup(scalarName)

	// 1. Filter input extension
	if s != nil {
		if fi, ok := s.(types.Filterable); ok {
			filterFieldType := fi.FilterTypeName()
			if isList {
				if lfi, ok := s.(types.ListFilterable); ok {
					filterFieldType = lfi.ListFilterTypeName()
				}
			}
			filterField := &ast.FieldDefinition{
				Name:     fieldName,
				Type:     ast.NamedType(filterFieldType, pos),
				Position: pos,
			}
			// Copy PK and other relevant directives
			for _, d := range field.Directives {
				if d.Name == "pk" || d.Name == "default" || d.Name == "measurement" || d.Name == "timescale_key" || d.Name == "field_references" {
					filterField.Directives = append(filterField.Directives, d)
				}
			}
			if field.Directives.ForName("filter_required") != nil {
				filterField.Type.NonNull = true
			}
			emit(&ast.Definition{
				Kind:     ast.InputObject,
				Name:     typeName + "_filter",
				Position: pos,
				Fields:   ast.FieldList{filterField},
			})
		}
	}

	// 2. Mutation input data extension (skip virtual fields, computed fields, readonly)
	if !opts.ReadOnly && !isVirtualField(field) && field.Directives.ForName("sql") == nil {
		fieldType := cloneFieldType(field.Type, pos)
		emit(&ast.Definition{
			Kind:     ast.InputObject,
			Name:     typeName + "_mut_input_data",
			Position: pos,
			Fields: ast.FieldList{
				{Name: fieldName, Type: fieldType, Position: pos},
			},
		})

		// 3. Mutation data extension
		fieldType2 := cloneFieldType(field.Type, pos)
		emit(&ast.Definition{
			Kind:     ast.InputObject,
			Name:     typeName + "_mut_data",
			Position: pos,
			Fields: ast.FieldList{
				{Name: fieldName, Type: fieldType2, Position: pos},
			},
		})
	}

	// 4. Aggregation type extension (only scalar, non-list, aggregatable)
	if s != nil && !isList {
		if agg, ok := s.(types.Aggregatable); ok {
			aggFieldType := agg.AggregationTypeName()
			aggField := &ast.FieldDefinition{
				Name: fieldName,
				Type: ast.NamedType(aggFieldType, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(fieldName, pos),
				},
				Position: pos,
			}
			// Copy field arguments (bucket, transforms, etc.)
			if fap, ok := s.(types.FieldArgumentsProvider); ok {
				aggField.Arguments = fap.FieldArguments()
			}
			emit(&ast.Definition{
				Kind:     ast.Object,
				Name:     "_" + typeName + "_aggregation",
				Position: pos,
				Fields:   ast.FieldList{aggField},
			})

			// 5. Sub-aggregation type extension (if scalar has SubAggregation variant)
			if sub, ok := s.(types.SubAggregatable); ok {
				subAggFieldType := sub.SubAggregationTypeName()
				subAggField := &ast.FieldDefinition{
					Name: fieldName,
					Type: ast.NamedType(subAggFieldType, pos),
					Directives: ast.DirectiveList{
						fieldAggregationDirective(fieldName, pos),
					},
					Position: pos,
				}
				if fap, ok := s.(types.FieldArgumentsProvider); ok {
					subAggField.Arguments = fap.FieldArguments()
				}
				emit(&ast.Definition{
					Kind:     ast.Object,
					Name:     "_" + typeName + "_aggregation_sub_aggregation",
					Position: pos,
					Fields:   ast.FieldList{subAggField},
				})
			}
		}
	}
}

// EmitScalarFieldDrop generates @drop extensions for all derived types
// when a scalar field is removed.
func EmitScalarFieldDrop(
	emit func(*ast.Definition),
	typeName string,
	fieldName string,
	opts base.Options,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-field-drop")

	// Drop from filter input
	EmitDropFieldOnTypeIfExists(emit, typeName+"_filter", fieldName, pos, typeExists)

	// Drop from mutation inputs (unless readonly)
	if !opts.ReadOnly {
		EmitDropFieldOnTypeIfExists(emit, typeName+"_mut_input_data", fieldName, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, typeName+"_mut_data", fieldName, pos, typeExists)
	}

	// Drop from aggregation type
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation", fieldName, pos, typeExists)

	// Drop from sub-aggregation types (only if they exist — not all types have them)
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation_sub_aggregation", fieldName, pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation_sub_aggregation_sub_aggregation", fieldName, pos, typeExists)
}

// EmitVirtualFieldAggregations generates aggregation extensions for
// @join/@table_function_call_join/@function_call fields.
func EmitVirtualFieldAggregations(
	emit func(*ast.Definition),
	typeName string,
	field *ast.FieldDefinition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-virtual-field-add")

	isFuncCall := field.Directives.ForName("function_call") != nil

	if isFuncCall {
		emitFuncCallAggExtensions(emit, typeName, field, scalarLookup, typeExists, pos)
		return
	}

	isJoin := field.Directives.ForName("join") != nil
	isTFCJ := field.Directives.ForName("table_function_call_join") != nil
	if !isJoin && !isTFCJ {
		return
	}

	// @join and @table_function_call_join: only list fields
	if field.Type.NamedType != "" {
		return
	}

	targetName := field.Type.Name()
	targetFilterName := targetName + "_filter"
	targetAggName := "_" + targetName + "_aggregation"
	bucketAggName := "_" + targetName + "_aggregation_bucket"
	subAggName := AggTypeNameAtDepth(targetName, 1)
	aggTypeName := "_" + typeName + "_aggregation"

	catDir := CatalogDirective(opts.Name, opts.EngineType)

	// Add @catalog directive to the virtual field so the planner knows the engine
	if field.Directives.ForName(base.CatalogDirectiveName) == nil {
		field.Directives = append(field.Directives, catDir)
	}

	if isJoin {
		// Add subquery args to the field itself
		if len(field.Arguments) == 0 {
			field.Arguments = SubQueryArgs(targetFilterName, pos)
		}

		// Add {name}_aggregation and {name}_bucket_aggregation on base object
		emit(&ast.Definition{
			Kind:     ast.Object,
			Name:     typeName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:        field.Name + "_aggregation",
					Description: "The aggregation for " + field.Name,
					Type:        ast.NamedType(targetAggName, pos),
					Arguments:   SubQueryArgs(targetFilterName, pos),
					Directives: ast.DirectiveList{
						{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
							{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: base.ArgName, Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catDir,
					},
					Position: pos,
				},
				{
					Name:        field.Name + "_bucket_aggregation",
					Description: "The bucket aggregation for " + field.Name,
					Type:        ast.ListType(ast.NamedType(bucketAggName, pos), pos),
					Arguments:   SubQueryArgs(targetFilterName, pos),
					Directives: ast.DirectiveList{
						{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
							{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: base.ArgName, Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catDir,
					},
					Position: pos,
				},
			},
		})

		// Add direct agg + sub-agg fields on aggregation type
		if typeExists(aggTypeName) {
			emit(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      field.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: AggRefArgs(targetFilterName, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      field.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: AggSubRefArgs(targetFilterName, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
				},
			})
		}
	} else if isTFCJ {
		// @table_function_call_join: use original field arguments
		origArgs := CloneArgDefs(field.Arguments, pos)

		// Add {name}_aggregation and {name}_bucket_aggregation on base object
		emit(&ast.Definition{
			Kind:     ast.Object,
			Name:     typeName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:        field.Name + "_aggregation",
					Description: "The aggregation for " + field.Name,
					Type:        ast.NamedType(targetAggName, pos),
					Arguments:   origArgs,
					Directives: ast.DirectiveList{
						{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
							{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: base.ArgName, Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catDir,
					},
					Position: pos,
				},
				{
					Name:        field.Name + "_bucket_aggregation",
					Description: "The bucket aggregation for " + field.Name,
					Type:        ast.ListType(ast.NamedType(bucketAggName, pos), pos),
					Arguments:   CloneArgDefs(field.Arguments, pos),
					Directives: ast.DirectiveList{
						{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
							{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: base.ArgName, Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catDir,
					},
					Position: pos,
				},
			},
		})

		// Add direct agg + sub-agg fields on aggregation type
		if typeExists(aggTypeName) {
			emit(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      field.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: CloneArgDefs(field.Arguments, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      field.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: CloneArgDefs(field.Arguments, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
				},
			})
		}
	}
}

// EmitVirtualFieldAggDrop drops aggregation fields generated by virtual fields.
func EmitVirtualFieldAggDrop(
	emit func(*ast.Definition),
	typeName string,
	fieldName string,
	existingField *ast.FieldDefinition,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-virtual-field-drop")
	aggTypeName := "_" + typeName + "_aggregation"

	isFuncCall := existingField.Directives.ForName("function_call") != nil

	if isFuncCall {
		// @function_call: only added a field on the aggregation type
		EmitDropFieldOnTypeIfExists(emit, aggTypeName, fieldName, pos, typeExists)
		return
	}

	// @join and @table_function_call_join: drop aggregation/bucket_aggregation on base type
	EmitDropFieldOnTypeIfExists(emit, typeName, fieldName+"_aggregation", pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, typeName, fieldName+"_bucket_aggregation", pos, typeExists)

	// Drop direct agg + sub-agg fields on aggregation type
	EmitDropFieldOnTypeIfExists(emit, aggTypeName, fieldName, pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, aggTypeName, fieldName+"_aggregation", pos, typeExists)
}

// EmitReferenceFieldsDrop drops all reference-generated fields when a
// field with @field_references is removed. It looks up the corresponding
// @references directive on the type to find the generated field names.
func EmitReferenceFieldsDrop(
	emit func(*ast.Definition),
	typeDef *ast.Definition,
	fieldName string,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-ref-drop")

	// Find the @references directive that corresponds to this field.
	var refDir *ast.Directive
	for _, d := range typeDef.Directives.ForNames(base.ReferencesDirectiveName) {
		sourceFields := base.DirectiveArgStrings(d, base.ArgSourceFields)
		for _, sf := range sourceFields {
			if sf == fieldName {
				refDir = d
				break
			}
		}
		if refDir != nil {
			break
		}
	}
	if refDir == nil {
		return
	}

	query := base.DirectiveArgString(refDir, base.ArgQuery)
	refQuery := base.DirectiveArgString(refDir, base.ArgReferencesQuery)
	refName := base.DirectiveArgString(refDir, base.ArgReferencesName)
	isM2M := base.DirectiveArgString(refDir, base.ArgIsM2M) == "true"
	typeName := typeDef.Name

	// Drop forward reference field on source type
	EmitDropFieldOnTypeIfExists(emit, typeName, query, pos, typeExists)

	// Drop forward reference filter field on source filter
	EmitDropFieldOnTypeIfExists(emit, typeName+"_filter", query, pos, typeExists)

	// Drop forward reference aggregation field on source aggregation type
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation", query, pos, typeExists)

	// Drop forward reference mutation input fields (same-catalog references)
	EmitDropFieldOnTypeIfExists(emit, typeName+"_mut_input_data", query, pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, typeName+"_mut_data", query, pos, typeExists)

	if isM2M {
		// For M2M, the forward ref is a list — drop aggregation/bucket_aggregation on base
		EmitDropFieldOnTypeIfExists(emit, typeName, query+"_aggregation", pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, typeName, query+"_bucket_aggregation", pos, typeExists)
		// Also drop sub-agg fields
		EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation", query+"_aggregation", pos, typeExists)
	}

	// Drop back-reference fields on target type (if refQuery is set and not M2M)
	if refQuery != "" && !isM2M && refName != "" {
		EmitDropFieldOnTypeIfExists(emit, refName, refQuery, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, refName, refQuery+"_aggregation", pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, refName, refQuery+"_bucket_aggregation", pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, refName+"_filter", refQuery, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, "_"+refName+"_aggregation", refQuery, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, "_"+refName+"_aggregation", refQuery+"_aggregation", pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, "_"+refName+"_aggregation_sub_aggregation", refQuery, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, "_"+refName+"_aggregation_sub_aggregation", refQuery+"_aggregation", pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, refName+"_mut_input_data", refQuery, pos, typeExists)
		EmitDropFieldOnTypeIfExists(emit, refName+"_mut_data", refQuery, pos, typeExists)
	}

	// Drop @references directive from the type
	dirName := base.DirectiveArgString(refDir, base.ArgName)
	if dirName != "" {
		emit(&ast.Definition{
			Kind:     ast.Object,
			Name:     typeName,
			Position: pos,
			Directives: ast.DirectiveList{
				{
					Name: base.DropDirectiveDirectiveName,
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: base.ReferencesDirectiveName, Kind: ast.StringValue, Position: pos}, Position: pos},
						{Name: "match", Value: &ast.Value{
							Kind: ast.ObjectValue,
							Children: ast.ChildValueList{
								{Name: "name", Value: &ast.Value{Raw: dirName, Kind: ast.StringValue, Position: pos}},
							},
							Position: pos,
						}, Position: pos},
					},
					Position: pos,
				},
			},
		})
	}
}

// EmitFunctionFieldDrop drops a function field and its aggregation fields from
// the Function/MutationFunction type (or module function type).
func EmitFunctionFieldDrop(
	emit func(*ast.Definition),
	funcTypeName string,
	fieldName string,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-func-drop")

	// Drop the function field itself
	EmitDropFieldOnType(emit, funcTypeName, fieldName, pos)

	// Drop aggregation fields
	EmitDropFieldOnTypeIfExists(emit, funcTypeName, fieldName+"_aggregation", pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, funcTypeName, fieldName+"_bucket_aggregation", pos, typeExists)
}

// EmitExtraFieldAdd generates extra fields (like _departure_part for Timestamp,
// _geom_measurement for Geometry) when a scalar field is added. This mirrors what
// ExtraFieldRule does during full compilation.
func EmitExtraFieldAdd(
	emit func(*ast.Definition),
	typeName string,
	field *ast.FieldDefinition,
	scalarLookup func(string) types.ScalarType,
) {
	if isVirtualField(field) {
		return
	}
	scalarName := field.Type.Name()
	s := scalarLookup(scalarName)
	if s == nil {
		return
	}
	efp, ok := s.(types.ExtraFieldProvider)
	if !ok {
		return
	}
	extraField := efp.GenerateExtraField(field.Name)
	if extraField == nil {
		return
	}
	pos := base.CompiledPos("incremental-extra-field-add")

	// Add extra field to the base type
	emit(&ast.Definition{
		Kind:     ast.Object,
		Name:     typeName,
		Position: pos,
		Fields:   ast.FieldList{extraField},
	})

	// Add extra field to the aggregation type (if scalar is aggregatable)
	aggField := incrementalExtraFieldForAggregation(extraField, scalarLookup, pos)
	if aggField != nil {
		emit(&ast.Definition{
			Kind:     ast.Object,
			Name:     "_" + typeName + "_aggregation",
			Position: pos,
			Fields:   ast.FieldList{aggField},
		})

		// Add SubAggregation variant
		subAggName := AggTypeNameAtDepth(typeName, 1)
		subTypeName := scalarSubAggTypeName(aggField.Type.Name())
		if subTypeName != "" {
			subField := &ast.FieldDefinition{
				Name:     aggField.Name,
				Type:     ast.NamedType(subTypeName, pos),
				Position: pos,
			}
			if len(aggField.Directives) > 0 {
				subField.Directives = make(ast.DirectiveList, len(aggField.Directives))
				copy(subField.Directives, aggField.Directives)
			}
			if len(aggField.Arguments) > 0 {
				subField.Arguments = make(ast.ArgumentDefinitionList, len(aggField.Arguments))
				copy(subField.Arguments, aggField.Arguments)
			}
			emit(&ast.Definition{
				Kind:     ast.Object,
				Name:     subAggName,
				Position: pos,
				Fields:   ast.FieldList{subField},
			})
		}
	}
}

// EmitExtraFieldDrop drops extra fields generated by ExtraFieldProvider when
// the base field is removed. For example, dropping a Timestamp field also drops
// the _field_part extra field from the type and its aggregation types.
func EmitExtraFieldDrop(
	emit func(*ast.Definition),
	typeName string,
	fieldName string,
	scalarLookup func(string) types.ScalarType,
	existingFieldTypeName string,
	typeExists func(string) bool,
) {
	s := scalarLookup(existingFieldTypeName)
	if s == nil {
		return
	}
	efp, ok := s.(types.ExtraFieldProvider)
	if !ok {
		return
	}
	extraField := efp.GenerateExtraField(fieldName)
	if extraField == nil {
		return
	}
	pos := base.CompiledPos("incremental-extra-field-drop")

	// Drop extra field from the base type
	EmitDropFieldOnType(emit, typeName, extraField.Name, pos)

	// Drop from aggregation types
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation", extraField.Name, pos, typeExists)
	EmitDropFieldOnTypeIfExists(emit, "_"+typeName+"_aggregation_sub_aggregation", extraField.Name, pos, typeExists)
}

// incrementalExtraFieldForAggregation creates an aggregation version of an extra field.
// Uses scalarLookup instead of CompilationContext for use in incremental compiler.
func incrementalExtraFieldForAggregation(field *ast.FieldDefinition, scalarLookup func(string) types.ScalarType, pos *ast.Position) *ast.FieldDefinition {
	typeName := field.Type.Name()
	s := scalarLookup(typeName)
	if s == nil {
		return nil
	}
	a, ok := s.(types.Aggregatable)
	if !ok {
		return nil
	}
	aggTypeName := a.AggregationTypeName()

	aggField := &ast.FieldDefinition{
		Name:     field.Name,
		Type:     ast.NamedType(aggTypeName, pos),
		Position: pos,
		Directives: ast.DirectiveList{
			fieldAggregationDirective(field.Name, pos),
		},
	}
	if len(field.Arguments) > 0 {
		aggField.Arguments = make(ast.ArgumentDefinitionList, len(field.Arguments))
		copy(aggField.Arguments, field.Arguments)
	}
	return aggField
}

// --- Low-level helpers ---

// EmitDropFieldOnTypeIfExists emits a drop extension only if the target type
// exists (according to typeExists). Prevents "definition not found" errors
// when dropping fields from derived types that may not have been created.
func EmitDropFieldOnTypeIfExists(emit func(*ast.Definition), targetType, fieldName string, pos *ast.Position, typeExists func(string) bool) {
	if typeExists != nil && !typeExists(targetType) {
		return
	}
	EmitDropFieldOnType(emit, targetType, fieldName, pos)
}

// EmitDropFieldOnType emits an extension with @drop(if_exists: true) for a
// single field on a target type.
func EmitDropFieldOnType(emit func(*ast.Definition), targetType, fieldName string, pos *ast.Position) {
	emit(&ast.Definition{
		Kind:     ast.Object,
		Name:     targetType,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name: fieldName,
				Type: ast.NamedType("Boolean", pos),
				Directives: ast.DirectiveList{
					DropIfExistsDirective(pos),
				},
				Position: pos,
			},
		},
	})
}

// DropIfExistsDirective creates a @drop(if_exists: true) directive.
func DropIfExistsDirective(pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: base.DropDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "if_exists", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// CatalogDirective creates a @catalog(name, engine) directive.
// This is the exported version for use by the incremental compiler.
func CatalogDirective(name, engine string) *ast.Directive {
	return catalogDirective(name, engine)
}

// emitFuncCallAggExtensions adds aggregation fields for a @function_call scalar
// field on the aggregation type (if the scalar is aggregatable).
func emitFuncCallAggExtensions(
	emit func(*ast.Definition),
	typeName string,
	field *ast.FieldDefinition,
	scalarLookup func(string) types.ScalarType,
	typeExists func(string) bool,
	pos *ast.Position,
) {
	scalarName := field.Type.Name()
	s := scalarLookup(scalarName)
	if s == nil {
		return
	}
	agg, ok := s.(types.Aggregatable)
	if !ok {
		return
	}
	aggTypeName := "_" + typeName + "_aggregation"
	if !typeExists(aggTypeName) {
		return
	}

	aggFieldType := agg.AggregationTypeName()
	emit(&ast.Definition{
		Kind:     ast.Object,
		Name:     aggTypeName,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name: field.Name,
				Type: ast.NamedType(aggFieldType, pos),
				Directives: ast.DirectiveList{
					fieldAggregationDirective(field.Name, pos),
				},
				Position: pos,
			},
		},
	})
}

// cloneFieldType creates a simple clone of an ast.Type for use in extensions.
func cloneFieldType(t *ast.Type, pos *ast.Position) *ast.Type {
	if t == nil {
		return nil
	}
	if t.Elem != nil {
		return ast.ListType(cloneFieldType(t.Elem, pos), pos)
	}
	result := ast.NamedType(t.NamedType, pos)
	result.NonNull = t.NonNull
	return result
}
