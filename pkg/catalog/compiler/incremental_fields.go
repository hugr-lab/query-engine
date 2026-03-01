package compiler

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// emitFieldAddExtensions generates extensions for all derived types when a new
// scalar field is added to an existing type. This covers:
//   - filter input type (scalar → filter type mapping)
//   - mutation input data (unless readonly)
//   - mutation data (unless readonly)
//   - aggregation type (if scalar is Aggregatable)
//   - aggregation bucket type (all agg fields are bucket fields)
//
// Virtual fields (@join, @function_call, @table_function_call_join) are excluded
// from all derived types — they only appear on the base type itself.
func emitFieldAddExtensions(
	output *indexedOutput,
	typeName string,
	field *ast.FieldDefinition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
) {
	// Virtual fields are excluded from all derived types (filter, mutation, aggregation).
	// They only appear on the base type itself.
	if isIncrementalVirtualField(field) {
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
			output.AddExtension(&ast.Definition{
				Kind:     ast.InputObject,
				Name:     typeName + "_filter",
				Position: pos,
				Fields:   ast.FieldList{filterField},
			})
		}
	}

	// 2. Mutation input data extension (skip virtual fields, computed fields, readonly)
	if !opts.ReadOnly && !isIncrementalVirtualField(field) && field.Directives.ForName("sql") == nil {
		fieldType := cloneFieldType(field.Type, pos)
		output.AddExtension(&ast.Definition{
			Kind:     ast.InputObject,
			Name:     typeName + "_mut_input_data",
			Position: pos,
			Fields: ast.FieldList{
				{Name: fieldName, Type: fieldType, Position: pos},
			},
		})

		// 3. Mutation data extension
		fieldType2 := cloneFieldType(field.Type, pos)
		output.AddExtension(&ast.Definition{
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
			output.AddExtension(&ast.Definition{
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
				output.AddExtension(&ast.Definition{
					Kind:     ast.Object,
					Name:     "_" + typeName + "_aggregation_sub_aggregation",
					Position: pos,
					Fields:   ast.FieldList{subAggField},
				})
			}

			// 6. Bucket aggregation already includes key (base type) which will
			// pick up the new field automatically. No explicit extension needed.
		}
	}
}

// emitFieldDropExtensions generates @drop(if_exists: true) extension fields
// for all derived types when a field is removed from a type.
// The typeExists function is used to check if a derived type exists in the
// provider before emitting a drop extension (to avoid "definition not found" errors).
func emitFieldDropExtensions(
	output *indexedOutput,
	typeName string,
	fieldName string,
	opts base.Options,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-field-drop")

	// Drop from filter input
	emitDropFieldOnTypeIfExists(output, typeName+"_filter", fieldName, pos, typeExists)

	// Drop from mutation inputs (unless readonly)
	if !opts.ReadOnly {
		emitDropFieldOnTypeIfExists(output, typeName+"_mut_input_data", fieldName, pos, typeExists)
		emitDropFieldOnTypeIfExists(output, typeName+"_mut_data", fieldName, pos, typeExists)
	}

	// Drop from aggregation type
	emitDropFieldOnTypeIfExists(output, "_"+typeName+"_aggregation", fieldName, pos, typeExists)

	// Drop from sub-aggregation types (only if they exist — not all types have them)
	emitDropFieldOnTypeIfExists(output, "_"+typeName+"_aggregation_sub_aggregation", fieldName, pos, typeExists)
	emitDropFieldOnTypeIfExists(output, "_"+typeName+"_aggregation_sub_aggregation_sub_aggregation", fieldName, pos, typeExists)
}

// emitFieldReplaceExtensions combines drop + add for a field replacement.
func emitFieldReplaceExtensions(
	output *indexedOutput,
	typeName string,
	field *ast.FieldDefinition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
	typeExists func(string) bool,
) {
	emitFieldDropExtensions(output, typeName, field.Name, opts, typeExists)
	emitFieldAddExtensions(output, typeName, field, opts, scalarLookup)
}

// emitDropFieldOnTypeIfExists emits a drop extension only if the target type
// exists (according to typeExists). Prevents "definition not found" errors
// when dropping fields from derived types that may not have been created.
func emitDropFieldOnTypeIfExists(output *indexedOutput, targetType, fieldName string, pos *ast.Position, typeExists func(string) bool) {
	if typeExists != nil && !typeExists(targetType) {
		return
	}
	emitDropFieldOnType(output, targetType, fieldName, pos)
}

// emitDropFieldOnType emits an extension with @drop(if_exists: true) for a
// single field on a target type.
func emitDropFieldOnType(output *indexedOutput, targetType, fieldName string, pos *ast.Position) {
	output.AddExtension(&ast.Definition{
		Kind:     ast.Object, // Kind doesn't matter for extensions — they target existing types
		Name:     targetType,
		Position: pos,
		Fields: ast.FieldList{
			{
				Name: fieldName,
				Type: ast.NamedType("Boolean", pos),
				Directives: ast.DirectiveList{
					dropIfExistsDirective(pos),
				},
				Position: pos,
			},
		},
	})
}

// fieldAggregationDirective creates a @field_aggregation(name=X) directive.
func fieldAggregationDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: "field_aggregation",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// dropIfExistsDirective creates a @drop(if_exists: true) directive.
func dropIfExistsDirective(pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: base.DropDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "if_exists", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// isIncrementalVirtualField checks if a field is virtual (function_call, join, table_function_call_join).
func isIncrementalVirtualField(f *ast.FieldDefinition) bool {
	return f.Directives.ForName("function_call") != nil ||
		f.Directives.ForName("table_function_call_join") != nil ||
		f.Directives.ForName("join") != nil
}

// emitVirtualFieldAddExtensions generates aggregation extensions when a virtual
// field (@join or @table_function_call_join) is added to an existing type.
// For @join list fields: adds subquery args to the field, {name}_aggregation and
// {name}_bucket_aggregation on base type, direct agg + sub-agg on aggregation type.
// For @table_function_call_join list fields: preserves original args, adds same
// aggregation structure.
// For @function_call scalar fields: adds aggregation field if scalar is aggregatable.
func emitVirtualFieldAddExtensions(
	output *indexedOutput,
	typeName string,
	field *ast.FieldDefinition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
	typeExists func(string) bool,
	providerLookup func(string) *ast.Definition,
) {
	pos := base.CompiledPos("incremental-virtual-field-add")

	isJoin := field.Directives.ForName("join") != nil
	isTFCJ := field.Directives.ForName("table_function_call_join") != nil
	isFuncCall := field.Directives.ForName("function_call") != nil

	if isFuncCall {
		emitFuncCallFieldAggExtensions(output, typeName, field, scalarLookup, typeExists, pos)
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
	subAggName := rules.AggTypeNameAtDepth(targetName, 1)
	aggTypeName := "_" + typeName + "_aggregation"

	catDir := catalogDirective(opts.Name, opts.EngineType)

	// Add @catalog directive to the virtual field so the planner knows the engine
	if field.Directives.ForName(base.CatalogDirectiveName) == nil {
		field.Directives = append(field.Directives, catDir)
	}

	if isJoin {
		// Add subquery args to the field itself
		if len(field.Arguments) == 0 {
			field.Arguments = rules.SubQueryArgs(targetFilterName, pos)
		}

		// Add {name}_aggregation and {name}_bucket_aggregation on base object
		output.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     typeName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name:        field.Name + "_aggregation",
					Description: "The aggregation for " + field.Name,
					Type:        ast.NamedType(targetAggName, pos),
					Arguments:   rules.SubQueryArgs(targetFilterName, pos),
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
					Arguments:   rules.SubQueryArgs(targetFilterName, pos),
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
			output.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      field.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: rules.AggRefArgs(targetFilterName, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      field.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: rules.AggSubRefArgs(targetFilterName, pos),
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
		origArgs := rules.CloneArgDefs(field.Arguments, pos)

		// Add {name}_aggregation and {name}_bucket_aggregation on base object
		output.AddExtension(&ast.Definition{
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
					Arguments:   rules.CloneArgDefs(field.Arguments, pos),
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
			output.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      field.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: rules.CloneArgDefs(field.Arguments, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(field.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      field.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: rules.CloneArgDefs(field.Arguments, pos),
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

// emitFuncCallFieldAggExtensions adds aggregation fields for a @function_call scalar
// field on the aggregation type (if the scalar is aggregatable).
func emitFuncCallFieldAggExtensions(
	output *indexedOutput,
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
	output.AddExtension(&ast.Definition{
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

// emitVirtualFieldDropExtensions drops aggregation fields generated by a virtual
// field (@join, @function_call, @table_function_call_join) when that field is dropped.
func emitVirtualFieldDropExtensions(
	output *indexedOutput,
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
		emitDropFieldOnTypeIfExists(output, aggTypeName, fieldName, pos, typeExists)
		return
	}

	// @join and @table_function_call_join: drop aggregation/bucket_aggregation on base type
	emitDropFieldOnTypeIfExists(output, typeName, fieldName+"_aggregation", pos, typeExists)
	emitDropFieldOnTypeIfExists(output, typeName, fieldName+"_bucket_aggregation", pos, typeExists)

	// Drop direct agg + sub-agg fields on aggregation type
	emitDropFieldOnTypeIfExists(output, aggTypeName, fieldName, pos, typeExists)
	emitDropFieldOnTypeIfExists(output, aggTypeName, fieldName+"_aggregation", pos, typeExists)
}

// emitReferenceDropExtensions drops all reference-generated fields when a
// field with @field_references is removed. It looks up the corresponding
// @references directive on the type in the provider to find the generated
// field names (forward/back query fields, filter refs, aggregation refs).
func emitReferenceDropExtensions(
	output *indexedOutput,
	typeDef *ast.Definition,
	fieldName string,
	typeExists func(string) bool,
) {
	pos := base.CompiledPos("incremental-ref-drop")

	// Find the @references directive that corresponds to this field.
	// Match by checking if source_fields contains our fieldName.
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
	emitDropFieldOnTypeIfExists(output, typeName, query, pos, typeExists)

	// Drop forward reference filter field on source filter
	emitDropFieldOnTypeIfExists(output, typeName+"_filter", query, pos, typeExists)

	// Drop forward reference aggregation field on source aggregation type
	emitDropFieldOnTypeIfExists(output, "_"+typeName+"_aggregation", query, pos, typeExists)

	// Drop forward reference mutation input fields (same-catalog references)
	emitDropFieldOnTypeIfExists(output, typeName+"_mut_input_data", query, pos, typeExists)
	emitDropFieldOnTypeIfExists(output, typeName+"_mut_data", query, pos, typeExists)

	if isM2M {
		// For M2M, the forward ref is a list — drop aggregation/bucket_aggregation on base
		emitDropFieldOnTypeIfExists(output, typeName, query+"_aggregation", pos, typeExists)
		emitDropFieldOnTypeIfExists(output, typeName, query+"_bucket_aggregation", pos, typeExists)
		// Also drop sub-agg fields
		emitDropFieldOnTypeIfExists(output, "_"+typeName+"_aggregation", query+"_aggregation", pos, typeExists)
	}

	// Drop back-reference fields on target type (if refQuery is set and not M2M)
	if refQuery != "" && !isM2M && refName != "" {
		// Drop back-reference query field
		emitDropFieldOnTypeIfExists(output, refName, refQuery, pos, typeExists)

		// Drop back-reference aggregation and bucket_aggregation on target base type
		emitDropFieldOnTypeIfExists(output, refName, refQuery+"_aggregation", pos, typeExists)
		emitDropFieldOnTypeIfExists(output, refName, refQuery+"_bucket_aggregation", pos, typeExists)

		// Drop back-reference filter field on target filter
		emitDropFieldOnTypeIfExists(output, refName+"_filter", refQuery, pos, typeExists)

		// Drop back-reference fields on target aggregation type
		emitDropFieldOnTypeIfExists(output, "_"+refName+"_aggregation", refQuery, pos, typeExists)
		emitDropFieldOnTypeIfExists(output, "_"+refName+"_aggregation", refQuery+"_aggregation", pos, typeExists)

		// Drop back-reference fields on sub-aggregation types
		emitDropFieldOnTypeIfExists(output, "_"+refName+"_aggregation_sub_aggregation", refQuery, pos, typeExists)
		emitDropFieldOnTypeIfExists(output, "_"+refName+"_aggregation_sub_aggregation", refQuery+"_aggregation", pos, typeExists)

		// Drop back-reference mutation input fields (same-catalog references)
		emitDropFieldOnTypeIfExists(output, refName+"_mut_input_data", refQuery, pos, typeExists)
		emitDropFieldOnTypeIfExists(output, refName+"_mut_data", refQuery, pos, typeExists)
	}

	// Drop @references directive from the type
	dirName := base.DirectiveArgString(refDir, base.ArgName)
	if dirName != "" {
		output.AddExtension(&ast.Definition{
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

// hasFieldReferences checks if an extension has any fields with @field_references.
func hasFieldReferences(ext *ast.Definition) bool {
	for _, f := range ext.Fields {
		if f.Directives.ForName("field_references") != nil {
			return true
		}
	}
	return false
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

// emitModuleChangeExtensions moves a type's query/mutation fields from the old
// module type hierarchy to a new one when a type's @module directive changes.
//
// This handles:
//  1. Drop query/mutation fields from old module types
//  2. Create new module types if they don't exist
//  3. Copy field definitions to new module types
//  4. Add new module entry on Query/Mutation if needed
func emitModuleChangeExtensions(
	output *indexedOutput,
	typeDef *ast.Definition,
	oldModule string,
	newModule string,
	opts base.Options,
	typeExists func(string) bool,
	providerLookup func(string) *ast.Definition,
) {
	pos := base.CompiledPos("incremental-module-change")

	// Compute full module paths (as_module prepends catalog name)
	oldModPath := oldModule
	newModPath := newModule
	if opts.AsModule {
		oldModPath = opts.Name + "." + oldModule
		newModPath = opts.Name + "." + newModule
	}

	oldQueryType := moduleLeafTypeName(oldModPath, "query")
	oldMutType := moduleLeafTypeName(oldModPath, "mutation")
	newQueryType := moduleLeafTypeName(newModPath, "query")
	newMutType := moduleLeafTypeName(newModPath, "mutation")

	// Extract field names from @query/@mutation directives on the type
	var queryFieldNames []string
	for _, d := range typeDef.Directives.ForNames(base.QueryDirectiveName) {
		if name := base.DirectiveArgString(d, base.ArgName); name != "" {
			queryFieldNames = append(queryFieldNames, name)
		}
	}
	var mutFieldNames []string
	for _, d := range typeDef.Directives.ForNames(base.MutationDirectiveName) {
		if name := base.DirectiveArgString(d, base.ArgName); name != "" {
			mutFieldNames = append(mutFieldNames, name)
		}
	}

	// Look up old module types in provider to copy field definitions
	oldQueryDef := providerLookup(oldQueryType)
	oldMutDef := providerLookup(oldMutType)

	modCatDir := incrementalModuleCatalogDirective(opts.Name, pos)

	// 1. Drop fields from old module types
	for _, fieldName := range queryFieldNames {
		emitDropFieldOnTypeIfExists(output, oldQueryType, fieldName, pos, typeExists)
	}
	for _, fieldName := range mutFieldNames {
		emitDropFieldOnTypeIfExists(output, oldMutType, fieldName, pos, typeExists)
	}

	// 2. Create new module types if they don't exist
	if !typeExists(newQueryType) {
		output.AddDefinition(&ast.Definition{
			Kind:     ast.Object,
			Name:     newQueryType,
			Position: pos,
			Directives: ast.DirectiveList{
				incrementalModuleRootDirective(newModPath, "QUERY", pos),
				modCatDir,
			},
			Fields: ast.FieldList{
				{Name: "_stub", Type: ast.NamedType("Boolean", pos), Position: pos},
			},
		})
	}
	if len(mutFieldNames) > 0 && !typeExists(newMutType) {
		output.AddDefinition(&ast.Definition{
			Kind:     ast.Object,
			Name:     newMutType,
			Position: pos,
			Directives: ast.DirectiveList{
				incrementalModuleRootDirective(newModPath, "MUTATION", pos),
				modCatDir,
			},
			Fields: ast.FieldList{
				{Name: "_stub", Type: ast.NamedType("Boolean", pos), Position: pos},
			},
		})
	}

	// 3. Copy fields from old module types to new ones
	if oldQueryDef != nil {
		var fields ast.FieldList
		for _, name := range queryFieldNames {
			if f := oldQueryDef.Fields.ForName(name); f != nil {
				fields = append(fields, f)
			}
		}
		if len(fields) > 0 {
			output.AddExtension(&ast.Definition{
				Kind: ast.Object, Name: newQueryType, Position: pos,
				Fields: fields,
			})
		}
	}
	if oldMutDef != nil {
		var fields ast.FieldList
		for _, name := range mutFieldNames {
			if f := oldMutDef.Fields.ForName(name); f != nil {
				fields = append(fields, f)
			}
		}
		if len(fields) > 0 {
			output.AddExtension(&ast.Definition{
				Kind: ast.Object, Name: newMutType, Position: pos,
				Fields: fields,
			})
		}
	}

	// 4. Add new module entry on Query/Mutation if the module root doesn't exist yet
	topMod := strings.Split(newModPath, ".")[0]
	topModQueryType := newQueryType
	topModMutType := newMutType
	// For dotted paths, the top-level type uses only the first segment
	if strings.Contains(newModPath, ".") {
		topModQueryType = moduleLeafTypeName(topMod, "query")
		topModMutType = moduleLeafTypeName(topMod, "mutation")
	}

	if queryDef := providerLookup("Query"); queryDef != nil {
		if queryDef.Fields.ForName(topMod) == nil {
			output.AddExtension(&ast.Definition{
				Kind: ast.Object, Name: "Query", Position: pos,
				Fields: ast.FieldList{
					{Name: topMod, Type: ast.NamedType(topModQueryType, pos), Position: pos,
						Directives: ast.DirectiveList{modCatDir}},
				},
			})
		}
	}
	if len(mutFieldNames) > 0 {
		if mutDef := providerLookup("Mutation"); mutDef != nil {
			if mutDef.Fields.ForName(topMod) == nil {
				output.AddExtension(&ast.Definition{
					Kind: ast.Object, Name: "Mutation", Position: pos,
					Fields: ast.FieldList{
						{Name: topMod, Type: ast.NamedType(topModMutType, pos), Position: pos,
							Directives: ast.DirectiveList{modCatDir}},
					},
				})
			}
		}
	}
}

// detectModuleChange checks if extension directives represent a module change.
// Returns (oldModule, newModule) — either may be empty if not detectable.
func detectModuleChange(directives ast.DirectiveList) (oldModule, newModule string) {
	for _, d := range directives.ForNames(base.DropDirectiveDirectiveName) {
		if base.DirectiveArgString(d, "name") == base.ModuleDirectiveName {
			// Extract old module name from match argument
			if matchArg := d.Arguments.ForName("match"); matchArg != nil && matchArg.Value != nil {
				for _, child := range matchArg.Value.Children {
					if child.Name == "name" {
						oldModule = child.Value.Raw
					}
				}
			}
		}
	}
	if md := directives.ForName(base.ModuleDirectiveName); md != nil {
		newModule = base.DirectiveArgString(md, base.ArgName)
	}
	return
}

// moduleLeafTypeName computes the module type name for a given module path and suffix.
// Module path dots are replaced with underscores.
func moduleLeafTypeName(modPath, suffix string) string {
	return "_module_" + strings.ReplaceAll(modPath, ".", "_") + "_" + suffix
}

// incrementalModuleRootDirective creates a @module_root(name, type) directive.
func incrementalModuleRootDirective(modPath, rootType string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: base.ModuleRootDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: modPath, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: rootType, Kind: ast.EnumValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// incrementalModuleCatalogDirective creates a @module_catalog(name) directive.
func incrementalModuleCatalogDirective(catalogName string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: base.ModuleCatalogDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: catalogName, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}
