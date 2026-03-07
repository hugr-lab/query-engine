package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*TableRule)(nil)

type TableRule struct{}

func (r *TableRule) Name() string     { return "TableRule" }
func (r *TableRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *TableRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("table") != nil
}

func (r *TableRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}
	opts := ctx.CompileOptions()
	pos := compiledPos(def.Name)

	addDef := ctx.AddDefinition
	if info.IsReplace {
		addDef = ctx.AddDefinitionReplaceOrCreate
	}

	// 1. Add the definition itself to output
	addDef(def)

	// 1b. Add @catalog and @module to function_call and table_function_call_join fields
	for _, f := range def.Fields {
		if !isVirtualField(f) {
			continue
		}
		if f.Directives.ForName("catalog") == nil {
			f.Directives = append(f.Directives, catalogDirective(opts.Name, opts.EngineType))
		}
		// When AsModule, add module=<name> to the function_call/table_function_call_join directive
		if opts.AsModule {
			addModuleToFuncCallDirective(f, opts.Name)
		}
	}

	// 2. Generate filter input type
	filterName := def.Name + "_filter"
	filterDef := generateFilterInput(ctx, def, filterName, pos)
	addDef(filterDef)
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "filter_input",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: filterName, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	})

	// Note: _list_filter types are created lazily by gen_references.go
	// when a back-reference or M2M reference needs them.

	// 3. Generate data input types (unless ReadOnly)
	if !opts.ReadOnly {
		// Insert input: includes all table fields
		insertInputName := def.Name + "_mut_input_data"
		insertInputDef := generateMutInputData(ctx, def, insertInputName, pos)
		addDef(insertInputDef)

		// Update input: includes all table fields (including non-scalar)
		updateInputName := def.Name + "_mut_data"
		updateInputDef := generateMutData(ctx, def, updateInputName, pos)
		addDef(updateInputDef)

		def.Directives = append(def.Directives,
			&ast.Directive{
				Name: "data_input",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			},
			&ast.Directive{
				Name: "data_input",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: updateInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			},
		)
	}

	// 3c. Set scalar-specific field arguments (bucket, transforms, struct, etc.)
	setScalarFieldArguments(ctx, def)

	// Note: aggregation types (_X_aggregation, _X_aggregation_bucket) are generated
	// by AggregationRule which runs after TableRule/ViewRule.

	// Use original (unprefixed) name for query/mutation field names when AsModule
	fieldName := def.Name
	if opts.AsModule && info.OriginalName != "" && info.OriginalName != def.Name {
		fieldName = info.OriginalName
	}

	// 5. Register query fields and add @query directives on def
	queryFields := generateQueryFields(ctx, def, info, filterName, pos)
	ctx.RegisterQueryFields(def.Name, queryFields)
	// Add @query directive for SELECT
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "query",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
		},
		Position: pos,
	})
	// Add @query for by_pk
	if len(info.PrimaryKey) > 0 && !info.IsM2M {
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "query",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: fieldName + "_by_pk", Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "SELECT_ONE", Kind: ast.EnumValue, Position: pos}, Position: pos},
			},
			Position: pos,
		})
	}
	// Note: @query(AGGREGATE/AGGREGATE_BUCKET) directives are added by AggregationRule
	// (must come after UniqueRule to match old compiler directive ordering).

	// 6. Register mutation fields (unless ReadOnly)
	if !opts.ReadOnly {
		mutFields := generateMutationFields(ctx, def, info, pos)
		ctx.RegisterMutationFields(def.Name, mutFields)
		// Add @mutation directives on def
		insertInputName := def.Name + "_mut_input_data"
		updateInputName := def.Name + "_mut_data"
		if opts.SupportInsert() {
			def.Directives = append(def.Directives, &ast.Directive{
				Name: "mutation",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "insert_" + fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "INSERT", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			})
		}
		if opts.SupportUpdate() && (opts.SupportUpdateWithoutPKs() || len(info.PrimaryKey) > 0) {
			def.Directives = append(def.Directives, &ast.Directive{
				Name: "mutation",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "update_" + fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "UPDATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: updateInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			})
		}
		if opts.SupportDelete() && (opts.SupportDeleteWithoutPKs() || len(info.PrimaryKey) > 0) {
			def.Directives = append(def.Directives, &ast.Directive{
				Name: "mutation",
				Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "delete_" + fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "DELETE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			})
		}
	}

	return nil
}


// generateFilterInput creates a <Name>_filter input object with scalar filter
// fields and logical operators (_and, _or, _not).
func generateFilterInput(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	opts := ctx.CompileOptions()
	filterDef := &ast.Definition{
		Kind:        ast.InputObject,
		Name:        name,
		Description: "Filter for " + def.Name + " objects",
		Position:    pos,
		Directives: ast.DirectiveList{
			{Name: "filter_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if isVirtualField(f) {
			continue
		}
		typeName := f.Type.Name()

		if s := ctx.ScalarLookup(typeName); s != nil {
			if fi, ok := s.(types.Filterable); ok {
				filterFieldType := fi.FilterTypeName()
				// Use list filter type for list fields
				if f.Type.NamedType == "" {
					if lfi, ok := s.(types.ListFilterable); ok {
						filterFieldType = lfi.ListFilterTypeName()
					}
				}
				field := &ast.FieldDefinition{
					Name:        f.Name,
					Description: f.Description,
					Type:        ast.NamedType(filterFieldType, pos),
					Position:    pos,
				}
				// Copy directives from original field (pk, default, measurement, timescale_key)
				for _, d := range f.Directives {
					if d.Name == "pk" || d.Name == "default" || d.Name == "measurement" || d.Name == "timescale_key" {
						field.Directives = append(field.Directives, d)
					}
				}
				// @filter_required makes the filter type NonNull
				if f.Directives.ForName("filter_required") != nil {
					field.Type.NonNull = true
				}
				filterDef.Fields = append(filterDef.Fields, field)
			}
		} else if td := lookupObjectDef(ctx, typeName); td != nil && td.Kind == ast.Object && !isTableOrView(td) {
			// For structural Object-typed fields (not tables/views), reference the filter by name.
			// The filter type is created eagerly by PassthroughRule.
			isList := f.Type.NamedType == ""
			filterTypeName := td.Name + "_filter"
			if isList {
				listFilterName := td.Name + "_list_filter"
				if ctx.LookupType(listFilterName) == nil {
					ctx.AddDefinition(generateListFilterInput(td.Name, filterTypeName, listFilterName, opts, pos))
				}
				filterTypeName = listFilterName
			}
			filterDef.Fields = append(filterDef.Fields, &ast.FieldDefinition{
				Name:     f.Name,
				Type:     ast.NamedType(filterTypeName, pos),
				Position: pos,
			})
		}
	}

	// Logical operators — old compiler uses nullable inner type
	filterDef.Fields = append(filterDef.Fields,
		&ast.FieldDefinition{Name: "_and", Type: ast.ListType(ast.NamedType(name, pos), pos), Position: pos},
		&ast.FieldDefinition{Name: "_or", Type: ast.ListType(ast.NamedType(name, pos), pos), Position: pos},
		&ast.FieldDefinition{Name: "_not", Type: ast.NamedType(name, pos), Position: pos},
	)

	return filterDef
}

// generateMutInputData creates a <Name>_mut_input_data input object for insert mutation.
// Includes all table fields. Computed (@sql) fields are skipped.
func generateMutInputData(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	opts := ctx.CompileOptions()
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName(base.FieldSqlDirectiveName) != nil {
			continue // skip computed fields
		}
		if isVirtualField(f) {
			continue
		}

		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, pos)
		isList := f.Type.NamedType == ""
		// For structural Object-typed fields (not tables/views), use the recursively generated insert input type
		if !ctx.IsScalar(typeName) {
			if td := lookupObjectDef(ctx, typeName); td != nil && td.Kind == ast.Object {
				if isTableOrView(td) {
					continue // skip table/view Object references — handled by gen_references
				}
				nestedName := generateNestedMutInputData(ctx, td, pos)
				fieldType = ast.NamedType(nestedName, pos)
			}
		}
		if isList { // preserve list wrapper
			fieldType = ast.ListType(fieldType, pos)
		}
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: pos,
		})
	}

	return inputDef
}

// generateMutData creates a <Name>_mut_data input object for update mutation.
// Includes all table fields (including non-scalar). Computed (@sql) fields are skipped.
func generateMutData(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	opts := ctx.CompileOptions()
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName(base.FieldSqlDirectiveName) != nil {
			continue // skip computed fields
		}
		if isVirtualField(f) {
			continue
		}

		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, pos)
		isList := f.Type.NamedType == ""
		// For structural Object-typed fields (not tables/views), use the recursively generated update data type
		if !ctx.IsScalar(typeName) {
			if td := lookupObjectDef(ctx, typeName); td != nil && td.Kind == ast.Object {
				if isTableOrView(td) {
					continue // skip table/view Object references — handled by gen_references
				}
				nestedName := generateNestedMutData(ctx, td, pos)
				fieldType = ast.NamedType(nestedName, pos)
			}
		}
		if isList { // preserve list wrapper
			fieldType = ast.ListType(fieldType, pos)
		}
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: pos,
		})
	}

	return inputDef
}

// generateListFilterInput creates a <Name>_list_filter input with any_of, all_of, none_of fields.
func generateListFilterInput(objectName, filterName, listFilterName string, opts base.Options, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind:     ast.InputObject,
		Name:     listFilterName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "filter_list_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: objectName, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Fields: ast.FieldList{
			{Name: "any_of", Type: ast.NamedType(filterName, pos), Position: pos},
			{Name: "all_of", Type: ast.NamedType(filterName, pos), Position: pos},
			{Name: "none_of", Type: ast.NamedType(filterName, pos), Position: pos},
		},
	}
}

// generateNestedFilterInput generates a filter input type for a nested Object type.
// If isList is true, wraps it in a list filter (any_of/all_of/none_of).
// Returns the filter type name to reference, or "" if the type cannot be filtered.
func generateNestedFilterInput(ctx base.CompilationContext, def *ast.Definition, isList bool, pos *ast.Position) string {
	opts := ctx.CompileOptions()
	filterName := def.Name + "_filter"

	// Check if already generated
	if ctx.LookupType(filterName) == nil {
		// Generate filter input for the nested Object
		filterDef := &ast.Definition{
			Kind:     ast.InputObject,
			Name:     filterName,
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "filter_input", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
		}
		for _, f := range def.Fields {
			if f.Name == "_stub" {
				continue
			}
			typeName := f.Type.Name()
			if s := ctx.ScalarLookup(typeName); s != nil {
				if fi, ok := s.(types.Filterable); ok {
					filterFieldType := fi.FilterTypeName()
					if f.Type.NamedType == "" {
						if lfi, ok := s.(types.ListFilterable); ok {
							filterFieldType = lfi.ListFilterTypeName()
						}
					}
					filterDef.Fields = append(filterDef.Fields, &ast.FieldDefinition{
						Name:     f.Name,
						Type:     ast.NamedType(filterFieldType, pos),
						Position: pos,
					})
				}
			}
		}
		filterDef.Fields = append(filterDef.Fields,
			&ast.FieldDefinition{Name: "_and", Type: ast.ListType(ast.NamedType(filterName, pos), pos), Position: pos},
			&ast.FieldDefinition{Name: "_or", Type: ast.ListType(ast.NamedType(filterName, pos), pos), Position: pos},
			&ast.FieldDefinition{Name: "_not", Type: ast.NamedType(filterName, pos), Position: pos},
		)
		ctx.AddDefinition(filterDef)
	}

	if isList {
		listFilterName := def.Name + "_list_filter"
		if ctx.LookupType(listFilterName) == nil {
			ctx.AddDefinition(generateListFilterInput(def.Name, filterName, listFilterName, opts, pos))
		}
		return listFilterName
	}
	return filterName
}

// generateNestedMutInputData generates a _mut_input_data input type for a nested Object type.
// Returns the input type name.
func generateNestedMutInputData(ctx base.CompilationContext, def *ast.Definition, pos *ast.Position) string {
	inputName := def.Name + "_mut_input_data"
	if ctx.LookupType(inputName) != nil {
		return inputName
	}
	opts := ctx.CompileOptions()
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     inputName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, pos)
		isList := f.Type.NamedType == ""
		if !ctx.IsScalar(typeName) {
			if td := lookupObjectDef(ctx, typeName); td != nil && td.Kind == ast.Object {
				// Reference by name — the type is created eagerly by PassthroughRule
				fieldType = ast.NamedType(td.Name+"_mut_input_data", pos)
			}
		}
		if isList {
			fieldType = ast.ListType(fieldType, pos)
		}
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: pos,
		})
	}
	ctx.AddDefinition(inputDef)
	return inputName
}

// generateNestedMutData generates a _mut_data input type for a nested Object type.
// Returns the input type name.
func generateNestedMutData(ctx base.CompilationContext, def *ast.Definition, pos *ast.Position) string {
	inputName := def.Name + "_mut_data"
	if ctx.LookupType(inputName) != nil {
		return inputName
	}
	opts := ctx.CompileOptions()
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     inputName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, pos)
		isList := f.Type.NamedType == ""
		if !ctx.IsScalar(typeName) {
			if td := lookupObjectDef(ctx, typeName); td != nil && td.Kind == ast.Object {
				// Reference by name — the type is created eagerly by PassthroughRule
				fieldType = ast.NamedType(td.Name+"_mut_data", pos)
			}
		}
		if isList {
			fieldType = ast.ListType(fieldType, pos)
		}
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: pos,
		})
	}
	ctx.AddDefinition(inputDef)
	return inputName
}

// generateAggregationType creates a _<Name>_aggregation object with _rows_count
// and per-field aggregation type fields.
func generateAggregationType(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	opts := ctx.CompileOptions()
	aggDef := &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
	}

	aggDef.Fields = append(aggDef.Fields, &ast.FieldDefinition{
		Name:     "_rows_count",
		Type:     ast.NamedType("BigInt", pos),
		Position: pos,
	})

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()

		switch {
		case ctx.ScalarLookup(typeName) != nil:
			if f.Type.NamedType == "" {
				continue // skip list scalars — can't aggregate arrays
			}
			s := ctx.ScalarLookup(typeName)
			a, ok := s.(types.Aggregatable)
			if !ok {
				continue
			}
			field := &ast.FieldDefinition{
				Name:     f.Name,
				Type:     ast.NamedType(a.AggregationTypeName(), pos),
				Position: pos,
				Directives: ast.DirectiveList{
					{Name: "field_aggregation", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
				},
			}
			// Copy arguments from original field (bucket, bucket_interval, struct, etc.)
			if len(f.Arguments) > 0 {
				field.Arguments = make(ast.ArgumentDefinitionList, len(f.Arguments))
				copy(field.Arguments, f.Arguments)
			}
			aggDef.Fields = append(aggDef.Fields, field)

		default:
			// Structural Object fields (not tables/views, not arrays, not virtual)
			if f.Type.NamedType == "" {
				continue
			}
			if isVirtualField(f) {
				continue
			}
			td := lookupObjectDef(ctx, typeName)
			if td == nil || td.Kind != ast.Object || isTableOrView(td) {
				continue
			}
			// Reference by name — the type is created eagerly by PassthroughRule
			nestedAggName := "_" + td.Name + "_aggregation"
			aggDef.Fields = append(aggDef.Fields, &ast.FieldDefinition{
				Name:     f.Name,
				Type:     ast.NamedType(nestedAggName, pos),
				Position: pos,
				Directives: ast.DirectiveList{
					{Name: "field_aggregation", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
				},
			})
		}
	}

	return aggDef
}

// generateBucketAggregationType creates a _<Name>_aggregation_bucket object
// with key (typed as the base object) and aggregations (typed as the aggregation type).
func generateBucketAggregationType(def *ast.Definition, aggTypeName, filterName, bucketName string, opts base.Options, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind:        ast.Object,
		Name:        bucketName,
		Description: "Bucket aggregation for " + def.Name,
		Position:    pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Fields: ast.FieldList{
			{
				Name:        "key",
				Description: base.DescBucketKey,
				Type:        ast.NamedType(def.Name, pos),
				Position:    pos,
			},
			{
				Name:        "aggregations",
				Description: "The aggregations of the bucket",
				Type:        ast.NamedType(aggTypeName, pos),
				Arguments: ast.ArgumentDefinitionList{
					{Name: "filter", Description: base.DescFilter, Type: ast.NamedType(filterName, pos), Position: pos},
					{Name: "order_by", Description: base.DescOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
				},
				Position: pos,
			},
		},
	}
}


// generateQueryFields produces the list query (with filter/order/limit/offset),
// the by-PK query, and aggregation query fields for a data object.
func generateQueryFields(ctx base.CompilationContext, def *ast.Definition, info *base.ObjectInfo, filterName string, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition
	opts := ctx.CompileOptions()

	queryName := def.Name
	if opts.AsModule && info.OriginalName != "" && info.OriginalName != def.Name {
		queryName = info.OriginalName
	}

	// Select query (list) — old compiler returns [Type] (nullable list)
	selectField := &ast.FieldDefinition{
		Name:        queryName,
		Description: def.Description,
		Type:        ast.ListType(ast.NamedType(def.Name, pos), pos),
		Arguments:   queryArgsWithViewArgs(info, filterName, pos),
		Directives: ast.DirectiveList{
			{Name: "query", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Position: pos,
	}
	fields = append(fields, selectField)

	// Select one by PK
	if len(info.PrimaryKey) > 0 && !info.IsM2M {
		selectOneField := &ast.FieldDefinition{
			Name:        queryName + "_by_pk",
			Description: def.Description,
			Type:        ast.NamedType(def.Name, pos),
			Directives: ast.DirectiveList{
				{Name: "query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "SELECT_ONE", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		}
		// Prepend view args to SELECT_ONE as well (fix: old compiler omitted this)
		if info.InputArgsName != "" {
			var argType *ast.Type
			if info.RequiredArgs {
				argType = ast.NonNullNamedType(info.InputArgsName, pos)
			} else {
				argType = ast.NamedType(info.InputArgsName, pos)
			}
			selectOneField.Arguments = append(selectOneField.Arguments, &ast.ArgumentDefinition{
				Name:        "args",
				Description: base.DescArgs,
				Type:        argType,
				Position:    pos,
			})
		}
		for _, pk := range info.PrimaryKey {
			f := def.Fields.ForName(pk)
			if f == nil {
				continue
			}
			selectOneField.Arguments = append(selectOneField.Arguments, &ast.ArgumentDefinition{
				Name:     pk,
				Type:     ast.NonNullNamedType(f.Type.Name(), pos),
				Position: pos,
			})
		}
		fields = append(fields, selectOneField)
	}

	// Aggregation query fields — generated inline so they flow through
	// RegisterQueryFields → ModuleAssembler/RootTypeAssembler automatically.
	// @aggregation_query(name: queryName) points to the SELECT field on the same type.
	// Note: aggregation types are created by AggregationRule (runs after Table/ViewRule).
	aggTypeName := "_" + def.Name + "_aggregation"
	bucketAggTypeName := "_" + def.Name + "_aggregation_bucket"

	aggField := &ast.FieldDefinition{
		Name:        queryName + "_aggregation",
		Description: "The aggregation for " + queryName,
		Type:        ast.NamedType(aggTypeName, pos),
		Arguments:   queryArgsWithViewArgs(info, filterName, pos),
		Directives: ast.DirectiveList{
			{Name: "aggregation_query", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: queryName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Position: pos,
	}
	fields = append(fields, aggField)

	bucketAggField := &ast.FieldDefinition{
		Name:        queryName + "_bucket_aggregation",
		Description: "The aggregation for " + queryName,
		Type:        ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
		Arguments:   queryArgsWithViewArgs(info, filterName, pos),
		Directives: ast.DirectiveList{
			{Name: "aggregation_query", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: queryName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Position: pos,
	}
	fields = append(fields, bucketAggField)

	return fields
}

// generateMutationFields produces insert, update, and delete mutation fields
// for a data object based on engine capabilities.
func generateMutationFields(ctx base.CompilationContext, def *ast.Definition, info *base.ObjectInfo, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition
	opts := ctx.CompileOptions()
	insertInputName := def.Name + "_mut_input_data"
	updateInputName := def.Name + "_mut_data"
	filterName := def.Name + "_filter"

	// Use original (unprefixed) name for field names when AsModule
	mutName := def.Name
	if opts.AsModule && info.OriginalName != "" && info.OriginalName != def.Name {
		mutName = info.OriginalName
	}

	// Insert
	if opts.SupportInsert() {
		// Old compiler: singular data arg, returns object type if SupportInsertReturning + has PKs + not M2M
		outType := ast.NamedType(def.Name, pos)
		if !opts.SupportInsertReturning() || len(info.PrimaryKey) == 0 || info.IsM2M {
			outType = ast.NamedType("OperationResult", pos)
		}
		insertField := &ast.FieldDefinition{
			Name:        "insert_" + mutName,
			Description: def.Description,
			Type:        outType,
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullNamedType(insertInputName, pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "INSERT", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		}
		fields = append(fields, insertField)
	}

	// Update — old compiler uses filter arg
	if opts.SupportUpdate() && (opts.SupportUpdateWithoutPKs() || len(info.PrimaryKey) > 0) {
		updateField := &ast.FieldDefinition{
			Name:        "update_" + mutName,
			Description: def.Description,
			Type:        ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
				{Name: "data", Type: ast.NonNullNamedType(updateInputName, pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "UPDATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: updateInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		}
		fields = append(fields, updateField)
	}

	// Delete — old compiler uses filter arg
	if opts.SupportDelete() && (opts.SupportDeleteWithoutPKs() || len(info.PrimaryKey) > 0) {
		deleteField := &ast.FieldDefinition{
			Name:        "delete_" + mutName,
			Description: def.Description,
			Type:        ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "DELETE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		}
		fields = append(fields, deleteField)
	}

	return fields
}

// addVirtualFieldAggregations adds aggregation fields for @join and @table_function_call_join
// list fields on a data object. For each such field:
// - @join: adds subquery args, standard aggregation args (aggRefArgs/aggSubRefArgs)
// - @table_function_call_join: preserves original field arguments (function call args may not be in the mapping)
// Both: adds {name}_aggregation and {name}_bucket_aggregation on base object,
// and direct agg + sub-agg fields on the aggregation type.
func addVirtualFieldAggregations(ctx base.CompilationContext, def *ast.Definition, aggTypeName string, opts base.Options, pos *ast.Position) {
	for _, f := range def.Fields {
		isJoin := f.Directives.ForName("join") != nil
		isTFCJ := f.Directives.ForName("table_function_call_join") != nil
		if !isJoin && !isTFCJ {
			continue
		}
		// Only list fields (NamedType == "" means it's a list)
		if f.Type.NamedType != "" {
			continue
		}
		targetName := f.Type.Name()
		// Skip if target is not a known data object
		if ctx.GetObject(targetName) == nil {
			continue
		}

		targetFilterName := targetName + "_filter"
		targetAggName := "_" + targetName + "_aggregation"
		bucketAggName := "_" + targetName + "_aggregation_bucket"
		subAggName := AggTypeNameAtDepth(targetName, 1)

		if isJoin {
			// @join fields: add subquery args to the field itself
			if len(f.Arguments) == 0 {
				f.Arguments = SubQueryArgs(targetFilterName, pos)
			}

			// Add {name}_aggregation and {name}_bucket_aggregation on base object (with subQueryArgs)
			addReferenceAggregationFields(ctx, def.Name, f.Name, targetName, targetFilterName, opts, pos)

			// Add direct agg + sub-agg fields on aggregation type (with aggRefArgs/aggSubRefArgs)
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      f.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: AggRefArgs(targetFilterName, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(f.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      f.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: AggSubRefArgs(targetFilterName, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(f.Name, pos),
						},
						Position: pos,
					},
				},
			})
		} else {
			// @table_function_call_join: use original field's arguments everywhere.
			// TFCJ fields may have function call arguments that aren't in the args mapping,
			// so we must preserve them exactly as-is rather than using standard subquery args.
			origArgs := CloneArgDefs(f.Arguments, pos)

			// Add {name}_aggregation and {name}_bucket_aggregation on base object (with original args)
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     def.Name,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:        f.Name + "_aggregation",
						Description: "The aggregation for " + f.Name,
						Type:        ast.NamedType(targetAggName, pos),
						Arguments:   origArgs,
						Directives: ast.DirectiveList{
							{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
								{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
								{Name: base.ArgName, Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
							}, Position: pos},
							optsCatalogDirective(opts),
						},
						Position: pos,
					},
					{
						Name:        f.Name + "_bucket_aggregation",
						Description: "The bucket aggregation for " + f.Name,
						Type:        ast.ListType(ast.NamedType(bucketAggName, pos), pos),
						Arguments:   CloneArgDefs(f.Arguments, pos),
						Directives: ast.DirectiveList{
							{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
								{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
								{Name: base.ArgName, Value: &ast.Value{Raw: f.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
							}, Position: pos},
							optsCatalogDirective(opts),
						},
						Position: pos,
					},
				},
			})

			// Add direct agg + sub-agg fields on aggregation type (with original args)
			ctx.AddExtension(&ast.Definition{
				Kind:     ast.Object,
				Name:     aggTypeName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name:      f.Name,
						Type:      ast.NamedType(targetAggName, pos),
						Arguments: CloneArgDefs(f.Arguments, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(f.Name, pos),
						},
						Position: pos,
					},
					{
						Name:      f.Name + "_aggregation",
						Type:      ast.NamedType(subAggName, pos),
						Arguments: CloneArgDefs(f.Arguments, pos),
						Directives: ast.DirectiveList{
							fieldAggregationDirective(f.Name, pos),
						},
						Position: pos,
					},
				},
			})
		}
	}
}
