package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
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
		if !isFunctionCallField(f) {
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

	// 4. Generate aggregation type
	aggName := "_" + def.Name + "_aggregation"
	aggDef := generateAggregationType(ctx, def, aggName, pos)
	addDef(aggDef)

	// 4b. Generate bucket aggregation type
	bucketAggName := "_" + def.Name + "_aggregation_bucket"
	bucketAggDef := generateBucketAggregationType(def, aggName, filterName, bucketAggName, pos)
	addDef(bucketAggDef)

	// 4c. Add sub-aggregation fields for table_function_call_join list fields
	addTableFuncJoinSubAggregations(ctx, def, aggName, pos)

	// 5. Register query fields and add @query directives on def
	queryFields := generateQueryFields(ctx, def, info, filterName, pos)
	ctx.RegisterQueryFields(def.Name, queryFields)
	// Add @query directive for SELECT
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "query",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
		},
		Position: pos,
	})
	// Add @query for by_pk
	if len(info.PrimaryKey) > 0 && !info.IsM2M {
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "query",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name + "_by_pk", Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "SELECT_ONE", Kind: ast.EnumValue, Position: pos}, Position: pos},
			},
			Position: pos,
		})
	}

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
					{Name: "name", Value: &ast.Value{Raw: "insert_" + def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
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
					{Name: "name", Value: &ast.Value{Raw: "update_" + def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
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
					{Name: "name", Value: &ast.Value{Raw: "delete_" + def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "DELETE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: "", Kind: ast.StringValue, Position: pos}, Position: pos},
				},
				Position: pos,
			})
		}
	}

	return nil
}

// compiledPos creates an ast.Position tagged with a compiled-instruction source.
func compiledPos(name string) *ast.Position {
	src := "compiled-instruction"
	if name != "" {
		src = "compiled-instruction-" + name
	}
	return &ast.Position{Src: &ast.Source{Name: src}}
}

// addModuleToFuncCallDirective adds module=<name> to a function_call or table_function_call_join directive.
func addModuleToFuncCallDirective(f *ast.FieldDefinition, moduleName string) {
	for _, dirName := range []string{"function_call", "table_function_call_join"} {
		d := f.Directives.ForName(dirName)
		if d == nil {
			continue
		}
		if a := d.Arguments.ForName("module"); a != nil {
			a.Value.Raw = moduleName
		} else {
			d.Arguments = append(d.Arguments, &ast.Argument{
				Name:     "module",
				Value:    &ast.Value{Kind: ast.StringValue, Raw: moduleName},
				Position: d.Position,
			})
		}
	}
}

// isFunctionCallField returns true for fields with @function_call or @table_function_call_join.
// These are function call subquery fields that should be excluded from filters and mutation inputs.
func isFunctionCallField(f *ast.FieldDefinition) bool {
	return f.Directives.ForName("function_call") != nil || f.Directives.ForName("table_function_call_join") != nil
}

// generateFilterInput creates a <Name>_filter input object with scalar filter
// fields and logical operators (_and, _or, _not).
func generateFilterInput(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	filterDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "filter_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if isFunctionCallField(f) {
			continue
		}
		typeName := f.Type.Name()

		if s := ctx.ScalarLookup(typeName); s != nil {
			if fi, ok := s.(types.Filterable); ok {
				filterFieldType := fi.FilterTypeName()
				field := &ast.FieldDefinition{
					Name:     f.Name,
					Type:     ast.NamedType(filterFieldType, pos),
					Position: pos,
				}
				// Copy directives from original field (pk, default, measurement, timescale_key)
				for _, d := range f.Directives {
					if d.Name == "pk" || d.Name == "default" || d.Name == "measurement" || d.Name == "timescale_key" {
						field.Directives = append(field.Directives, d)
					}
				}
				filterDef.Fields = append(filterDef.Fields, field)
			}
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
func generateMutInputData(_ base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName("sql") != nil {
			continue // skip computed fields
		}
		if isFunctionCallField(f) {
			continue
		}

		typeName := f.Type.Name()
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(typeName, pos),
			Position: pos,
		})
	}

	return inputDef
}

// generateMutData creates a <Name>_mut_data input object for update mutation.
// Includes all table fields (including non-scalar). Computed (@sql) fields are skipped.
func generateMutData(_ base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	inputDef := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "data_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName("sql") != nil {
			continue // skip computed fields
		}
		if isFunctionCallField(f) {
			continue
		}

		typeName := f.Type.Name()
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(typeName, pos),
			Position: pos,
		})
	}

	return inputDef
}

// generateListFilterInput creates a <Name>_list_filter input with any_of, all_of, none_of fields.
func generateListFilterInput(objectName, filterName, listFilterName string, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind:     ast.InputObject,
		Name:     listFilterName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "filter_list_input", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: objectName, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Fields: ast.FieldList{
			{Name: "any_of", Type: ast.NamedType(filterName, pos), Position: pos},
			{Name: "all_of", Type: ast.NamedType(filterName, pos), Position: pos},
			{Name: "none_of", Type: ast.NamedType(filterName, pos), Position: pos},
		},
	}
}

// generateAggregationType creates a _<Name>_aggregation object with _rows_count
// and per-field aggregation type fields.
func generateAggregationType(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
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
		if s := ctx.ScalarLookup(typeName); s != nil {
			if a, ok := s.(types.Aggregatable); ok {
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
			}
		}
	}

	return aggDef
}

// generateBucketAggregationType creates a _<Name>_aggregation_bucket object
// with key (typed as the base object) and aggregations (typed as the aggregation type).
func generateBucketAggregationType(def *ast.Definition, aggTypeName, filterName, bucketName string, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind:     ast.Object,
		Name:     bucketName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Fields: ast.FieldList{
			{
				Name:     "key",
				Type:     ast.NamedType(def.Name, pos),
				Position: pos,
			},
			{
				Name: "aggregations",
				Type: ast.NamedType(aggTypeName, pos),
				Arguments: ast.ArgumentDefinitionList{
					{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
					{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
				},
				Position: pos,
			},
		},
	}
}

// queryArgs returns the standard query arguments for list queries.
func queryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return queryArgsWithViewArgs(nil, filterName, pos)
}

// queryArgsWithViewArgs returns standard query arguments, optionally prepending
// an "args" parameter for parameterized views (@args directive).
func queryArgsWithViewArgs(info *base.ObjectInfo, filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	var args ast.ArgumentDefinitionList
	if info != nil && info.InputArgsName != "" {
		var argType *ast.Type
		if info.RequiredArgs {
			argType = ast.NonNullNamedType(info.InputArgsName, pos)
		} else {
			argType = ast.NamedType(info.InputArgsName, pos)
		}
		args = append(args, &ast.ArgumentDefinition{
			Name:     "args",
			Type:     argType,
			Position: pos,
		})
	}
	args = append(args,
		&ast.ArgumentDefinition{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		&ast.ArgumentDefinition{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		&ast.ArgumentDefinition{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		&ast.ArgumentDefinition{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		&ast.ArgumentDefinition{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
	)
	return args
}

// subQueryArgs returns the standard query arguments for sub-queries (references).
func subQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	args := queryArgs(filterName, pos)
	args = append(args,
		&ast.ArgumentDefinition{
			Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue},
		},
		&ast.ArgumentDefinition{
			Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos,
		},
		&ast.ArgumentDefinition{
			Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos,
		},
		&ast.ArgumentDefinition{
			Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos,
		},
	)
	return args
}

// setScalarFieldArguments sets field arguments from scalar type definitions
// (e.g., bucket for Timestamp, transforms for Geometry, struct for JSON).
// Must be called before generateAggregationType so args get copied.
func setScalarFieldArguments(ctx base.CompilationContext, def *ast.Definition) {
	for _, f := range def.Fields {
		if f.Name == "_stub" || f.Type.NamedType == "" {
			continue
		}
		s := ctx.ScalarLookup(f.Type.Name())
		if s == nil {
			continue
		}
		if fap, ok := s.(types.FieldArgumentsProvider); ok {
			f.Arguments = fap.FieldArguments()
		}
	}
}

// generateQueryFields produces the list query (with filter/order/limit/offset)
// and the by-PK query for a data object.
func generateQueryFields(ctx base.CompilationContext, def *ast.Definition, info *base.ObjectInfo, filterName string, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition
	opts := ctx.CompileOptions()

	queryName := def.Name

	// Select query (list) — old compiler returns [Type] (nullable list)
	selectField := &ast.FieldDefinition{
		Name:      queryName,
		Type:      ast.ListType(ast.NamedType(def.Name, pos), pos),
		Arguments: queryArgsWithViewArgs(info, filterName, pos),
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
			Name: queryName + "_by_pk",
			Type: ast.NamedType(def.Name, pos),
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
				Name:     "args",
				Type:     argType,
				Position: pos,
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

	return fields
}

// optsCatalogDirective creates a @catalog directive from compile options.
func optsCatalogDirective(opts base.Options) *ast.Directive {
	return catalogDirective(opts.Name, opts.EngineType)
}

// generateMutationFields produces insert, update, and delete mutation fields
// for a data object based on engine capabilities.
func generateMutationFields(ctx base.CompilationContext, def *ast.Definition, info *base.ObjectInfo, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition
	opts := ctx.CompileOptions()
	insertInputName := def.Name + "_mut_input_data"
	updateInputName := def.Name + "_mut_data"
	filterName := def.Name + "_filter"

	// Insert
	if opts.SupportInsert() {
		// Old compiler: singular data arg, returns object type if SupportInsertReturning + has PKs + not M2M
		outType := ast.NamedType(def.Name, pos)
		if !opts.SupportInsertReturning() || len(info.PrimaryKey) == 0 || info.IsM2M {
			outType = ast.NamedType("OperationResult", pos)
		}
		insertField := &ast.FieldDefinition{
			Name: "insert_" + def.Name,
			Type: outType,
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
			Name: "update_" + def.Name,
			Type: ast.NamedType("OperationResult", pos),
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
			Name: "delete_" + def.Name,
			Type: ast.NamedType("OperationResult", pos),
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

// addTableFuncJoinSubAggregations adds sub-aggregation fields for table_function_call_join
// list fields on a data object. For each such field (that is a list), the old compiler
// adds a {field}_aggregation sub-field on the aggregation type pointing to a sub-aggregation type.
// Fields with bound args still get sub-aggregation (unlike main aggregation which is skipped).
func addTableFuncJoinSubAggregations(ctx base.CompilationContext, def *ast.Definition, aggTypeName string, pos *ast.Position) {
	for _, f := range def.Fields {
		if f.Directives.ForName("table_function_call_join") == nil {
			continue
		}
		// Only list fields (NamedType == "" means it's a list)
		if f.Type.NamedType != "" {
			continue
		}
		targetName := f.Type.Name()
		targetAggName := "_" + targetName + "_aggregation"
		if ctx.LookupType(targetAggName) == nil {
			continue
		}

		// Create sub-aggregation type for the target (without extra fields,
		// matching old compiler which creates this during field iteration
		// before extra fields are added to the agg type).
		subAggName := aggTypeNameAtDepth(targetName, 1)
		ensureSubAggregationTypeNoExtra(ctx, targetName, subAggName, 1, pos)

		// Propagate any reference fields that were already added to the target's
		// base aggregation type as extensions. This handles the case where references
		// were processed BEFORE this table_function_call_join created the sub-agg type.
		propagateRefFieldsToSubAgg(ctx, targetName, subAggName, 1, pos)

		// Add {field}_aggregation field to the aggregation type
		ctx.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     aggTypeName,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name: f.Name + "_aggregation",
					Type: ast.NamedType(subAggName, pos),
					Directives: ast.DirectiveList{
						fieldAggregationDirective(f.Name, pos),
					},
					Position: pos,
				},
			},
		})
	}
}
