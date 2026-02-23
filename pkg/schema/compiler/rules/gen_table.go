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

	// 2b. Generate list filter input type
	listFilterName := def.Name + "_list_filter"
	listFilterDef := generateListFilterInput(filterName, listFilterName, pos)
	addDef(listFilterDef)

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

		def.Directives = append(def.Directives, &ast.Directive{
			Name: "data_input",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
			},
			Position: pos,
		})
	}

	// 4. Generate aggregation type
	aggName := "_" + def.Name + "_aggregation"
	aggDef := generateAggregationType(ctx, def, aggName, pos)
	addDef(aggDef)

	// 4b. Generate bucket aggregation type
	bucketAggName := "_" + def.Name + "_aggregation_bucket"
	bucketAggDef := generateBucketAggregationType(def, aggName, filterName, bucketAggName, pos)
	addDef(bucketAggDef)

	// 5. Register query fields
	queryFields := generateQueryFields(def, info, filterName, pos)
	ctx.RegisterQueryFields(def.Name, queryFields)

	// 6. Register mutation fields (unless ReadOnly)
	if !opts.ReadOnly {
		mutFields := generateMutationFields(ctx, def, info, pos)
		ctx.RegisterMutationFields(def.Name, mutFields)
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

// generateFilterInput creates a <Name>Filter input object with scalar filter
// fields and logical operators (_and, _or, _not).
func generateFilterInput(ctx base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	filterDef := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       name,
		Position:   pos,
		Directives: ast.DirectiveList{{Name: "system", Position: pos}},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()

		if s := ctx.ScalarLookup(typeName); s != nil {
			if fi, ok := s.(types.Filterable); ok {
				filterFieldType := fi.FilterTypeName()
				filterDef.Fields = append(filterDef.Fields, &ast.FieldDefinition{
					Name:     f.Name,
					Type:     ast.NamedType(filterFieldType, pos),
					Position: pos,
				})
			}
		}
	}

	// Logical operators
	filterDef.Fields = append(filterDef.Fields,
		&ast.FieldDefinition{Name: "_and", Type: ast.ListType(ast.NonNullNamedType(name, pos), pos), Position: pos},
		&ast.FieldDefinition{Name: "_or", Type: ast.ListType(ast.NonNullNamedType(name, pos), pos), Position: pos},
		&ast.FieldDefinition{Name: "_not", Type: ast.NamedType(name, pos), Position: pos},
	)

	return filterDef
}

// generateMutInputData creates a <Name>_mut_input_data input object for insert mutation.
// Includes all table fields. Computed (@sql) fields are skipped.
func generateMutInputData(_ base.CompilationContext, def *ast.Definition, name string, pos *ast.Position) *ast.Definition {
	inputDef := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       name,
		Position:   pos,
		Directives: ast.DirectiveList{{Name: "system", Position: pos}},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName("sql") != nil {
			continue // skip computed fields
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
		Kind:       ast.InputObject,
		Name:       name,
		Position:   pos,
		Directives: ast.DirectiveList{{Name: "system", Position: pos}},
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName("sql") != nil {
			continue // skip computed fields
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
func generateListFilterInput(filterName, listFilterName string, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind:       ast.InputObject,
		Name:       listFilterName,
		Position:   pos,
		Directives: ast.DirectiveList{{Name: "system", Position: pos}},
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
			{Name: "system", Position: pos},
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "0", Kind: ast.IntValue, Position: pos}, Position: pos},
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
				aggDef.Fields = append(aggDef.Fields, &ast.FieldDefinition{
					Name:     f.Name,
					Type:     ast.NamedType(a.AggregationTypeName(), pos),
					Position: pos,
				})
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
			{Name: "system", Position: pos},
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "0", Kind: ast.IntValue, Position: pos}, Position: pos},
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
					{Name: "order_by", Type: ast.ListType(ast.NonNullNamedType("OrderByField", pos), pos), Position: pos},
				},
				Position: pos,
			},
		},
	}
}

// generateQueryFields produces the list query (with filter/order/limit/offset)
// and the by-PK query for a data object.
func generateQueryFields(def *ast.Definition, info *base.ObjectInfo, filterName string, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition

	queryName := def.Name

	// Select query (list)
	selectField := &ast.FieldDefinition{
		Name: queryName,
		Type: ast.NonNullListType(ast.NamedType(def.Name, pos), pos),
		Arguments: ast.ArgumentDefinitionList{
			{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
			{Name: "order_by", Type: ast.ListType(ast.NonNullNamedType("OrderByField", pos), pos), Position: pos},
			{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos},
			{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos},
		},
		Directives: ast.DirectiveList{
			{Name: "query", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Position: pos,
	}
	fields = append(fields, selectField)

	// Select one by PK
	if len(info.PrimaryKey) > 0 {
		selectOneField := &ast.FieldDefinition{
			Name: queryName + "_by_pk",
			Type: ast.NamedType(def.Name, pos),
			Directives: ast.DirectiveList{
				{Name: "query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "SELECT_ONE", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
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

// generateMutationFields produces insert, update, and delete mutation fields
// for a data object based on engine capabilities.
func generateMutationFields(ctx base.CompilationContext, def *ast.Definition, info *base.ObjectInfo, pos *ast.Position) []*ast.FieldDefinition {
	var fields []*ast.FieldDefinition
	opts := ctx.CompileOptions()
	insertInputName := def.Name + "_mut_input_data"
	updateInputName := def.Name + "_mut_data"

	// Insert
	if opts.SupportInsert() {
		insertField := &ast.FieldDefinition{
			Name: "insert_" + def.Name,
			Type: ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullListType(ast.NonNullNamedType(insertInputName, pos), pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "INSERT", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}
		fields = append(fields, insertField)
	}

	// Update
	if opts.SupportUpdate() && len(info.PrimaryKey) > 0 {
		updateField := &ast.FieldDefinition{
			Name: "update_" + def.Name,
			Type: ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullNamedType(updateInputName, pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "UPDATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: updateInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}
		fields = append(fields, updateField)
	}

	// Delete
	if opts.SupportDelete() && len(info.PrimaryKey) > 0 {
		deleteField := &ast.FieldDefinition{
			Name: "delete_" + def.Name,
			Type: ast.NamedType("OperationResult", pos),
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "DELETE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: insertInputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}
		for _, pk := range info.PrimaryKey {
			f := def.Fields.ForName(pk)
			if f == nil {
				continue
			}
			deleteField.Arguments = append(deleteField.Arguments, &ast.ArgumentDefinition{
				Name:     pk,
				Type:     ast.NonNullNamedType(f.Type.Name(), pos),
				Position: pos,
			})
		}
		fields = append(fields, deleteField)
	}

	return fields
}
