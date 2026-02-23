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
	filterName := def.Name + "Filter"
	filterDef := generateFilterInput(ctx, def, filterName, pos)
	addDef(filterDef)
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "filter_input",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: filterName, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	})

	// 3. Generate data input type (unless ReadOnly)
	if !opts.ReadOnly {
		inputName := def.Name + "Input"
		inputDef := generateDataInput(ctx, def, inputName, info.PrimaryKey, pos)
		addDef(inputDef)
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "data_input",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: inputName, Kind: ast.StringValue, Position: pos}, Position: pos},
			},
			Position: pos,
		})
	}

	// 4. Generate aggregation type
	aggName := "_" + def.Name + "_aggregation"
	aggDef := generateAggregationType(ctx, def, aggName, pos)
	addDef(aggDef)

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

// generateDataInput creates a <Name>Input input object for mutation data.
// PK fields are nullable (for updates); computed (@sql) fields are skipped.
func generateDataInput(_ base.CompilationContext, def *ast.Definition, name string, pks []string, pos *ast.Position) *ast.Definition {
	inputDef := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       name,
		Position:   pos,
		Directives: ast.DirectiveList{{Name: "system", Position: pos}},
	}

	pkSet := make(map[string]bool, len(pks))
	for _, pk := range pks {
		pkSet[pk] = true
	}

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		if f.Directives.ForName("sql") != nil {
			continue // skip computed fields
		}

		typeName := f.Type.Name()
		// All input fields are nullable (PK fields optional for update)
		inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(typeName, pos),
			Position: pos,
		})
	}

	return inputDef
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
	inputName := def.Name + "Input"

	// Insert
	if opts.SupportInsert() {
		insertField := &ast.FieldDefinition{
			Name: def.Name + "_insert",
			Type: ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullListType(ast.NonNullNamedType(inputName, pos), pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "INSERT", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: inputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}
		fields = append(fields, insertField)
	}

	// Update
	if opts.SupportUpdate() && len(info.PrimaryKey) > 0 {
		updateField := &ast.FieldDefinition{
			Name: def.Name + "_update",
			Type: ast.NamedType("OperationResult", pos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "data", Type: ast.NonNullNamedType(inputName, pos), Position: pos},
			},
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "UPDATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: inputName, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
			Position: pos,
		}
		fields = append(fields, updateField)
	}

	// Delete
	if opts.SupportDelete() && len(info.PrimaryKey) > 0 {
		deleteField := &ast.FieldDefinition{
			Name: def.Name + "_delete",
			Type: ast.NamedType("OperationResult", pos),
			Directives: ast.DirectiveList{
				{Name: "mutation", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "DELETE", Kind: ast.EnumValue, Position: pos}, Position: pos},
					{Name: "data_input", Value: &ast.Value{Raw: inputName, Kind: ast.StringValue, Position: pos}, Position: pos},
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
