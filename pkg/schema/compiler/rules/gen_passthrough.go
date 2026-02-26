package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*PassthroughRule)(nil)

// PassthroughRule adds source definitions that are not handled by other
// Generate-phase rules (e.g. structural Object types, InputObject, Interface,
// Union, Enum) directly to the compiler output so they appear in the final schema.
type PassthroughRule struct{}

func (r *PassthroughRule) Name() string     { return "PassthroughRule" }
func (r *PassthroughRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *PassthroughRule) Match(def *ast.Definition) bool {
	// Skip definitions handled by TableRule, ViewRule
	if def.Directives.ForName("table") != nil {
		return false
	}
	if def.Directives.ForName("view") != nil {
		return false
	}
	if def.Directives.ForName("function") != nil {
		return false
	}
	// Skip Function/MutationFunction — handled by FunctionRule
	if def.Name == "Function" || def.Name == "MutationFunction" {
		return false
	}
	// Match structural types that need to be passed through
	switch def.Kind {
	case ast.Object, ast.InputObject, ast.Interface, ast.Union, ast.Enum:
		return true
	}
	return false
}

func (r *PassthroughRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	// Skip types that already exist in the target schema (system types).
	// These will be extended rather than re-added.
	if ctx.LookupType(def.Name) != nil {
		return nil
	}
	ctx.AddDefinition(def)

	// For structural Object types, eagerly generate all derived types
	// (filter, aggregation, sub-aggregation, mutation inputs).
	if def.Kind == ast.Object {
		opts := ctx.CompileOptions()
		pos := compiledPos(def.Name)

		// Set scalar-specific field arguments (bucket, transforms, struct, etc.)
		setScalarFieldArguments(ctx, def)

		// Filter input
		generateNestedFilterInput(ctx, def, false, pos)

		// Aggregation type
		aggName := "_" + def.Name + "_aggregation"
		aggDef := generateAggregationType(ctx, def, aggName, pos)
		ctx.AddDefinition(aggDef)

		// Sub-aggregation type
		generateStructSubAggType(ctx, aggDef, pos)

		// Mutation input types (unless ReadOnly)
		if !opts.ReadOnly {
			generateNestedMutInputData(ctx, def, pos)
			generateNestedMutData(ctx, def, pos)
		}
	}

	return nil
}

// generateStructSubAggType creates a sub-aggregation type for a structural Object's
// aggregation type. For example, given _Specs_aggregation, it creates
// _Specs_aggregation_sub_aggregation with scalar fields mapped to SubAggregation variants.
func generateStructSubAggType(ctx base.CompilationContext, aggDef *ast.Definition, pos *ast.Position) {
	subAggName := aggDef.Name + "_sub_aggregation"
	if ctx.LookupType(subAggName) != nil {
		return
	}

	var fields ast.FieldList

	// _rows_count: BigIntAggregation with @field_aggregation(name: "aggregation_field")
	fields = append(fields, &ast.FieldDefinition{
		Name:     "_rows_count",
		Type:     ast.NamedType("BigIntAggregation", pos),
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: base.ObjectFieldAggregationDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgName, Value: &ast.Value{Raw: "aggregation_field", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	})

	// Map each scalar aggregation field to its SubAggregation variant
	for _, f := range aggDef.Fields {
		if f.Name == "_rows_count" {
			continue
		}
		subTypeName := scalarSubAggTypeName(f.Type.Name())
		if subTypeName == "" {
			continue
		}
		subField := &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(subTypeName, pos),
			Position: pos,
		}
		if len(f.Directives) > 0 {
			subField.Directives = make(ast.DirectiveList, len(f.Directives))
			copy(subField.Directives, f.Directives)
		}
		fields = append(fields, subField)
	}

	subAgg := &ast.Definition{
		Kind:     ast.Object,
		Name:     subAggName,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: base.ObjectAggregationDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgName, Value: &ast.Value{Raw: aggDef.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "2", Kind: ast.IntValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(ctx.CompileOptions()),
		},
		Fields: fields,
	}
	ctx.AddDefinition(subAgg)
}
