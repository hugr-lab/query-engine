package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*ExtraFieldRule)(nil)

type ExtraFieldRule struct{}

func (r *ExtraFieldRule) Name() string     { return "ExtraFieldRule" }
func (r *ExtraFieldRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ExtraFieldRule) Match(def *ast.Definition) bool {
	// Only match data objects (@table or @view) that have at least one
	// field whose scalar implements ExtraFieldProvider.
	if def.Directives.ForName("table") == nil && def.Directives.ForName("view") == nil {
		return false
	}
	return true
}

func (r *ExtraFieldRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	pos := compiledPos(def.Name)

	var extraFields ast.FieldList
	var aggExtraFields ast.FieldList

	for _, f := range def.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		s := ctx.ScalarLookup(typeName)
		if s == nil {
			continue
		}
		efp, ok := s.(types.ExtraFieldProvider)
		if !ok {
			continue
		}

		extraField := efp.GenerateExtraField(f.Name)
		if extraField == nil {
			continue
		}
		extraFields = append(extraFields, extraField)

		// For aggregation type: use AggregationType of the extra field's return type
		aggField := extraFieldForAggregation(ctx, extraField, pos)
		if aggField != nil {
			aggExtraFields = append(aggExtraFields, aggField)
		}
	}

	if len(extraFields) == 0 {
		return nil
	}

	// Add extra fields as an extension on the original object
	ext := &ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: pos,
		Fields:   extraFields,
	}
	ctx.AddExtension(ext)

	// Also add extra fields to the aggregation type (with aggregation types)
	aggName := "_" + def.Name + "_aggregation"
	if ctx.LookupType(aggName) != nil && len(aggExtraFields) > 0 {
		aggExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     aggName,
			Position: pos,
			Fields:   aggExtraFields,
		}
		ctx.AddExtension(aggExt)
	}

	return nil
}

// extraFieldForAggregation creates an aggregation version of an extra field.
// The return type is mapped to its aggregation type (e.g., BigInt → BigIntAggregation).
func extraFieldForAggregation(ctx base.CompilationContext, field *ast.FieldDefinition, pos *ast.Position) *ast.FieldDefinition {
	typeName := field.Type.Name()
	s := ctx.ScalarLookup(typeName)
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
	// Copy args from the original extra field
	if len(field.Arguments) > 0 {
		aggField.Arguments = make(ast.ArgumentDefinitionList, len(field.Arguments))
		copy(aggField.Arguments, field.Arguments)
	}
	// Note: old compiler does NOT copy @extra_field/@sql directives to aggregation extra fields
	return aggField
}
