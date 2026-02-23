package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*AggregationRule)(nil)

type AggregationRule struct{}

func (r *AggregationRule) Name() string     { return "AggregationRule" }
func (r *AggregationRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *AggregationRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("table") != nil || def.Directives.ForName("view") != nil
}

func (r *AggregationRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{Name: def.Name, OriginalName: def.Name}
	}
	pos := compiledPos(def.Name)

	// The aggregation type was already created by TableRule or ViewRule as
	// _<Name>_aggregation. Here we register the aggregation query field.
	aggTypeName := "_" + def.Name + "_aggregation"

	// Verify the aggregation type exists in output
	if ctx.LookupType(aggTypeName) == nil {
		// If for some reason the aggregation type wasn't generated yet,
		// skip silently. This can happen if ordering is not guaranteed.
		return nil
	}

	filterName := def.Name + "Filter"

	aggField := &ast.FieldDefinition{
		Name: def.Name + "_aggregate",
		Type: ast.NamedType(aggTypeName, pos),
		Arguments: ast.ArgumentDefinitionList{
			{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		},
		Directives: ast.DirectiveList{
			{Name: "query", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: info.OriginalName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "AGGREGATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Position: pos,
	}

	ctx.RegisterQueryFields(def.Name, []*ast.FieldDefinition{aggField})

	return nil
}
