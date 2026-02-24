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
	opts := ctx.CompileOptions()
	pos := compiledPos(def.Name)

	// The aggregation type was already created by TableRule or ViewRule as
	// _<Name>_aggregation. Here we register the aggregation query field.
	aggTypeName := "_" + def.Name + "_aggregation"

	// Verify the aggregation type exists in output
	if ctx.LookupType(aggTypeName) == nil {
		return nil
	}

	filterName := def.Name + "_filter"
	bucketAggTypeName := "_" + def.Name + "_aggregation_bucket"

	// Single-row aggregation query: Type_aggregation
	aggField := &ast.FieldDefinition{
		Name:      def.Name + "_aggregation",
		Type:      ast.NamedType(aggTypeName, pos),
		Arguments: queryArgsWithViewArgs(info, filterName, pos),
		Directives: ast.DirectiveList{
			{Name: "aggregation_query", Arguments: ast.ArgumentList{
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Position: pos,
	}

	// Bucket aggregation query: Type_bucket_aggregation
	bucketAggField := &ast.FieldDefinition{
		Name:      def.Name + "_bucket_aggregation",
		Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
		Arguments: queryArgsWithViewArgs(info, filterName, pos),
		Directives: ast.DirectiveList{
			{Name: "aggregation_query", Arguments: ast.ArgumentList{
				{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "name", Value: &ast.Value{Raw: def.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			optsCatalogDirective(opts),
		},
		Position: pos,
	}

	// Add aggregation query directives to the object itself
	def.Directives = append(def.Directives,
		&ast.Directive{Name: "query", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: def.Name + "_aggregation", Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: "AGGREGATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
		}, Position: pos},
		&ast.Directive{Name: "query", Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: def.Name + "_bucket_aggregation", Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: "AGGREGATE_BUCKET", Kind: ast.EnumValue, Position: pos}, Position: pos},
		}, Position: pos},
	)

	ctx.RegisterQueryFields(def.Name, []*ast.FieldDefinition{aggField, bucketAggField})

	return nil
}
