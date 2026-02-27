package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*AggregationRule)(nil)

// AggregationRule generates aggregation types for @table and @view definitions:
//   - _X_aggregation (base aggregation type with scalar fields)
//   - _X_aggregation_bucket (bucket aggregation with filter + base agg fields)
//   - _X_aggregation_sub_aggregation (sub-aggregation for nested reference queries)
//
// Also adds @query(AGGREGATE/AGGREGATE_BUCKET) metadata directives on the data object.
// Must run after UniqueRule to ensure correct directive ordering (SELECT_ONE before AGGREGATE).
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

	addDef := ctx.AddDefinition
	if info.IsReplace {
		addDef = ctx.AddDefinitionReplaceOrCreate
	}

	filterName := def.Name + "_filter"

	// 1. Generate aggregation type
	aggName := "_" + def.Name + "_aggregation"
	aggDef := generateAggregationType(ctx, def, aggName, pos)
	addDef(aggDef)

	// 2. Generate bucket aggregation type
	bucketAggName := "_" + def.Name + "_aggregation_bucket"
	bucketAggDef := generateBucketAggregationType(def, aggName, filterName, bucketAggName, opts, pos)
	addDef(bucketAggDef)

	// 3. Generate sub-aggregation type (scalar fields only — ExtraFieldRule adds extra fields later)
	subAggName := aggTypeNameAtDepth(def.Name, 1)
	ensureSubAggregationTypeNoExtra(ctx, def.Name, subAggName, 1, opts, pos)

	// 3b. Add aggregation fields for @join and @table_function_call_join fields
	addVirtualFieldAggregations(ctx, def, aggName, opts, pos)

	// 4. Add @query directives for AGGREGATE and AGGREGATE_BUCKET on the data object.
	// Use unprefixed field name when AsModule (matches query field names).
	fieldName := def.Name
	if opts.AsModule && info.OriginalName != "" && info.OriginalName != def.Name {
		fieldName = info.OriginalName
	}
	def.Directives = append(def.Directives,
		&ast.Directive{
			Name: "query",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: fieldName + "_aggregation", Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "AGGREGATE", Kind: ast.EnumValue, Position: pos}, Position: pos},
			},
			Position: pos,
		},
		&ast.Directive{
			Name: "query",
			Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: fieldName + "_bucket_aggregation", Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "type", Value: &ast.Value{Raw: "AGGREGATE_BUCKET", Kind: ast.EnumValue, Position: pos}, Position: pos},
			},
			Position: pos,
		},
	)

	return nil
}
