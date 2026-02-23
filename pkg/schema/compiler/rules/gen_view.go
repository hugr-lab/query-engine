package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*ViewRule)(nil)

type ViewRule struct{}

func (r *ViewRule) Name() string     { return "ViewRule" }
func (r *ViewRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ViewRule) Match(def *ast.Definition) bool {
	// Match @view but NOT @table (TableRule handles those)
	return def.Directives.ForName("view") != nil && def.Directives.ForName("table") == nil
}

func (r *ViewRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		info = &base.ObjectInfo{
			Name:         def.Name,
			OriginalName: def.Name,
			IsView:       true,
		}
	}
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

	// 3. Generate aggregation type
	aggName := "_" + def.Name + "_aggregation"
	aggDef := generateAggregationType(ctx, def, aggName, pos)
	addDef(aggDef)

	// 3b. Generate bucket aggregation type
	bucketAggName := "_" + def.Name + "_aggregation_bucket"
	bucketAggDef := generateBucketAggregationType(def, aggName, filterName, bucketAggName, pos)
	addDef(bucketAggDef)

	// 4. Register query fields (views are read-only -- no mutation fields)
	queryFields := generateQueryFields(def, info, filterName, pos)
	ctx.RegisterQueryFields(def.Name, queryFields)

	return nil
}
