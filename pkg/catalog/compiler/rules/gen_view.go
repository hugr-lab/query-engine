package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.DefinitionRule = (*ViewRule)(nil)

type ViewRule struct{}

func (r *ViewRule) Name() string     { return "ViewRule" }
func (r *ViewRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *ViewRule) Match(def *ast.Definition) bool {
	// Match @view but NOT @table (TableRule handles those)
	return def.Directives.ForName(base.ObjectViewDirectiveName) != nil && def.Directives.ForName(base.ObjectTableDirectiveName) == nil
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

	// Parse @args directive for parameterized views
	if argsDir := def.Directives.ForName(base.ViewArgsDirectiveName); argsDir != nil {
		argInputName := base.DirectiveArgString(argsDir, base.ArgName)
		if argInputName == "" {
			return gqlerror.ErrorPosf(argsDir.Position, "object %s: @args directive requires 'name' argument", def.Name)
		}
		// Validate input type exists in source
		inputDef := ctx.Source().ForName(ctx.Context(), argInputName)
		if inputDef == nil {
			return gqlerror.ErrorPosf(argsDir.Position, "object %s: @args input type %q not found", def.Name, argInputName)
		}
		// Auto-compute required: true if any field in input type is NonNull
		required := base.DirectiveArgString(argsDir, base.ArgRequired) == "true"
		if !required {
			for _, f := range inputDef.Fields {
				if f.Type.NonNull {
					required = true
					break
				}
			}
		}
		info.InputArgsName = argInputName
		info.RequiredArgs = required

		// Propagate computed required to @args directive for downstream consumers
		if required && base.DirectiveArgString(argsDir, base.ArgRequired) != "true" {
			argsDir.Arguments = append(argsDir.Arguments, &ast.Argument{
				Name:     base.ArgRequired,
				Value:    &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos},
				Position: pos,
			})
		}

		// Pass the input type definition through to DDL feed
		// Add @catalog directive to the input type if this is a named catalog
		if viewOpts := ctx.CompileOptions(); viewOpts.Name != "" {
			inputDef.Directives = append(inputDef.Directives, optsCatalogDirective(viewOpts))
		}
		ctx.AddDefinition(inputDef)
	}

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

	// Note: _list_filter types are created lazily by gen_references.go
	// when a back-reference or M2M reference needs them.

	// 2c. Set scalar-specific field arguments
	setScalarFieldArguments(ctx, def)

	// Note: aggregation types (_X_aggregation, _X_aggregation_bucket) are generated
	// by AggregationRule which runs after TableRule/ViewRule.

	opts := ctx.CompileOptions()

	// Use original (unprefixed) name for query field names when AsModule
	fieldName := def.Name
	if opts.AsModule && info.OriginalName != "" && info.OriginalName != def.Name {
		fieldName = info.OriginalName
	}

	// 4. Register query fields (views are read-only -- no mutation fields)
	queryFields := generateQueryFields(ctx, def, info, filterName, pos)
	ctx.RegisterQueryFields(def.Name, queryFields)

	// Add @query directives on def
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "query",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
		},
		Position: pos,
	})
	if len(info.PrimaryKey) > 0 {
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

	return nil
}
