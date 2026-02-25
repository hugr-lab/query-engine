package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*FunctionRule)(nil)

type FunctionRule struct{}

func (r *FunctionRule) Name() string     { return "FunctionRule" }
func (r *FunctionRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *FunctionRule) Match(def *ast.Definition) bool {
	// Match Function and MutationFunction type definitions.
	// These come from "extend type Function { ... }" in user SDL,
	// merged into definitions during source extraction.
	return def.Name == "Function" || def.Name == "MutationFunction"
}

func (r *FunctionRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	opts := ctx.CompileOptions()
	pos := compiledPos("function")

	for _, field := range def.Fields {
		// Skip stub/placeholder fields
		if field.Name == "_stub" || field.Name == "_placeholder" {
			continue
		}

		funcDir := field.Directives.ForName("function")
		if funcDir == nil {
			continue
		}

		// Add @catalog directive
		if field.Directives.ForName("catalog") == nil {
			field.Directives = append(field.Directives, catalogDirective(opts.Name, opts.EngineType))
		}

		// Handle @module for AsModule option
		if opts.AsModule {
			if d := field.Directives.ForName(base.ModuleDirectiveName); d != nil {
				if a := d.Arguments.ForName(base.ArgName); a != nil {
					if a.Value.Raw == "" {
						a.Value.Raw = opts.Name
					} else {
						a.Value.Raw = opts.Name + "." + a.Value.Raw
					}
				}
			} else {
				field.Directives = append(field.Directives, &ast.Directive{
					Name: "module",
					Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: opts.Name, Position: pos}, Position: pos},
					},
					Position: pos,
				})
			}
		}
	}

	// Emit empty Function/MutationFunction definition with @if_not_exists
	// (multi-catalog: type may already exist from a previous catalog)
	ctx.AddDefinition(&ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "system", Position: pos},
			{Name: "if_not_exists", Position: pos},
		},
	})

	// Emit function fields as extension (merged into existing type by provider.Update)
	ext := &ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: pos,
		Fields:   def.Fields,
	}
	ctx.AddExtension(ext)

	// Add aggregation/bucket_aggregation fields for table-returning functions.
	// The old compiler adds these for functions that return list types pointing
	// to data objects (e.g., find_nearby: [Airport] → find_nearby_aggregation).
	addFunctionAggregationFields(ctx, ext, opts, pos)

	return nil
}

// addFunctionAggregationFields adds _aggregation and _bucket_aggregation fields
// to the Function/MutationFunction type for each function that returns a list of
// a data object. This matches the old compiler's addAggregationQueryField behavior.
func addFunctionAggregationFields(ctx base.CompilationContext, def *ast.Definition, opts base.Options, pos *ast.Position) {
	var aggFields ast.FieldList

	for _, field := range def.Fields {
		if field.Name == "_stub" || field.Name == "_placeholder" {
			continue
		}
		if field.Directives.ForName("function") == nil {
			continue
		}
		// Skip fields that have @module — the ModuleAssembler's
		// addModuleFuncAggregations handles agg fields for module function types.
		if field.Directives.ForName(base.ModuleDirectiveName) != nil {
			continue
		}
		// Only table-returning functions (list type)
		if field.Type.NamedType != "" {
			continue
		}
		targetName := field.Type.Name()
		if ctx.IsScalar(targetName) {
			continue
		}
		// Check that target has an aggregation type
		aggTypeName := "_" + targetName + "_aggregation"
		bucketAggTypeName := "_" + targetName + "_aggregation_bucket"
		if ctx.LookupType(aggTypeName) == nil {
			continue
		}

		// Aggregation field
		aggFields = append(aggFields, &ast.FieldDefinition{
			Name:      field.Name + "_aggregation",
			Type:      ast.NamedType(aggTypeName, pos),
			Arguments: cloneArgDefs(field.Arguments, pos),
			Directives: ast.DirectiveList{
				{Name: "aggregation_query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		})

		// Bucket aggregation field
		aggFields = append(aggFields, &ast.FieldDefinition{
			Name:      field.Name + "_bucket_aggregation",
			Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
			Arguments: cloneArgDefs(field.Arguments, pos),
			Directives: ast.DirectiveList{
				{Name: "aggregation_query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: field.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				}, Position: pos},
				optsCatalogDirective(opts),
			},
			Position: pos,
		})
	}

	// Add directly to the definition so fields stay on the Function type.
	def.Fields = append(def.Fields, aggFields...)
}
