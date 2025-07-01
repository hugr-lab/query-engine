package compiler

import (
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// addH3Queries adds H3 query type and field to the schema if not already present.
func addH3Queries(schema *ast.SchemaDocument) {
	if schema.Definitions.ForName(base.H3QueryTypeName) != nil {
		return
	}
	if schema.Definitions.ForName(base.QueryTimeSpatialTypeName) == nil {
		return
	}
	qt := schema.Definitions.ForName(base.QueryBaseName)
	if qt == nil {
		return
	}
	// add h3 data queries type
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        base.H3DataQueryTypeName,
		Description: "Data queries for H3 aggregation",
		Position:    compiledPos(),
		Directives:  ast.DirectiveList{base.SystemDirective},
	}
	spatial := schema.Definitions.ForName(base.QueryTimeSpatialTypeName)
	for _, f := range spatial.Fields {
		if f.Name == base.H3QueryFieldName || IsFunctionCall(f) {
			continue
		}
		query := &ast.FieldDefinition{
			Name:        f.Name,
			Description: f.Description,
			Arguments:   copyArgumentDefinitionList(schema, def, f.Arguments),
			Type:        copyType(f.Type),
			Position:    compiledPos(),
			Directives:  copyDirectiveList(schema, def, f.Directives),
		}
		query.Arguments = append(query.Arguments,
			&ast.ArgumentDefinition{
				Name:        "transform_from",
				Description: "Reproject geometry from SRID before aggregation",
				Type:        ast.NamedType("Int", compiledPos()),
				Position:    compiledPos(),
			},
			&ast.ArgumentDefinition{
				Name:        "buffer",
				Description: "Buffer distance in meters to apply to the geometry before aggregation",
				Type:        ast.NamedType("Float", compiledPos()),
				Position:    compiledPos(),
			},
			&ast.ArgumentDefinition{
				Name:        "divide_values",
				Description: "Divide the aggregated values by a count of split h3 cells",
				Type:        ast.NamedType("Boolean", compiledPos()),
				Position:    compiledPos(),
			},
			&ast.ArgumentDefinition{
				Name:        "simplify",
				Description: "Simplify the geometry before cast to h3 cells",
				Type:        ast.NamedType("Boolean", compiledPos()),
				DefaultValue: &ast.Value{
					Kind:     ast.BooleanValue,
					Raw:      "true",
					Position: compiledPos(),
				},
				Position: compiledPos(),
			},
		)
		def.Fields = append(def.Fields, query)
	}
	if len(def.Fields) == 0 {
		return // no fields to add, skip
	}
	schema.Definitions = append(schema.Definitions, def)
	schema.Definitions = append(schema.Definitions, base.H3QueryDefinitionTemplate())
	qt.Fields = append(qt.Fields, &ast.FieldDefinition{
		Name:        base.H3QueryFieldName,
		Description: "H3 aggregation query",
		Type:        ast.ListType(ast.NamedType(base.H3QueryTypeName, compiledPos()), compiledPos()),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "resolution",
				Description: "H3 resolution to use for the query",
				Type:        ast.NonNullNamedType("Int", compiledPos()),
				Position:    compiledPos(),
			},
		},
		Position:   compiledPos(),
		Directives: ast.DirectiveList{base.SystemDirective},
	})
	// add h3 aggregation query type
}
