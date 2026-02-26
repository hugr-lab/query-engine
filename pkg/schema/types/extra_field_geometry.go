package types

import "github.com/vektah/gqlparser/v2/ast"

func generateGeometryExtraField(fieldName string) *ast.FieldDefinition {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction-extra-field"}}
	return &ast.FieldDefinition{
		Name:        "_" + fieldName + "_measurement",
		Description: "Calculate measurement of geometry field " + fieldName,
		Type:        ast.NamedType("Float", pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "type",
				Description: "Measurement type",
				Type:        ast.NonNullNamedType("GeometryMeasurementTypes", pos),
				Position:    pos,
			},
			{
				Name:        "transform",
				Description: "Reproject geometry (parameters from and to are required)",
				Type:        ast.NamedType("Boolean", pos),
				Position:    pos,
			},
			{
				Name:        "from",
				Description: "Converts geometry from the specified SRID",
				Type:        ast.NamedType("Int", pos),
				Position:    pos,
			},
			{
				Name:        "to",
				Description: "Converts geometry to the specified SRID",
				Type:        ast.NamedType("Int", pos),
				Position:    pos,
			},
		},
		Directives: ast.DirectiveList{
			{
				Name: "extra_field",
				Arguments: ast.ArgumentList{
					{
						Name:     "name",
						Value:    &ast.Value{Raw: "Measurement", Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_field",
						Value:    &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_type",
						Value:    &ast.Value{Raw: "Geometry", Kind: ast.EnumValue, Position: pos},
						Position: pos,
					},
				},
				Position: pos,
			},
			{
				Name: "sql",
				Arguments: ast.ArgumentList{
					{
						Name:     "exp",
						Value:    &ast.Value{Raw: "[" + fieldName + "]", Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
				},
				Position: pos,
			},
		},
		Position: pos,
	}
}
