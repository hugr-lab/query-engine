package types

import "github.com/vektah/gqlparser/v2/ast"

func generateGeometryExtraField(fieldName string) *ast.FieldDefinition {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction-extra-field"}}
	return &ast.FieldDefinition{
		Name:        "_" + fieldName + "_measurement",
		Description: "Calculate measurement of geometry field " + fieldName,
		Type:        ast.NonNullNamedType("Float", pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:     "type",
				Type:     ast.NonNullNamedType("GeometryMeasurementTypes", pos),
				Position: pos,
			},
			{
				Name:     "transform",
				Type:     ast.NamedType("Boolean", pos),
				Position: pos,
			},
			{
				Name:     "from",
				Type:     ast.NamedType("Int", pos),
				Position: pos,
			},
			{
				Name:     "to",
				Type:     ast.NamedType("Int", pos),
				Position: pos,
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
