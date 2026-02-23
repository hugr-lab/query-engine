package types

import "github.com/vektah/gqlparser/v2/ast"

func generateVectorExtraField(fieldName string) *ast.FieldDefinition {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction-extra-field"}}
	return &ast.FieldDefinition{
		Name:        "_" + fieldName + "_distance",
		Description: "Calculate distance for vector field " + fieldName,
		Type:        ast.NonNullNamedType("Float", pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:     "vector",
				Type:     ast.NonNullListType(ast.NonNullNamedType("Float", pos), pos),
				Position: pos,
			},
			{
				Name:     "distance",
				Type:     ast.NonNullNamedType("VectorDistanceType", pos),
				Position: pos,
			},
		},
		Directives: ast.DirectiveList{
			{
				Name: "extra_field",
				Arguments: ast.ArgumentList{
					{
						Name:     "name",
						Value:    &ast.Value{Raw: "VectorDistance", Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_field",
						Value:    &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_type",
						Value:    &ast.Value{Raw: "Vector", Kind: ast.EnumValue, Position: pos},
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
