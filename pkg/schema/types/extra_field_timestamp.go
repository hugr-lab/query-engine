package types

import "github.com/vektah/gqlparser/v2/ast"

func generateTimestampExtraField(fieldName, baseType string) *ast.FieldDefinition {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction-extra-field"}}
	return &ast.FieldDefinition{
		Name:        "_" + fieldName + "_part",
		Description: "Extract part of " + baseType + " field " + fieldName,
		Type:        ast.NonNullNamedType("BigInt", pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:     "extract",
				Type:     ast.NonNullNamedType("TimeExtract", pos),
				Position: pos,
			},
			{
				Name:     "extract_divide",
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
						Value:    &ast.Value{Raw: "Extract", Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_field",
						Value:    &ast.Value{Raw: fieldName, Kind: ast.StringValue, Position: pos},
						Position: pos,
					},
					{
						Name:     "base_type",
						Value:    &ast.Value{Raw: baseType, Kind: ast.EnumValue, Position: pos},
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
