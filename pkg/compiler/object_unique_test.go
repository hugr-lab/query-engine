package compiler

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestValidateObjectUnique(t *testing.T) {
	tests := []struct {
		name    string
		defs    ast.DefinitionList
		defName string
		unique  *ast.Directive
		wantErr bool
	}{
		{
			name:    "nil unique directive",
			defs:    ast.DefinitionList{},
			defName: "",
			unique:  nil,
			wantErr: false,
		},
		{
			name: "valid unique directive",
			defs: ast.DefinitionList{
				&ast.Definition{
					Name: "TestType",
					Fields: []*ast.FieldDefinition{
						{
							Name: "id",
							Type: ast.NamedType("Int", nil),
						},
					},
				},
			},
			defName: "TestType",
			unique: &ast.Directive{
				Name: "unique",
				Arguments: ast.ArgumentList{
					{
						Name: "fields",
						Value: &ast.Value{
							Children: []*ast.ChildValue{{Value: &ast.Value{Raw: "id"}}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid field in unique directive",
			defs: ast.DefinitionList{
				&ast.Definition{
					Name: "TestType",
					Fields: []*ast.FieldDefinition{
						{
							Name: "id",
							Type: ast.NamedType("ID", nil),
						},
					},
				},
			},
			defName: "TestType",
			unique: &ast.Directive{
				Name: "unique",
				Arguments: ast.ArgumentList{
					{
						Name: "fields",
						Value: &ast.Value{
							Children: []*ast.ChildValue{
								{Value: &ast.Value{Raw: "nonexistent"}},
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := tt.defs.ForName(tt.defName)

			err := validateObjectUnique(tt.defs, def, tt.unique)
			if tt.wantErr != (err != nil) {
				t.Errorf("expected error: %v, got: %v", tt.wantErr, err)
			}
		})
	}
}
