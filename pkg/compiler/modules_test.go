package compiler

import (
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestModuleObject(t *testing.T) {
	tests := []struct {
		module          string
		objectType      ModuleObjectType
		expected        string
		expectedParents []string
	}{
		{"", ModuleQuery, queryBaseName, nil},
		{"", ModuleMutation, mutationBaseName, nil},
		{"module1", ModuleQuery, "module1_query", []string{queryBaseName}},
		{"module1.module2", ModuleQuery, "module1_module2_query", []string{queryBaseName, "module1"}},
		{"module1", ModuleMutation, "module1_mutation", []string{mutationBaseName}},
		{"module1.module2", ModuleMutation, "module1_module2_mutation", []string{mutationBaseName, "module1"}},
		{"module1", ModuleFunction, "module1_function", []string{base.FunctionTypeName}},
		{"module1.module2", ModuleFunction, "module1_module2_function", []string{base.FunctionTypeName, "module1"}},
		{"module1", ModuleMutationFunction, "module1_function_mutation", []string{base.FunctionMutationTypeName}},
		{"module1.module2", ModuleMutationFunction, "module1_module2_function_mutation", []string{base.FunctionMutationTypeName, "module1"}},
	}

	for _, tt := range tests {
		t.Run(tt.module, func(t *testing.T) {
			schema := &ast.SchemaDocument{
				Definitions: ast.DefinitionList{
					&ast.Definition{
						Kind: ast.Object,
						Name: queryBaseName,
					},
					&ast.Definition{
						Kind: ast.Object,
						Name: mutationBaseName,
					},
					&ast.Definition{
						Kind: ast.Object,
						Name: base.FunctionTypeName,
					},
					&ast.Definition{
						Kind: ast.Object,
						Name: base.FunctionMutationTypeName,
					},
				},
			}

			def, err := moduleType(schema, tt.module, tt.objectType)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if def.Name != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, def.Name)
			}
			for _, def := range schema.Definitions {
				idx := slices.Index(tt.expectedParents, def.Name)
				found := false
				for _, field := range def.Fields {
					if idx == -1 {
						// check that parent doesn't have a field with the returned object name and parents names
						if field.Name == tt.expected {
							t.Errorf("expected %s to not have a field %s", def.Name, tt.expected)
							continue
						}
						if slices.Contains(tt.expectedParents, field.Name) {
							t.Errorf("expected %s to not have a field %s", def.Name, field.Name)
						}
						continue
					}
					if found && idx > -1 {
						t.Errorf("expected %s to have only one field from hier %s", def.Name, tt.expectedParents)
					}
					if idx < len(tt.expectedParents) && tt.expectedParents[idx] == field.Name {
						found = true
					}
				}
			}
		})
	}
}
