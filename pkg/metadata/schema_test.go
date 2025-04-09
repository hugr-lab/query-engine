package metadata

import (
	"reflect"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func Test_directiveResolver(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
		},
	}

	directiveDef := &ast.DirectiveDefinition{
		Name:        "testDirective",
		Description: "A test directive",
		Locations:   []ast.DirectiveLocation{ast.LocationField, ast.LocationFragmentSpread},
		Arguments: []*ast.ArgumentDefinition{
			{
				Name: "arg1",
				Type: ast.NamedType("String", &ast.Position{}),
			},
		},
		IsRepeatable: true,
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "locations",
		},
		&ast.Field{
			Name: "args",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "type",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
					},
				},
			},
		},
		&ast.Field{
			Name: "isRepeatable",
		},
	}

	expected := map[string]interface{}{
		"name":        "testDirective",
		"description": "A test directive",
		"locations":   []ast.DirectiveLocation{ast.LocationField, ast.LocationFragmentSpread},
		"args": []map[string]interface{}{
			{
				"name": "arg1",
				"type": map[string]interface{}{
					"name": "String",
				},
			},
		},
		"isRepeatable": true,
	}

	result, err := directiveResolver(t.Context(), schema, directiveDef, selectionSet, 10)
	if err != nil {
		t.Fatalf("directiveResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("directiveResolver() = %v, expected %v", result, expected)
	}
}

func Test_inputValueResolver(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
		},
	}

	fieldDef := &ast.FieldDefinition{
		Name:        "testField",
		Description: "A test field",
		Type:        ast.NamedType("String", &ast.Position{}),
		DefaultValue: &ast.Value{
			Raw: "defaultValue",
		},
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "type",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
			},
		},
		&ast.Field{
			Name: "defaultValue",
		},
	}

	expected := map[string]interface{}{
		"name":        "testField",
		"description": "A test field",
		"type": map[string]interface{}{
			"name": "String",
		},
		"defaultValue": "defaultValue",
	}

	result, err := inputValueResolver(t.Context(), schema, fieldDef, selectionSet, 10)
	if err != nil {
		t.Fatalf("inputValueResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("inputValueResolver() = %v, expected %v", result, expected)
	}
}

func Test_argumentResolver(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
		},
	}

	argDef := &ast.ArgumentDefinition{
		Name:        "testArg",
		Description: "A test argument",
		Type:        ast.NamedType("String", &ast.Position{}),
		DefaultValue: &ast.Value{
			Raw: "defaultValue",
		},
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "type",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
			},
		},
		&ast.Field{
			Name: "defaultValue",
		},
	}

	expected := map[string]interface{}{
		"name":        "testArg",
		"description": "A test argument",
		"type": map[string]interface{}{
			"name": "String",
		},
		"defaultValue": "defaultValue",
	}

	result, err := argumentResolver(t.Context(), schema, argDef, selectionSet, 10)
	if err != nil {
		t.Fatalf("argumentResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("argumentResolver() = %v, expected %v", result, expected)
	}
}

func Test_enumValueResolver(t *testing.T) {
	enumDef := &ast.EnumValueDefinition{
		Name:        "TEST_ENUM",
		Description: "A test enum value",
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "isDeprecated",
		},
		&ast.Field{
			Name: "deprecationReason",
		},
	}

	expected := map[string]interface{}{
		"name":              "TEST_ENUM",
		"description":       "A test enum value",
		"isDeprecated":      false,
		"deprecationReason": nil,
	}

	result, err := enumValueResolver(t.Context(), enumDef, selectionSet)
	if err != nil {
		t.Fatalf("enumValueResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("enumValueResolver() = %v, expected %v", result, expected)
	}
}

func Test_enumValueResolver_withDeprecation(t *testing.T) {
	enumDef := &ast.EnumValueDefinition{
		Name:        "TEST_ENUM",
		Description: "A test enum value",
		Directives: ast.DirectiveList{
			&ast.Directive{
				Name: "deprecated",
				Arguments: ast.ArgumentList{
					{
						Name: "reason",
						Value: &ast.Value{
							Raw:  "Deprecated for testing",
							Kind: ast.StringValue,
						},
					},
				},
			},
		},
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "isDeprecated",
		},
		&ast.Field{
			Name: "deprecationReason",
		},
	}

	expected := map[string]interface{}{
		"name":              "TEST_ENUM",
		"description":       "A test enum value",
		"isDeprecated":      true,
		"deprecationReason": "Deprecated for testing",
	}

	result, err := enumValueResolver(t.Context(), enumDef, selectionSet)
	if err != nil {
		t.Fatalf("enumValueResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("enumValueResolver() = %v, expected %v", result, expected)
	}
}

func Test_fieldResolver(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
		},
	}

	fieldDef := &ast.FieldDefinition{
		Name:        "testField",
		Description: "A test field",
		Type:        ast.NamedType("String", &ast.Position{}),
		Arguments: []*ast.ArgumentDefinition{
			{
				Name: "arg1",
				Type: ast.NamedType("String", &ast.Position{}),
			},
		},
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "args",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "type",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
					},
				},
			},
		},
		&ast.Field{
			Name: "type",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
			},
		},
		&ast.Field{
			Name: "isDeprecated",
		},
		&ast.Field{
			Name: "deprecationReason",
		},
	}

	expected := map[string]interface{}{
		"name":        "testField",
		"description": "A test field",
		"args": []map[string]interface{}{
			{
				"name": "arg1",
				"type": map[string]interface{}{
					"name": "String",
				},
			},
		},
		"type": map[string]interface{}{
			"name": "String",
		},
		"isDeprecated":      false,
		"deprecationReason": nil,
	}

	result, err := fieldResolver(t.Context(), schema, fieldDef, selectionSet, 10)
	if err != nil {
		t.Fatalf("fieldResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("fieldResolver() = %v, expected %v", result, expected)
	}
}

func Test_fieldResolver_withDeprecation(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
		},
	}

	fieldDef := &ast.FieldDefinition{
		Name:        "testField",
		Description: "A test field",
		Type:        ast.NamedType("String", &ast.Position{}),
		Directives: ast.DirectiveList{
			&ast.Directive{
				Name: "deprecated",
				Arguments: ast.ArgumentList{
					{
						Name: "reason",
						Value: &ast.Value{
							Raw:  "Deprecated for testing",
							Kind: ast.StringValue,
						},
					},
				},
			},
		},
	}

	selectionSet := ast.SelectionSet{
		&ast.Field{
			Name: "name",
		},
		&ast.Field{
			Name: "description",
		},
		&ast.Field{
			Name: "args",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "type",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
					},
				},
			},
		},
		&ast.Field{
			Name: "type",
			SelectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "name",
				},
			},
		},
		&ast.Field{
			Name: "isDeprecated",
		},
		&ast.Field{
			Name: "deprecationReason",
		},
	}

	expected := map[string]interface{}{
		"name":        "testField",
		"description": "A test field",
		"args":        []map[string]any{},
		"type": map[string]any{
			"name": any("String"),
		},
		"isDeprecated":      true,
		"deprecationReason": "Deprecated for testing",
	}

	result, err := fieldResolver(t.Context(), schema, fieldDef, selectionSet, 10)
	if err != nil {
		t.Fatalf("fieldResolver() error = %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("fieldResolver() = %v, expected %v", result, expected)
	}
}

func Test_typeResolver(t *testing.T) {
	schema := &ast.Schema{
		Types: map[string]*ast.Definition{
			"String": {
				Kind: ast.Scalar,
				Name: "String",
			},
			"TestType": {
				Kind:        ast.Object,
				Name:        "TestType",
				Description: "A test type",
				Fields: []*ast.FieldDefinition{
					{
						Name: "field1",
						Type: ast.NamedType("String", &ast.Position{}),
					},
				},
			},
			"TestEnum": {
				Kind: ast.Enum,
				Name: "TestEnum",
				EnumValues: []*ast.EnumValueDefinition{
					{
						Name: "VALUE1",
					},
					{
						Name: "VALUE2",
					},
				},
			},
			"TestUnion": {
				Kind: ast.Union,
				Name: "TestUnion",
				Types: []string{
					"Type1",
					"Type2",
				},
			},
			"Type1": {
				Kind: ast.Object,
				Name: "Type1",
			},
			"Type2": {
				Kind: ast.Object,
				Name: "Type2",
			},
		},
	}

	tests := []struct {
		name         string
		typeDef      *ast.Type
		selectionSet ast.SelectionSet
		expected     map[string]interface{}
		expectedErr  error
	}{
		{
			name:    "Object type",
			typeDef: ast.NamedType("TestType", &ast.Position{}),
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "description",
				},
				&ast.Field{
					Name: "fields",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
						&ast.Field{
							Name: "type",
							SelectionSet: ast.SelectionSet{
								&ast.Field{
									Name: "name",
								},
							},
						},
					},
				},
			},
			expected: map[string]interface{}{
				"kind":        ast.Object,
				"name":        "TestType",
				"description": "A test type",
				"fields": []map[string]interface{}{
					{
						"name": "field1",
						"type": map[string]interface{}{
							"name": "String",
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "NonNull type",
			typeDef: &ast.Type{
				NamedType: "String",
				NonNull:   true,
			},
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
			},
			expected: map[string]interface{}{
				"kind": "NON_NULL",
			},
			expectedErr: nil,
		},
		{
			name: "List type",
			typeDef: &ast.Type{
				Elem: ast.NamedType("String", &ast.Position{}),
			},
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
			},
			expected: map[string]interface{}{
				"kind": "LIST",
			},
			expectedErr: nil,
		},
		{
			name:    "Type not found",
			typeDef: ast.NamedType("UnknownType", &ast.Position{}),
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
			},
			expected:    nil,
			expectedErr: ErrTypeNotFound,
		},
		{
			name:    "Enum type",
			typeDef: ast.NamedType("TestEnum", &ast.Position{}),
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "enumValues",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
					},
				},
			},
			expected: map[string]interface{}{
				"kind": ast.Enum,
				"name": "TestEnum",
				"enumValues": []map[string]interface{}{
					{
						"name": "VALUE1",
					},
					{
						"name": "VALUE2",
					},
				},
			},
			expectedErr: nil,
		},
		{
			name:    "Union type",
			typeDef: ast.NamedType("TestUnion", &ast.Position{}),
			selectionSet: ast.SelectionSet{
				&ast.Field{
					Name: "kind",
				},
				&ast.Field{
					Name: "name",
				},
				&ast.Field{
					Name: "possibleTypes",
					SelectionSet: ast.SelectionSet{
						&ast.Field{
							Name: "name",
						},
					},
				},
			},
			expected: map[string]interface{}{
				"kind": ast.Union,
				"name": "TestUnion",
				"possibleTypes": []map[string]interface{}{
					{
						"name": "Type1",
					},
					{
						"name": "Type2",
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := typeResolver(t.Context(), schema, tt.typeDef, tt.selectionSet, 10)
			if err != tt.expectedErr {
				t.Fatalf("typeResolver() error = %v, expectedErr %v", err, tt.expectedErr)
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("typeResolver() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
