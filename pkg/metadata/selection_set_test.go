package metadata

import (
	"context"
	"reflect"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestProcessSelectionSet(t *testing.T) {
	tests := []struct {
		name      string
		ss        ast.SelectionSet
		resolvers map[string]fieldResolverFunc
		onType    string
		expected  map[string]any
		wantErr   bool
	}{
		{
			name: "single field",
			ss: ast.SelectionSet{
				&ast.Field{Name: "field1"},
			},
			resolvers: map[string]fieldResolverFunc{
				"field1": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value1", nil
				},
			},
			expected: map[string]any{"field1": "value1"},
		},
		{
			name: "field resolver not found",
			ss: ast.SelectionSet{
				&ast.Field{Name: "field1", Position: &ast.Position{}},
			},
			resolvers: map[string]fieldResolverFunc{},
			wantErr:   true,
		},
		{
			name: "fragment spread",
			ss: ast.SelectionSet{
				&ast.FragmentSpread{
					Name: "fragment1",
					Definition: &ast.FragmentDefinition{
						SelectionSet: ast.SelectionSet{
							&ast.Field{Name: "field1"},
						},
					},
				},
			},
			resolvers: map[string]fieldResolverFunc{
				"field1": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value1", nil
				},
			},
			expected: map[string]any{"field1": "value1"},
		},
		{
			name: "inline fragment",
			ss: ast.SelectionSet{
				&ast.InlineFragment{
					TypeCondition: "Type1",
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "field1"},
					},
				},
			},
			resolvers: map[string]fieldResolverFunc{
				"field1": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value1", nil
				},
			},
			onType:   "Type1",
			expected: map[string]any{"field1": "value1"},
		},
		{
			name: "nested fields",
			ss: ast.SelectionSet{
				&ast.Field{
					Name: "field1",
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "nestedField1"},
					},
				},
			},
			resolvers: map[string]fieldResolverFunc{
				"field1": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return map[string]any{"nestedField1": "nestedValue1"}, nil
				},
			},
			expected: map[string]any{"field1": map[string]any{"nestedField1": "nestedValue1"}},
		},
		{
			name: "multiple fragments and fields",
			ss: ast.SelectionSet{
				&ast.Field{Name: "field1"},
				&ast.FragmentSpread{
					Name: "fragment1",
					Definition: &ast.FragmentDefinition{
						SelectionSet: ast.SelectionSet{
							&ast.Field{Name: "field2"},
						},
					},
				},
				&ast.InlineFragment{
					TypeCondition: "Type1",
					SelectionSet: ast.SelectionSet{
						&ast.Field{Name: "field3"},
					},
				},
			},
			resolvers: map[string]fieldResolverFunc{
				"field1": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value1", nil
				},
				"field2": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value2", nil
				},
				"field3": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
					return "value3", nil
				},
			},
			onType:   "Type1",
			expected: map[string]any{"field1": "value1", "field2": "value2", "field3": "value3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processSelectionSet(t.Context(), tt.ss, tt.resolvers, tt.onType)
			if (err != nil) != tt.wantErr {
				t.Errorf("processSelectionSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("processSelectionSet() = %v, expected %v", got, tt.expected)
			}
		})
	}
}
