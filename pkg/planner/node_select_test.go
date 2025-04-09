package planner

import (
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestWhereFieldNode(t *testing.T) {
	tests := []struct {
		name           string
		objectName     string
		prefix         string
		field          string
		value          any
		expected       string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "Valid field with value",
			prefix:         "_objects",
			objectName:     "table_object",
			field:          "field1",
			value:          map[string]any{"eq": "value1"},
			expected:       "_objects.field1 = $1",
			expectedParams: []any{"value1"},
		},
		{
			name:           "Valid field with value",
			prefix:         "_objects",
			objectName:     "table_object",
			field:          "field2",
			value:          map[string]any{"eq": 1},
			expected:       "_objects.field2 = $1",
			expectedParams: []any{1},
		},
		{
			name:           "Valid field with value pg",
			prefix:         "_objects",
			objectName:     "pg_table_object",
			field:          "field2",
			value:          map[string]any{"eq": 1},
			expected:       "_objects.field2 = $1",
			expectedParams: []any{1},
		},
		{
			name:       "Valid field with nested value pg",
			prefix:     "_objects",
			objectName: "pg_table_object",
			field:      "nested_field",
			value: map[string]any{
				"field2": map[string]any{
					"eq": 1,
				},
			},
			expected:       "(COALESCE(_objects.nested_field @@ '$.field2 == 1', false))",
			expectedParams: []any{},
		},
		{
			name:       "Valid field with nested value pg",
			prefix:     "_objects",
			objectName: "pg_table_object",
			field:      "nested_field",
			value: map[string]any{
				"field1": map[string]any{
					"eq": "value1",
				},
			},
			expected:       "(COALESCE(_objects.nested_field @@ '$.field1 == \"value1\"', false))",
			expectedParams: []any{},
		},
		{
			name:       "Valid field with deep nested array value pg",
			prefix:     "_objects",
			objectName: "pg_table_object",
			field:      "nested_field",
			value: map[string]any{
				"fieldNested": map[string]any{
					"array_field": map[string]any{
						"intersects": []string{"value1", "value2"},
					},
				},
			},
			expected:       "(((COALESCE(_objects.nested_field @@ '$.fieldNested.array_field[*] == \"value1\"', false)) OR (COALESCE(_objects.nested_field @@ '$.fieldNested.array_field[*] == \"value2\"', false))))",
			expectedParams: []any{},
		},
		{
			name:       "Valid field with nested value",
			prefix:     "_objects",
			objectName: "table_object",
			field:      "nested_field",
			value: map[string]any{
				"field2": map[string]any{
					"eq": 1,
				},
			},
			expected:       "(_objects.nested_field['field2'] = $1)",
			expectedParams: []any{1},
		},
		{
			name:       "Valid field with nested value",
			prefix:     "_objects",
			objectName: "table_object",
			field:      "nested_field",
			value: map[string]any{
				"field1": map[string]any{
					"eq": "value1",
				},
			},
			expected:       "(_objects.nested_field['field1'] = $1)",
			expectedParams: []any{"value1"},
		},
		{
			name:       "Valid field with deep nested array value ",
			prefix:     "_objects",
			objectName: "table_object",
			field:      "nested_field",
			value: map[string]any{
				"fieldNested": map[string]any{
					"array_field": map[string]any{
						"intersects": []string{"value1", "value2"},
					},
				},
			},
			expected:       "((list_has_any(_objects.nested_field['fieldNested']['array_field'],$1)))",
			expectedParams: []any{[]string{"value1", "value2"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testSchema := testCats.Schema()
			def := testSchema.Types[tt.objectName]
			if def == nil {
				t.Fatalf("object %s not found", tt.objectName)
			}
			info := compiler.DataObjectInfo(def)
			node, err := whereFieldNode(info, tt.prefix, tt.field, tt.value, false)
			if (err != nil) != tt.wantErr {
				t.Fatalf("whereFieldNode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if node != nil {
				node.schema = testSchema
				node.engines = testCats
				sql, params, err := node.CollectFunc(node, nil, nil)
				if (err != nil) != tt.wantErr {
					t.Fatalf("CollectFunc() error = %v", err)
				}
				if sql != tt.expected {
					t.Errorf("CollectFunc() sql = %v, want %v", sql, tt.expected)
				}
				if len(params) != len(tt.expectedParams) {
					t.Errorf("CollectFunc() params = %v, want %v", params, tt.expectedParams)
				}
			}
		})
	}
}

func equalFields(expected, actual *ast.Field) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}
	if expected.Name != actual.Name {
		return false
	}
	if len(expected.SelectionSet) != len(actual.SelectionSet) {
		return false
	}
	for i := range expected.SelectionSet {
		if expected.SelectionSet[i].(*ast.Field).Name != actual.SelectionSet[i].(*ast.Field).Name {
			return false
		}
	}
	return true
}

func TestSplit(t *testing.T) {
	t.Log(strings.SplitN("_a_b_c", "_", 2)[0])
}
