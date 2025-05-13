package engines

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/vektah/gqlparser/v2/ast"
)

func Test_repackStructRecursive(t *testing.T) {
	tests := []struct {
		name     string
		field    *ast.Field
		expected string
	}{
		{
			name: "simple field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "testSubfield1", Name: "subfield1",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "test"},
						},
					},
					&ast.Field{
						Alias: "testSubfield2", Name: "subfield2",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "test"},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Name: "field1", Type: &ast.Type{NamedType: "Object"},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "Object"}},
				}},
			},
			expected: "{testSubfield1: field1['subfield1'],testSubfield2: field1['subfield2']}",
		},
		{
			name: "full selection",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1", Name: "subfield1",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "test"},
						},
					},
					&ast.Field{
						Alias: "subfield2", Name: "subfield2",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "test"},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Name: "field1", Type: &ast.Type{NamedType: "Object"},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "Object"}},
				}},
			},
			expected: "field1",
		},
		{
			name: "nested field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1A",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "String"},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Name: "field1", Type: &ast.Type{NamedType: "Object"},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "Object"}},
				}},
			},
			expected: "{subfield1A: {subsubfield1: field1['subfield1']['subsubfield1']}}",
		},
		{
			name: "nested complex field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "String"},
						},
					},
					&ast.Field{
						Alias: "test",
						Name:  "subfield2",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield2A", Name: "subsubfield2",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield2", Type: &ast.Type{NamedType: "String"},
								},
							},
							&ast.Field{
								Alias: "subsubfield3", Name: "subsubfield3",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield3", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "String"},
						},
					},
					&ast.Field{
						Alias: "test2", Name: "subfield3",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield3", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield3", Type: &ast.Type{NamedType: "String"},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Name: "field1", Type: &ast.Type{NamedType: "Object"},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "Object"}},
				}},
			},
			expected: "{subfield1: {subsubfield1: field1['subfield1']['subsubfield1']},test: {subsubfield2A: field1['subfield2']['subsubfield2'],subsubfield3: field1['subfield2']['subsubfield3']},test2: field1['subfield3']}",
		},
		{
			name: "empty selection set",
			field: &ast.Field{
				Alias:        "field1",
				SelectionSet: ast.SelectionSet{},
			},
			expected: "field1",
		},
		{
			name: "nested array field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1A",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: ""},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Name: "field1", Type: &ast.Type{NamedType: "Object"},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "Object"}},
				}},
			},
			expected: "{subfield1A: list_transform(field1['subfield1'],_value->{subsubfield1: _value['subsubfield1']})}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := repackStructRecursive(tt.field.Alias, tt.field, "")
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func Test_extractStructFieldByPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "simple path",
			path:     "field1",
			expected: "['field1']",
		},
		{
			name:     "nested path",
			path:     "field1.subfield1",
			expected: "['field1']['subfield1']",
		},
		{
			name:     "deeply nested path",
			path:     "field1.subfield1.subsubfield1",
			expected: "['field1']['subfield1']['subsubfield1']",
		},
		{
			name:     "empty path",
			path:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractStructFieldByPath(tt.path)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func Test_duckdb_SQLValue(t *testing.T) {
	engine := &DuckDB{}
	tm := time.Now().Add(-1 * time.Hour)
	tstr := tm.Format(time.RFC3339)
	tests := []struct {
		name     string
		value    any
		expected string
		wantErr  bool
	}{
		{"nil value", nil, "NULL", false},
		{"bool value", true, "true", false},
		{"int value", 123, "123", false},
		{"float value", 123.45, "123.45", false},
		{"string value", "test", "'test'", false},
		{"string with single quote", "test's", "'test''s'", false},
		{"time value", time.Date(2023, 10, 10, 10, 10, 10, 0, time.UTC), "'2023-10-10T10:10:10Z'::TIMESTAMP", false},
		{"duration value", 35 * time.Second, "'35 seconds'::INTERVAL", false},
		{"duration value", 120 * time.Second, "'2 minutes'::INTERVAL", false},
		{"duration value", 122 * time.Second, "'2 minutes 2 seconds'::INTERVAL", false},
		{"duration value", 3600 * time.Second, "'1 hours'::INTERVAL", false},
		{"duration value", 3720 * time.Second, "'1 hours 2 minutes'::INTERVAL", false},
		{"duration value", 3620 * time.Second, "'1 hours 20 seconds'::INTERVAL", false},
		{"duration value", 7322 * time.Second, "'2 hours 2 minutes 2 seconds'::INTERVAL", false},
		{"bool array value", []bool{true, false}, "ARRAY[true,false]", false},
		{"int array value", []int{123, 1234}, "ARRAY[123,1234]", false},
		{"float array value", []float64{123.45, 3.14}, "ARRAY[123.45,3.14]", false},
		{"string array value", []string{"test", "test2"}, "ARRAY['test','test2']", false},
		{"string array with single quote", []string{"test's", "tt"}, "ARRAY['test''s','tt']", false},
		{"time array value", []time.Time{tm, tm}, fmt.Sprintf("ARRAY['%s'::TIMESTAMP,'%[1]s'::TIMESTAMP]", tstr), false},
		{"duration array value", []time.Duration{35 * time.Second, time.Second}, "ARRAY['35 seconds'::INTERVAL,'1 seconds'::INTERVAL]", false},
		{"map value", map[string]any{"key": "value"}, "'{\"key\":\"value\"}'::JSON", false},
		{"geometry value", orb.LineString{{32, 43}, {23, 55}}, "ST_GeomFromWKB('\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00@@\x00\x00\x00\x00\x00\x80E@\x00\x00\x00\x00\x00\x007@\x00\x00\x00\x00\x00\x80K@'::BLOB)", false},
		{"unsupported value", struct{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.SQLValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v", tt.wantErr, err)
			}
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDuckDBUnpackObject(t *testing.T) {
	e := &DuckDB{}
	tests := []struct {
		name     string
		field    *ast.Field
		expected string
	}{
		{
			name: "simple field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "testSubfield1", Name: "subfield1",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "test"},
						},
					},
					&ast.Field{
						Alias: "testSubfield2", Name: "subfield2",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "test"},
						},
					},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "String"}},
				}},
			},
			expected: "field1['subfield1'] AS testSubfield1,field1['subfield2'] AS testSubfield2",
		},
		{
			name: "nested field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1A",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield2", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "String"},
						},
					},
				},
			},
			expected: "{subsubfield1: field1['subfield1']['subsubfield1']} AS subfield1A",
		},
		{
			name: "nested complex field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield2", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: "String"},
						},
					},
					&ast.Field{
						Alias: "test",
						Name:  "subfield2",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield2A", Name: "subsubfield2",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield2", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield2", Type: &ast.Type{NamedType: "String"},
								},
							},
							&ast.Field{
								Alias: "subsubfield3", Name: "subsubfield3",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield2", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield3", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield3", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield2", Type: &ast.Type{NamedType: "String"},
						},
					},
					&ast.Field{
						Alias: "test2", Name: "subfield3",
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield3", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield3", Type: &ast.Type{NamedType: "String"},
						},
					},
				},
			},
			expected: "{subsubfield1: field1['subfield1']['subsubfield1']} AS subfield1,{subsubfield2A: field1['subfield2']['subsubfield2'],subsubfield3: field1['subfield2']['subsubfield3']} AS test,field1['subfield3'] AS test2",
		},
		{
			name: "nested array field",
			field: &ast.Field{
				Alias: "field1",
				Name:  "field1",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias: "subfield1A",
						Name:  "subfield1",
						SelectionSet: ast.SelectionSet{
							&ast.Field{
								Alias: "subsubfield1", Name: "subsubfield1",
								ObjectDefinition: &ast.Definition{Name: "test",
									Fields: []*ast.FieldDefinition{
										{Name: "subsubfield1", Type: &ast.Type{NamedType: "String"}},
										{Name: "subsubfield2", Type: &ast.Type{NamedType: "String"}},
									},
								},
								Definition: &ast.FieldDefinition{
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
								},
							},
						},
						ObjectDefinition: &ast.Definition{Name: "test",
							Fields: []*ast.FieldDefinition{
								{Name: "subfield1", Type: &ast.Type{NamedType: "String"}},
								{Name: "subfield2", Type: &ast.Type{NamedType: "String"}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: ""},
						},
					},
				},
			},
			expected: "list_transform(field1['subfield1'],_value->{subsubfield1: _value['subsubfield1']}) AS subfield1A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := e.UnpackObjectToFieldList(tt.field.Alias, tt.field)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDuckDBEngineFilterOperationSQLValue(t *testing.T) {
	engine := &DuckDB{}
	tests := []struct {
		name     string
		sqlName  string
		path     string
		op       string
		value    any
		params   []any
		expected string
		wantErr  bool
	}{
		{
			name:     "is_null operation",
			sqlName:  "field",
			op:       "is_null",
			expected: "field IS NULL",
		},
		{
			name:     "eq operation with int",
			sqlName:  "field",
			op:       "eq",
			value:    123,
			expected: "field = $1",
			params:   []any{123},
		},
		{
			name:     "gt operation with float",
			sqlName:  "field",
			op:       "gt",
			value:    123.45,
			expected: "field > $1",
			params:   []any{123.45},
		},
		{
			name:     "like operation with string",
			sqlName:  "field",
			op:       "like",
			value:    "test",
			expected: "field LIKE $1",
			params:   []any{"test"},
		},
		{
			name:     "contains operation with int array",
			sqlName:  "field",
			op:       "contains",
			value:    []int{1, 2, 3},
			expected: "list_has_all(field,$1)",
			params:   []any{[]int{1, 2, 3}},
		},
		{
			name:     "eq operation with geometry",
			sqlName:  "field",
			op:       "eq",
			value:    orb.Point{1, 2},
			expected: "ST_Equals(field,$1)",
			params:   []any{orb.Point{1, 2}},
		},
		{
			name:     "has operation with json",
			sqlName:  "field",
			op:       "has",
			value:    "value",
			expected: "json_exists(field,$1)",
			params:   []any{"value"},
		},
		{
			name:    "unsupported filter operator",
			sqlName: "field",
			op:      "unsupported",
			value:   123,
			wantErr: true,
		},
		{
			name:     "eq operation with nested path",
			sqlName:  "field",
			path:     "nested.field",
			op:       "eq",
			value:    123,
			expected: "field['nested']['field'] = $1",
			params:   []any{123},
		},
		{
			name:     "contains operation with nested array",
			sqlName:  "field",
			path:     "nested.array",
			op:       "contains",
			value:    []int{1, 2, 3},
			expected: "list_has_all(field['nested']['array'],$1)",
			params:   []any{[]int{1, 2, 3}},
		},
		{
			name:     "like operation with deeply nested string",
			sqlName:  "field",
			path:     "nested.deep.field",
			op:       "like",
			value:    "test",
			expected: "field['nested']['deep']['field'] LIKE $1",
			params:   []any{"test"},
		},
		{
			name:    "unsupported filter value type",
			sqlName: "field",
			op:      "eq",
			value:   struct{}{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, params, err := engine.FilterOperationSQLValue(tt.sqlName, tt.path, tt.op, tt.value, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v", tt.wantErr, err)
			}
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
			if !tt.wantErr && !reflect.DeepEqual(params, tt.params) {
				t.Errorf("expected params %v, got %v", tt.params, params)
			}
		})
	}
}
