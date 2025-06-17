package engines

import (
	"fmt"
	"testing"
	"time"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/paulmach/orb"
	"github.com/vektah/gqlparser/v2/ast"
)

func Test_extractPGJsonFieldByPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		asText   bool
		expected string
	}{
		{
			name:     "simple path",
			path:     "field1",
			expected: "->'field1'",
		},
		{
			name:     "nested path",
			path:     "field1.subfield1",
			expected: "->'field1'->'subfield1'",
		},
		{
			name:     "deeply nested path",
			path:     "field1.subfield1.subsubfield1",
			expected: "->'field1'->'subfield1'->'subsubfield1'",
		},
		{
			name:     "empty path",
			path:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPGJsonFieldByPath(tt.path, tt.asText)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestRepackPGJsonRecursive(t *testing.T) {
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
							Directives: []*ast.Directive{
								{Name: "field_source", Arguments: []*ast.Argument{{Name: "field", Value: &ast.Value{Raw: "long short name"}}}},
							},
						},
					},
				},
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "String"}},
				}},
			},
			expected: "jsonb_build_object('testSubfield1',field1->'subfield1','testSubfield2',field1->'long short name')",
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
				ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
					{Name: "field1", Type: &ast.Type{NamedType: "String"}},
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
			},
			expected: "jsonb_build_object('subfield1A',jsonb_build_object('subsubfield1',field1->'subfield1'->'subsubfield1'))",
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
			},
			expected: "jsonb_build_object('subfield1',jsonb_build_object('subsubfield1',field1->'subfield1'->'subsubfield1'),'test',jsonb_build_object('subsubfield2A',field1->'subfield2'->'subsubfield2','subsubfield3',field1->'subfield2'->'subsubfield3'),'test2',field1->'subfield3')",
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
								{Name: "subfield1", Type: &ast.Type{NamedType: ""}},
								{Name: "subfield2", Type: &ast.Type{NamedType: ""}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: ""},
						},
					},
				},
			},
			expected: "jsonb_build_object('subfield1A', (SELECT array_agg(jsonb_build_object('subsubfield1',_value->'subsubfield1')) FROM jsonb_array_elements(field1->'subfield1') AS _value))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := repackPGJsonRecursive(tt.field.Alias, tt.field, "")
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func Test_postgres_SQLValue(t *testing.T) {
	engine := &Postgres{}
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
		{"time value", tm, fmt.Sprintf("'%s'::TIMESTAMP", tstr), false},
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
		{"geometry value", orb.LineString{{32, 43}, {23, 55}}, "ST_GeomFromText('LINESTRING(32 43,23 55)')", false},
		{"map value", map[string]any{"key": "value"}, "'{\"key\":\"value\"}'::JSONB", false},
		{"unsupported value", struct{}{}, "", true},
		// postgres specific types
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10}, "'(1,10)'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeLowerInclusive}, "'[1,10)'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeUpperInclusive}, "'(1,10]'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeLowerInclusive | types.RangeUpperInclusive}, "'[1,10]'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeEmpty}, "'empty'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeLowerInfinity}, "'(,10)'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeUpperInfinity}, "'(1,)'::INT4RANGE", false},
		{"int4range value", types.Int32Range{Lower: 1, Upper: 10, Detail: types.RangeLowerInfinity | types.RangeUpperInfinity}, "'(,)'::INT4RANGE", false},
		{"int8range value", types.Int64Range{Lower: 1, Upper: 10, Detail: types.RangeLowerInclusive | types.RangeUpperInfinity}, "'[1,)'::INT8RANGE", false},
		{"tstzrange value", types.TimeRange{
			Lower:  tm,
			Detail: types.RangeLowerInclusive | types.RangeUpperInfinity,
		}, fmt.Sprintf("'[%s,)'::TSTZRANGE", tstr), false},
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

func TestPostgresUnpackPGJsonRecursive(t *testing.T) {
	e := &Postgres{}
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
			expected: "field1->'subfield1' AS testSubfield1,field1->'subfield2' AS testSubfield2",
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
			},
			expected: "jsonb_build_object('subsubfield1',field1->'subfield1'->'subsubfield1') AS subfield1A",
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
									Name: "subsubfield1", Type: &ast.Type{NamedType: "String"},
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
			expected: "jsonb_build_object('subsubfield1',field1->'subfield1'->'subsubfield1') AS subfield1,jsonb_build_object('subsubfield2A',field1->'subfield2'->'subsubfield2','subsubfield3',field1->'subfield2'->'subsubfield3') AS test,field1->'subfield3' AS test2",
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
								{Name: "subfield1", Type: &ast.Type{NamedType: ""}},
								{Name: "subfield2", Type: &ast.Type{NamedType: ""}},
							},
						},
						Definition: &ast.FieldDefinition{
							Name: "subfield1", Type: &ast.Type{NamedType: ""},
						},
					},
				},
			},
			expected: "(SELECT array_agg(jsonb_build_object('subsubfield1',_values->'subsubfield1')) FROM jsonb_array_elements(field1->'subfield1') AS _value) AS subfield1A",
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

func TestPostgresEngineFilterOperationSQLValue(t *testing.T) {
	engine := &Postgres{}
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
			name:     "simple equality",
			sqlName:  "field",
			path:     "",
			op:       "eq",
			value:    123,
			params:   []any{123},
			expected: "field = $1",
			wantErr:  false,
		},
		{
			name:     "json path equality",
			sqlName:  "field",
			path:     "subfield",
			op:       "eq",
			value:    "value",
			params:   []any{},
			expected: "COALESCE(field @@ '$.subfield == \"value\"', false)",
			wantErr:  false,
		},
		{
			name:     "json path has",
			sqlName:  "field",
			path:     "subfield",
			op:       "has",
			value:    "value",
			params:   []any{},
			expected: "field @? '$.subfield'",
			wantErr:  false,
		},
		{
			name:     "json path like",
			sqlName:  "field",
			path:     "subfield",
			op:       "like",
			value:    "value",
			params:   []any{"value"},
			expected: "field->>'subfield' LIKE $1",
			wantErr:  false,
		},
		{
			name:     "json path ilike",
			sqlName:  "field",
			path:     "subfield",
			op:       "ilike",
			value:    "value",
			params:   []any{"value"},
			expected: "field->>'subfield' ILIKE $1",
			wantErr:  false,
		},
		{
			name:     "unsupported filter operator",
			sqlName:  "field",
			path:     "subfield",
			op:       "unsupported",
			value:    "value",
			params:   []any{},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "is null",
			sqlName:  "field",
			path:     "",
			op:       "is_null",
			value:    true,
			params:   []any{},
			expected: "field IS NULL",
			wantErr:  false,
		},
		{
			name:     "is not null",
			sqlName:  "field",
			path:     "",
			op:       "is_null",
			value:    false,
			params:   []any{},
			expected: "field IS NOT NULL",
			wantErr:  false,
		},
		{
			name:     "contains",
			sqlName:  "field",
			path:     "",
			op:       "contains",
			value:    []int{1, 2, 3},
			params:   []any{[]int{1, 2, 3}},
			expected: "field @> $1",
			wantErr:  false,
		},
		{
			name:     "intersects",
			sqlName:  "field",
			path:     "",
			op:       "intersects",
			value:    []int{1, 2, 3},
			params:   []any{[]int{1, 2, 3}},
			expected: "field && $1",
			wantErr:  false,
		},
		{
			name:     "geometry equals",
			sqlName:  "field",
			path:     "",
			op:       "eq",
			value:    orb.Point{1, 2},
			params:   []any{orb.Point{1, 2}},
			expected: "ST_Equals(field,$1)",
			wantErr:  false,
		},
		{
			name:     "geometry intersects",
			sqlName:  "field",
			path:     "",
			op:       "intersects",
			value:    orb.Point{1, 2},
			params:   []any{orb.Point{1, 2}},
			expected: "ST_Intersects(field,$1)",
			wantErr:  false,
		},
		{
			name:     "geometry contains",
			sqlName:  "field",
			path:     "",
			op:       "contains",
			value:    orb.Point{1, 2},
			params:   []any{orb.Point{1, 2}},
			expected: "ST_Contains(field,$1)",
			wantErr:  false,
		},
		{
			name:     "range equals",
			sqlName:  "field",
			path:     "",
			op:       "eq",
			value:    types.Int32Range{Lower: 1, Upper: 10},
			params:   []any{types.Int32Range{Lower: 1, Upper: 10}},
			expected: "field = $1",
			wantErr:  false,
		},
		{
			name:     "range intersects",
			sqlName:  "field",
			path:     "",
			op:       "intersects",
			value:    types.Int32Range{Lower: 1, Upper: 10},
			params:   []any{types.Int32Range{Lower: 1, Upper: 10}},
			expected: "field && $1",
			wantErr:  false,
		},
		{
			name:     "range includes",
			sqlName:  "field",
			path:     "",
			op:       "includes",
			value:    types.Int32Range{Lower: 1, Upper: 10},
			params:   []any{types.Int32Range{Lower: 1, Upper: 10}},
			expected: "field @> $1",
			wantErr:  false,
		},
		{
			name:     "eq operation with nested path",
			sqlName:  "field",
			path:     "nested.field",
			op:       "eq",
			value:    123,
			expected: "COALESCE(field @@ '$.nested.field == 123', false)",
			params:   []any{},
		},
		{
			name:     "contains operation with nested array",
			sqlName:  "field",
			path:     "nested.array",
			op:       "contains",
			value:    []int{1, 2, 3},
			expected: "(COALESCE(field @@ '$.nested.array[*] == 1', false)) AND (COALESCE(field @@ '$.nested.array[*] == 2', false)) AND (COALESCE(field @@ '$.nested.array[*] == 3', false))",
			params:   []any{},
		},
		{
			name:     "like operation with deeply nested string",
			sqlName:  "field",
			path:     "nested.deep.field",
			op:       "like",
			value:    "test",
			expected: "field->'nested'->'deep'->>'field' LIKE $1",
			params:   []any{"test"},
		},
		{
			name:     "in operation with int array",
			sqlName:  "field",
			op:       "in",
			value:    []int{1, 2, 3},
			expected: "field = ANY($1)",
			params:   []any{[]int{1, 2, 3}},
		},
		{
			name:     "in operation with nested path",
			sqlName:  "field",
			path:     "nested.field",
			op:       "in",
			value:    []int{1, 2, 3},
			expected: "(COALESCE(field @@ '$.nested.field == 1', false)) OR (COALESCE(field @@ '$.nested.field == 2', false)) OR (COALESCE(field @@ '$.nested.field == 3', false))",
			params:   []any{},
		},
		{
			name:     "eq operation with json",
			sqlName:  "field",
			op:       "eq",
			value:    map[string]any{"key": "value"},
			expected: "field = $1",
			params:   []any{map[string]any{"key": "value"}},
		},
		{
			name:     "contains operation with json",
			sqlName:  "field",
			op:       "contains",
			value:    map[string]any{"key": "value"},
			expected: "field @> $1",
			params:   []any{map[string]any{"key": "value"}},
		},
		{
			name:     "unsupported filter value type",
			sqlName:  "field",
			path:     "",
			op:       "eq",
			value:    struct{}{},
			params:   []any{},
			expected: "",
			wantErr:  true,
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
			if len(params) != len(tt.params) {
				t.Errorf("expected params length %d, got %d", len(tt.params), len(params))
			}
		})
	}
}

func TestExtractJSONStruct(t *testing.T) {
	engine := &Postgres{}
	tests := []struct {
		name       string
		sql        string
		jsonStruct map[string]any
		expected   string
	}{
		{
			name: "simple scalar field",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": "string",
			},
			expected: "jsonb_build_object('field1',(CASE WHEN jsonb_typeof(data->'field1') = 'string' THEN (data->'field1')::TEXT ELSE NULL END))",
		},
		{
			name: "nested object",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": map[string]any{
					"subfield1": "string",
				},
			},
			expected: "jsonb_build_object('field1',(SELECT jsonb_build_object('subfield1',(CASE WHEN jsonb_typeof(_value->'subfield1') = 'string' THEN (_value->'subfield1')::TEXT ELSE NULL END)) FROM (SELECT data->'field1' AS _value) AS _value))",
		},
		{
			name: "array of objects",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": []any{
					map[string]any{
						"subfield1": "string",
					},
				},
			},
			expected: "jsonb_build_object('field1',(CASE WHEN jsonb_typeof(data->'field1') = 'array' THEN (SELECT array_agg(_value) FROM (SELECT jsonb_build_object('subfield1',(CASE WHEN jsonb_typeof(_value->'subfield1') = 'string' THEN (_value->'subfield1')::TEXT ELSE NULL END)) AS _value FROM (SELECT jsonb_array_elements(data->'field1') AS _value) AS _value) WHERE _value IS NOT NULL AND _value != '{}'::JSONB) ELSE NULL END))",
		},
		{
			name: "array of scalars",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": []any{"string"},
			},
			expected: "jsonb_build_object('field1',(CASE WHEN jsonb_typeof(data->'field1') = 'array' THEN (SELECT array_agg((CASE WHEN jsonb_typeof(_value) = 'string' THEN (_value)::TEXT ELSE NULL END)) FROM (SELECT jsonb_array_elements(data->'field1') AS _value) AS _value) ELSE NULL END))",
		},
		{
			name: "empty array",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": []any{},
			},
			expected: "jsonb_build_object('field1',NULL)",
		},
		{
			name: "multiple fields",
			sql:  "data",
			jsonStruct: map[string]any{
				"field1": "string",
				"field2": map[string]any{
					"subfield1": "string",
				},
				"field3": []any{
					map[string]any{
						"subfield1": "string",
					},
				},
			},
			expected: "jsonb_build_object('field1',(CASE WHEN jsonb_typeof(data->'field1') = 'string' THEN (data->'field1')::TEXT ELSE NULL END),'field2',(SELECT jsonb_build_object('subfield1',(CASE WHEN jsonb_typeof(_value->'subfield1') = 'string' THEN (_value->'subfield1')::TEXT ELSE NULL END)) FROM (SELECT data->'field2' AS _value) AS _value),'field3',(CASE WHEN jsonb_typeof(data->'field3') = 'array' THEN (SELECT array_agg(_value) FROM (SELECT jsonb_build_object('subfield1',(CASE WHEN jsonb_typeof(_value->'subfield1') = 'string' THEN (_value->'subfield1')::TEXT ELSE NULL END)) AS _value FROM (SELECT jsonb_array_elements(data->'field3') AS _value) AS _value) WHERE _value IS NOT NULL AND _value != '{}'::JSONB) ELSE NULL END))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.ExtractJSONStruct(tt.sql, tt.jsonStruct)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}
