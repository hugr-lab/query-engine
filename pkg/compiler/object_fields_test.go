package compiler

import (
	"reflect"
	"testing"
)

func TestExtractFieldsFromSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "empty SQL",
			sql:  "",
			want: nil,
		},
		{
			name: "no fields",
			sql:  "SELECT * FROM table",
			want: nil,
		},
		{
			name: "single field",
			sql:  "SELECT [field1] FROM table",
			want: []string{"field1"},
		},
		{
			name: "multiple fields",
			sql:  "SELECT [field1], [field2] FROM table",
			want: []string{"field1", "field2"},
		},
		{
			name: "nested fields",
			sql:  "SELECT [table1.field1], [table2.field2] FROM table",
			want: []string{"table1.field1", "table2.field2"},
		},
		{
			name: "deep nested fields",
			sql:  "SELECT [table1.table2.field1], [table2.table3.field2.table3] FROM table",
			want: []string{"table1.table2.field1", "table2.table3.field2.table3"},
		},
		{
			name: "deep nested fields",
			sql:  "SELECT [table1:[$table2.field1], [table2.table3.field2.table3]] FROM table",
			want: []string{"$table2.field1", "table2.table3.field2.table3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractFieldsFromSQL(tt.sql); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractFieldsFromSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
