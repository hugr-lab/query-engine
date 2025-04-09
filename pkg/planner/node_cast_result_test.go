package planner

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

func Test_castResultsNode(t *testing.T) {
	caster := &engines.Postgres{}
	node := &QueryPlanNode{
		engines: testService.engines,
		schema:  testCats.Schema(),
	}

	tests := []struct {
		name              string
		field             *ast.Field
		params            []any
		wrappedSQL        string
		expectedSQL       string
		expectedParamsLen int
	}{
		{
			name: "selection scalar value",
			field: &ast.Field{
				Alias: "selected",
				Name:  "selected",
				SelectionSet: ast.SelectionSet{
					&ast.Field{Alias: "int", Name: "field1", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Int"}}},
					&ast.Field{Alias: "string", Name: "field2", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "String"}}},
					&ast.Field{Alias: "float", Name: "field3", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Float"}}},
					&ast.Field{Alias: "bool", Name: "field4", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Boolean"}}},
				},
				Definition: &ast.FieldDefinition{
					Type: ast.ListType(ast.NamedType("NamedObjectType", nil), nil),
					Directives: ast.DirectiveList{
						&ast.Directive{Name: "catalog", Arguments: ast.ArgumentList{{Name: "name", Value: &ast.Value{Raw: "db"}}}},
					},
				},
				ObjectDefinition: &ast.Definition{},
			},
			wrappedSQL:  "[wrapped sql]",
			expectedSQL: `SELECT "int" AS "int","string" AS "string","float" AS "float","bool" AS "bool" FROM postgres_query(db,' (SELECT "int" AS "int","string" AS "string","float" AS "float","bool" AS "bool" FROM [wrapped sql] AS _objects) ') AS _objects`,
		},
		{
			name: "selection with objects value",
			field: &ast.Field{
				Alias: "selected",
				Name:  "selected",
				SelectionSet: ast.SelectionSet{
					&ast.Field{Alias: "int", Name: "field1", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Int"}}},
					&ast.Field{Alias: "string", Name: "field2", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "String"}}},
					&ast.Field{Alias: "float", Name: "field3", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Float"}}},
					&ast.Field{Alias: "bool", Name: "field4", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Boolean"}}},
					&ast.Field{
						Alias: "object", Name: "field5",
						Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "NamedObjectType"}},
						SelectionSet: ast.SelectionSet{
							&ast.Field{Alias: "int", Name: "field1", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Int"}}},
						},
						ObjectDefinition: &ast.Definition{
							Fields: ast.FieldList{
								&ast.FieldDefinition{Name: "field1", Type: &ast.Type{NamedType: "Int"}},
								&ast.FieldDefinition{Name: "field2", Type: &ast.Type{NamedType: "Int"}},
							},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Type: ast.ListType(ast.NamedType("NamedObjectType", nil), nil),
					Directives: ast.DirectiveList{
						&ast.Directive{Name: "catalog", Arguments: ast.ArgumentList{{Name: "name", Value: &ast.Value{Raw: "db"}}}},
					},
				},
				ObjectDefinition: &ast.Definition{},
			},
			wrappedSQL:  "[wrapped sql]",
			expectedSQL: `SELECT "int" AS "int","string" AS "string","float" AS "float","bool" AS "bool",json_transform("object", '{"int":"INTEGER"}') AS "object" FROM postgres_query(db,' (SELECT "int" AS "int","string" AS "string","float" AS "float","bool" AS "bool","object" AS "object" FROM [wrapped sql] AS _objects) ') AS _objects`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node.Query = tc.field
			node.Name = tc.field.Name
			node.engines = testService.engines
			node.schema = testCats.Schema()
			cast, err := castResultsNode(context.Background(), caster, node, false, false)
			if err != nil {
				t.Fatalf("castResultsNode: %v", err)
			}

			sql, _, err := cast.CollectFunc(node, Results{
				{Name: node.Name, Result: tc.wrappedSQL},
			}, tc.params)
			if err != nil {
				t.Fatalf("castResultsNode: %v", err)
			}
			if sql != tc.expectedSQL {
				t.Errorf("castResultsNode: expected %s, got %s", tc.expectedSQL, sql)
			}
		})
	}
}

func Test_castScalarResultsNode(t *testing.T) {
	caster := &engines.Postgres{}
	node := &QueryPlanNode{
		engines: testService.engines,
		schema:  testCats.Schema(),
	}

	tests := []struct {
		name              string
		field             *ast.Field
		params            []any
		wrappedSQL        string
		expectedSQL       string
		expectedParamsLen int
	}{
		{
			name: "scalar value without params",
			field: &ast.Field{
				Alias: "call",
				Name:  "call",
				Definition: &ast.FieldDefinition{
					Type: &ast.Type{NamedType: "String"},
					Directives: ast.DirectiveList{
						&ast.Directive{Name: "catalog", Arguments: ast.ArgumentList{{Name: "name", Value: &ast.Value{Raw: "db"}}}},
					},
				},
				ObjectDefinition: &ast.Definition{},
			},
			params:      []interface{}{"test", 1, 2, 23.44, map[string]any{"key": "value"}},
			wrappedSQL:  "[wrapped sql]",
			expectedSQL: `SELECT "call" AS "call" FROM postgres_query(db,' (SELECT "call" AS "call" FROM (SELECT [wrapped sql] AS "call")) ')`,
		},
		{
			name: "scalar value with params",
			field: &ast.Field{
				Alias: "call",
				Name:  "call",
				Definition: &ast.FieldDefinition{
					Type: &ast.Type{NamedType: "String"},
					Directives: ast.DirectiveList{
						&ast.Directive{Name: "catalog", Arguments: ast.ArgumentList{{Name: "name", Value: &ast.Value{Raw: "db"}}}},
					},
				},
			},
			params:      []interface{}{"test", 1, 2, 23.44, map[string]any{"key": "value"}},
			wrappedSQL:  "[wrapped sql $1, $2 $3 $4 $5]",
			expectedSQL: `SELECT "call" AS "call" FROM postgres_query(db,' (SELECT "call" AS "call" FROM (SELECT [wrapped sql ''test'', 1 2 23.44 ''{"key":"value"}''::JSONB] AS "call")) ')`,
		},
		{
			name: "scalar object (JSON) with params",
			field: &ast.Field{
				Alias: "call",
				Name:  "call",
				SelectionSet: ast.SelectionSet{
					&ast.Field{Alias: "int", Name: "field1", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Int"}}},
					&ast.Field{Alias: "string", Name: "field2", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "String"}}},
					&ast.Field{Alias: "float", Name: "field3", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Float"}}},
					&ast.Field{Alias: "bool", Name: "field4", Definition: &ast.FieldDefinition{Type: &ast.Type{NamedType: "Boolean"}}},
					&ast.Field{
						Alias: "object", Name: "field5",
						Definition: &ast.FieldDefinition{Type: ast.ListType(ast.NamedType("NamedObjectType", nil), nil)},
						SelectionSet: ast.SelectionSet{
							&ast.Field{Alias: "int", Name: "field1", Definition: &ast.FieldDefinition{Type: ast.ListType(ast.NamedType("Int", nil), nil)}},
						},
					},
				},
				Definition: &ast.FieldDefinition{
					Type: &ast.Type{NamedType: "NamedObjectType"},
					Directives: ast.DirectiveList{
						&ast.Directive{Name: "catalog", Arguments: ast.ArgumentList{{Name: "name", Value: &ast.Value{Raw: "db"}}}},
					},
				},
			},
			params:      []interface{}{"test", 1, 2, 23.44, map[string]any{"key": "value"}},
			wrappedSQL:  "[wrapped sql $1, $2 $3 $4 $5]",
			expectedSQL: `SELECT json_transform("call", '{"int":"INTEGER","string":"VARCHAR","float":"FLOAT","bool":"BOOLEAN","object":[{"int":["INTEGER"]}]}') AS "call" FROM postgres_query(db,' (SELECT "call" AS "call" FROM (SELECT [wrapped sql ''test'', 1 2 23.44 ''{"key":"value"}''::JSONB] AS "call")) ')`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node.Query = tc.field
			node.Name = tc.field.Name
			node.engines = testService.engines
			node.schema = testCats.Schema()
			cast, err := castScalarResultsNode(context.Background(), caster, node, true, false)
			if err != nil {
				t.Fatalf("castResultsNode: %v", err)
			}

			sql, _, err := cast.CollectFunc(node, Results{
				{Name: node.Name, Result: tc.wrappedSQL},
			}, tc.params)
			if err != nil {
				t.Fatalf("castResultsNode: %v", err)
			}
			if sql != tc.expectedSQL {
				t.Errorf("castResultsNode: expected %s, got %s", tc.expectedSQL, sql)
			}
		})
	}
}
