package planner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

const testSchemaData = `
	extend type Function {
		func_scalar_string(arg1: String, arg2:Int): String
			@function(name: "func_scalar_string")
		
		func_scalar_array(arg1: Int, arg2:Int): [Int]
			@function(name: "func_scalar_int", sql: "func_scalar_int(0, [arg1], [arg2])")

		func_scalar_object(arg1: Int, arg2:Int): test_object
			@function(name: "func_scalar_object", sql: "func_scalar_object(0, [arg1], [arg2])")

		func_table_object(arg1: Int, arg2:Int): [test_object]
			@function(name: "func_table_object", sql: "func_table_object(0, [arg1], [arg2])")
	}

	type test_object {
		field1: String
		field2: Int
		fieldNested: nested_object
	}

	type nested_object {
		field1: String
		field2: Int
		array_field: [String]
		array_nested: [nested_object2]
	}

	type nested_object2 {
		field1: String
		field2: Int
	}

	type table_object @table(name: "table_object") {
		field1: String @pk
		field2: Int
		nested_field: test_object
		nested_array_field: [nested_object2]
		field_func_call(arg1: String, arg2:Int): String @function_call(references_name: "func_scalar_string")
		field_func_call_fields: [Int] @function_call(references_name: "func_scalar_array", args: {arg1: "field1", arg2: "field2"})
		func_table_object(arg2:Int): [test_object] @function_call(references_name: "func_table_object", args: {arg1: "field2"})
	}
`

const testPGSchemaData = `
	extend type Function {
		func_scalar_string(arg1: String, arg2:Int): String
			@function(name: "test_pg_func_scalar_string")

		func_scalar_array(arg1: Int, arg2:Int): [Int]
			@function(name: "func_scalar_int", sql: "func_scalar_int(0, [arg1], [arg2])")

		func_scalar_object(arg1: Int, arg2:Int): test_object
			@function(name: "func_scalar_object", sql: "func_scalar_object(0, [arg1], [arg2])")

		func_table_object(arg1: Int, arg2:Int): [test_object]
			@function(name: "func_table_object", sql: "func_table_object(0, [arg1], [arg2])")
	}

	type test_object {
		field1: String
		field2: Int
		fieldNested: nested_object
	}

	type nested_object {
		field1: String
		field2: Int
		array_field: [String]
		array_nested: [nested_object2]
	}

	type nested_object2 {
		field1: String
		field2: Int
	}

	type table_object @table(name: "table_object") {
		field1: String @pk
		field2: Int
		nested_field: test_object
		nested_array_field: [nested_object2]
		field_func_call(arg1: String, arg2:Int): String @function_call(references_name: "func_scalar_string")
		field_func_call_fields: [Int] @function_call(references_name: "func_scalar_array", args: {arg1: "field1", arg2: "field2"})
		func_table_object(arg2:Int): [test_object] @function_call(references_name: "func_table_object", args: {arg1: "field2"})
	}
`

var (
	testCats    *catalogs.Service
	testService *Service
)

func TestMain(m *testing.M) {
	cs := catalogs.New()
	c, err := catalogs.NewCatalog(context.Background(), "test", "", &engines.DuckDB{}, sources.NewStringSource(testSchemaData), false)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = cs.AddCatalog(context.Background(), "test", c)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	c, err = catalogs.NewCatalog(context.Background(), "pg_test", "pg", &engines.Postgres{}, sources.NewStringSource(testPGSchemaData), false)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = cs.AddCatalog(context.Background(), "pg_test", c)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}

	testCats = cs
	testService = New(testCatalog{
		"test":    &engines.DuckDB{},
		"pg_test": &engines.Postgres{},
	})

	os.Exit(m.Run())
}

type testCatalog map[string]engines.Engine

func (t testCatalog) Engine(name string) (engines.Engine, error) {
	if e, ok := t[name]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("engine not found")
}
