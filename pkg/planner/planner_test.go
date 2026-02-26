package planner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/schema/sources"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/hugr-lab/query-engine/pkg/types"
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
		field_func_call_fields: [Int] @function_call(references_name: "func_scalar_array", args: {arg1: "field2", arg2: "field2"})
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
		field_func_call_fields: [Int] @function_call(references_name: "func_scalar_array", args: {arg1: "field2", arg2: "field2"})
		func_table_object(arg2:Int): [test_object] @function_call(references_name: "func_table_object", args: {arg1: "field2"})
	}
`

var (
	testSchemaService *schema.Service
	testService       *Service
)

func TestMain(m *testing.M) {
	provider, err := static.New()
	if err != nil {
		fmt.Printf("Failed to init static provider: %v", err)
		os.Exit(1)
	}
	ss := schema.NewService(provider)

	cat, err := sources.NewCatalog(context.Background(),
		types.DataSource{Name: "test"},
		&engines.DuckDB{},
		sources.NewStringSource(testSchemaData),
		false,
	)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = ss.AddCatalog(context.Background(), "test", &engines.DuckDB{}, cat)
	if err != nil {
		fmt.Printf("Failed to add catalog: %v", err)
		os.Exit(1)
	}

	pgCat, err := sources.NewCatalog(context.Background(),
		types.DataSource{Name: "pg_test", Prefix: "pg"},
		&engines.Postgres{},
		sources.NewStringSource(testPGSchemaData),
		false,
	)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = ss.AddCatalog(context.Background(), "pg_test", &engines.Postgres{}, pgCat)
	if err != nil {
		fmt.Printf("Failed to add catalog: %v", err)
		os.Exit(1)
	}

	testSchemaService = ss
	testService = New(ss, nil)

	os.Exit(m.Run())
}
