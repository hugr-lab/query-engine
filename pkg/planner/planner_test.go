package planner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	catalogstore "github.com/hugr-lab/query-engine/pkg/catalog/store"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
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
		field_func_call_fields: [Int] @function_call(references_name: "func_scalar_array", args: {arg1: "field2", arg2: "field2"})
		func_table_object(arg2:Int): [test_object] @function_call(references_name: "func_table_object", args: {arg1: "field2"})
	}
`

// Type names collide with testSchemaData on purpose — the prefix disambiguates
// them. Function FIELD names are not prefixed, and a root function name is one
// GraphQL root field, so they carry the prefix by hand.
const testPGSchemaData = `
	extend type Function {
		pg_func_scalar_string(arg1: String, arg2:Int): String
			@function(name: "test_pg_func_scalar_string")

		pg_func_scalar_array(arg1: Int, arg2:Int): [Int]
			@function(name: "func_scalar_int", sql: "func_scalar_int(0, [arg1], [arg2])")

		pg_func_scalar_object(arg1: Int, arg2:Int): test_object
			@function(name: "func_scalar_object", sql: "func_scalar_object(0, [arg1], [arg2])")

		pg_func_table_object(arg1: Int, arg2:Int): [test_object]
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
		field_func_call(arg1: String, arg2:Int): String @function_call(references_name: "pg_func_scalar_string")
		field_func_call_fields: [Int] @function_call(references_name: "pg_func_scalar_array", args: {arg1: "field2", arg2: "field2"})
		pg_func_table_object(arg2:Int): [test_object] @function_call(references_name: "pg_func_table_object", args: {arg1: "field2"})
	}
`

var (
	testSchemaService *catalog.Service
	testService       *Service
)

func TestMain(m *testing.M) {
	// The entity catalog storage over a fresh in-memory CoreDB: it is both the
	// Provider and the CatalogManager, so AddCatalog below runs the real write
	// path — the storage is what compiles a schema.
	ctx := context.Background()
	pool, err := db.NewPool("")
	if err != nil {
		fmt.Printf("Failed to open pool: %v", err)
		os.Exit(1)
	}
	if err = coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool); err != nil {
		fmt.Printf("Failed to attach CoreDB: %v", err)
		os.Exit(1)
	}
	provider, err := catalogstore.New(ctx, pool, catalogstore.Config{VecSize: 8}, nil)
	if err != nil {
		fmt.Printf("Failed to init catalog store: %v", err)
		os.Exit(1)
	}
	ss := catalog.NewService(provider)

	e := &engines.DuckDB{}
	cat, err := sources.NewStringSource("test", e, base.Options{
		Name:         "test",
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, testSchemaData)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = ss.AddCatalog(context.Background(), "test", cat)
	if err != nil {
		fmt.Printf("Failed to add catalog: %v", err)
		os.Exit(1)
	}

	pe := &engines.Postgres{}
	pgCat, err := sources.NewStringSource("pg_test", pe, base.Options{
		Name:         "pg_test",
		Prefix:       "pg",
		EngineType:   string(pe.Type()),
		Capabilities: pe.Capabilities(),
	}, testPGSchemaData)
	if err != nil {
		fmt.Printf("Failed to create catalog: %v", err)
		os.Exit(1)
	}
	err = ss.AddCatalog(context.Background(), "pg_test", pgCat)
	if err != nil {
		fmt.Printf("Failed to add catalog: %v", err)
		os.Exit(1)
	}

	testSchemaService = ss
	testService = New(ss, nil)

	code := m.Run()
	pool.Close()
	os.Exit(code)
}
