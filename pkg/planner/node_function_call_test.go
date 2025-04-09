package planner

import (
	"context"
	"reflect"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

func Test_functionCallNode(t *testing.T) {
	query := `
		query ($arg1: String!, $arg2: Int!, $arg3: Int!) {
			function {
				call: func_scalar_string(arg1: $arg1, arg2: $arg2)
				call2: func_scalar_array(arg1: $arg2, arg2: $arg3)
				call3: func_scalar_object(arg1: $arg2, arg2: $arg3) {
					field1
					field2
					fieldNested {
						field1
						field2
					}
				}
				call4: func_table_object(arg1: $arg2, arg2: $arg3) {
					field2
					fieldNested {
						field1
						field2
						array_field
						array_nested {
							field1
							field2
						}
					}
				}
			}
			table_object {
				str: field_func_call(arg1: $arg1, arg2: $arg2)
				str2: field_func_call(arg1: $arg1, arg2: $arg3)
			}
		}
	`
	q, gqlErr := gqlparser.LoadQuery(testCats.Schema(), query)
	if len(gqlErr) != 0 {
		t.Fatal(gqlErr)
	}
	vars := map[string]interface{}{
		"arg1": "test",
		"arg2": 1,
		"arg3": 2,
	}
	calls := filterFields(q.Operations[0].SelectionSet, func(f *ast.Field) bool {
		return compiler.IsFunctionCall(f.Definition) || compiler.IsFunctionCallQuery(f)
	}, true)

	tests := map[string]struct {
		expectedFunctionCall string
		expectedFCParams     []interface{}
		expectedSelect       string
		expectedSelectParams []interface{}
	}{
		"call": {
			expectedFunctionCall: "func_scalar_string($1, $2)",
			expectedFCParams:     []interface{}{"test", int64(1)},
			expectedSelect:       "SELECT func_scalar_string($1, $2) AS \"call\"",
			expectedSelectParams: []interface{}{"test", int64(1)},
		},
		"call2": {
			expectedFunctionCall: "func_scalar_int(0, $1, $2)",
			expectedFCParams:     []interface{}{int64(1), int64(2)},
			expectedSelect:       "SELECT unnest(func_scalar_int(0, $1, $2)) AS call2",
			expectedSelectParams: []interface{}{int64(1), int64(2)},
		},
		"call3": {
			expectedFunctionCall: "SELECT {field1: _value['field1'],field2: _value['field2'],fieldNested: {field1: _value['fieldNested']['field1'],field2: _value['fieldNested']['field2']}} AS call3 FROM (SELECT func_scalar_object(0, $1, $2) AS _value)",
			expectedFCParams:     []interface{}{int64(1), int64(2)},
			expectedSelect:       "SELECT _value.* FROM (SELECT (SELECT {field1: _value['field1'],field2: _value['field2'],fieldNested: {field1: _value['fieldNested']['field1'],field2: _value['fieldNested']['field2']}} AS call3 FROM (SELECT func_scalar_object(0, $1, $2) AS _value)) AS _value)",
			expectedSelectParams: []interface{}{int64(1), int64(2)},
		},
		"call4": {
			expectedFunctionCall: "SELECT field2,fieldNested FROM func_table_object(0, $1, $2) AS _objects",
			expectedFCParams:     []interface{}{int64(1), int64(2)},
			expectedSelect:       "SELECT field2,fieldNested FROM func_table_object(0, $1, $2) AS _objects",
			expectedSelectParams: []interface{}{int64(1), int64(2)},
		},
		"str": {
			expectedFunctionCall: "func_scalar_string($1, $2)",
			expectedFCParams:     []interface{}{"test", int64(1)},
		},
		"str2": {
			expectedFunctionCall: "func_scalar_string($1, $2)",
			expectedFCParams:     []interface{}{"test", int64(2)},
		},
	}

	for _, call := range calls {
		t.Run(call.Alias, func(t *testing.T) {
			tc, ok := tests[call.Alias]
			if !ok {
				t.Fatalf("test %s case not found", call.Alias)
			}
			prefix := ""
			if compiler.IsDataObject(call.ObjectDefinition) {
				prefix = "_objects"
			}
			funcNode, err := functionCallNode(context.Background(), compiler.SchemaDefs(testCats.Schema()), testService.engines, prefix, call, vars)
			if err != nil {
				t.Fatal("functionCallNode", err)
			}
			funcNode.schema = testCats.Schema()
			funcNode.engines = testService.engines
			res, err := funcNode.Compile(funcNode, nil)
			if err != nil {
				t.Fatalf("functionCallNode.Execute:%v", err)
			}
			if res.Result != tc.expectedFunctionCall {
				t.Errorf("functionCallNode.Execute: expected %s, got %s", tc.expectedFunctionCall, res.Result)
			}
			if !reflect.DeepEqual(tc.expectedFCParams, res.Params) {
				t.Errorf("functionCallNode.Execute: expected %v, got %v", tc.expectedFCParams, res.Params)
			}
			if compiler.IsDataObject(call.ObjectDefinition) {
				return
			}
			selectNode := selectFromFunctionCallNode(context.Background(), compiler.SchemaDefs(testCats.Schema()), funcNode)
			selectNode.schema = testCats.Schema()
			selectNode.engines = testService.engines
			res, err = selectNode.Compile(selectNode, nil)
			if err != nil {
				t.Fatalf("selectFromFunctionNode.Execute:%v", err)
			}
			if res.Result != tc.expectedSelect {
				t.Errorf("selectFromFunctionNode.Execute: expected %s, got %s", tc.expectedSelect, res.Result)
			}
			if !reflect.DeepEqual(tc.expectedSelectParams, res.Params) {
				t.Errorf("selectFromFunctionNode.Execute: expected %v, got %v", tc.expectedSelectParams, res.Params)
			}
		})
	}
}
