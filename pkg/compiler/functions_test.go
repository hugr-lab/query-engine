package compiler

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func TestFunctionInfo(t *testing.T) {
	str := `
	extend type Function {
		testFunction(arg3: String, arg2: JSON): String 
			@function(
				name: "testFunction"
				sql: "function([arg3], 32, 88. 'test', [arg2])"
			)
	}
	`

	sd, err := parser.ParseSchema(&ast.Source{Input: str, Name: "test"})
	if err != nil {
		t.Fatal(err)
	}

	info, err := FunctionInfo(sd.Extensions[0].Fields.ForName("testFunction"))
	if err != nil {
		t.Fatal(err)
	}

	if info.Name != "testFunction" {
		t.Errorf("unexpected name: %s", info.Name)
	}
	if info.SQL() != "function([arg3], 32, 88. 'test', [arg2])" {
		t.Errorf("unexpected sql: %s", info.SQL())
	}

}
