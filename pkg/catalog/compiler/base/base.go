package base

import (
	_ "embed"

	"github.com/vektah/gqlparser/v2/ast"
)

//go:embed "prelude.graphql"
var preludeDef string

//go:embed "system_types.graphql"
var systemTypesDef string

//go:embed "base.graphql"
var baseSchemaData string

//go:embed "query_directives.graphql"
var queryDirectivesDef string

//go:embed "gis.graphql"
var gisDirectives string

const FieldSourceDirectiveName = "field_source"

// CompiledPos creates a position marker for compiled/generated AST elements.
func CompiledPos(name string) *ast.Position {
	if name != "" {
		name = "compiled-instruction-" + name
	}
	return &ast.Position{
		Src: &ast.Source{Name: name},
	}
}

// FieldSourceDirective creates a @field_source directive for renamed fields.
func FieldSourceDirective(name string) *ast.Directive {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
	return &ast.Directive{
		Name: FieldSourceDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "field", Value: &ast.Value{Kind: ast.StringValue, Raw: name, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// Sources returns the base system type SDL sources.
// Scalar type SDL is provided separately by the types package.
func Sources() []*ast.Source {
	return []*ast.Source{
		{Name: "prelude.graphql", Input: preludeDef, BuiltIn: true},
		{Name: "system_types.graphql", Input: systemTypesDef},
		{Name: "base.graphql", Input: baseSchemaData},
		{Name: "query_directives.graphql", Input: queryDirectivesDef},
		{Name: "gis.graphql", Input: gisDirectives},
	}
}
