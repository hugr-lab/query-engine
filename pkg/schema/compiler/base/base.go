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
