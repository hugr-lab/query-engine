package ingest

import "github.com/vektah/gqlparser/v2/ast"

// catalogDirective creates a @catalog directive with name and engine — the tag
// CatalogTagger stamps on every object type of a source.
func catalogDirective(name, engine string) *ast.Directive {
	pos := &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}
	return &ast.Directive{
		Name: "catalog",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "engine", Value: &ast.Value{Raw: engine, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}
