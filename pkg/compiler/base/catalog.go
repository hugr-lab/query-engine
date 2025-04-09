package base

import "github.com/vektah/gqlparser/v2/ast"

const (
	CatalogDirectiveName    = "catalog"
	DeprecatedDirectiveName = "deprecated"
)

func CatalogDirective(catalog string, engine string) *ast.Directive {
	return &ast.Directive{
		Name: CatalogDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: catalog, Kind: ast.StringValue}, Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}},
			{Name: "engine", Value: &ast.Value{Raw: engine, Kind: ast.StringValue}, Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}}},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}

func FieldCatalogName(def *ast.FieldDefinition) string {
	if d := def.Directives.ForName(CatalogDirectiveName); d != nil {
		if a := d.Arguments.ForName("name"); a != nil {
			return a.Value.Raw
		}
	}
	return ""
}

func CatalogName(def *ast.Definition) string {
	if d := def.Directives.ForName(CatalogDirectiveName); d != nil {
		if a := d.Arguments.ForName("name"); a != nil {
			return a.Value.Raw
		}
	}
	return ""
}
