package base

import "github.com/vektah/gqlparser/v2/ast"

const (
	OriginalNameDirectiveName = "original_name"
)

func OriginalNameDirective(name string) *ast.Directive {
	return &ast.Directive{
		Name: OriginalNameDirectiveName,
		Arguments: []*ast.Argument{
			{
				Name: "name", Value: &ast.Value{
					Raw: name, Kind: ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}

func SqlFieldDirective(sql string) *ast.Directive {
	return &ast.Directive{
		Name: FieldSqlDirectiveName,
		Arguments: []*ast.Argument{
			{
				Name: "exp", Value: &ast.Value{
					Raw: sql, Kind: ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}

func ExtraFieldDirective(name, baseType string) *ast.Directive {
	return &ast.Directive{
		Name: FieldExtraFieldDirectiveName,
		Arguments: []*ast.Argument{
			{
				Name: "name", Value: &ast.Value{
					Raw: name, Kind: ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
			{
				Name: "base_type", Value: &ast.Value{
					Raw: baseType, Kind: ast.StringValue,
					Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
				},
				Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
			},
		},
		Position: &ast.Position{Src: &ast.Source{Name: "compiled-instruction"}},
	}
}
