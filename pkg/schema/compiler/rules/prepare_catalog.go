package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*CatalogTagger)(nil)

type CatalogTagger struct{}

func (r *CatalogTagger) Name() string     { return "CatalogTagger" }
func (r *CatalogTagger) Phase() base.Phase { return base.PhasePrepare }

func (r *CatalogTagger) ProcessAll(ctx base.CompilationContext) error {
	opts := ctx.CompileOptions()
	if opts.Name == "" {
		return nil
	}
	catalogDir := catalogDirective(opts.Name, opts.EngineType)
	for def := range ctx.Source().Definitions(ctx.Context()) {
		// Only tag OBJECT types (tables/views); INPUT_OBJECT, ENUM, SCALAR etc.
		// are supporting types that don't get @catalog in the DDL feed.
		if def.Kind != ast.Object {
			continue
		}
		def.Directives = append(def.Directives, catalogDir)
	}
	return nil
}

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
