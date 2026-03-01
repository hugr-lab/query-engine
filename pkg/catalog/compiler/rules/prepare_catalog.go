package rules

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
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

