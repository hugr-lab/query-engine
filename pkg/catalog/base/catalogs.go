package base

import "github.com/vektah/gqlparser/v2/ast"

const (
	CatalogDirectiveName       = "catalog"
	DependencyDirectiveName    = "dependency"
	ModuleCatalogDirectiveName = "module_catalog"
)

func DefinitionCatalog(def *ast.Definition) string {
	return DirectiveArgString(def.Directives.ForName(CatalogDirectiveName), "name")
}

func FieldDefCatalog(field *ast.FieldDefinition) string {
	return DirectiveArgString(field.Directives.ForName(CatalogDirectiveName), "name")
}

// FieldDefDependency returns the dependency name from a field's @dependency directive.
func FieldDefDependency(field *ast.FieldDefinition) string {
	return DirectiveArgString(field.Directives.ForName(DependencyDirectiveName), "name")
}

func DefinitionDependencies(def *ast.Definition) []string {
	deps := def.Directives.ForNames(DependencyDirectiveName)
	if len(deps) == 0 {
		return nil
	}
	depsList := make([]string, len(deps))
	for i, d := range deps {
		name := DirectiveArgString(d, "name")
		if name != "" {
			depsList[i] = name
		}
	}

	return depsList
}

// The readers this file used to also hold — EnumValueCatalog,
// DefinitionCatalogEngine, DefinitionModuleCatalogs, FieldDefModuleCatalogs —
// are gone with the code that called them: the compiled-schema provider, the
// GENERATE rules and the static provider's DropCatalog. Their directives are
// still emitted: @catalog(engine:) rides along on every tagged object, and
// @module_catalog is stamped on module roots by the catalog storage
// (store/gen_roots.go). Nothing reads either back, because the storage answers
// those questions from catalog.* rows instead of from the SDL.
