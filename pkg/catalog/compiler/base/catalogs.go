package base

import "github.com/vektah/gqlparser/v2/ast"

const (
	CatalogDirectiveName    = "catalog"
	DependencyDirectiveName = "dependency"
)

func DefinitionCatalog(def *ast.Definition) string {
	return DirectiveArgString(def.Directives.ForName(CatalogDirectiveName), "name")
}

func FieldDefCatalog(field *ast.FieldDefinition) string {
	return DirectiveArgString(field.Directives.ForName(CatalogDirectiveName), "name")
}

func EnumValueCatalog(enumValue *ast.EnumValueDefinition) string {
	return DirectiveArgString(enumValue.Directives.ForName(CatalogDirectiveName), "name")
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
