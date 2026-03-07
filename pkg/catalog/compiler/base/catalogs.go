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

func EnumValueCatalog(enumValue *ast.EnumValueDefinition) string {
	return DirectiveArgString(enumValue.Directives.ForName(CatalogDirectiveName), "name")
}

// DefinitionModuleCatalogs returns all catalog names from @module_catalog directives on a definition.
func DefinitionModuleCatalogs(def *ast.Definition) []string {
	dirs := def.Directives.ForNames(ModuleCatalogDirectiveName)
	if len(dirs) == 0 {
		return nil
	}
	result := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if name := DirectiveArgString(d, "name"); name != "" {
			result = append(result, name)
		}
	}
	return result
}

// FieldDefModuleCatalogs returns all catalog names from @module_catalog directives on a field.
func FieldDefModuleCatalogs(field *ast.FieldDefinition) []string {
	dirs := field.Directives.ForNames(ModuleCatalogDirectiveName)
	if len(dirs) == 0 {
		return nil
	}
	result := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if name := DirectiveArgString(d, "name"); name != "" {
			result = append(result, name)
		}
	}
	return result
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
