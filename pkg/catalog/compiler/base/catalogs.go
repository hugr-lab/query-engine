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

// DefinitionCatalogEngine returns the engine type string recorded next to the
// data source name on a definition's @catalog directive.
func DefinitionCatalogEngine(def *ast.Definition) string {
	return DirectiveArgString(def.Directives.ForName(CatalogDirectiveName), "engine")
}

func FieldDefCatalog(field *ast.FieldDefinition) string {
	return DirectiveArgString(field.Directives.ForName(CatalogDirectiveName), "name")
}

// FieldDefDependency returns the dependency name from a field's @dependency directive.
func FieldDefDependency(field *ast.FieldDefinition) string {
	return DirectiveArgString(field.Directives.ForName(DependencyDirectiveName), "name")
}

// EnumValueCatalog is gone with DropCatalog, its only caller: attributing an
// enum value to a source mattered only while a schema container had to take one
// source's definitions back out of a merged whole.
//
// DefinitionCatalogEngine, DefinitionModuleCatalogs and FieldDefModuleCatalogs
// below are likewise reader-only and lost their callers earlier in design-036,
// with the compiled-schema provider and the GENERATE rules. The @module_catalog
// DIRECTIVE is still live — the storage EMITS it on module roots (gen_roots.go)
// — nothing reads it back. Sweeping them belongs with the package move.

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
