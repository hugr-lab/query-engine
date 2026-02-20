package compiler

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Type aliases for backward compatibility — canonical definitions in base.
type ModuleObjectType = base.ModuleObjectType
type ModuleRoot = base.ModuleRoot

const (
	ModuleQuery            = base.ModuleQuery
	ModuleMutation         = base.ModuleMutation
	ModuleFunction         = base.ModuleFunction
	ModuleMutationFunction = base.ModuleMutationFunction
)

const (
	moduleRootDirectiveName = base.ModuleRootDirectiveName

	queryBaseFunctionFieldName = "function"

	moduleQuerySuffix            = "_query"
	moduleMutationSuffix         = "_mutation"
	moduleFunctionSuffix         = "_function"
	moduleMutationFunctionSuffix = "_function_mutation"
)

func ObjectModule(def *ast.Definition) string {
	if def == nil {
		return ""
	}
	if d := def.Directives.ForName(base.ModuleDirectiveName); d != nil {
		if a := d.Arguments.ForName("name"); a != nil {
			return a.Value.Raw
		}
	}
	return ""
}

func objectModuleType(defs Definitions, def *ast.Definition, objectType ModuleObjectType) *ast.Definition {
	return defs.ForName(ModuleTypeName(ObjectModule(def), objectType))
}

func functionModule(def *ast.FieldDefinition) string {
	if def == nil {
		return ""
	}
	if d := def.Directives.ForName(base.ModuleDirectiveName); d != nil {
		if a := d.Arguments.ForName("name"); a != nil {
			return a.Value.Raw
		}
	}
	return ""
}

func moduleType(schema *ast.SchemaDocument, module string, objectType ModuleObjectType) (*ast.Definition, error) {
	moduleHierarchy := strings.Split(module, ".")
	moduleObjectName := ModuleTypeName(module, objectType)
	m := schema.Definitions.ForName(moduleObjectName)
	if m != nil {
		return m, nil
	}
	switch moduleObjectName {
	case base.QueryBaseName:
		return rootType(schema, ModuleQuery)
	case base.MutationBaseName:
		return rootType(schema, ModuleMutation)
	case base.FunctionTypeName:
		return rootType(schema, ModuleFunction)
	case base.FunctionMutationTypeName:
		return rootType(schema, ModuleMutationFunction)
	}

	parent, err := moduleType(schema, strings.Join(moduleHierarchy[:len(moduleHierarchy)-1], "."), objectType)
	if err != nil {
		return nil, err
	}

	m = &ast.Definition{
		Kind:        ast.Object,
		Name:        moduleObjectName,
		Description: "The root query object of the module " + module,
		Directives:  ast.DirectiveList{moduleRootDirective(module, objectType)},
		Position:    compiledPos(),
	}
	schema.Definitions = append(schema.Definitions, m)
	parent.Fields = append(parent.Fields, &ast.FieldDefinition{
		Name:        moduleHierarchy[len(moduleHierarchy)-1],
		Description: "The root query object of the module " + module,
		Type:        ast.NamedType(moduleObjectName, nil),
		Position:    compiledPos(),
	})

	return m, err
}

func ModuleTypeName(module string, objectType ModuleObjectType) string {
	if module == "" {
		switch objectType {
		case ModuleQuery:
			return base.QueryBaseName
		case ModuleMutation:
			return base.MutationBaseName
		case ModuleFunction:
			return base.FunctionTypeName
		case ModuleMutationFunction:
			return base.FunctionMutationTypeName
		}
	}
	suffix := ""
	switch objectType {
	case ModuleQuery:
		suffix = moduleQuerySuffix
	case ModuleMutation:
		suffix = moduleMutationSuffix
	case ModuleFunction:
		suffix = moduleFunctionSuffix
	case ModuleMutationFunction:
		suffix = moduleMutationFunctionSuffix
	}
	return "_module_" + strings.ReplaceAll(module, ".", "_") + suffix
}

func moduleRootDirective(name string, objectType ModuleObjectType) *ast.Directive {
	val := ""
	switch objectType {
	case ModuleQuery:
		val = "QUERY"
	case ModuleMutation:
		val = "MUTATION"
	case ModuleFunction:
		val = "FUNCTION"
	case ModuleMutationFunction:
		val = "MUT_FUNCTION"
	}

	return &ast.Directive{
		Name: moduleRootDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: name}, Position: compiledPos()},
			{Name: "type", Value: &ast.Value{Kind: ast.EnumValue, Raw: val}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

// ModuleRootInfo is re-exported from base for backward compatibility.
var ModuleRootInfo = base.ModuleRootInfo
