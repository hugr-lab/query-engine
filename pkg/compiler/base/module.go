package base

import "github.com/vektah/gqlparser/v2/ast"

// ModuleObjectType identifies the kind of module root object.
type ModuleObjectType int

const (
	ModuleQuery ModuleObjectType = iota + 1
	ModuleMutation
	ModuleFunction
	ModuleMutationFunction
)

// ModuleRootDirectiveName is the directive that marks a type as a module root.
const ModuleRootDirectiveName = "module_root"

// ModuleRoot holds extracted module information from a type definition.
type ModuleRoot struct {
	Name string
	Type ModuleObjectType
}

// ModuleRootInfo extracts module root information from @module_root directive
// or by well-known type name (Query, Mutation, Function, MutationFunction).
func ModuleRootInfo(def *ast.Definition) *ModuleRoot {
	if def == nil {
		return nil
	}
	d := def.Directives.ForName(ModuleRootDirectiveName)
	if d == nil {
		switch def.Name {
		case QueryBaseName:
			return &ModuleRoot{Type: ModuleQuery}
		case MutationBaseName:
			return &ModuleRoot{Type: ModuleMutation}
		case FunctionTypeName:
			return &ModuleRoot{Type: ModuleFunction}
		case FunctionMutationTypeName:
			return &ModuleRoot{Type: ModuleMutationFunction}
		default:
			return nil
		}
	}
	module := &ModuleRoot{}
	if a := d.Arguments.ForName("name"); a != nil {
		module.Name = a.Value.Raw
	}
	if a := d.Arguments.ForName("type"); a != nil {
		switch a.Value.Raw {
		case "QUERY":
			module.Type = ModuleQuery
		case "MUTATION":
			module.Type = ModuleMutation
		case "FUNCTION":
			module.Type = ModuleFunction
		case "MUT_FUNCTION":
			module.Type = ModuleMutationFunction
		}
	}
	return module
}
