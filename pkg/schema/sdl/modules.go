package sdl

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Type aliases re-exported from base.
type ModuleObjectType = base.ModuleObjectType
type ModuleRoot = base.ModuleRoot

const (
	ModuleQuery            = base.ModuleQuery
	ModuleMutation         = base.ModuleMutation
	ModuleFunction         = base.ModuleFunction
	ModuleMutationFunction = base.ModuleMutationFunction
)

var ModuleRootInfo = base.ModuleRootInfo

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

func FunctionModule(def *ast.FieldDefinition) string {
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

const (
	moduleQuerySuffix            = "_query"
	moduleMutationSuffix         = "_mutation"
	moduleFunctionSuffix         = "_function"
	moduleMutationFunctionSuffix = "_function_mutation"
)

func ModuleTypeName(module string, objectType base.ModuleObjectType) string {
	if module == "" {
		switch objectType {
		case base.ModuleQuery:
			return base.QueryBaseName
		case base.ModuleMutation:
			return base.MutationBaseName
		case base.ModuleFunction:
			return base.FunctionTypeName
		case base.ModuleMutationFunction:
			return base.FunctionMutationTypeName
		}
	}
	suffix := ""
	switch objectType {
	case base.ModuleQuery:
		suffix = moduleQuerySuffix
	case base.ModuleMutation:
		suffix = moduleMutationSuffix
	case base.ModuleFunction:
		suffix = moduleFunctionSuffix
	case base.ModuleMutationFunction:
		suffix = moduleMutationFunctionSuffix
	}
	return "_module_" + strings.ReplaceAll(module, ".", "_") + suffix
}
