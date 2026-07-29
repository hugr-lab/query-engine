package sdl

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
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
	ModuleSubscription     = base.ModuleSubscription
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
	// NOTE: must match the module assembler's inline naming
	// (compiler/rules/assemble_modules.go): mutation-function module types are
	// "_module_<m>_mut_function", subscription module types are
	// "_module_<m>_subscription".
	moduleMutationFunctionSuffix = "_mut_function"
	moduleSubscriptionSuffix     = "_subscription"
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
		case base.ModuleSubscription:
			return base.SubscriptionBaseName
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
	case base.ModuleSubscription:
		suffix = moduleSubscriptionSuffix
	}
	return "_module_" + strings.ReplaceAll(module, ".", "_") + suffix
}
