package compiler

import "github.com/vektah/gqlparser/v2/ast"

const DeprecatedDirectiveName = "deprecated"

func FieldDeprecatedInfo(def *ast.FieldDefinition) Deprecated {
	if def == nil {
		return Deprecated{IsDeprecated: true}
	}
	return deprecatedInfo(def.Directives)
}

func EnumDeprecatedInfo(def *ast.EnumValueDefinition) Deprecated {
	if def == nil {
		return Deprecated{IsDeprecated: true}
	}
	return deprecatedInfo(def.Directives)
}

func deprecatedInfo(dd ast.DirectiveList) Deprecated {
	if dd.ForName(DeprecatedDirectiveName) == nil {
		return Deprecated{IsDeprecated: false}
	}
	if a := dd.ForName(DeprecatedDirectiveName).Arguments.ForName("reason"); a != nil {
		return Deprecated{
			IsDeprecated: true,
			Reason:       a.Value.Raw,
		}
	}

	return Deprecated{IsDeprecated: true}
}
