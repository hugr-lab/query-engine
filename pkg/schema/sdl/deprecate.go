package sdl

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type Deprecated struct {
	IsDeprecated bool
	Reason       string
}

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
	d := dd.ForName(base.DeprecatedDirectiveName)
	if d == nil {
		return Deprecated{IsDeprecated: false}
	}
	if a := d.Arguments.ForName("reason"); a != nil {
		return Deprecated{
			IsDeprecated: true,
			Reason:       a.Value.Raw,
		}
	}
	return Deprecated{IsDeprecated: true}
}
