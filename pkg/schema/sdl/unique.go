package sdl

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

type Unique struct {
	Fields      []string
	QuerySuffix string
	Skip        bool
	def         *ast.Directive
}

func UniqueInfo(def *ast.Directive) *Unique {
	if def == nil {
		return nil
	}
	if def.Name != base.ObjectUniqueDirectiveName {
		return nil
	}
	return &Unique{
		def:         def,
		Fields:      base.DirectiveArgStrings(def, "fields"),
		QuerySuffix: base.DirectiveArgString(def, base.ArgQuerySuffix),
		Skip:        base.DirectiveArgString(def, base.ArgSkipQuery) == "true",
	}
}
