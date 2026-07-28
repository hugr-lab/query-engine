package store

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/vektah/gqlparser/v2/ast"
)

// partialRules is the write-side compiler. Since design-036 deleted the
// GENERATE / ASSEMBLE rules there is only ONE rule set left, so this is the
// whole of it — the name survives as the store's word for "compile to the
// PHYSICAL model I persist", which is now what compiling means.
//
// The @join / @function_call validators run LAST, after the definitions are
// prefixed and merged, and before collect: compileAndWrite returns on a
// compile error, so a rejected declaration never reaches writeSource. They
// resolve cross-source targets through the Store itself, passed as the
// compilation provider (manager.go) — the same on-demand reconstruction the
// served schema uses.
func partialRules() []base.Rule { return rules.RegisterAll() }

// asExtensionsSource adapts a catalog source for collect, which walks both
// Definitions() and Extensions(). Sources that carry extensions (module roots,
// cross-source `extend type`) already implement ExtensionsSource; a plain
// DefinitionsSource is wrapped with an empty extension set. The wrapper keeps
// CompileOptions visible — collect reads AsModule through it.
func asExtensionsSource(src base.DefinitionsSource) base.ExtensionsSource {
	if es, ok := src.(base.ExtensionsSource); ok {
		return es
	}
	out := definitionsOnlySource{DefinitionsSource: src}
	if co, ok := src.(interface{ CompileOptions() base.Options }); ok {
		out.opts = co.CompileOptions()
	}
	return out
}

type definitionsOnlySource struct {
	base.DefinitionsSource
	opts base.Options
}

func (s definitionsOnlySource) DefinitionExtensions(context.Context, string) iter.Seq[*ast.Definition] {
	return func(func(*ast.Definition) bool) {}
}

func (s definitionsOnlySource) Extensions(context.Context) iter.Seq[*ast.Definition] {
	return func(func(*ast.Definition) bool) {}
}

func (s definitionsOnlySource) CompileOptions() base.Options { return s.opts }
