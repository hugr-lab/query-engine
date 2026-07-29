package store

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/vektah/gqlparser/v2/ast"
)

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
