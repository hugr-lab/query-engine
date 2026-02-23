package compiler

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
)

type Catalog interface {
	base.DefinitionsSource
	CompileOptions() Options
}

type CompiledCatalog interface {
	base.ExtensionsSource
	Version() string
	CompiledOptions() Options
}

func Compile(ctx context.Context, provider base.Provider, catalog Catalog, opts Options) (CompiledCatalog, error) {

	return nil, nil
}
