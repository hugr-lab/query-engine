package catalogs

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

type Extension struct {
	name      string
	engine    engines.Engine
	extSource *ast.SchemaDocument
	schema    *ast.Schema
	ext       *ast.SchemaDocument
	source    sources.Source
	def       types.DataSource
	depends   []string
	s         *Service
}

func (s *Service) NewExtension(ctx context.Context, def types.DataSource, source sources.Source) (*Extension, error) {
	e := &Extension{
		name:   def.Name,
		engine: generalEngine,
		source: source,
		def:    def,
		s:      s,
	}
	err := e.Reload(ctx, s.Schema())
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Extension) Name() string {
	return e.name
}

func (e *Extension) Engine() engines.Engine {
	return e.engine
}

func (e *Extension) Depends() []string {
	return e.depends
}

func (e *Extension) Source() *ast.SchemaDocument {
	return e.extSource
}

func (e *Extension) Schema() *ast.Schema {
	return e.schema
}

func (e *Extension) Reload(ctx context.Context, base *ast.Schema) error {
	if l, ok := e.source.(sources.CatalogSourceLoader); ok {
		err := l.Reload(ctx)
		if err != nil {
			return err
		}
		e.extSource = nil
	}
	if e.extSource == nil {
		ss, err := e.source.SchemaDocument(ctx)
		if err != nil {
			return err
		}
		schema, extSource, depends, err := compiler.CompileExtension(base, ss, compiler.Options{})
		if err != nil {
			return err
		}
		e.extSource = extSource
		e.schema = schema
		e.depends = depends
	}
	return nil
}
