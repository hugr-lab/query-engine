package catalogs

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type Catalog struct {
	name   string
	prefix string
	engine engines.Engine

	schema   *ast.Schema
	source   sources.Source
	readOnly bool
	asModule bool
}

func NewCatalog(ctx context.Context, name, prefix string, engine engines.Engine, source sources.Source, asModule, readOnly bool) (*Catalog, error) {
	c := &Catalog{
		name:     name,
		prefix:   prefix,
		source:   source,
		engine:   engine,
		readOnly: readOnly,
		asModule: asModule,
	}

	err := c.Reload(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Catalog) Reload(ctx context.Context) error {
	l, ok := c.source.(sources.CatalogSourceLoader)
	if ok {
		err := l.Reload(ctx)
		if err != nil {
			return err
		}
	}

	schema, err := c.baseSchema(ctx)
	if err != nil {
		return err
	}

	c.schema = schema
	return nil
}

func (c *Catalog) Schema() *ast.Schema {
	return c.schema
}

func (c *Catalog) baseSchema(ctx context.Context) (*ast.Schema, error) {
	sd, err := c.source.SchemaDocument(ctx)
	if err != nil {
		return nil, err
	}

	return compiler.Compile(sd, compiler.Options{
		Name:       c.name,
		ReadOnly:   c.readOnly,
		Prefix:     c.prefix,
		EngineType: string(c.engine.Type()),
		AsModule:   c.asModule,
	})
}

func (c *Catalog) Name() string {
	return c.name
}
