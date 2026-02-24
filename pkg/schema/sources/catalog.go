package sources

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ schema.Catalog = (*Catalog)(nil)
var _ Source = (*Catalog)(nil)

type Catalog struct {
	def      types.DataSource
	source   Source
	cap      *compiler.EngineCapabilities
	et       engines.Type
	isExt    bool
	provider schema.Provider
}

func NewCatalog(ctx context.Context, def types.DataSource, engine engines.Engine, source Source, isExtension bool) *Catalog {
	return &Catalog{
		def:    def,
		source: source,
		cap:    engine.Capabilities(),
		et:     engine.Type(),
		isExt:  isExtension,
	}
}

// Source implements [Source].
func (c *Catalog) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	return c.source.SchemaDocument(ctx)
}

func (c *Catalog) Reload(ctx context.Context) error {
	if l, ok := c.source.(CatalogSourceLoader); ok {
		err := l.Reload(ctx)
		if err != nil {
			return err
		}
	}
	sd, err := c.SchemaDocument(ctx)
	if err != nil {
		return err
	}
	c.provider = static.NewDocumentProvider(sd)
	return nil
}

// CompileOptions implements [schema.Catalog].
func (c *Catalog) CompileOptions() compiler.Options {
	return compiler.Options{
		Name:         c.Name(),
		ReadOnly:     c.def.ReadOnly,
		Prefix:       c.def.Prefix,
		EngineType:   string(c.et),
		AsModule:     c.def.AsModule,
		IsExtension:  c.isExt,
		Capabilities: c.cap,
	}
}

// Definitions implements [schema.Catalog].
func (c *Catalog) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Definitions(ctx)
}

// Description implements [schema.Catalog].
func (c *Catalog) Description() string {
	return c.def.Description
}

// DirectiveDefinitions implements [schema.Catalog].
func (c *Catalog) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return c.provider.DirectiveDefinitions(ctx)
}

// DirectiveForName implements [schema.Catalog].
func (c *Catalog) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return c.provider.DirectiveForName(ctx, name)
}

// ForName implements [schema.Catalog].
func (c *Catalog) ForName(ctx context.Context, name string) *ast.Definition {
	return c.provider.ForName(ctx, name)
}

// Name implements [schema.Catalog].
func (c *Catalog) Name() string {
	return c.def.Name
}
