package sources

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ catalog.Catalog = (*Catalog)(nil)
var _ Source = (*Catalog)(nil)

type Catalog struct {
	def      types.DataSource
	source   Source
	cap      *compiler.EngineCapabilities
	et       engines.Type
	isExt    bool
	provider catalog.Provider
}

func NewCatalog(ctx context.Context, def types.DataSource, engine engines.Engine, source Source, isExtension bool) (*Catalog, error) {
	c := &Catalog{
		def:    def,
		source: source,
		cap:    engine.Capabilities(),
		et:     engine.Type(),
		isExt:  isExtension,
	}
	if err := c.Reload(ctx); err != nil {
		return nil, err
	}
	return c, nil
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

// CompileOptions implements [catalog.Catalog].
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

// Definitions implements [catalog.Catalog].
func (c *Catalog) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Definitions(ctx)
}

// Description implements [catalog.Catalog].
func (c *Catalog) Description() string {
	return c.def.Description
}

// DirectiveDefinitions implements [catalog.Catalog].
func (c *Catalog) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return c.provider.DirectiveDefinitions(ctx)
}

// DirectiveForName implements [catalog.Catalog].
func (c *Catalog) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return c.provider.DirectiveForName(ctx, name)
}

// ForName implements [catalog.Catalog].
func (c *Catalog) ForName(ctx context.Context, name string) *ast.Definition {
	return c.provider.ForName(ctx, name)
}

// Extensions implements [base.ExtensionsSource].
// Delegates to the underlying docProvider so the compiler can process
// internal extensions (extend type within the same catalog).
func (c *Catalog) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	if ep, ok := c.provider.(interface {
		Extensions(context.Context) iter.Seq[*ast.Definition]
	}); ok {
		return ep.Extensions(ctx)
	}
	return func(yield func(*ast.Definition) bool) {}
}

// DefinitionExtensions implements [base.ExtensionsSource].
func (c *Catalog) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	if ep, ok := c.provider.(interface {
		DefinitionExtensions(context.Context, string) iter.Seq[*ast.Definition]
	}); ok {
		return ep.DefinitionExtensions(ctx, name)
	}
	return func(yield func(*ast.Definition) bool) {}
}

// Name implements [catalog.Catalog].
func (c *Catalog) Name() string {
	return c.def.Name
}
