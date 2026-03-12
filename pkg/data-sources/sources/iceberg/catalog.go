package iceberg

import (
	"context"
	"fmt"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/ducklake"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// Ensure Source implements SelfDescriber at compile time.
var _ interface {
	CatalogSource(ctx context.Context, db *db.Pool) (cs.Catalog, error)
} = (*Source)(nil)

// CatalogSource implements the SelfDescriber interface.
// It introspects Iceberg metadata via duckdb_tables() + DESCRIBE and generates a GraphQL catalog.
func (s *Source) CatalogSource(ctx context.Context, pool *db.Pool) (cs.Catalog, error) {
	prefix := s.prefix()

	filter, err := ducklake.NewIntrospectFilter(s.schemaFilter, s.tableFilter)
	if err != nil {
		return nil, err
	}

	tables, err := IntrospectSchema(ctx, pool, prefix, filter)
	if err != nil {
		return nil, fmt.Errorf("iceberg: introspect failed for %s: %w", prefix, err)
	}

	doc := ducklake.GenerateSchemaDocument(tables)
	version := ducklake.ContentHash(tables)

	opts := compiler.Options{
		Name:         s.ds.Name,
		ReadOnly:     s.ds.ReadOnly,
		Prefix:       s.ds.Prefix,
		EngineType:   string(s.engine.Type()),
		AsModule:     s.ds.AsModule,
		Capabilities: s.engine.Capabilities(),
	}

	cat := &icebergCatalog{
		name:     s.ds.Name,
		desc:     s.ds.Description,
		opts:     opts,
		engine:   s.engine,
		provider: static.NewDocumentProvider(doc),
		version:  version,
		pool:     pool,
		prefix:   prefix,
		source:   s,
	}

	return cat, nil
}

// icebergCatalog implements sources.Catalog (but not IncrementalCatalog).
// On schema change, full recompile is triggered via DiffSchemas.
type icebergCatalog struct {
	name     string
	desc     string
	opts     compiler.Options
	engine   engines.Engine
	provider *static.DocProvider
	version  string

	pool   *db.Pool
	prefix string
	source *Source
}

func (c *icebergCatalog) Name() string                     { return c.name }
func (c *icebergCatalog) Description() string              { return c.desc }
func (c *icebergCatalog) CompileOptions() compiler.Options  { return c.opts }
func (c *icebergCatalog) Engine() engines.Engine            { return c.engine }

func (c *icebergCatalog) Version(ctx context.Context) (string, error) {
	// Re-introspect and compute content hash to detect schema changes
	filter, err := ducklake.NewIntrospectFilter(c.source.schemaFilter, c.source.tableFilter)
	if err != nil {
		return c.version, nil
	}

	tables, err := IntrospectSchema(ctx, c.pool, c.prefix, filter)
	if err != nil {
		return c.version, nil // fall back to cached version
	}

	newVersion := ducklake.ContentHash(tables)
	if newVersion != c.version {
		// Update cached provider for DiffSchemas
		doc := ducklake.GenerateSchemaDocument(tables)
		c.provider = static.NewDocumentProvider(doc)
		c.version = newVersion
	}
	return c.version, nil
}

func (c *icebergCatalog) ForName(ctx context.Context, name string) *ast.Definition {
	return c.provider.ForName(ctx, name)
}

func (c *icebergCatalog) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return c.provider.DirectiveForName(ctx, name)
}

func (c *icebergCatalog) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Definitions(ctx)
}

func (c *icebergCatalog) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return c.provider.DirectiveDefinitions(ctx)
}

func (c *icebergCatalog) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Extensions(ctx)
}

func (c *icebergCatalog) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return c.provider.DefinitionExtensions(ctx, name)
}
