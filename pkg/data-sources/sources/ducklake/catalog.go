package ducklake

import (
	"context"
	"fmt"
	"iter"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// Ensure Source implements SelfDescriber at compile time.
var _ interface {
	CatalogSource(ctx context.Context, db *db.Pool) (cs.Catalog, error)
} = (*Source)(nil)

// CatalogSource implements the SelfDescriber interface.
// It introspects DuckLake metadata tables and generates a GraphQL catalog.
func (s *Source) CatalogSource(ctx context.Context, pool *db.Pool) (cs.Catalog, error) {
	prefix := s.prefix()

	filter, err := NewIntrospectFilter(s.schemaFilter, s.tableFilter)
	if err != nil {
		return nil, err
	}

	result, err := IntrospectAll(ctx, pool, prefix, filter)
	if err != nil {
		return nil, fmt.Errorf("ducklake: introspect failed for %s: %w", prefix, err)
	}

	doc := GenerateSchemaDocumentFull(result.Tables, result.Views)
	version := ContentHashFull(result.Tables, result.Views)

	opts := base.Options{
		Name:         s.ds.Name,
		ReadOnly:     s.ds.ReadOnly,
		Prefix:       s.ds.Prefix,
		EngineType:   string(s.engine.Type()),
		AsModule:     s.ds.AsModule,
		Capabilities: s.engine.Capabilities(),
	}

	cat := &duckLakeCatalog{
		name:     s.ds.Name,
		desc:     s.ds.Description,
		opts:     opts,
		engine:   s.engine,
		provider: static.NewDocumentProvider(doc),
		version:  version,
		pool:     pool,
		prefix:   prefix,
	}

	return cat, nil
}

// duckLakeCatalog implements sources.Catalog.
type duckLakeCatalog struct {
	name     string
	desc     string
	opts     base.Options
	engine   engines.Engine
	provider *static.DocProvider
	version  string

	pool   *db.Pool
	prefix string
}

func (c *duckLakeCatalog) Name() string                 { return c.name }
func (c *duckLakeCatalog) Description() string          { return c.desc }
func (c *duckLakeCatalog) CompileOptions() base.Options { return c.opts }
func (c *duckLakeCatalog) Engine() engines.Engine       { return c.engine }

func (c *duckLakeCatalog) Version(ctx context.Context) (string, error) {
	// Check DuckLake schema version from metadata
	sv, err := SchemaVersion(ctx, c.pool, c.prefix)
	if err != nil {
		return c.version, nil // fall back to content hash
	}
	return strconv.Itoa(sv), nil
}

func (c *duckLakeCatalog) ForName(ctx context.Context, name string) *ast.Definition {
	return c.provider.ForName(ctx, name)
}

func (c *duckLakeCatalog) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return c.provider.DirectiveForName(ctx, name)
}

func (c *duckLakeCatalog) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Definitions(ctx)
}

func (c *duckLakeCatalog) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return c.provider.DirectiveDefinitions(ctx)
}

func (c *duckLakeCatalog) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.provider.Extensions(ctx)
}

func (c *duckLakeCatalog) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return c.provider.DefinitionExtensions(ctx, name)
}
