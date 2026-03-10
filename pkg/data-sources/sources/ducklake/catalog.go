package ducklake

import (
	"context"
	"fmt"
	"iter"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
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

// Ensure duckLakeCatalog implements IncrementalCatalog at compile time.
var _ cs.IncrementalCatalog = (*duckLakeCatalog)(nil)

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

	opts := compiler.Options{
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

// duckLakeCatalog implements sources.Catalog and sources.IncrementalCatalog.
type duckLakeCatalog struct {
	name     string
	desc     string
	opts     compiler.Options
	engine   engines.Engine
	provider *static.DocProvider
	version  string

	pool   *db.Pool
	prefix string
}

func (c *duckLakeCatalog) Name() string                    { return c.name }
func (c *duckLakeCatalog) Description() string             { return c.desc }
func (c *duckLakeCatalog) CompileOptions() compiler.Options { return c.opts }
func (c *duckLakeCatalog) Engine() engines.Engine           { return c.engine }

func (c *duckLakeCatalog) Version(ctx context.Context) (string, error) {
	// Check DuckLake schema version from metadata
	sv, err := SchemaVersion(ctx, c.pool, c.prefix)
	if err != nil {
		return c.version, nil // fall back to content hash
	}
	return strconv.Itoa(sv), nil
}

func (c *duckLakeCatalog) ForName(_ context.Context, name string) *ast.Definition {
	return c.provider.ForName(context.Background(), name)
}

func (c *duckLakeCatalog) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return c.provider.DirectiveForName(context.Background(), name)
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

// IncrementalCatalog interface implementation.
// Changes queries DuckLake metadata for schema changes since fromVersion,
// touching only tables with begin_snapshot/end_snapshot after fromVersion.
// This is O(changed) instead of O(all tables) for large schemas.
func (c *duckLakeCatalog) Changes(ctx context.Context, fromVersion string) (base.DefinitionsSource, string, error) {
	// Check current schema version
	currentVersion, err := c.Version(ctx)
	if err != nil {
		return nil, "", err
	}

	// No changes if version hasn't changed
	if currentVersion == fromVersion {
		return nil, fromVersion, nil
	}

	fromSnapshot, err := strconv.Atoi(fromVersion)
	if err != nil {
		// Non-numeric fromVersion (e.g. content hash from initial load).
		// Fall back to full re-introspect + diff.
		return c.changesFull(ctx, currentVersion)
	}

	// Query only tables/columns that changed since fromSnapshot.
	sc, err := IntrospectChanges(ctx, c.pool, c.prefix, fromSnapshot)
	if err != nil {
		// Fall back to full diff on metadata query errors.
		return c.changesFull(ctx, currentVersion)
	}

	// Build DiffResult directly from the metadata delta.
	diff := c.buildDiffFromChanges(ctx, sc)

	// Update cached provider: apply adds/drops/modifications to the provider.
	// Re-introspect fully to rebuild the cached provider for future diffs.
	// This is still cheaper than diffing — the diff was already computed from metadata.
	tables, err := IntrospectSchema(ctx, c.pool, c.prefix)
	if err == nil {
		doc := GenerateSchemaDocument(tables)
		c.provider = static.NewDocumentProvider(doc)
	}
	c.version = currentVersion

	return diff, currentVersion, nil
}

// changesFull is the fallback path: full re-introspect + DiffSchemas.
// Used when fromVersion is non-numeric or metadata queries fail.
func (c *duckLakeCatalog) changesFull(ctx context.Context, currentVersion string) (base.DefinitionsSource, string, error) {
	tables, err := IntrospectSchema(ctx, c.pool, c.prefix)
	if err != nil {
		return nil, "", fmt.Errorf("ducklake: re-introspect failed: %w", err)
	}

	doc := GenerateSchemaDocument(tables)
	newProvider := static.NewDocumentProvider(doc)

	diff := cs.DiffSchemas(ctx, c.provider, newProvider)

	c.provider = newProvider
	c.version = currentVersion

	return diff, currentVersion, nil
}

// buildDiffFromChanges constructs a DiffResult from SchemaChanges
// without comparing the full old/new schemas.
func (c *duckLakeCatalog) buildDiffFromChanges(ctx context.Context, sc *SchemaChanges) *cs.DiffResult {
	// For added/dropped tables: generate definitions and use DiffResult.
	// For modified tables: diff old definition (from cached provider) with new.
	var oldDefs, newDefs []*ast.Definition

	// Dropped tables → old has the definition, new doesn't.
	for _, tbl := range sc.Dropped {
		name := identGraphQL(dataObjectName(tbl.SchemaName, tbl.TableName))
		if def := c.provider.ForName(ctx, name); def != nil {
			oldDefs = append(oldDefs, def)
		}
	}

	// Added tables → new has the definition, old doesn't.
	for _, tbl := range sc.Added {
		def := tableToDefinition(tbl)
		if def != nil {
			newDefs = append(newDefs, def)
		}
	}

	// Modified tables → both old and new.
	for _, tbl := range sc.Modified {
		name := identGraphQL(dataObjectName(tbl.SchemaName, tbl.TableName))
		if def := c.provider.ForName(ctx, name); def != nil {
			oldDefs = append(oldDefs, def)
		}
		newDef := tableToDefinition(tbl)
		if newDef != nil {
			newDefs = append(newDefs, newDef)
		}
	}

	// Use DiffSchemas on only the affected definitions.
	oldSrc := &sliceDefinitionsSource{defs: oldDefs}
	newSrc := &sliceDefinitionsSource{defs: newDefs}
	return cs.DiffSchemas(ctx, oldSrc, newSrc)
}

// sliceDefinitionsSource is a minimal DefinitionsSource backed by a slice.
type sliceDefinitionsSource struct {
	defs []*ast.Definition
}

func (s *sliceDefinitionsSource) ForName(_ context.Context, name string) *ast.Definition {
	for _, d := range s.defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *sliceDefinitionsSource) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}

func (s *sliceDefinitionsSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *sliceDefinitionsSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(_ func(string, *ast.DirectiveDefinition) bool) {}
}
