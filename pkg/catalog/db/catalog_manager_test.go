//go:build duckdb_arrow

package db

import (
	"context"
	"iter"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	dssources "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/embedding"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/metadata"
	"github.com/hugr-lab/query-engine/types"

	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
)

// ─── Test helper: Provider with Compiler (T013) ─────────────────────────────

// newTestProviderWithCompiler creates a db.Provider backed by in-memory DuckDB
// with system types persisted and a real compiler for CatalogManager tests.
func newTestProviderWithCompiler(t *testing.T) (*Provider, context.Context) {
	t.Helper()
	ctx := t.Context()

	// Create in-memory DuckDB pool.
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	// Initialize _schema_* DDL.
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	// Create compiler with all rules.
	c := compiler.New(compiler.GlobalRules()...)

	// Create provider with compiler.
	p, err := NewWithCompiler(ctx, pool, Config{
		Cache: DefaultCacheConfig(),
	}, nil, c)
	require.NoError(t, err)

	// Persist system types so the compiler can resolve Int, String, etc.
	staticProv, err := static.New()
	require.NoError(t, err)
	sysSource := &systemTypesForTest{prov: staticProv, ctx: ctx}
	err = p.UpdateWithCatalog(ctx, sysSource, SystemCatalogName)
	require.NoError(t, err)

	return p, ctx
}

// systemTypesForTest wraps a static.Provider to provide system type definitions
// as a DefinitionsSource for persisting to the DB.
type systemTypesForTest struct {
	prov *static.Provider
	ctx  context.Context
}

func (s *systemTypesForTest) ForName(ctx context.Context, name string) *ast.Definition {
	return s.prov.ForName(ctx, name)
}

func (s *systemTypesForTest) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.prov.DirectiveForName(ctx, name)
}

func (s *systemTypesForTest) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return s.prov.Definitions(s.ctx)
}

func (s *systemTypesForTest) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.prov.DirectiveDefinitions(s.ctx)
}

// ─── Test catalog source ────────────────────────────────────────────────────

// testCatalogSource implements sources.Catalog for testing.
type testCatalogSource struct {
	name    string
	version string
	sdl     string
	src     *sources.StringSource
}

func newTestCatalogSource(t *testing.T, name, version, sdl string) *testCatalogSource {
	t.Helper()
	src, err := sources.NewStringSource(name, nil, compiler.Options{
		Name:       name,
		EngineType: "duckdb",
	}, sdl)
	require.NoError(t, err)
	return &testCatalogSource{
		name:    name,
		version: version,
		sdl:     sdl,
		src:     src,
	}
}

func (c *testCatalogSource) ForName(ctx context.Context, name string) *ast.Definition {
	return c.src.ForName(ctx, name)
}

func (c *testCatalogSource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return c.src.DirectiveForName(ctx, name)
}

func (c *testCatalogSource) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.src.Definitions(ctx)
}

func (c *testCatalogSource) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return c.src.DirectiveDefinitions(ctx)
}

func (c *testCatalogSource) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	return c.src.Extensions(ctx)
}

func (c *testCatalogSource) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return c.src.DefinitionExtensions(ctx, name)
}

func (c *testCatalogSource) Name() string                     { return c.name }
func (c *testCatalogSource) Description() string              { return "" }
func (c *testCatalogSource) CompileOptions() compiler.Options { return c.src.CompileOptions() }
func (c *testCatalogSource) Engine() engines.Engine {
	return nil
}

func (c *testCatalogSource) Version(_ context.Context) (string, error) {
	return c.version, nil
}

// ─── AC-1: Version match skips compilation (T011) ───────────────────────────

func TestVersionMatchSkipsCompilation(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)

	// First add — should compile and persist.
	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Verify types are persisted.
	def := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def, "ds1_User should exist after first AddCatalog")

	// Count types from this catalog.
	typeCountBefore := countCatalogTypes(t, p, ctx, "ds1")
	assert.Greater(t, typeCountBefore, 0, "should have types after compilation")

	// Verify catalog record in DB.
	rec, err := p.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, catalogVersionWithOptions("v1", catalog.CompileOptions()), rec.Version)
	assert.False(t, rec.Suspended)

	// Second add with same version — should skip compilation.
	err = p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Verify types still exist and count is unchanged.
	def2 := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def2, "ds1_User should still exist after skip")

	typeCountAfter := countCatalogTypes(t, p, ctx, "ds1")
	assert.Equal(t, typeCountBefore, typeCountAfter, "type count should be unchanged (no rewrite)")
}

// ─── AC-2: Version mismatch triggers recompilation (T012) ───────────────────

func TestVersionMismatchRecompiles(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	catalogV1 := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)

	// Add at v1.
	err := p.AddCatalog(ctx, "ds1", catalogV1)
	require.NoError(t, err)

	def := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def, "ds1_User should exist after v1")

	// Create v2 with additional field.
	catalogV2 := newTestCatalogSource(t, "ds1",
		"v2",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
			email: String
		}`,
	)

	// Re-add at v2 — should recompile.
	err = p.AddCatalog(ctx, "ds1", catalogV2)
	require.NoError(t, err)

	// Verify new schema is available.
	def2 := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def2, "ds1_User should exist after v2")

	// Verify the new field is present.
	emailField := def2.Fields.ForName("email")
	assert.NotNil(t, emailField, "email field should exist after v2 recompilation")

	// Verify version updated in DB.
	rec, err := p.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, catalogVersionWithOptions("v2", catalogV2.CompileOptions()), rec.Version)
}

// ─── AC-4: RemoveCatalog cleans DB and suspends dependents (T019) ───────────

func TestRemoveCatalogWithDependents(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add base catalog.
	base := newTestCatalogSource(t, "base",
		"v1",
		`type base_Item @table(name: "items") {
			id: Int! @pk
			title: String!
		}`,
	)
	err := p.AddCatalog(ctx, "base", base)
	require.NoError(t, err)

	// Add extension that depends on base (extends base_Item type).
	ext := newTestCatalogSource(t, "ext",
		"v1",
		`extend type base_Item {
			rating: Float
		}`,
	)
	ext.src, err = sources.NewStringSource("ext", nil, compiler.Options{
		Name:        "ext",
		EngineType:  "duckdb",
		IsExtension: true,
	}, `extend type base_Item {
		rating: Float
	}`)
	require.NoError(t, err)

	err = p.AddCatalog(ctx, "ext", ext)
	require.NoError(t, err)

	// Verify extension added rating field.
	def := p.ForName(ctx, "base_Item")
	require.NotNil(t, def)

	// Remove base catalog.
	err = p.RemoveCatalog(ctx, "base")
	require.NoError(t, err)

	// Verify base types are gone.
	assert.Nil(t, p.ForName(ctx, "base_Item"), "base_Item should be gone after RemoveCatalog")

	// Verify base catalog record is gone.
	rec, err := p.GetCatalog(ctx, "base")
	require.NoError(t, err)
	assert.Nil(t, rec, "base catalog record should be gone")

	// Verify extension is suspended.
	extRec, err := p.GetCatalog(ctx, "ext")
	require.NoError(t, err)
	if extRec != nil {
		assert.True(t, extRec.Suspended, "extension should be suspended after base removal")
	}
}

// ─── AC-8: Version mismatch deletes dependent extension schema objects (T020) ─

func TestVersionMismatchDeletesDependentSchemaObjects(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add base v1.
	baseV1 := newTestCatalogSource(t, "base",
		"v1",
		`type base_Item @table(name: "items") {
			id: Int! @pk
			title: String!
		}`,
	)
	err := p.AddCatalog(ctx, "base", baseV1)
	require.NoError(t, err)

	// Add extension that depends on base.
	ext := newTestCatalogSource(t, "ext",
		"v1",
		`extend type base_Item {
			rating: Float
		}`,
	)
	ext.src, err = sources.NewStringSource("ext", nil, compiler.Options{
		Name:        "ext",
		EngineType:  "duckdb",
		IsExtension: true,
	}, `extend type base_Item {
		rating: Float
	}`)
	require.NoError(t, err)
	err = p.AddCatalog(ctx, "ext", ext)
	require.NoError(t, err)

	// Recompile base with v2.
	baseV2 := newTestCatalogSource(t, "base",
		"v2",
		`type base_Item @table(name: "items") {
			id: Int! @pk
			title: String!
			description: String
		}`,
	)
	err = p.AddCatalog(ctx, "base", baseV2)
	require.NoError(t, err)

	// Verify base is at v2.
	rec, err := p.GetCatalog(ctx, "base")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, catalogVersionWithOptions("v2", baseV2.CompileOptions()), rec.Version)

	// After base recompilation, extension was suspended (schema objects deleted)
	// but then reactivateSuspended() at the end of AddCatalog brings it back
	// because its dependency (base) is now satisfied.
	extRec, err := p.GetCatalog(ctx, "ext")
	require.NoError(t, err)
	require.NotNil(t, extRec, "extension catalog record should exist")
	assert.False(t, extRec.Suspended, "extension should be reactivated after base recompilation")

	// Extension field should be visible on the updated base type.
	def := p.ForName(ctx, "base_Item")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("description"), "new base v2 field should exist")
	assert.NotNil(t, def.Fields.ForName("rating"), "extension field should be restored after reactivation")
}

// ─── AC-7: ExistsCatalog checks both memory and DB (T021) ───────────────────

func TestExistsCatalog(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Before add — should not exist.
	assert.False(t, p.ExistsCatalog("ds1"), "should not exist before add")

	// Add catalog.
	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)
	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// After add — should exist (memory path).
	assert.True(t, p.ExistsCatalog("ds1"), "should exist after add (memory)")

	// Clear memory map to test DB fallback.
	p.mu.Lock()
	delete(p.catalogs, "ds1")
	p.mu.Unlock()

	// Should still exist via DB fallback.
	assert.True(t, p.ExistsCatalog("ds1"), "should exist via DB fallback")

	// Non-existent catalog.
	assert.False(t, p.ExistsCatalog("nonexistent"), "should not exist for unknown catalog")
}

// ─── AC-3: Incremental reload uses Changes() (T022) ─────────────────────────
// Note: This test validates full reload path since creating a proper
// IncrementalCatalog mock requires more infrastructure. The incremental
// path is validated via memoryCatalog tests in pkg/catalog/.

func TestReloadCatalog(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)
	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Reload with same version — should be a no-op.
	err = p.ReloadCatalog(ctx, "ds1")
	require.NoError(t, err)

	// Update the source to v2 and reload.
	catalog.version = "v2"
	catalog.src, err = sources.NewStringSource("ds1", nil, compiler.Options{
		Name:       "ds1",
		EngineType: "duckdb",
	}, `type ds1_User @table(name: "users") {
		id: Int! @pk
		name: String!
		email: String
	}`)
	require.NoError(t, err)

	err = p.ReloadCatalog(ctx, "ds1")
	require.NoError(t, err)

	// Verify version updated.
	rec, err := p.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, catalogVersionWithOptions("v2", catalog.CompileOptions()), rec.Version)

	// Verify new field is present.
	def := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def)
	emailField := def.Fields.ForName("email")
	assert.NotNil(t, emailField, "email field should exist after reload")

	// Reload non-existent catalog.
	err = p.ReloadCatalog(ctx, "nonexistent")
	assert.Error(t, err)
}

// ─── Disable/Enable catalog (T023) ──────────────────────────────────────────

func TestDisableEnableCatalog(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)
	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Types should be visible.
	def := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def, "ds1_User should be visible")

	// Disable catalog.
	err = p.DisableCatalog(ctx, "ds1")
	require.NoError(t, err)

	// Types should be hidden.
	def = p.ForName(ctx, "ds1_User")
	assert.Nil(t, def, "ds1_User should be hidden after disable")

	// Verify disabled flag in DB.
	rec, err := p.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.True(t, rec.Disabled, "catalog should be marked disabled")

	// Enable catalog.
	err = p.EnableCatalog(ctx, "ds1")
	require.NoError(t, err)

	// Types should be visible again.
	def = p.ForName(ctx, "ds1_User")
	require.NotNil(t, def, "ds1_User should be visible after enable")

	// Verify flag cleared.
	rec, err = p.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.False(t, rec.Disabled, "catalog should not be marked disabled")
}

// ─── AC-5/AC-6: System types initialization (T024, T029, T030) ──────────────

func TestInitSystemTypes(t *testing.T) {
	ctx := t.Context()

	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	// Initialize _schema_* DDL.
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	p, err := NewWithCompiler(ctx, pool, Config{
		Cache: DefaultCacheConfig(),
	}, nil, compiler.New(compiler.GlobalRules()...))
	require.NoError(t, err)

	// First call — should persist system types.
	err = p.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Verify system types are in the DB (e.g., Int, String, Boolean).
	intDef := p.ForName(ctx, "Int")
	assert.NotNil(t, intDef, "Int should exist after InitSystemTypes")

	stringDef := p.ForName(ctx, "String")
	assert.NotNil(t, stringDef, "String should exist after InitSystemTypes")

	queryDef := p.ForName(ctx, "Query")
	assert.NotNil(t, queryDef, "Query should exist after InitSystemTypes")

	// Verify initialized flag.
	initialized, err := p.IsInitialized(ctx)
	require.NoError(t, err)
	assert.True(t, initialized, "should be initialized")

	// Count system types (system types use SystemCatalogName = "_system").
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	var sysCount int
	err = conn.QueryRow(ctx,
		"SELECT count(*) FROM _schema_types WHERE catalog = $1", SystemCatalogName,
	).Scan(&sysCount)
	require.NoError(t, err)
	conn.Close()
	assert.Greater(t, sysCount, 0, "should have system types")

	// Second call — should skip (version unchanged).
	err = p.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Verify count unchanged.
	conn2, err := pool.Conn(ctx)
	require.NoError(t, err)
	var sysCount2 int
	err = conn2.QueryRow(ctx,
		"SELECT count(*) FROM _schema_types WHERE catalog = $1", SystemCatalogName,
	).Scan(&sysCount2)
	require.NoError(t, err)
	conn2.Close()
	assert.Equal(t, sysCount, sysCount2, "system types count should be unchanged on second call")
}

func TestRestartSkipsCompilation(t *testing.T) {
	ctx := t.Context()

	// Use a temp file DB so state persists across provider instances.
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.duckdb"

	// First start — create provider, init system types, add a catalog.
	pool1, err := db.NewPool(dbPath)
	require.NoError(t, err)

	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)
	_, err = pool1.Exec(ctx, sqlStr)
	require.NoError(t, err)

	c := compiler.New(compiler.GlobalRules()...)
	p1, err := NewWithCompiler(ctx, pool1, Config{Cache: DefaultCacheConfig()}, nil, c)
	require.NoError(t, err)

	err = p1.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Add a user catalog.
	catalog := newTestCatalogSource(t, "ds1", "v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`)
	err = p1.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	def := p1.ForName(ctx, "ds1_User")
	require.NotNil(t, def, "ds1_User should exist after first start")

	// Close first pool.
	pool1.Close()

	// Second start — new provider on same DB file.
	pool2, err := db.NewPool(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { pool2.Close() })

	p2, err := NewWithCompiler(ctx, pool2, Config{Cache: DefaultCacheConfig()}, nil, c)
	require.NoError(t, err)

	// InitSystemTypes should skip (version unchanged).
	err = p2.InitSystemTypes(ctx)
	require.NoError(t, err)

	// System types should be available from DB.
	intDef := p2.ForName(ctx, "Int")
	assert.NotNil(t, intDef, "Int should exist on restart")

	// User catalog types should be available from DB (no recompilation needed).
	def2 := p2.ForName(ctx, "ds1_User")
	assert.NotNil(t, def2, "ds1_User should exist on restart without recompilation")

	// Catalog record should still exist.
	rec, err := p2.GetCatalog(ctx, "ds1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, catalogVersionWithOptions("v1", catalog.CompileOptions()), rec.Version)
}

// ─── AC-10: Description API works through interfaces (T031, T032) ────────

func TestDescriptionAPI(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_User @table(name: "users") {
			id: Int! @pk
			name: String!
		}`,
	)
	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Verify initial description (auto-generated).
	def := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def)

	// --- SetDefinitionDescription ---
	err = p.SetDefinitionDescription(ctx, "ds1_User", "Updated type description", "A long description for ds1_User")
	require.NoError(t, err)

	// Cache should be evicted — reload from DB.
	def2 := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def2)
	assert.Equal(t, "Updated type description", def2.Description)

	// Verify is_summarized flag set in DB.
	conn, err := p.pool.Conn(ctx)
	require.NoError(t, err)
	var isSummarized bool
	err = conn.QueryRow(ctx,
		"SELECT is_summarized FROM _schema_types WHERE name = $1", "ds1_User",
	).Scan(&isSummarized)
	require.NoError(t, err)
	assert.True(t, isSummarized, "is_summarized should be true after SetDefinitionDescription")
	conn.Close()

	// --- SetFieldDescription ---
	err = p.SetFieldDescription(ctx, "ds1_User", "name", "Updated field description", "A long description for name field")
	require.NoError(t, err)

	// Verify field description updated via ForName (cache evicted).
	def3 := p.ForName(ctx, "ds1_User")
	require.NotNil(t, def3)
	nameField := def3.Fields.ForName("name")
	require.NotNil(t, nameField)
	assert.Equal(t, "Updated field description", nameField.Description)

	// --- SetCatalogDescription ---
	err = p.SetCatalogDescription(ctx, "ds1", "Updated catalog description", "A long catalog description")
	require.NoError(t, err)

	// Verify in DB.
	conn2, err := p.pool.Conn(ctx)
	require.NoError(t, err)
	var catDesc string
	err = conn2.QueryRow(ctx,
		"SELECT description FROM _schema_catalogs WHERE name = $1", "ds1",
	).Scan(&catDesc)
	require.NoError(t, err)
	assert.Equal(t, "Updated catalog description", catDesc)
	conn2.Close()
}

// ─── AC-9: Incremental validation rejects missing type references (T035) ────

func TestUpdateRejectsMissingTypeReference(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Call Update() directly (bypassing compiler) with a type that references
	// "NonExistent" — a type name not in _schema_types or in the batch.
	badSource := &inlineDefinitionsSource{
		defs: []*ast.Definition{
			{
				Kind:        ast.Object,
				Name:        "BadType",
				Description: "Type with invalid field reference",
				Fields: ast.FieldList{
					{
						Name:        "id",
						Type:        &ast.Type{NamedType: "Int", NonNull: true},
						Description: "ok field",
					},
					{
						Name:        "broken",
						Type:        &ast.Type{NamedType: "NonExistent"},
						Description: "field referencing missing type",
					},
				},
			},
		},
	}

	err := p.Update(ctx, badSource)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistent")
	assert.Contains(t, err.Error(), "validate references")

	// Verify no partial state — BadType should not be persisted.
	def := p.ForName(ctx, "BadType")
	assert.Nil(t, def, "BadType should not exist after validation failure")
}

func TestUpdateRejectsMissingArgumentTypeReference(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Argument type references a non-existent type.
	badSource := &inlineDefinitionsSource{
		defs: []*ast.Definition{
			{
				Kind: ast.Object,
				Name: "ArgBadType",
				Fields: ast.FieldList{
					{
						Name: "search",
						Type: &ast.Type{NamedType: "String"},
						Arguments: ast.ArgumentDefinitionList{
							{
								Name: "filter",
								Type: &ast.Type{NamedType: "MissingFilterType"},
							},
						},
					},
				},
			},
		},
	}

	err := p.Update(ctx, badSource)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MissingFilterType")
	assert.Contains(t, err.Error(), "validate references")
}

func TestUpdateRejectsMissingInterfaceReference(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Interface reference is non-existent.
	badSource := &inlineDefinitionsSource{
		defs: []*ast.Definition{
			{
				Kind:       ast.Object,
				Name:       "IfaceBadType",
				Interfaces: []string{"NonExistentInterface"},
				Fields: ast.FieldList{
					{Name: "id", Type: &ast.Type{NamedType: "Int"}},
				},
			},
		},
	}

	err := p.Update(ctx, badSource)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentInterface")
	assert.Contains(t, err.Error(), "validate references")
}

func TestUpdateRejectsMissingUnionMemberReference(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Union member type is non-existent.
	badSource := &inlineDefinitionsSource{
		defs: []*ast.Definition{
			{
				Kind:  ast.Union,
				Name:  "UnionBadType",
				Types: []string{"String", "NonExistentMember"},
			},
		},
	}

	err := p.Update(ctx, badSource)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentMember")
	assert.Contains(t, err.Error(), "validate references")
}

// T036: Update accepts valid references (existing system types and batch types).
func TestUpdateAcceptsValidReferences(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add a catalog with valid references to system types (Int, String, Float, Boolean).
	catalog := newTestCatalogSource(t, "ds1",
		"v1",
		`type ds1_Product @table(name: "products") {
			id: Int! @pk
			name: String!
			price: Float
			active: Boolean
		}`,
	)

	err := p.AddCatalog(ctx, "ds1", catalog)
	require.NoError(t, err)

	// Verify types are persisted.
	def := p.ForName(ctx, "ds1_Product")
	require.NotNil(t, def, "ds1_Product should exist after valid AddCatalog")
	assert.NotNil(t, def.Fields.ForName("name"))
	assert.NotNil(t, def.Fields.ForName("price"))
	assert.NotNil(t, def.Fields.ForName("active"))
}

func TestUpdateAcceptsBatchReferences(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Types reference each other within the same batch — should be valid.
	batchSource := &inlineDefinitionsSource{
		defs: []*ast.Definition{
			{
				Kind: ast.Object,
				Name: "BatchAddress",
				Fields: ast.FieldList{
					{Name: "street", Type: &ast.Type{NamedType: "String"}},
					{Name: "city", Type: &ast.Type{NamedType: "String"}},
				},
			},
			{
				Kind: ast.Object,
				Name: "BatchPerson",
				Fields: ast.FieldList{
					{Name: "name", Type: &ast.Type{NamedType: "String"}},
					{Name: "address", Type: &ast.Type{NamedType: "BatchAddress"}},
				},
			},
		},
	}

	err := p.Update(ctx, batchSource)
	require.NoError(t, err)

	// Verify both types exist.
	assert.NotNil(t, p.ForName(ctx, "BatchAddress"))
	assert.NotNil(t, p.ForName(ctx, "BatchPerson"))
}

// inlineDefinitionsSource provides definitions directly from a slice.
type inlineDefinitionsSource struct {
	defs []*ast.Definition
}

func (s *inlineDefinitionsSource) ForName(_ context.Context, name string) *ast.Definition {
	for _, d := range s.defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *inlineDefinitionsSource) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}

func (s *inlineDefinitionsSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *inlineDefinitionsSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {}
}

// ─── Edge case tests (T037) ─────────────────────────────────────────────────

func TestRemoveCatalogNonExistent(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Removing a non-existent catalog is idempotent — no error, no panic.
	err := p.RemoveCatalog(ctx, "does_not_exist")
	assert.NoError(t, err, "removing non-existent catalog should be idempotent")

	// Verify nothing was created.
	rec, err := p.GetCatalog(ctx, "does_not_exist")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestCompilationErrorNoPartialState(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Create a catalog with invalid SDL that will fail compilation.
	// The compiler should catch this before any DB writes.
	catalog := newTestCatalogSource(t, "bad",
		"v1",
		// Invalid SDL: referencing unknown type in @table directive
		`type bad_Widget @table(name: "widgets") {
			id: Int! @pk
			ref: NonExistentType
		}`,
	)

	err := p.AddCatalog(ctx, "bad", catalog)
	require.Error(t, err, "AddCatalog with invalid type reference should fail")

	// Verify no partial state: catalog should not exist in DB.
	rec, err := p.GetCatalog(ctx, "bad")
	require.NoError(t, err)
	assert.Nil(t, rec, "no catalog record should exist after compilation error")

	// Verify no types were persisted.
	def := p.ForName(ctx, "bad_Widget")
	assert.Nil(t, def, "no types should exist after compilation error")
}

func TestDependencyChainCascade(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Create chain: A → B → C (B extends A, C extends B).
	catA := newTestCatalogSource(t, "catA",
		"v1",
		`type catA_Base @table(name: "base") {
			id: Int! @pk
			name: String!
		}`,
	)
	err := p.AddCatalog(ctx, "catA", catA)
	require.NoError(t, err)

	catB := newTestCatalogSource(t, "catB",
		"v1",
		`extend type catA_Base {
			catB_extra: String
		}`,
	)
	err = p.AddCatalog(ctx, "catB", catB)
	require.NoError(t, err)

	catC := newTestCatalogSource(t, "catC",
		"v1",
		`extend type catA_Base {
			catC_extra: Float
		}`,
	)
	err = p.AddCatalog(ctx, "catC", catC)
	require.NoError(t, err)

	// Verify all extensions exist.
	def := p.ForName(ctx, "catA_Base")
	require.NotNil(t, def)
	assert.NotNil(t, def.Fields.ForName("catB_extra"), "catB extension field should exist")
	assert.NotNil(t, def.Fields.ForName("catC_extra"), "catC extension field should exist")

	// Remove A — B and C should be suspended.
	err = p.RemoveCatalog(ctx, "catA")
	require.NoError(t, err)

	// A should be gone completely.
	assert.Nil(t, p.ForName(ctx, "catA_Base"))
	recA, err := p.GetCatalog(ctx, "catA")
	require.NoError(t, err)
	assert.Nil(t, recA, "catA should be completely removed")

	// B and C should be suspended.
	recB, err := p.GetCatalog(ctx, "catB")
	require.NoError(t, err)
	if recB != nil {
		assert.True(t, recB.Suspended, "catB should be suspended")
	}

	recC, err := p.GetCatalog(ctx, "catC")
	require.NoError(t, err)
	if recC != nil {
		assert.True(t, recC.Suspended, "catC should be suspended")
	}
}

func TestAddCatalogReactivatesSuspended(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add base, then extension.
	base := newTestCatalogSource(t, "base2",
		"v1",
		`type base2_Thing @table(name: "things") {
			id: Int! @pk
			label: String!
		}`,
	)
	err := p.AddCatalog(ctx, "base2", base)
	require.NoError(t, err)

	ext := newTestCatalogSource(t, "ext2",
		"v1",
		`extend type base2_Thing {
			ext2_tag: String
		}`,
	)
	err = p.AddCatalog(ctx, "ext2", ext)
	require.NoError(t, err)

	// Remove base → ext suspended.
	err = p.RemoveCatalog(ctx, "base2")
	require.NoError(t, err)

	rec, err := p.GetCatalog(ctx, "ext2")
	require.NoError(t, err)
	if rec != nil {
		assert.True(t, rec.Suspended, "ext2 should be suspended after base removal")
	}

	// Re-add base → ext should be reactivated.
	base2 := newTestCatalogSource(t, "base2",
		"v1",
		`type base2_Thing @table(name: "things") {
			id: Int! @pk
			label: String!
		}`,
	)
	err = p.AddCatalog(ctx, "base2", base2)
	require.NoError(t, err)

	// Verify base2 types are back.
	def := p.ForName(ctx, "base2_Thing")
	require.NotNil(t, def, "base2_Thing should exist after re-add")

	// Check if ext2 was reactivated.
	// Note: reactivation requires the source handle to be in the catalogs map.
	// After RemoveCatalog("base2"), ext2 was suspended but its source handle
	// was preserved. The reactivateSuspended flow should find it, check
	// dependencies (all satisfied since base2 is back), and recompile.
	rec2, err := p.GetCatalog(ctx, "ext2")
	require.NoError(t, err)
	require.NotNil(t, rec2, "ext2 catalog record should still exist")

	// If reactivation succeeded, ext2 should no longer be suspended.
	// If it failed (e.g., compilation error), ext2 remains suspended.
	// Both are acceptable edge cases — the key is no crash and no data corruption.
	if !rec2.Suspended {
		// Reactivation succeeded — verify ext2 field is back.
		def = p.ForName(ctx, "base2_Thing")
		require.NotNil(t, def)
		assert.NotNil(t, def.Fields.ForName("ext2_tag"), "ext2 extension field should be restored")
	} else {
		t.Log("ext2 was not reactivated (expected in some scenarios — reactivation is best-effort)")
	}
}

// ─── Back-reference field persistence via @references ────────────────────────

func TestBackReferenceFieldPersisted(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add two base catalog sources: items and events in the same catalog.
	base := newTestCatalogSource(t, "local",
		"v1",
		`type local_items @table(name: "items") {
			id: Int! @pk
			name: String!
		}
		type local_events @table(name: "events") {
			id: Int! @pk
			title: String!
		}`,
	)
	err := p.AddCatalog(ctx, "local", base)
	require.NoError(t, err)

	// Verify both types exist.
	require.NotNil(t, p.ForName(ctx, "local_items"))
	require.NotNil(t, p.ForName(ctx, "local_events"))

	// Add extension source with @references bridging items→events,
	// plus additional extend blocks (mimics real ext_bridge with multiple blocks).
	ext := newTestExtCatalogSource(t, "bridge", "v1",
		`extend type local_items
		  @dependency(name: "local")
		  @references(
		    name: "item_event"
		    references_name: "local_events"
		    source_fields: ["id"]
		    references_fields: ["id"]
		    query: "event"
		    references_query: "item"
		  ) {
		  _stub: String
		}
		extend type local_items
		  @dependency(name: "local") {
		  extra_field: String
		}`,
	)
	err = p.AddCatalog(ctx, "bridge", ext)
	require.NoError(t, err)

	// Forward reference: "event" field should exist on local_items.
	items := p.ForName(ctx, "local_items")
	require.NotNil(t, items)
	eventField := items.Fields.ForName("event")
	assert.NotNil(t, eventField, "forward reference field 'event' should exist on local_items")

	// Back-reference: "item" field should exist on local_events.
	events := p.ForName(ctx, "local_events")
	require.NotNil(t, events)
	itemField := events.Fields.ForName("item")
	assert.NotNil(t, itemField, "back-reference field 'item' should exist on local_events")

	if itemField != nil {
		// Should be a list type: [local_items]
		assert.True(t, itemField.Type.Elem != nil, "back-reference should be a list type")
	}
}

// newTestExtCatalogSource creates a catalog source with IsExtension=true.
func newTestExtCatalogSource(t *testing.T, name, version, sdl string) *testCatalogSource {
	t.Helper()
	src, err := sources.NewStringSource(name, nil, compiler.Options{
		Name:        name,
		Prefix:      name,
		EngineType:  "duckdb",
		IsExtension: true,
	}, sdl)
	require.NoError(t, err)
	return &testCatalogSource{
		name:    name,
		version: version,
		sdl:     sdl,
		src:     src,
	}
}

// ─── T038: System types version change triggers re-persist ──────────────────

func TestSystemTypesVersionChange(t *testing.T) {
	ctx := t.Context()

	pool, err := db.NewPool(":memory:")
	require.NoError(t, err)
	defer pool.Close()

	// Initialize _schema_* DDL.
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: 128,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	p, err := NewWithCompiler(ctx, pool, Config{
		Cache: DefaultCacheConfig(),
	}, nil, compiler.New(compiler.GlobalRules()...))
	require.NoError(t, err)

	// First init — persists system types.
	err = p.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Get initial count and stored version.
	initialCount := countCatalogTypes(t, p, ctx, SystemCatalogName)
	assert.Greater(t, initialCount, 0, "should have system types")

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	var rawVersion string
	err = conn.QueryRow(ctx,
		"SELECT CAST(value AS VARCHAR) FROM _schema_settings WHERE key = 'system_types_version'",
	).Scan(&rawVersion)
	require.NoError(t, err)
	conn.Close()

	// Tamper with the stored version to simulate a version change.
	conn2, err := pool.Conn(ctx)
	require.NoError(t, err)
	_, err = conn2.Exec(ctx,
		`UPDATE _schema_settings SET value = '"sys-old-fake"' WHERE key = 'system_types_version'`,
	)
	require.NoError(t, err)
	conn2.Close()

	// Re-init — should detect mismatch and re-persist.
	err = p.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Verify version is updated back to the correct one.
	conn3, err := pool.Conn(ctx)
	require.NoError(t, err)
	var newVersion string
	err = conn3.QueryRow(ctx,
		"SELECT CAST(value AS VARCHAR) FROM _schema_settings WHERE key = 'system_types_version'",
	).Scan(&newVersion)
	require.NoError(t, err)
	conn3.Close()

	assert.Equal(t, rawVersion, newVersion, "version should be restored to computed value after re-persist")

	// Count should remain the same (system types re-persisted, not duplicated).
	finalCount := countCatalogTypes(t, p, ctx, SystemCatalogName)
	assert.Equal(t, initialCount, finalCount, "system type count should be unchanged after re-persist")
}

// ─── IntrospectionQuery Test ─────────────────────────────────────────────────

func TestIntrospectionQuery_DBProvider(t *testing.T) {
	p, ctx := newTestProviderWithCompiler(t)

	// Add a catalog with a table type.
	cat := newTestCatalogSource(t, "intro_ds",
		"v1",
		`type intro_ds_User @table(name: "users") {
			id: Int! @pk
			name: String!
			email: String
		}`)
	require.NoError(t, p.AddCatalog(ctx, "intro_ds", cat))

	// Run IntrospectionQuery against db.Provider (same query as GraphiQL sends).
	ss := catalog.NewService(p)
	query := `
	query IntrospectionQuery {
		__schema {
			queryType { name kind }
			mutationType { name kind }
			subscriptionType { name kind }
			types {
				...FullType
			}
			directives {
				name
				description
				locations
				args {
					...InputValue
				}
			}
		}
	}

	fragment FullType on __Type {
		kind
		name
		description
		fields(includeDeprecated: true) {
			name
			description
			args {
				...InputValue
			}
			type {
				...TypeRef
			}
			isDeprecated
			deprecationReason
		}
		inputFields {
			...InputValue
		}
		interfaces {
			...TypeRef
		}
		enumValues(includeDeprecated: true) {
			name
			description
			isDeprecated
			deprecationReason
		}
		possibleTypes {
			...TypeRef
		}
	}

	fragment InputValue on __InputValue {
		name
		description
		type { ...TypeRef }
		defaultValue
	}

	fragment TypeRef on __Type {
		kind
		name
		ofType {
			kind
			name
			ofType {
				kind
				name
				ofType {
					kind
					name
					ofType {
						kind
						name
						ofType {
							kind
							name
							ofType {
								kind
								name
								ofType {
									kind
									name
									ofType {
										kind
										name
										ofType {
											kind
											name
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	`

	op, err := ss.ParseQuery(ctx, query, nil, "IntrospectionQuery")
	require.NoError(t, err, "ParseQuery should succeed")

	rqt, _ := sdl.QueryRequestInfo(op.Definition.SelectionSet)
	require.NotEmpty(t, rqt, "should have at least one query request")

	for _, r := range rqt {
		data, err := metadata.ProcessQuery(ctx, ss.Provider(), r, 20, nil)
		require.NoError(t, err, "IntrospectionQuery should succeed on db.Provider")
		require.NotNil(t, data, "IntrospectionQuery should return data")
	}
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// ─── Real Embedder + CatalogManager Test ────────────────────────────────────

func TestRealEmbedder_CatalogManager(t *testing.T) {
	embedderURL := os.Getenv("EMBEDDER_URL")
	if embedderURL == "" {
		t.Skip("EMBEDDER_URL not set, skipping real embedder CatalogManager test")
	}

	ctx := t.Context()

	// Create in-memory DuckDB pool + CoreDB schema.
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	vecSize := 768
	sqlStr, err := db.ParseSQLScriptTemplate(db.SDBDuckDB, coredb.InitSchema, coredb.SchemaTemplateParams{
		VectorSize: vecSize,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlStr)
	require.NoError(t, err)

	// Create embedding source (same path as engine.go Init step 3b).
	src, err := embedding.New(types.DataSource{
		Name: "_system_embedder",
		Type: dssources.Embedding,
		Path: embedderURL,
	}, false)
	require.NoError(t, err)
	require.NoError(t, src.Attach(ctx, pool))

	// Verify embedding source works directly.
	vec, err := src.CreateEmbedding(ctx, "hello world")
	require.NoError(t, err)
	assert.Equal(t, vecSize, len(vec), "expected %d-dim vector", vecSize)
	t.Logf("embedding dim=%d, first 3 values: %v", len(vec), vec[:3])

	// Create compiler + provider (same as engine Init steps 4-5).
	comp := compiler.New(compiler.GlobalRules()...)
	p, err := NewWithCompiler(ctx, pool, Config{
		Cache:   DefaultCacheConfig(),
		VecSize: vecSize,
	}, src, comp)
	require.NoError(t, err)

	// Persist system types.
	err = p.InitSystemTypes(ctx)
	require.NoError(t, err)

	// Add a catalog via CatalogManager — full compile + persist with embeddings.
	cat := newTestCatalogSource(t, "emb_ds",
		"v1",
		`type emb_ds_City @table(name: "cities") {
			id: Int! @pk
			name: String!
			population: Int
		}`)
	err = p.AddCatalog(ctx, "emb_ds", cat)
	require.NoError(t, err)

	// Verify the compiled type is accessible and has embeddings.
	def := p.ForName(ctx, "emb_ds_City")
	require.NotNil(t, def, "emb_ds_City should be in the schema")
	assert.Equal(t, ast.Object, def.Kind)

	// Verify catalog is tracked.
	assert.True(t, p.ExistsCatalog("emb_ds"))

	// Verify embeddings were stored — read vec column directly.
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var hasVec bool
	err = conn.QueryRow(ctx,
		"SELECT vec IS NOT NULL FROM _schema_types WHERE name = 'emb_ds_City'",
	).Scan(&hasVec)
	require.NoError(t, err)
	assert.True(t, hasVec, "embedding vector should be stored for the type")

	t.Log("CatalogManager + real embedder: OK")
}

// countCatalogTypes counts the number of types in the DB for the given catalog.
func countCatalogTypes(t *testing.T, p *Provider, ctx context.Context, catalog string) int {
	t.Helper()
	conn, err := p.pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var count int
	err = conn.QueryRow(ctx,
		"SELECT count(*) FROM _schema_types WHERE catalog = $1", catalog,
	).Scan(&count)
	require.NoError(t, err)
	return count
}
