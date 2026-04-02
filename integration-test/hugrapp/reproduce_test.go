//go:build duckdb_arrow

package hugrapp_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	catalogdb "github.com/hugr-lab/query-engine/pkg/catalog/db"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

// TestFunctionFieldSurvivesAddRemoveAdd reproduces the compiler/provider bug
// where Function root type loses module fields after a catalog that extends
// Function is added, removed, and re-added.
//
// Pure compiler+provider level — no Service, no HTTP, no Airport.
//
// Debug breakpoints:
//   - pkg/catalog/db/drop.go → DropCatalog()
//   - pkg/catalog/db/catalog_manager.go → RemoveCatalog() / AddCatalog()
//   - pkg/catalog/compiler/rules/assemble_modules.go → ModuleAssembler.ProcessAll()
func TestFunctionFieldSurvivesAddRemoveAdd(t *testing.T) {
	ctx := context.Background()
	coreDBPath := t.TempDir() + "/core.duckdb"
	t.Logf("CoreDB path: %s", coreDBPath)

	// === 1. Connect DuckDB ===
	pool, err := db.Connect(ctx, db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	// === 2. Attach CoreDB ===
	core := coredb.New(coredb.Config{Path: coreDBPath})
	require.NoError(t, core.Attach(ctx, pool))

	// === 3. Create db.Provider with compiler ===
	c := compiler.New(compiler.GlobalRules()...)
	dbProvider, err := catalogdb.NewWithCompiler(ctx, pool, catalogdb.Config{
		TablePrefix: "core.",
		Cache:       catalogdb.DefaultCacheConfig(),
	}, nil, c)
	require.NoError(t, err)

	// === 4. Init system types ===
	require.NoError(t, dbProvider.InitSystemTypes(ctx))

	// === 5. Create catalog service ===
	svc := catalog.NewService(dbProvider)

	// === 6. Attach CoreDB runtime source ===
	coreCat, err := core.Catalog(ctx)
	require.NoError(t, err)
	require.NoError(t, svc.AddCatalog(ctx, core.Name(), coreCat))
	t.Log("CoreDB catalog added")

	// === 7. Verify Function type exists in schema ===
	funcType := svc.ForName(ctx, "Function")
	require.NotNil(t, funcType, "Function type must exist after CoreDB init")
	t.Logf("Function type has %d fields", len(funcType.Fields))

	// === 8. Create a test catalog that extends Function ===
	appSDL := `
extend type Function {
  test_func(a: BigInt!): BigInt @function(name: "\"default\".\"TEST_FUNC\"")
}

type test_items @view(name: "\"default\".\"TEST_ITEMS\"") {
  id: BigInt! @pk
  name: String!
}
`
	e := engines.NewDuckDB()
	opts := base.Options{
		Name:       "test_app",
		Prefix:     "test_app",
		AsModule:   true,
		EngineType: string(e.Type()),
	}
	appCat, err := sources.NewStringSource("test_app", e, opts, appSDL)
	require.NoError(t, err)

	// === 9. ADD test catalog ===
	err = svc.AddCatalog(ctx, "test_app", appCat)
	require.NoError(t, err)
	t.Log("test_app catalog ADDED")

	// Verify Function still has core fields + test_app field
	funcType = svc.ForName(ctx, "Function")
	require.NotNil(t, funcType, "Function must exist after AddCatalog")
	t.Logf("After ADD: Function has %d fields", len(funcType.Fields))

	hasCore := false
	hasTestApp := false
	for _, f := range funcType.Fields {
		if f.Name == "core" {
			hasCore = true
		}
		if f.Name == "test_app" {
			hasTestApp = true
		}
	}
	require.True(t, hasCore, "Function must have 'core' field after ADD")
	require.True(t, hasTestApp, "Function must have 'test_app' field after ADD")

	// === 10. REMOVE test catalog ===
	err = svc.RemoveCatalog(ctx, "test_app")
	require.NoError(t, err)
	t.Log("test_app catalog REMOVED")

	// === KEY CHECK: Function.core must survive removal of test_app ===
	funcType = svc.ForName(ctx, "Function")
	require.NotNil(t, funcType, "Function must exist after RemoveCatalog")
	t.Logf("After REMOVE: Function has %d fields", len(funcType.Fields))

	hasCore = false
	for _, f := range funcType.Fields {
		if f.Name == "core" {
			hasCore = true
		}
		t.Logf("  field: %s", f.Name)
	}
	// SET BREAKPOINT HERE if this fails:
	require.True(t, hasCore,
		"BUG: Function lost 'core' field after removing test_app catalog. "+
			"Breakpoint in DropCatalog or ModuleAssembler.ProcessAll()")

	// === 11. RE-ADD test catalog ===
	appCat2, err := sources.NewStringSource("test_app", e, opts, appSDL)
	require.NoError(t, err)
	err = svc.AddCatalog(ctx, "test_app", appCat2)
	require.NoError(t, err)
	t.Log("test_app catalog RE-ADDED")

	funcType = svc.ForName(ctx, "Function")
	require.NotNil(t, funcType, "Function must exist after re-add")
	t.Logf("After RE-ADD: Function has %d fields", len(funcType.Fields))

	hasCore = false
	hasTestApp = false
	for _, f := range funcType.Fields {
		if f.Name == "core" {
			hasCore = true
		}
		if f.Name == "test_app" {
			hasTestApp = true
		}
	}
	require.True(t, hasCore, "Function must have 'core' after re-add")
	require.True(t, hasTestApp, "Function must have 'test_app' after re-add")

	t.Log("ALL CHECKS PASSED")
}
