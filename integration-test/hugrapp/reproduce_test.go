//go:build duckdb_arrow

package hugrapp_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
)

// TestReproduceFunctionFieldLostAfterReload reproduces the bug where
// "Function" root type loses module fields after hugr-app source unload+reload.
//
// Steps:
// 1. Start hugr engine with in-memory CoreDB
// 2. Verify { function { core { info { version } } } } works
// 3. Register + load a hugr-app source
// 4. Verify queries still work
// 5. Unload the hugr-app source
// 6. Reload (or load new) the hugr-app source
// 7. Query { function { core { embedder_settings { ... } } } }
//    → BUG: "Cannot query field 'function' on type 'Query'"
//
// To debug: set breakpoint in pkg/catalog/compiler/rules/assemble_modules.go
// at ModuleAssembler.ProcessAll() or in pkg/catalog/catalogs.go at RemoveCatalog/AddCatalog.
func TestReproduceFunctionFieldLostAfterReload(t *testing.T) {
	ctx := context.Background()

	// 1. Start hugr engine
	service, err := hugr.New(hugr.Config{
		DB: db.Config{Path: ""},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{Allowed: true, Role: "admin"}),
			},
		},
	})
	require.NoError(t, err)
	defer service.Close()

	err = service.Init(ctx)
	require.NoError(t, err)

	// 2. Verify Function.core works initially
	resp, err := service.Query(ctx,
		`{ function { core { embedder_settings { is_enabled } } } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Errors, "initial query should work: %v", resp.Errors)
	resp.Close()
	t.Log("Step 2: initial Function.core query OK")

	// 3. Register a hugr-app source (simulate)
	resp, err = service.Query(ctx, `mutation {
		core {
			insert_data_sources(data: {
				name: "fake_app"
				type: "hugr-app"
				description: "test"
				prefix: "fake_app"
				path: "grpc://localhost:99999"
				as_module: true
				self_defined: true
			}) { name }
		}
	}`, nil)
	require.NoError(t, err)
	if resp != nil {
		t.Logf("Step 3 register: errors=%v", resp.Errors)
		resp.Close()
	}

	// 4. Try to load (will fail because no grpc server, but that's OK — we want to test unload)
	resp, err = service.Query(ctx, `mutation {
		function { core { load_data_source(name: "fake_app") { success message } } }
	}`, nil)
	if resp != nil {
		var result struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		}
		_ = resp.ScanData("function.core.load_data_source", &result)
		t.Logf("Step 4 load: success=%v message=%s errors=%v", result.Success, result.Message, resp.Errors)
		resp.Close()
	}

	// 5. Verify Function.core STILL works after failed load
	resp, err = service.Query(ctx,
		`{ function { core { embedder_settings { is_enabled } } } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("Step 5 after load: errors=%v", resp.Errors)
	require.Empty(t, resp.Errors, "Function.core should still work after load attempt")
	resp.Close()
	t.Log("Step 5: Function.core still OK after load attempt")

	// 6. Unload
	resp, err = service.Query(ctx, `mutation {
		function { core { unload_data_source(name: "fake_app") { success message } } }
	}`, nil)
	if resp != nil {
		t.Logf("Step 6 unload: errors=%v", resp.Errors)
		resp.Close()
	}

	// Small delay to let catalog compilation settle
	time.Sleep(500 * time.Millisecond)

	// 7. KEY TEST: verify Function.core STILL works after unload
	// This is where the bug manifests — "Cannot query field 'function' on type 'Query'"
	resp, err = service.Query(ctx,
		`{ function { core { embedder_settings { is_enabled } } } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("Step 7 after unload: errors=%v", resp.Errors)
	// SET BREAKPOINT HERE to inspect catalog state if this fails:
	require.Empty(t, resp.Errors,
		"BUG: Function.core lost after hugr-app source unload. "+
			"Set breakpoint in ModuleAssembler.ProcessAll() or memoryCatalog.removeCatalog()")
	resp.Close()
	t.Log("Step 7: Function.core OK after unload — bug NOT reproduced")

	// 8. Try to load again (will fail, but check if Function.core survives)
	resp, err = service.Query(ctx, `mutation {
		function { core { load_data_source(name: "fake_app") { success message } } }
	}`, nil)
	if resp != nil {
		t.Logf("Step 8 reload: errors=%v", resp.Errors)
		resp.Close()
	}

	// 9. Final check
	resp, err = service.Query(ctx,
		`{ function { core { embedder_settings { is_enabled } } } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("Step 9 final: errors=%v", resp.Errors)
	require.Empty(t, resp.Errors,
		"BUG: Function.core lost after second load attempt")
	resp.Close()
	t.Log("Step 9: Function.core OK — all good")
}
