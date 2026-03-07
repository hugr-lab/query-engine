package cluster

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	dsruntime "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/types"
)

// registerUDFs registers all cluster UDFs on the DuckDB pool.
// Each @function(name: "X") in schema.graphql must have a corresponding UDF registered here.
func (s *Source) registerUDFs(ctx context.Context, pool *db.Pool) error {
	// --- Query functions ---

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionNoArgs[int64]{
		Name: "core_cluster_schema_version",
		Execute: func(ctx context.Context) (int64, error) {
			return s.schemaVersion(ctx)
		},
		ConvertOutput: func(out int64) (any, error) { return out, nil },
		OutputType:    dsruntime.DuckDBTypeInfoByNameMust("BIGINT"),
	}); err != nil {
		return fmt.Errorf("register schema_version UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionNoArgs[string]{
		Name: "core_cluster_my_role",
		Execute: func(ctx context.Context) (string, error) {
			return s.config.Role, nil
		},
		ConvertOutput: func(out string) (any, error) { return out, nil },
		OutputType:    dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"),
	}); err != nil {
		return fmt.Errorf("register my_role UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionNoArgs[string]{
		Name: "core_cluster_management_url",
		Execute: func(ctx context.Context) (string, error) {
			return s.managementURL(ctx)
		},
		ConvertOutput: func(out string) (any, error) { return out, nil },
		OutputType:    dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"),
	}); err != nil {
		return fmt.Errorf("register management_url UDF: %w", err)
	}

	// --- User-facing mutations ---

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_load_source",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.loadSource(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register load_source UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_unload_source",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.unloadSource(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register unload_source UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_reload_source",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.reloadSource(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register reload_source UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_invalidate_cache",
		Execute: func(ctx context.Context, catalog string) (*types.OperationResult, error) {
			return s.invalidateCache(ctx, catalog)
		},
		ConvertInput:          convertStringArg,
		ConvertOutput:         convertOperationResult,
		InputTypes:            []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:            types.DuckDBOperationResult(),
		IsSpecialNullHandling: true, // catalog arg is optional (nullable)
	}); err != nil {
		return fmt.Errorf("register invalidate_cache UDF: %w", err)
	}

	// --- Internal broadcast targets ---

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_handle_source_load",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.handleSourceLoad(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register handle_source_load UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_handle_source_unload",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.handleSourceUnload(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register handle_source_unload UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_handle_cache_invalidate",
		Execute: func(ctx context.Context, catalog string) (*types.OperationResult, error) {
			return s.handleCacheInvalidate(ctx, catalog)
		},
		ConvertInput:          convertStringArg,
		ConvertOutput:         convertOperationResult,
		InputTypes:            []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:            types.DuckDBOperationResult(),
		IsSpecialNullHandling: true, // catalog arg is optional (nullable)
	}); err != nil {
		return fmt.Errorf("register handle_cache_invalidate UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionNoArgs[*types.OperationResult]{
		Name: "core_cluster_handle_secret_sync",
		Execute: func(ctx context.Context) (*types.OperationResult, error) {
			return s.handleSecretSync(ctx)
		},
		ConvertOutput: convertOperationResult,
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register handle_secret_sync UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[StorageParams, *types.OperationResult]{
		Name: "core_cluster_register_storage",
		Execute: func(ctx context.Context, params StorageParams) (*types.OperationResult, error) {
			return s.registerStorage(ctx, params)
		},
		ConvertInput:  convertStorageParams,
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // type
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // name
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // scope
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // key
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // secret
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // region (nullable)
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // endpoint
			dsruntime.DuckDBTypeInfoByNameMust("BOOLEAN"), // use_ssl
			dsruntime.DuckDBTypeInfoByNameMust("VARCHAR"), // url_style
		},
		OutputType:            types.DuckDBOperationResult(),
		IsSpecialNullHandling: true, // region arg is nullable
	}); err != nil {
		return fmt.Errorf("register register_storage UDF: %w", err)
	}

	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "core_cluster_unregister_storage",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			return s.unregisterStorage(ctx, name)
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{dsruntime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return fmt.Errorf("register unregister_storage UDF: %w", err)
	}

	return nil
}

// --- UDF handlers ---

func (s *Source) schemaVersion(ctx context.Context) (int64, error) {
	return s.provider.GetSchemaVersion(ctx)
}

func (s *Source) managementURL(ctx context.Context) (string, error) {
	if s.config.IsManagement() {
		return s.config.NodeURL, nil
	}
	if s.worker != nil {
		return s.worker.ManagementURL(ctx)
	}
	return "", nil
}

// loadSource: management compiles + broadcasts; worker forwards to management.
func (s *Source) loadSource(ctx context.Context, name string) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result, err := s.coordinator.LoadSource(ctx, name)
		if err != nil {
			return types.ErrResult(err), nil
		}
		return resultToOperation("load_source", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation($name: String!) {
			function { core { cluster {
				load_source(name: $name) { success message }
			}}}
		}`, map[string]any{"name": name})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}

func (s *Source) unloadSource(ctx context.Context, name string) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result, err := s.coordinator.UnloadSource(ctx, name)
		if err != nil {
			return types.ErrResult(err), nil
		}
		return resultToOperation("unload_source", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation($name: String!) {
			function { core { cluster {
				unload_source(name: $name) { success message }
			}}}
		}`, map[string]any{"name": name})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}

func (s *Source) reloadSource(ctx context.Context, name string) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result, err := s.coordinator.ReloadSource(ctx, name)
		if err != nil {
			return types.ErrResult(err), nil
		}
		return resultToOperation("reload_source", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation($name: String!) {
			function { core { cluster {
				reload_source(name: $name) { success message }
			}}}
		}`, map[string]any{"name": name})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}

func (s *Source) invalidateCache(ctx context.Context, catalog string) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result := s.coordinator.Broadcast(ctx, `mutation($catalog: String) {
			function { core { cluster {
				handle_cache_invalidate(catalog: $catalog) { success message }
			}}}
		}`, map[string]any{"catalog": catalog})
		// Also invalidate locally on management.
		if catalog != "" {
			s.provider.InvalidateCatalog(catalog)
		} else {
			s.provider.InvalidateAll()
		}
		return resultToOperation("invalidate_cache", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation($catalog: String) {
			function { core { cluster {
				invalidate_cache(catalog: $catalog) { success message }
			}}}
		}`, map[string]any{"catalog": catalog})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}

// verifyClusterAuth checks that the request is authorized for internal cluster operations.
func (s *Source) verifyClusterAuth(ctx context.Context) error {
	if auth.IsFullAccess(ctx) {
		return nil
	}
	if s.config.Secret == "" {
		return nil
	}
	info := auth.AuthInfoFromContext(ctx)
	if info != nil && info.AuthProvider == "cluster-internal" {
		return nil
	}
	return fmt.Errorf("cluster secret required")
}

// --- Internal broadcast target handlers (called by management on workers) ---

func (s *Source) handleSourceLoad(ctx context.Context, name string) (*types.OperationResult, error) {
	if err := s.verifyClusterAuth(ctx); err != nil {
		return types.ErrResult(err), nil
	}
	if s.worker != nil {
		if err := s.worker.HandleSourceLoad(ctx, name); err != nil {
			return types.ErrResult(err), nil
		}
		s.provider.InvalidateAll()
		return types.Result("source loaded", 1, 0), nil
	}
	// Management can also handle — useful for self-broadcast scenarios.
	return types.Result("skipped (management)", 0, 0), nil
}

func (s *Source) handleSourceUnload(ctx context.Context, name string) (*types.OperationResult, error) {
	if err := s.verifyClusterAuth(ctx); err != nil {
		return types.ErrResult(err), nil
	}
	if s.worker != nil {
		if err := s.worker.HandleSourceUnload(ctx, name); err != nil {
			return types.ErrResult(err), nil
		}
		s.provider.InvalidateAll()
		return types.Result("source unloaded", 1, 0), nil
	}
	return types.Result("skipped (management)", 0, 0), nil
}

func (s *Source) handleCacheInvalidate(ctx context.Context, catalog string) (*types.OperationResult, error) {
	if err := s.verifyClusterAuth(ctx); err != nil {
		return types.ErrResult(err), nil
	}
	if catalog != "" {
		s.provider.InvalidateCatalog(catalog)
	} else {
		s.provider.InvalidateAll()
	}
	return types.Result("cache invalidated", 1, 0), nil
}

func (s *Source) handleSecretSync(ctx context.Context) (*types.OperationResult, error) {
	if err := s.verifyClusterAuth(ctx); err != nil {
		return types.ErrResult(err), nil
	}
	if s.worker != nil {
		if err := s.worker.SyncSecrets(ctx); err != nil {
			return types.ErrResult(err), nil
		}
		return types.Result("secrets synced", 1, 0), nil
	}
	return types.Result("skipped (management)", 0, 0), nil
}

// resultToOperation converts ClusterResult to OperationResult.
func resultToOperation(op string, r *ClusterResult) *types.OperationResult {
	if r.Success {
		return types.Result(fmt.Sprintf("%s: all workers ok", op), len(r.Results), 0)
	}
	var errs []string
	for _, nr := range r.Results {
		if !nr.Success {
			errs = append(errs, fmt.Sprintf("%s: %s", nr.Node, nr.Error))
		}
	}
	msg := fmt.Sprintf("%s: partial failure: %v", op, errs)
	return types.Result(msg, len(r.Results), len(errs))
}

// --- Converters ---

func convertStringArg(args []driver.Value) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return "", nil
	}
	s, ok := args[0].(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", args[0])
	}
	return s, nil
}

func convertOperationResult(out *types.OperationResult) (any, error) {
	return out.ToDuckdb(), nil
}

func convertStorageParams(args []driver.Value) (StorageParams, error) {
	if len(args) != 9 {
		return StorageParams{}, fmt.Errorf("expected 9 arguments, got %d", len(args))
	}
	getString := func(i int) (string, error) {
		if args[i] == nil {
			return "", nil
		}
		s, ok := args[i].(string)
		if !ok {
			return "", fmt.Errorf("arg %d: expected string, got %T", i, args[i])
		}
		return s, nil
	}
	var err error
	var p StorageParams
	if p.Type, err = getString(0); err != nil {
		return StorageParams{}, err
	}
	if p.Name, err = getString(1); err != nil {
		return StorageParams{}, err
	}
	if p.Scope, err = getString(2); err != nil {
		return StorageParams{}, err
	}
	if p.Key, err = getString(3); err != nil {
		return StorageParams{}, err
	}
	if p.Secret, err = getString(4); err != nil {
		return StorageParams{}, err
	}
	if p.Region, err = getString(5); err != nil {
		return StorageParams{}, err
	}
	if p.Endpoint, err = getString(6); err != nil {
		return StorageParams{}, err
	}
	if args[7] != nil {
		b, ok := args[7].(bool)
		if !ok {
			return StorageParams{}, fmt.Errorf("arg 7: expected bool, got %T", args[7])
		}
		p.UseSSL = b
	}
	if p.URLStyle, err = getString(8); err != nil {
		return StorageParams{}, err
	}
	return p, nil
}

// --- Storage handlers ---

// registerStorage: management calls storage module via Querier + broadcasts;
// worker forwards to management.
func (s *Source) registerStorage(ctx context.Context, params StorageParams) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result, err := s.coordinator.RegisterStorage(ctx, params)
		if err != nil {
			return types.ErrResult(err), nil
		}
		return resultToOperation("register_storage", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation(
			$type: String!, $name: String!, $scope: String!,
			$key: String!, $secret: String!, $region: String,
			$endpoint: String!, $use_ssl: Boolean!, $url_style: String!
		) {
			function { core { cluster {
				register_storage(
					type: $type, name: $name, scope: $scope,
					key: $key, secret: $secret, region: $region,
					endpoint: $endpoint, use_ssl: $use_ssl, url_style: $url_style
				) { success message }
			}}}
		}`, map[string]any{
			"type": params.Type, "name": params.Name, "scope": params.Scope,
			"key": params.Key, "secret": params.Secret, "region": params.Region,
			"endpoint": params.Endpoint, "use_ssl": params.UseSSL, "url_style": params.URLStyle,
		})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}

func (s *Source) unregisterStorage(ctx context.Context, name string) (*types.OperationResult, error) {
	if s.coordinator != nil {
		result, err := s.coordinator.UnregisterStorage(ctx, name)
		if err != nil {
			return types.ErrResult(err), nil
		}
		return resultToOperation("unregister_storage", result), nil
	}
	if s.worker != nil {
		return s.worker.ForwardToManagement(ctx, `mutation($name: String!) {
			function { core { cluster {
				unregister_storage(name: $name) { success message }
			}}}
		}`, map[string]any{"name": name})
	}
	return types.ErrResult(fmt.Errorf("cluster not initialized")), nil
}
