package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/trace"
	"github.com/hugr-lab/query-engine/types"
)

// Coordinator manages cluster operations on the management node.
// It reads the node registry via GraphQL, broadcasts commands to workers
// via GraphQL/IPC, and manages heartbeat/ghost cleanup.
type Coordinator struct {
	config ClusterConfig
	qe     types.Querier
}

// NewCoordinator creates a new cluster coordinator for the management node.
func NewCoordinator(config ClusterConfig, qe types.Querier) *Coordinator {
	return &Coordinator{
		config: config,
		qe:     qe,
	}
}

func (c *Coordinator) newClient(url string, opts ...client.Option) *client.Client {
	baseOpts := []client.Option{
		client.WithApiKeyCustomHeader(c.config.Secret, "x-hugr-secret"),
		client.WithTimeout(c.config.Heartbeat),
	}
	baseOpts = append(baseOpts, opts...)
	return client.NewClient(url, baseOpts...)
}

// ActiveWorkers reads active worker nodes via GraphQL.
func (c *Coordinator) ActiveWorkers(ctx context.Context) ([]NodeInfo, error) {
	ctx = auth.ContextWithFullAccess(ctx)
	res, err := c.qe.Query(ctx, `query {
		core { cluster { nodes(filter: {role: {eq: "worker"}}) {
			name url role version
		}}}
	}`, nil)
	if err != nil {
		return nil, fmt.Errorf("active workers: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, fmt.Errorf("active workers: %w", res.Err())
	}

	var workers []NodeInfo
	if err := res.ScanData("core.cluster.nodes", &workers); err != nil {
		// No workers found is fine.
		return nil, nil
	}
	return workers, nil
}

// Broadcast sends a GraphQL mutation to all active workers in parallel.
// Returns per-node results.
func (c *Coordinator) Broadcast(ctx context.Context, query string, vars map[string]any) *ClusterResult {
	workers, err := c.ActiveWorkers(ctx)
	if err != nil {
		return &ClusterResult{
			Success: false,
			Results: []NodeResult{{Node: "coordinator", Error: err.Error()}},
		}
	}

	if len(workers) == 0 {
		return &ClusterResult{Success: true}
	}

	results := make([]NodeResult, len(workers))
	var wg sync.WaitGroup

	traceID := trace.TraceIDFromContext(ctx)
	for i, w := range workers {
		wg.Add(1)
		go func(idx int, node NodeInfo) {
			defer wg.Done()
			cl := c.newClient(node.URL, client.WithTraceID(traceID))
			res, err := cl.Query(ctx, query, vars)
			if err != nil {
				results[idx] = NodeResult{Node: node.Name, Error: err.Error()}
				return
			}
			defer res.Close()
			if res.Err() != nil {
				results[idx] = NodeResult{Node: node.Name, Error: res.Err().Error()}
				return
			}
			results[idx] = NodeResult{Node: node.Name, Success: true}
		}(i, w)
	}

	wg.Wait()

	allOk := true
	for _, r := range results {
		if !r.Success {
			allOk = false
			break
		}
	}

	return &ClusterResult{Success: allOk, Results: results}
}

// LoadSource compiles a source on management, increments schema version,
// and broadcasts handle_source_load to all workers.
func (c *Coordinator) LoadSource(ctx context.Context, name string) (*ClusterResult, error) {
	// 1. Compile locally (management has full engine).
	if err := c.qe.LoadDataSource(ctx, name); err != nil {
		return nil, fmt.Errorf("load source locally: %w", err)
	}

	// 2. Schema version already incremented by AddCatalog in catalog_manager.

	// 3. Broadcast to workers.
	result := c.Broadcast(ctx, `mutation($name: String!) {
		function { core { cluster {
			handle_source_load(name: $name) { success message }
		}}}
	}`, map[string]any{"name": name})

	slog.Info("cluster: load_source broadcast complete",
		"source", name, "success", result.Success, "workers", len(result.Results))

	return result, nil
}

// UnloadSource unloads a source on management, increments schema version,
// and broadcasts handle_source_unload to all workers.
func (c *Coordinator) UnloadSource(ctx context.Context, name string) (*ClusterResult, error) {
	if err := c.qe.UnloadDataSource(ctx, name); err != nil {
		return nil, fmt.Errorf("unload source locally: %w", err)
	}

	result := c.Broadcast(ctx, `mutation($name: String!) {
		function { core { cluster {
			handle_source_unload(name: $name) { success message }
		}}}
	}`, map[string]any{"name": name})

	slog.Info("cluster: unload_source broadcast complete",
		"source", name, "success", result.Success)

	return result, nil
}

// ReloadSource reloads a source on management and broadcasts cache invalidation.
func (c *Coordinator) ReloadSource(ctx context.Context, name string) (*ClusterResult, error) {
	// Unload + Load locally.
	_ = c.qe.UnloadDataSource(ctx, name)
	if err := c.qe.LoadDataSource(ctx, name); err != nil {
		return nil, fmt.Errorf("reload source locally: %w", err)
	}

	// Broadcast: workers should unload old + load new (handle_source_load does attach-only).
	result := c.Broadcast(ctx, `mutation($name: String!) {
		function { core { cluster {
			handle_source_unload(name: $name) { success message }
		}}}
	}`, map[string]any{"name": name})

	resultLoad := c.Broadcast(ctx, `mutation($name: String!) {
		function { core { cluster {
			handle_source_load(name: $name) { success message }
		}}}
	}`, map[string]any{"name": name})

	// Merge results.
	merged := &ClusterResult{Success: result.Success && resultLoad.Success}
	merged.Results = append(merged.Results, result.Results...)
	merged.Results = append(merged.Results, resultLoad.Results...)

	slog.Info("cluster: reload_source broadcast complete",
		"source", name, "success", merged.Success)

	return merged, nil
}

// RegisterStorage calls the storage module to register a secret locally,
// then broadcasts secret sync to all workers.
func (c *Coordinator) RegisterStorage(ctx context.Context, params StorageParams) (*ClusterResult, error) {
	ctx = auth.ContextWithFullAccess(ctx)
	res, err := c.qe.Query(ctx, `mutation(
		$type: String!, $name: String!, $scope: String!,
		$key: String!, $secret: String!, $region: String,
		$endpoint: String!, $use_ssl: Boolean!, $url_style: String!
	) {
		function { core { storage {
			register_object_storage(
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
	if err != nil {
		return nil, fmt.Errorf("register storage: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, fmt.Errorf("register storage: %w", res.Err())
	}

	// Broadcast secret sync to all workers.
	result := c.Broadcast(ctx, `mutation {
		function { core { cluster {
			handle_secret_sync { success message }
		}}}
	}`, nil)

	slog.Info("cluster: register_storage broadcast complete", "success", result.Success)
	return result, nil
}

// UnregisterStorage calls the storage module to drop a secret locally,
// then broadcasts secret sync to all workers.
func (c *Coordinator) UnregisterStorage(ctx context.Context, name string) (*ClusterResult, error) {
	ctx = auth.ContextWithFullAccess(ctx)
	res, err := c.qe.Query(ctx, `mutation($name: String!) {
		function { core { storage {
			unregister_storage(name: $name) { success message }
		}}}
	}`, map[string]any{"name": name})
	if err != nil {
		return nil, fmt.Errorf("unregister storage: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return nil, fmt.Errorf("unregister storage: %w", res.Err())
	}

	result := c.Broadcast(ctx, `mutation {
		function { core { cluster {
			handle_secret_sync { success message }
		}}}
	}`, nil)

	slog.Info("cluster: unregister_storage broadcast complete",
		"name", name, "success", result.Success)
	return result, nil
}
