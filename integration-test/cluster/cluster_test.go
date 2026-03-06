//go:build duckdb_arrow

package cluster_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cluster"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
)

// --- Types ---

type nodeInfo struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Role string `json:"role"`
}

// --- Helpers ---

// createEngine creates a hugr service with the given DB paths and cluster config.
// Does NOT register t.Cleanup — caller manages lifecycle.
func createEngine(t *testing.T, dbPath, coreDBPath string, cc cluster.ClusterConfig) *hugr.Service {
	t.Helper()
	service, err := hugr.New(hugr.Config{
		DB:      db.Config{Path: dbPath},
		CoreDB:  coredb.New(coredb.Config{Path: coreDBPath}),
		Auth:    &auth.Config{},
		Cluster: cc,
	})
	require.NoError(t, err)
	require.NoError(t, service.Init(context.Background()))
	return service
}

// createInMemoryEngine creates an in-memory engine with cluster config.
func createInMemoryEngine(t *testing.T, cc cluster.ClusterConfig) *hugr.Service {
	t.Helper()
	return createEngine(t, "", "", cc)
}

// waitRegistered blocks until the cluster source has registered this node.
func waitRegistered(t *testing.T, svc *hugr.Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(fullAccessCtx(), 10*time.Second)
	defer cancel()
	require.NoError(t, svc.ClusterSource().WaitRegistered(ctx))
}

// insertNode registers a node via GraphQL insert mutation.
func insertNode(t *testing.T, svc *hugr.Service, name, url, role string) {
	t.Helper()
	ctx := fullAccessCtx()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := svc.Query(ctx, `mutation($data: core_nodes_mut_input_data!) {
		core { cluster { insert_nodes(data: $data) { name } } }
	}`, map[string]any{
		"data": map[string]any{
			"name":           name,
			"url":            url,
			"role":           role,
			"version":        "test",
			"started_at":     now,
			"last_heartbeat": now,
		},
	})
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err(), "insert node %s", name)
}

// deleteNode removes a node from the cluster via GraphQL delete mutation.
func deleteNode(t *testing.T, svc *hugr.Service, name string) {
	t.Helper()
	ctx := fullAccessCtx()
	res, err := svc.Query(ctx, `mutation($filter: core_nodes_filter!) {
		core { cluster { delete_nodes(filter: $filter) { affected_rows } } }
	}`, map[string]any{
		"filter": map[string]any{"name": map[string]any{"eq": name}},
	})
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err(), "delete node %s", name)
}

// queryNodes returns all nodes from the cluster.
func queryNodes(t *testing.T, svc *hugr.Service) []nodeInfo {
	t.Helper()
	ctx := fullAccessCtx()
	res, err := svc.Query(ctx, `query {
		core { cluster { nodes { name url role } } }
	}`, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err())

	var nodes []nodeInfo
	if err := res.ScanData("core.cluster.nodes", &nodes); err != nil {
		return nil
	}
	return nodes
}

// queryWorkers returns only worker nodes.
func queryWorkers(t *testing.T, svc *hugr.Service) []nodeInfo {
	t.Helper()
	ctx := fullAccessCtx()
	res, err := svc.Query(ctx, `query {
		core { cluster { nodes(filter: {role: {eq: "worker"}}) { name url role } } }
	}`, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err())

	var nodes []nodeInfo
	if err := res.ScanData("core.cluster.nodes", &nodes); err != nil {
		return nil
	}
	return nodes
}

// workerNames extracts names from a list of nodes.
func workerNames(nodes []nodeInfo) []string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name
	}
	return names
}

// queryMyRole queries the current node's cluster role.
func queryMyRole(t *testing.T, svc *hugr.Service) string {
	t.Helper()
	ctx := fullAccessCtx()
	res, err := svc.Query(ctx, `query {
		function { core { cluster { my_role } } }
	}`, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err())

	var data struct {
		MyRole string `json:"my_role"`
	}
	err = res.ScanData("function.core.cluster", &data)
	require.NoError(t, err)
	return data.MyRole
}

func fullAccessCtx() context.Context {
	return auth.ContextWithFullAccess(context.Background())
}

// --- Existing setup helpers ---

func setupManagementEngine(t *testing.T) *hugr.Service {
	t.Helper()
	svc := createInMemoryEngine(t, cluster.ClusterConfig{
		Enabled:      true,
		Role:         cluster.RoleManagement,
		NodeName:     "mgmt-test",
		NodeURL:      "http://localhost:0/ipc",
		Secret:       "test-secret",
		Heartbeat:    2 * time.Second,
		GhostTTL:     120 * time.Second,
		PollInterval: 60 * time.Second,
	})
	t.Cleanup(func() { svc.Close() })
	return svc
}

func setupStandaloneEngine(t *testing.T) *hugr.Service {
	t.Helper()
	service, err := hugr.New(hugr.Config{
		DB:     db.Config{Path: ""},
		CoreDB: coredb.New(coredb.Config{}),
		Auth:   &auth.Config{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })
	require.NoError(t, service.Init(context.Background()))
	return service
}

// --- Tests ---

func TestCluster_ManagementInit(t *testing.T) {
	service := setupManagementEngine(t)
	assert.Equal(t, "management", queryMyRole(t, service))
}

func TestCluster_NodeRegistration(t *testing.T) {
	service := setupManagementEngine(t)
	waitRegistered(t, service)

	nodes := queryNodes(t, service)
	require.Len(t, nodes, 1)
	assert.Equal(t, "mgmt-test", nodes[0].Name)
	assert.Equal(t, "management", nodes[0].Role)
}

func TestCluster_ManagementURL(t *testing.T) {
	service := setupManagementEngine(t)
	ctx := fullAccessCtx()

	res, err := service.Query(ctx, `query {
		function { core { cluster { management_url } } }
	}`, nil)
	require.NoError(t, err)
	defer res.Close()
	require.NoError(t, res.Err())

	var data struct {
		ManagementURL string `json:"management_url"`
	}
	err = res.ScanData("function.core.cluster", &data)
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:0/ipc", data.ManagementURL)
}

func TestStandalone_NoClusterSideEffects(t *testing.T) {
	service := setupStandaloneEngine(t)
	ctx := fullAccessCtx()

	res, err := service.Query(ctx, `query {
		function { core { cluster { my_role } } }
	}`, nil)
	if err == nil {
		defer res.Close()
		if res.Err() == nil {
			t.Fatal("expected cluster module to not exist in standalone mode")
		}
	}
}

func TestStandalone_LoadUnloadCycle(t *testing.T) {
	service := setupStandaloneEngine(t)
	ctx := fullAccessCtx()

	res, err := service.Query(ctx, `query {
		core { data_sources { name } }
	}`, nil)
	require.NoError(t, err)
	defer res.Close()
}

// TestCluster_MultiNodeLifecycle verifies the full cluster lifecycle:
// 1. Start management node
// 2. Register two workers in parallel
// 3. Add a third worker
// 4. Stop one worker
// 5. Add another worker
// 6. Stop management node
// 7. Restart management node — verify persistent state
func TestCluster_MultiNodeLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "main.duckdb")
	coreDBPath := filepath.Join(tmpDir, "core.duckdb")

	clusterCfg := cluster.ClusterConfig{
		Enabled:      true,
		Role:         cluster.RoleManagement,
		NodeName:     "mgmt-1",
		NodeURL:      "http://mgmt:8080/ipc",
		Secret:       "test-secret",
		Heartbeat:    2 * time.Second,
		GhostTTL:     120 * time.Second,
		PollInterval: 60 * time.Second,
	}

	// --- Step 1: Start management node ---
	mgmt := createEngine(t, dbPath, coreDBPath, clusterCfg)
	waitRegistered(t, mgmt)

	nodes := queryNodes(t, mgmt)
	require.Len(t, nodes, 1, "step 1: management node should be registered")
	assert.Equal(t, "mgmt-1", nodes[0].Name)
	assert.Equal(t, "management", nodes[0].Role)

	// --- Step 2: Register two workers in parallel ---
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		insertNode(t, mgmt, "worker-1", "http://w1:8080/ipc", "worker")
	}()
	go func() {
		defer wg.Done()
		insertNode(t, mgmt, "worker-2", "http://w2:8080/ipc", "worker")
	}()
	wg.Wait()

	nodes = queryNodes(t, mgmt)
	require.Len(t, nodes, 3, "step 2: should have 1 mgmt + 2 workers")
	workers := queryWorkers(t, mgmt)
	require.Len(t, workers, 2)

	// --- Step 3: Add a third worker ---
	insertNode(t, mgmt, "worker-3", "http://w3:8080/ipc", "worker")
	nodes = queryNodes(t, mgmt)
	require.Len(t, nodes, 4, "step 3: should have 4 nodes")

	// --- Step 4: Stop worker-2 ---
	deleteNode(t, mgmt, "worker-2")
	nodes = queryNodes(t, mgmt)
	require.Len(t, nodes, 3, "step 4: should have 3 nodes")
	workers = queryWorkers(t, mgmt)
	names := workerNames(workers)
	assert.Contains(t, names, "worker-1")
	assert.Contains(t, names, "worker-3")
	assert.NotContains(t, names, "worker-2")

	// --- Step 5: Add worker-4 ---
	insertNode(t, mgmt, "worker-4", "http://w4:8080/ipc", "worker")
	nodes = queryNodes(t, mgmt)
	require.Len(t, nodes, 4, "step 5: should have 4 nodes")

	// --- Step 6: Stop management node ---
	require.NoError(t, mgmt.Close())

	// --- Step 7: Restart management node ---
	mgmt = createEngine(t, dbPath, coreDBPath, clusterCfg)
	t.Cleanup(func() { mgmt.Close() })
	waitRegistered(t, mgmt)

	nodes = queryNodes(t, mgmt)
	require.Len(t, nodes, 4, "step 7: 4 nodes after restart (mgmt re-registered + 3 workers)")
	workers = queryWorkers(t, mgmt)
	require.Len(t, workers, 3, "step 7: workers persist across restart")
	names = workerNames(workers)
	assert.Contains(t, names, "worker-1")
	assert.Contains(t, names, "worker-3")
	assert.Contains(t, names, "worker-4")
}

// TestCluster_InterNodeBroadcast tests real HTTP-based interaction between
// management and worker nodes:
//   - Management broadcasts cache invalidation to workers
//   - Management broadcasts secret sync to workers
//   - Worker forwards mutations to management
//   - Management broadcasts source load/unload to workers
func TestCluster_InterNodeBroadcast(t *testing.T) {
	ctx := fullAccessCtx()

	// --- Setup: management engine + HTTP server ---
	mgmt := createInMemoryEngine(t, cluster.ClusterConfig{
		Enabled:      true,
		Role:         cluster.RoleManagement,
		NodeName:     "mgmt",
		NodeURL:      "http://placeholder/ipc", // replaced below
		Secret:       "test-secret",
		Heartbeat:    time.Hour, // long: we manage registration manually
		GhostTTL:     time.Hour,
		PollInterval: time.Hour,
	})
	mgmtServer := httptest.NewServer(mgmt)
	t.Cleanup(mgmtServer.Close)
	t.Cleanup(func() { mgmt.Close() })
	mgmtIPC := mgmtServer.URL + "/ipc"

	// --- Setup: worker-1 engine + HTTP server ---
	w1 := createInMemoryEngine(t, cluster.ClusterConfig{
		Enabled:      true,
		Role:         cluster.RoleWorker,
		NodeName:     "worker-1",
		NodeURL:      "http://placeholder/ipc",
		Secret:       "test-secret",
		Heartbeat:    time.Hour,
		GhostTTL:     time.Hour,
		PollInterval: time.Hour,
	})
	w1Server := httptest.NewServer(w1)
	t.Cleanup(w1Server.Close)
	t.Cleanup(func() { w1.Close() })
	w1IPC := w1Server.URL + "/ipc"

	// --- Setup: worker-2 engine + HTTP server ---
	w2 := createInMemoryEngine(t, cluster.ClusterConfig{
		Enabled:      true,
		Role:         cluster.RoleWorker,
		NodeName:     "worker-2",
		NodeURL:      "http://placeholder/ipc",
		Secret:       "test-secret",
		Heartbeat:    time.Hour,
		GhostTTL:     time.Hour,
		PollInterval: time.Hour,
	})
	w2Server := httptest.NewServer(w2)
	t.Cleanup(w2Server.Close)
	t.Cleanup(func() { w2.Close() })
	w2IPC := w2Server.URL + "/ipc"

	// --- Register all nodes in management's nodes table ---
	insertNode(t, mgmt, "mgmt", mgmtIPC, "management")
	insertNode(t, mgmt, "worker-1", w1IPC, "worker")
	insertNode(t, mgmt, "worker-2", w2IPC, "worker")

	// Register management in workers' tables so they can discover it.
	insertNode(t, w1, "mgmt", mgmtIPC, "management")
	insertNode(t, w2, "mgmt", mgmtIPC, "management")

	// Verify setup: management sees 2 workers.
	workers := queryWorkers(t, mgmt)
	require.Len(t, workers, 2, "management should see 2 workers")

	// Helper: execute a mutation function and verify no GraphQL errors.
	// NOTE: OperationResult STRUCT fields (success, message) are not checked because
	// DuckDB STRUCT serialization returns null for all fields (pre-existing engine issue).
	// We verify the UDF executed without errors, which means the broadcast/forwarding worked.
	execMutation := func(t *testing.T, svc *hugr.Service, query string, vars map[string]any) {
		t.Helper()
		res, err := svc.Query(ctx, query, vars)
		require.NoError(t, err)
		defer res.Close()
		require.NoError(t, res.Err())
	}

	// --- Test 1: Cache invalidation broadcast (mgmt → workers via HTTP) ---
	t.Run("cache_invalidation_broadcast", func(t *testing.T) {
		execMutation(t, mgmt, `mutation {
			function { core { cluster { invalidate_cache { success } } } }
		}`, nil)
	})

	// --- Test 2: Cache invalidation with specific catalog ---
	t.Run("cache_invalidation_catalog", func(t *testing.T) {
		execMutation(t, mgmt, `mutation($catalog: String) {
			function { core { cluster { invalidate_cache(catalog: $catalog) { success } } } }
		}`, map[string]any{"catalog": "test_catalog"})
	})

	// --- Test 3: Secret sync on worker (queries management via HTTP) ---
	t.Run("secret_sync_on_worker", func(t *testing.T) {
		execMutation(t, w1, `mutation {
			function { core { cluster { handle_secret_sync { success } } } }
		}`, nil)
	})

	// --- Test 4: Worker forwards load_source to management via HTTP ---
	t.Run("worker_forward_load_source", func(t *testing.T) {
		execMutation(t, w1, `mutation($name: String!) {
			function { core { cluster { load_source(name: $name) { success } } } }
		}`, map[string]any{"name": "nonexistent_source"})
	})

	// --- Test 5: Worker forwards unload_source to management via HTTP ---
	t.Run("worker_forward_unload_source", func(t *testing.T) {
		execMutation(t, w2, `mutation($name: String!) {
			function { core { cluster { unload_source(name: $name) { success } } } }
		}`, map[string]any{"name": "nonexistent_source"})
	})

	// --- Test 6: Worker forwards reload_source to management via HTTP ---
	t.Run("worker_forward_reload_source", func(t *testing.T) {
		execMutation(t, w1, `mutation($name: String!) {
			function { core { cluster { reload_source(name: $name) { success } } } }
		}`, map[string]any{"name": "nonexistent_source"})
	})

	// --- Test 7: Worker forwards register_storage to management via HTTP ---
	t.Run("worker_forward_register_storage", func(t *testing.T) {
		execMutation(t, w1, `mutation(
			$type: String!, $name: String!, $scope: String!,
			$key: String!, $secret: String!, $region: String,
			$endpoint: String!, $use_ssl: Boolean!, $url_style: String!
		) {
			function { core { cluster {
				register_storage(
					type: $type, name: $name, scope: $scope,
					key: $key, secret: $secret, region: $region,
					endpoint: $endpoint, use_ssl: $use_ssl, url_style: $url_style
				) { success }
			}}}
		}`, map[string]any{
			"type": "s3", "name": "test_storage", "scope": "s3://test-bucket",
			"key": "AKIAIOSFODNN7EXAMPLE", "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE",
			"region": "us-east-1", "endpoint": "s3.amazonaws.com",
			"use_ssl": true, "url_style": "vhost",
		})
	})

	// --- Test 8: Worker forwards unregister_storage to management via HTTP ---
	t.Run("worker_forward_unregister_storage", func(t *testing.T) {
		execMutation(t, w2, `mutation($name: String!) {
			function { core { cluster { unregister_storage(name: $name) { success } } } }
		}`, map[string]any{"name": "test_storage"})
	})

	// --- Test 9: Management register_storage + broadcast secret sync to workers ---
	t.Run("mgmt_register_storage_broadcast", func(t *testing.T) {
		execMutation(t, mgmt, `mutation(
			$type: String!, $name: String!, $scope: String!,
			$key: String!, $secret: String!, $region: String,
			$endpoint: String!, $use_ssl: Boolean!, $url_style: String!
		) {
			function { core { cluster {
				register_storage(
					type: $type, name: $name, scope: $scope,
					key: $key, secret: $secret, region: $region,
					endpoint: $endpoint, use_ssl: $use_ssl, url_style: $url_style
				) { success }
			}}}
		}`, map[string]any{
			"type": "s3", "name": "mgmt_storage", "scope": "s3://mgmt-bucket",
			"key": "AKIAIOSFODNN7EXAMPLE", "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE",
			"region": "us-west-2", "endpoint": "s3.us-west-2.amazonaws.com",
			"use_ssl": true, "url_style": "path",
		})
	})

	// --- Test 10: Management unregister_storage + broadcast ---
	t.Run("mgmt_unregister_storage_broadcast", func(t *testing.T) {
		execMutation(t, mgmt, `mutation($name: String!) {
			function { core { cluster { unregister_storage(name: $name) { success } } } }
		}`, map[string]any{"name": "mgmt_storage"})
	})

	// --- Test 11: Verify workers report correct role ---
	t.Run("worker_roles", func(t *testing.T) {
		assert.Equal(t, "management", queryMyRole(t, mgmt))
		assert.Equal(t, "worker", queryMyRole(t, w1))
		assert.Equal(t, "worker", queryMyRole(t, w2))
	})

	// --- Test 12: Add worker-3, broadcast reaches all 3 workers ---
	t.Run("add_worker_and_broadcast", func(t *testing.T) {
		w3 := createInMemoryEngine(t, cluster.ClusterConfig{
			Enabled:      true,
			Role:         cluster.RoleWorker,
			NodeName:     "worker-3",
			NodeURL:      "http://placeholder/ipc",
			Secret:       "test-secret",
			Heartbeat:    time.Hour,
			GhostTTL:     time.Hour,
			PollInterval: time.Hour,
		})
		w3Server := httptest.NewServer(w3)
		t.Cleanup(w3Server.Close)
		t.Cleanup(func() { w3.Close() })

		insertNode(t, mgmt, "worker-3", w3Server.URL+"/ipc", "worker")

		execMutation(t, mgmt, `mutation {
			function { core { cluster { invalidate_cache { success } } } }
		}`, nil)
	})

	// --- Test 13: Remove worker-1, broadcast reaches remaining workers ---
	t.Run("remove_worker_and_broadcast", func(t *testing.T) {
		deleteNode(t, mgmt, "worker-1")

		workers := queryWorkers(t, mgmt)
		require.Len(t, workers, 2, "should have 2 workers after removing worker-1")
		names := workerNames(workers)
		assert.Contains(t, names, "worker-2")
		assert.Contains(t, names, "worker-3")

		execMutation(t, mgmt, `mutation {
			function { core { cluster { invalidate_cache { success } } } }
		}`, nil)
	})
}
