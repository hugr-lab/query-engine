package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
)

// WorkerClient handles cluster operations on the worker side:
// secret sync from management, mutation forwarding, and schema version polling.
type WorkerClient struct {
	config ClusterConfig
	pool   *db.Pool // kept for SyncSecrets (DuckDB-specific CREATE SECRET)
	qe     types.Querier

	mgmtURL string // cached management URL
}

// NewWorkerClient creates a new worker client.
func NewWorkerClient(config ClusterConfig, qe types.Querier, pool *db.Pool) *WorkerClient {
	return &WorkerClient{
		config: config,
		pool:   pool,
		qe:     qe,
	}
}

func (w *WorkerClient) newClient(url string) *client.Client {
	return client.NewClient(url,
		client.WithApiKeyCustomHeader(w.config.Secret, "x-hugr-secret"),
		client.WithTimeout(w.config.Heartbeat),
	)
}

// ManagementURL discovers and caches the management node URL via GraphQL.
func (w *WorkerClient) ManagementURL(ctx context.Context) (string, error) {
	if w.mgmtURL != "" {
		return w.mgmtURL, nil
	}

	ctx = auth.ContextWithFullAccess(ctx)
	res, err := w.qe.Query(ctx, `query {
		core { cluster { nodes(filter: {role: {eq: "management"}}) {
			url
		}}}
	}`, nil)
	if err != nil {
		return "", fmt.Errorf("management url: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return "", fmt.Errorf("management url: %w", res.Err())
	}

	var nodes []struct {
		URL string `json:"url"`
	}
	if err := res.ScanData("core.cluster.nodes", &nodes); err != nil || len(nodes) == 0 {
		return "", fmt.Errorf("management url: no management node found")
	}

	w.mgmtURL = nodes[0].URL
	return w.mgmtURL, nil
}

// ForwardToManagement sends a GraphQL mutation to the management node
// and returns the OperationResult.
func (w *WorkerClient) ForwardToManagement(ctx context.Context, query string, vars map[string]any) (*types.OperationResult, error) {
	mgmtURL, err := w.ManagementURL(ctx)
	if err != nil {
		return types.ErrResult(fmt.Errorf("cannot find management node: %w", err)), nil
	}

	c := w.newClient(mgmtURL)
	res, err := c.Query(ctx, query, vars)
	if err != nil {
		return types.ErrResult(fmt.Errorf("forward to management: %w", err)), nil
	}
	defer res.Close()
	if res.Err() != nil {
		return types.ErrResult(fmt.Errorf("management error: %w", res.Err())), nil
	}

	return types.Result("forwarded to management", 1, 0), nil
}

// HandleSourceLoad attaches a data source without compilation (broadcast target).
func (w *WorkerClient) HandleSourceLoad(ctx context.Context, name string) error {
	ctx = ContextWithClusterBroadcast(ctx)
	if err := w.qe.LoadDataSource(ctx, name); err != nil {
		return fmt.Errorf("handle source load %q: %w", name, err)
	}
	slog.Info("cluster worker: source loaded", "source", name)
	return nil
}

// HandleSourceUnload detaches a data source (broadcast target).
func (w *WorkerClient) HandleSourceUnload(ctx context.Context, name string) error {
	if err := w.qe.UnloadDataSource(ctx, name); err != nil {
		return fmt.Errorf("handle source unload %q: %w", name, err)
	}
	slog.Info("cluster worker: source unloaded", "source", name)
	return nil
}

// SyncSecrets queries management's duckdb_secrets via GraphQL and creates
// them locally in this node's DuckDB using CREATE OR REPLACE PERSISTENT SECRET.
func (w *WorkerClient) SyncSecrets(ctx context.Context) error {
	mgmtURL, err := w.ManagementURL(ctx)
	if err != nil {
		return fmt.Errorf("sync secrets: %w", err)
	}

	c := w.newClient(mgmtURL)
	res, err := c.Query(ctx, `query {
		core { meta { secrets { name type scope secret_string } } }
	}`, nil)
	if err != nil {
		return fmt.Errorf("sync secrets query: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return fmt.Errorf("sync secrets error: %w", res.Err())
	}

	var secrets []struct {
		Name         string   `json:"name"`
		Type         string   `json:"type"`
		Scope        []string `json:"scope"`
		SecretString string   `json:"secret_string"`
	}
	if err := res.ScanData("core.meta.secrets", &secrets); err != nil {
		slog.Debug("sync secrets: no secrets found or scan error", "error", err)
		return nil
	}

	conn, err := w.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sync secrets conn: %w", err)
	}
	defer conn.Close()

	for _, s := range secrets {
		// secret_string is the full parameter string from duckdb_secrets()
		sql := fmt.Sprintf("CREATE OR REPLACE PERSISTENT SECRET %s (TYPE %s", s.Name, s.Type)
		if len(s.Scope) > 0 && s.Scope[0] != "" {
			sql += fmt.Sprintf(", SCOPE '%s'", s.Scope[0])
		}
		sql += ")"
		if _, err := conn.Exec(ctx, sql); err != nil {
			slog.Warn("sync secrets: failed to create secret", "name", s.Name, "error", err)
			continue
		}
		slog.Info("cluster worker: secret synced", "name", s.Name, "type", s.Type)
	}

	return nil
}
