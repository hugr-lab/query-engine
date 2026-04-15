package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/trace"
	"github.com/hugr-lab/query-engine/types"
)

// WorkerClient handles cluster operations on the worker side:
// secret sync from management, mutation forwarding, and schema version polling.
type WorkerClient struct {
	config ClusterConfig
	pool   *db.Pool // kept for SyncSecrets (DuckDB-specific CREATE SECRET)
	qe     types.Querier

	mgmtURL  string // cached management URL
	mgmtOnce sync.Once
	mgmtErr  error
}

// NewWorkerClient creates a new worker client.
func NewWorkerClient(config ClusterConfig, qe types.Querier, pool *db.Pool) *WorkerClient {
	return &WorkerClient{
		config: config,
		pool:   pool,
		qe:     qe,
	}
}

func (w *WorkerClient) newClient(url string, opts ...client.Option) *client.Client {
	baseOpts := []client.Option{
		client.WithApiKeyCustomHeader(w.config.Secret, "x-hugr-secret"),
		client.WithTimeout(w.config.Heartbeat),
	}
	baseOpts = append(baseOpts, opts...)
	return client.NewClient(url, baseOpts...)
}

// ManagementURL discovers and caches the management node URL via GraphQL.
// Uses sync.Once to avoid races when called from multiple goroutines.
func (w *WorkerClient) ManagementURL(ctx context.Context) (string, error) {
	w.mgmtOnce.Do(func() {
		ctx := auth.ContextWithFullAccess(ctx)
		res, err := w.qe.Query(ctx, `query {
			core { cluster { nodes(filter: {role: {eq: "management"}}) {
				url
			}}}
		}`, nil)
		if err != nil {
			w.mgmtErr = fmt.Errorf("management url: %w", err)
			return
		}
		defer res.Close()
		if res.Err() != nil {
			w.mgmtErr = fmt.Errorf("management url: %w", res.Err())
			return
		}

		var nodes []struct {
			URL string `json:"url"`
		}
		if err := res.ScanData("core.cluster.nodes", &nodes); err != nil || len(nodes) == 0 {
			w.mgmtErr = fmt.Errorf("management url: no management node found")
			return
		}

		w.mgmtURL = nodes[0].URL
	})
	return w.mgmtURL, w.mgmtErr
}

// ForwardToManagement sends a GraphQL mutation to the management node
// and returns the OperationResult.
func (w *WorkerClient) ForwardToManagement(ctx context.Context, query string, vars map[string]any) (*types.OperationResult, error) {
	mgmtURL, err := w.ManagementURL(ctx)
	if err != nil {
		return types.ErrResult(fmt.Errorf("cannot find management node: %w", err)), nil
	}

	traceID := trace.TraceIDFromContext(ctx)
	c := w.newClient(mgmtURL, client.WithTraceID(traceID))
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
// Schema compilation is skipped because the datasources.Service has skipCatalogOps=true
// on worker nodes — schemas are managed by the management node.
func (w *WorkerClient) HandleSourceLoad(ctx context.Context, name string) error {
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
		core { meta { secrets { name type_name scope secret_string } } }
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
		Type         string   `json:"type_name"`
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
		if !isIdentifier(s.Name) {
			slog.Warn("sync secrets: invalid secret name", "name", s.Name)
			continue
		}
		if !isIdentifier(s.Type) {
			slog.Warn("sync secrets: invalid secret type", "name", s.Name, "type", s.Type)
			continue
		}

		params := parseSecretString(s.SecretString)

		ddl := fmt.Sprintf("CREATE OR REPLACE PERSISTENT SECRET %s (TYPE %s", s.Name, s.Type)
		if len(s.Scope) > 0 && s.Scope[0] != "" {
			ddl += fmt.Sprintf(", SCOPE '%s'", escapeSQLString(s.Scope[0]))
		}
		for k, v := range params {
			switch k {
			case "type", "provider", "name":
				continue
			}
			ddl += fmt.Sprintf(", %s '%s'", k, escapeSQLString(v))
		}
		ddl += ")"
		if _, err := conn.Exec(ctx, ddl); err != nil {
			slog.Warn("sync secrets: failed to create secret", "name", s.Name, "error", err)
			continue
		}
		slog.Info("cluster worker: secret synced", "name", s.Name, "type", s.Type)
	}

	return nil
}

var identifierRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func isIdentifier(s string) bool {
	return identifierRe.MatchString(s)
}

// parseSecretString splits a DuckDB secret_string like "key_id=abc;secret=xyz;region=us-east-1"
// into a map. Keys are lowercased.
func parseSecretString(s string) map[string]string {
	result := make(map[string]string)
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		result[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return result
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
