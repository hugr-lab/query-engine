package cluster

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"

	catalogdb "github.com/hugr-lab/query-engine/pkg/catalog/db"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

//go:embed schema.graphql
var schema string

// Source is the cluster RuntimeSource.
// It exposes cluster queries and mutations via GraphQL.
// Role-aware: management executes operations locally + broadcasts;
// workers forward mutations to management.
type Source struct {
	config   ClusterConfig
	pool     *db.Pool
	qe       types.Querier
	provider *catalogdb.Provider

	// coordinator is non-nil only for management role.
	coordinator *Coordinator
	// worker is non-nil only for worker role.
	worker *WorkerClient

	// schemaVer tracks the last seen schema version for polling (worker only).
	schemaVer int64

	// registered is closed after the first successful node registration.
	registered chan struct{}
}

// NewSource creates a new cluster runtime source.
func NewSource(config ClusterConfig, qe types.Querier, provider *catalogdb.Provider) *Source {
	return &Source{
		config:     config,
		qe:         qe,
		provider:   provider,
		registered: make(chan struct{}),
	}
}

// WaitRegistered blocks until the node has registered itself in the cluster
// or the context is cancelled.
func (s *Source) WaitRegistered(ctx context.Context) error {
	select {
	case <-s.registered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*Source) Name() string           { return "core.cluster" }
func (*Source) Engine() engines.Engine { return engines.NewDuckDB() }
func (*Source) IsReadonly() bool       { return false }
func (*Source) AsModule() bool         { return true }

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.pool = pool

	// Node registration is deferred to the heartbeat goroutine because
	// the engine's planner is not yet initialized during Attach().

	// Initialize role-specific components.
	// Secret sync is deferred to heartbeatLoop (after first successful
	// registration) because the engine's planner is not yet initialized
	// during Attach().
	switch s.config.Role {
	case RoleManagement:
		s.coordinator = NewCoordinator(s.config, s.qe)
	case RoleWorker:
		s.worker = NewWorkerClient(s.config, s.qe, pool)
	}

	// Start heartbeat goroutine.
	go s.heartbeatLoop(ctx)

	// Workers: start schema version polling for missed broadcast recovery.
	if s.worker != nil {
		go s.pollSchemaVersion(ctx)
	}

	return s.registerUDFs(ctx, pool)
}

func (s *Source) Catalog(ctx context.Context) (cs.Catalog, error) {
	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		Prefix:       "core_cluster",
		ReadOnly:     s.IsReadonly(),
		AsModule:     s.AsModule(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return cs.NewStringSource(s.Name(), e, opts, schema)
}

// pollSchemaVersion periodically checks schema_version from CoreDB.
// If it changed since last check, invalidate caches and reconcile loaded sources.
func (s *Source) pollSchemaVersion(ctx context.Context) {
	interval := s.config.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ver, err := s.provider.GetSchemaVersion(ctx)
			if err != nil {
				slog.Warn("cluster worker: poll schema version failed", "error", err)
				continue
			}
			if ver != s.schemaVer {
				slog.Info("cluster worker: schema version changed",
					"old", s.schemaVer, "new", ver)
				s.schemaVer = ver
				s.provider.InvalidateAll()
				if err := s.reconcileLoadedSources(ctx); err != nil {
					slog.Warn("cluster worker: reconcile failed", "error", err)
				}
			}
		}
	}
}

// reconcileLoadedSources compares _schema_catalogs with locally attached sources
// and attaches/detaches as needed.
func (s *Source) reconcileLoadedSources(ctx context.Context) error {
	catalogs, err := s.provider.ListCatalogs(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: list catalogs: %w", err)
	}

	// Build set of expected active catalog names (not disabled, not suspended).
	expected := make(map[string]bool)
	for _, c := range catalogs {
		if !c.Disabled && !c.Suspended {
			expected[c.Name] = true
		}
	}

	// Check each expected source — load if not attached.
	for name := range expected {
		status, err := s.qe.DataSourceStatus(ctx, name)
		if err != nil || status != "attached" {
			if err := s.qe.LoadDataSource(ctx, name); err != nil {
				slog.Warn("cluster worker: reconcile load failed",
					"source", name, "error", err)
			} else {
				slog.Info("cluster worker: reconcile loaded source", "source", name)
			}
		}
	}

	return nil
}
