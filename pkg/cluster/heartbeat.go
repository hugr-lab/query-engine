package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

// registerNode registers this node in _cluster_nodes via GraphQL.
// Uses delete + insert (no upsert mutation available).
func (s *Source) registerNode(ctx context.Context) error {
	ctx = auth.ContextWithFullAccess(ctx)
	version := buildVersion()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Delete existing node entry (idempotent).
	res, err := s.qe.Query(ctx, `mutation($filter: core_nodes_filter!) {
		core { cluster { delete_nodes(filter: $filter) { affected_rows } } }
	}`, map[string]any{
		"filter": map[string]any{"name": map[string]any{"eq": s.config.NodeName}},
	})
	if err != nil {
		return fmt.Errorf("register node delete: %w", err)
	}
	res.Close()

	// Insert new node entry.
	res, err = s.qe.Query(ctx, `mutation($data: core_nodes_mut_input_data!) {
		core { cluster { insert_nodes(data: $data) { name } } }
	}`, map[string]any{
		"data": map[string]any{
			"name":           s.config.NodeName,
			"url":            s.config.NodeURL,
			"role":           s.config.Role,
			"version":        version,
			"started_at":     now,
			"last_heartbeat": now,
		},
	})
	if err != nil {
		return fmt.Errorf("register node insert: %w", err)
	}
	defer res.Close()
	if res.Err() != nil {
		return fmt.Errorf("register node: %w", res.Err())
	}

	slog.Info("cluster: node registered",
		"name", s.config.NodeName,
		"role", s.config.Role,
		"url", s.config.NodeURL,
	)
	return nil
}

// heartbeatLoop periodically updates last_heartbeat for this node
// and cleans up ghost nodes (management only).
// Also handles initial node registration (deferred from Attach because
// the engine's planner is not yet initialized during Attach).
func (s *Source) heartbeatLoop(ctx context.Context) {
	interval := s.config.Heartbeat
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	registered := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !registered {
				if err := s.registerNode(ctx); err != nil {
					slog.Warn("cluster: register node failed (will retry)", "error", err)
					continue
				}
				registered = true
				close(s.registered)
			}
			if err := s.sendHeartbeat(ctx); err != nil {
				slog.Warn("cluster: heartbeat failed", "error", err)
			}
			// Management node cleans up ghost nodes.
			if s.coordinator != nil {
				if err := s.cleanupGhosts(ctx); err != nil {
					slog.Warn("cluster: ghost cleanup failed", "error", err)
				}
			}
		}
	}
}

// sendHeartbeat updates last_heartbeat for this node via GraphQL.
func (s *Source) sendHeartbeat(ctx context.Context) error {
	ctx = auth.ContextWithFullAccess(ctx)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	res, err := s.qe.Query(ctx, `mutation($filter: core_nodes_filter!, $data: core_nodes_mut_data!) {
		core { cluster { update_nodes(filter: $filter, data: $data) { affected_rows } } }
	}`, map[string]any{
		"filter": map[string]any{"name": map[string]any{"eq": s.config.NodeName}},
		"data":   map[string]any{"last_heartbeat": now},
	})
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	res.Close()
	return nil
}

// cleanupGhosts removes nodes whose last_heartbeat is older than GhostTTL.
// Only run by management node.
func (s *Source) cleanupGhosts(ctx context.Context) error {
	ttl := s.config.GhostTTL
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}

	ctx = auth.ContextWithFullAccess(ctx)
	cutoff := time.Now().UTC().Add(-ttl).Format(time.RFC3339Nano)

	res, err := s.qe.Query(ctx, `mutation($filter: core_nodes_filter!) {
		core { cluster { delete_nodes(filter: $filter) { affected_rows } } }
	}`, map[string]any{
		"filter": map[string]any{
			"last_heartbeat": map[string]any{"lt": cutoff},
			"name":           map[string]any{"neq": s.config.NodeName},
		},
	})
	if err != nil {
		return fmt.Errorf("ghost cleanup: %w", err)
	}
	defer res.Close()

	return nil
}

// buildVersion returns the module version from Go build info, or "dev".
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}
