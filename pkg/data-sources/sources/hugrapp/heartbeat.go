package hugrapp

import (
	"context"
	"log/slog"
	"time"

	"github.com/hugr-lab/query-engine/pkg/db"
)

// HeartbeatConfig holds heartbeat monitoring settings.
type HeartbeatConfig struct {
	Interval   time.Duration // check interval (default: 30s)
	Timeout    time.Duration // per-check timeout (default: 10s)
	MaxRetries int           // failures before suspend (default: 3)
}

// DefaultHeartbeatConfig returns the default heartbeat configuration.
func DefaultHeartbeatConfig() HeartbeatConfig {
	return HeartbeatConfig{
		Interval:   30 * time.Second,
		Timeout:    10 * time.Second,
		MaxRetries: 3,
	}
}

// HeartbeatMonitor periodically checks if a hugr-app is reachable
// via _mount.info() and manages suspend/reactivate lifecycle.
type HeartbeatMonitor struct {
	source     *Source
	pool       *db.Pool
	config     HeartbeatConfig
	cancel     context.CancelFunc
	onSuspend  func(ctx context.Context, name string) error
	onRecover  func(ctx context.Context, name string, info *AppInfo) error
	failures   int
	suspended  bool
	lastInfo   *AppInfo
}

// NewHeartbeatMonitor creates a new heartbeat monitor for the given source.
// onSuspend is called when the app becomes unreachable (after maxRetries failures).
// onRecover is called when a suspended app becomes reachable again.
func NewHeartbeatMonitor(
	source *Source,
	pool *db.Pool,
	config HeartbeatConfig,
	onSuspend func(ctx context.Context, name string) error,
	onRecover func(ctx context.Context, name string, info *AppInfo) error,
) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		source:    source,
		pool:      pool,
		config:    config,
		onSuspend: onSuspend,
		onRecover: onRecover,
		lastInfo:  source.appInfo,
	}
}

// Start begins the heartbeat monitoring goroutine.
// Returns immediately. Call Stop() to cancel.
func (h *HeartbeatMonitor) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go h.run(ctx)
}

// Stop cancels the heartbeat monitoring goroutine.
func (h *HeartbeatMonitor) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
}

func (h *HeartbeatMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	name := h.source.Name()
	slog.Info("heartbeat monitor started", "app", name, "interval", h.config.Interval)

	for {
		select {
		case <-ctx.Done():
			slog.Info("heartbeat monitor stopped", "app", name)
			return
		case <-ticker.C:
			h.check(ctx, name)
		}
	}
}

func (h *HeartbeatMonitor) check(ctx context.Context, name string) {
	checkCtx, cancel := context.WithTimeout(ctx, h.config.Timeout)
	defer cancel()

	info, err := readMountInfo(checkCtx, h.pool, name)
	if err != nil {
		h.failures++
		slog.Warn("heartbeat failed", "app", name, "failures", h.failures, "max", h.config.MaxRetries, "error", err)

		if !h.suspended && h.failures >= h.config.MaxRetries {
			slog.Warn("suspending hugr-app", "app", name, "failures", h.failures)
			h.suspended = true
			if h.onSuspend != nil {
				if err := h.onSuspend(ctx, name); err != nil {
					slog.Error("suspend callback failed", "app", name, "error", err)
				}
			}
		}
		return
	}

	// Success — reset failures
	h.failures = 0

	if h.suspended {
		// Recovered from suspension
		slog.Info("hugr-app recovered", "app", name, "version", info.Version)
		h.suspended = false
		h.lastInfo = info
		if h.onRecover != nil {
			if err := h.onRecover(ctx, name, info); err != nil {
				slog.Error("recover callback failed", "app", name, "error", err)
			}
		}
		return
	}

	// Check for version change (hot-reload on restart)
	if h.lastInfo != nil && info.Version != h.lastInfo.Version {
		slog.Info("hugr-app version changed", "app", name, "old", h.lastInfo.Version, "new", info.Version)
		h.lastInfo = info
		if h.onRecover != nil {
			// Treat version change as recovery — re-fetch SDL, recompile
			if err := h.onRecover(ctx, name, info); err != nil {
				slog.Error("version change callback failed", "app", name, "error", err)
			}
		}
		return
	}

	h.lastInfo = info
}
