package hugrapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

var (
	_ sources.Source      = &Source{}
	_ sources.Provisioner = &Source{}
)

// AppInfo holds metadata returned by _mount.info() from a hugr-app source.
type AppInfo struct {
	Name            string
	Description     string
	Version         string
	URI             string
	IsDBInitializer bool
	IsDBMigrator    bool
}

// Source implements the hugr-app data source type.
// It connects to an external application via DuckDB Airport extension
// and manages its lifecycle (heartbeat, DB provisioning, schema mounting).
type Source struct {
	ds       types.DataSource
	engine   engines.Engine
	attached bool
	appInfo  *AppInfo
	pool     *db.Pool // stored during Attach for Provision/heartbeat use

	mu       sync.Mutex // protects hbCancel, hbDone
	hbCancel context.CancelFunc
	hbDone   chan struct{}
}

// New creates a new hugr-app source.
func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:       ds,
		engine:   engines.NewAirport(ds.Name),
		attached: attached,
	}, nil
}

// Name implements [sources.Source].
func (s *Source) Name() string { return s.ds.Name }

// Definition implements [sources.Source].
func (s *Source) Definition() types.DataSource { return s.ds }

// Engine implements [sources.Source].
func (s *Source) Engine() engines.Engine { return s.engine }

// IsAttached implements [sources.Source].
func (s *Source) IsAttached() bool { return s.attached }

// ReadOnly implements [sources.Source].
func (s *Source) ReadOnly() bool { return s.ds.ReadOnly }

// Info returns the cached app info read from _mount.info() after attach.
func (s *Source) Info() *AppInfo { return s.appInfo }

// StartHeartbeat starts periodic health monitoring.
// onSuspend is called when the app becomes unreachable.
// onRecover is called when a suspended app becomes reachable again.
func (s *Source) StartHeartbeat(
	config HeartbeatConfig,
	onSuspend func(ctx context.Context, name string) error,
	onRecover func(ctx context.Context, name string) error,
) {
	s.StopHeartbeat()
	s.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	s.hbCancel = cancel
	s.hbDone = make(chan struct{})
	s.mu.Unlock()
	go s.runHeartbeat(ctx, config, onSuspend, onRecover)
}

// StopHeartbeat stops the heartbeat monitor and waits for it to finish.
func (s *Source) StopHeartbeat() {
	s.mu.Lock()
	cancel := s.hbCancel
	done := s.hbDone
	s.hbCancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}

	s.mu.Lock()
	s.hbDone = nil
	s.mu.Unlock()
}

func (s *Source) runHeartbeat(
	ctx context.Context,
	config HeartbeatConfig,
	onSuspend func(ctx context.Context, name string) error,
	onRecover func(ctx context.Context, name string) error,
) {
	defer close(s.hbDone)
	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	name := s.ds.Name
	slog.Info("heartbeat started", "app", name, "interval", config.Interval)

	var failures int
	var suspended bool

	for {
		select {
		case <-ctx.Done():
			slog.Info("heartbeat stopped", "app", name)
			return
		case <-ticker.C:
		}

		checkCtx, cancel := context.WithTimeout(ctx, config.Timeout)
		info, err := readMountInfo(checkCtx, s.pool, name)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return // stopping, don't process
			}
			failures++
			slog.Warn("heartbeat failed", "app", name, "failures", failures, "max", config.MaxRetries, "error", err)
			if !suspended && failures >= config.MaxRetries {
				slog.Warn("suspending hugr-app", "app", name)
				suspended = true
				if err := onSuspend(context.Background(), name); err != nil {
					slog.Error("suspend callback failed", "app", name, "error", err)
				}
			}
			continue
		}

		failures = 0

		if suspended {
			slog.Info("hugr-app recovered", "app", name, "version", info.Version)
			suspended = false
			s.appInfo = info
			if err := onRecover(context.Background(), name); err != nil {
				slog.Error("recover callback failed", "app", name, "error", err)
			}
			continue
		}

		// Version change detection
		if s.appInfo != nil && info.Version != s.appInfo.Version {
			slog.Info("hugr-app version changed", "app", name, "old", s.appInfo.Version, "new", info.Version)
			s.appInfo = info
			if err := onRecover(context.Background(), name); err != nil {
				slog.Error("version change callback failed", "app", name, "error", err)
			}
			continue
		}

		s.appInfo = info
	}
}

// Attach implements [sources.Source].
func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	err := sources.CheckDBExists(ctx, pool, engines.Ident(s.ds.Name), sources.Airport)
	if err != nil {
		return err
	}

	path, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}
	if path.Proto != "grpc" && path.Proto != "grpc+tls" {
		return fmt.Errorf("invalid hugr-app DSN protocol %s: %w", path.Proto, sources.ErrInvalidDataSourcePath)
	}

	location := path.Proto + "://" + path.Host
	if path.Port != "" {
		location += ":" + path.Port
	}

	// secret_key in hugr-app maps to auth_token in Airport SECRET.
	token, ok := path.Params["secret_key"]
	if ok {
		name := strings.ReplaceAll(s.ds.Name, ".", "_")
		_, err = pool.Exec(ctx,
			fmt.Sprintf(
				"CREATE OR REPLACE SECRET _%s_secret (TYPE airport, auth_token '%s', SCOPE '%s');", name, token, location))
		if err != nil {
			return err
		}
	}

	// Pre-check: verify the app is reachable before DuckDB ATTACH.
	// DuckDB/gRPC caches DNS failures — a failed lookup poisons the cache
	// for the process lifetime, blocking subsequent ATTACH attempts even
	// after the app becomes available.
	dialAddr := path.Host
	if path.Port != "" {
		dialAddr += ":" + path.Port
	}
	conn, dialErr := net.DialTimeout("tcp", dialAddr, 5*time.Second)
	if dialErr != nil {
		return fmt.Errorf("hugr-app %s not reachable at %s: %w", s.ds.Name, dialAddr, dialErr)
	}
	conn.Close()

	// Attach the Airport source. DBName is empty for the main app catalog —
	// NamedCatalog on the server side returns the app name for routing.
	_, err = pool.Exec(ctx,
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE AIRPORT, LOCATION '%s');", path.DBName, engines.Ident(s.ds.Name), location))
	if err != nil {
		return err
	}

	s.attached = true
	s.pool = pool

	// Read _mount.info() from the attached source.
	info, err := readMountInfo(ctx, pool, s.ds.Name)
	if err != nil {
		// Detach on failure to avoid leaving a broken attach.
		_, _ = pool.Exec(ctx, fmt.Sprintf("DETACH %s;", engines.Ident(s.ds.Name)))
		s.attached = false
		return fmt.Errorf("hugr-app attach %s: failed to read mount info: %w", s.ds.Name, err)
	}
	s.appInfo = info

	return nil
}

// Detach implements [sources.Source].
func (s *Source) Detach(ctx context.Context, pool *db.Pool) error {
	_, err := pool.Exec(ctx,
		fmt.Sprintf("DETACH %s;", engines.Ident(s.ds.Name)))
	if err != nil {
		return err
	}
	s.attached = false
	s.appInfo = nil
	s.pool = nil
	return nil
}

// Provision implements [sources.Provisioner].
// Called by the data source service after Attach() succeeds.
// Reads _mount.data_sources, registers/initializes/migrates app databases.
// Template params (VectorSize, EmbedderName) are queried from system config via Querier.
func (s *Source) Provision(ctx context.Context, querier sources.Querier) error {
	if s.pool == nil || s.appInfo == nil {
		return nil
	}
	tmplParams, err := queryTemplateParams(ctx, querier)
	if err != nil {
		return fmt.Errorf("query template params: %w", err)
	}
	if err := ProvisionDataSources(ctx, s.pool, s, querier, tmplParams); err != nil {
		return err
	}
	// Notify the app that provisioning is complete.
	if err := callMountInit(ctx, s.pool, s.ds.Name); err != nil {
		slog.Warn("mount init failed (non-fatal)", "app", s.ds.Name, "error", err)
	}
	return nil
}

// queryTemplateParams fetches embedder settings via core.embedder_settings function.
func queryTemplateParams(ctx context.Context, querier sources.Querier) (TemplateParams, error) {
	params := TemplateParams{}
	resp, err := querier.Query(ctx, `{
		function { core { embedder_settings { is_enabled name model dimensions } } }
	}`, nil)
	if err != nil {
		return params, err
	}
	defer resp.Close()
	if len(resp.Errors) != 0 {
		return params, resp.Errors
	}
	var settings struct {
		Dimensions int    `json:"dimensions"`
		Name       string `json:"name"`
	}
	if err := resp.ScanData("function.core.embedder_settings", &settings); err != nil {
		return params, err
	}
	params.VectorSize = settings.Dimensions
	params.EmbedderName = settings.Name
	return params, nil
}
