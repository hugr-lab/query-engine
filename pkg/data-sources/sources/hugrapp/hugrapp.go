package hugrapp

import (
	"context"
	"fmt"
	"strings"

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
	version  string
	appInfo  *AppInfo
	pool     *db.Pool // stored during Attach for Provision/heartbeat use
}

// New creates a new hugr-app source.
func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:       ds,
		engine:   engines.NewAirport(),
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

// Attach implements [sources.Source].
func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	err := sources.CheckDBExists(ctx, pool, engines.Ident(s.ds.Name), sources.Postgres)
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

	// Store informational version from DSN params.
	if v, ok := path.Params["version"]; ok {
		s.version = v
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
	return ProvisionDataSources(ctx, s.pool, s, querier, tmplParams)
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
