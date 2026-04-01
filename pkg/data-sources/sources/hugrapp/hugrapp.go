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
		engine:   engines.NewDuckDB(),
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

	// Attach the Airport source.
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
	tmplParams := queryTemplateParams(ctx, querier)
	return ProvisionDataSources(ctx, s.pool, s, querier, tmplParams)
}

// queryTemplateParams fetches system-level template parameters via GraphQL.
func queryTemplateParams(ctx context.Context, querier sources.Querier) TemplateParams {
	params := TemplateParams{}
	resp, err := querier.Query(ctx, `{ function { core { info { embedder_vector_size embedder_name } } } }`, nil)
	if err != nil || resp == nil {
		return params
	}
	// Best-effort extraction — if fields don't exist, defaults are fine
	if data, ok := resp.Data["data"].(map[string]any); ok {
		if fn, ok := data["function"].(map[string]any); ok {
			if core, ok := fn["core"].(map[string]any); ok {
				if info, ok := core["info"].(map[string]any); ok {
					if vs, ok := info["embedder_vector_size"].(float64); ok {
						params.VectorSize = int(vs)
					}
					if en, ok := info["embedder_name"].(string); ok {
						params.EmbedderName = en
					}
				}
			}
		}
	}
	return params
}
