package airport

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

// The DuckDB Airport data source integration.

var _ sources.Source = &Source{}

type Source struct {
	ds       types.DataSource
	engine   engines.Engine
	attached bool
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:       ds,
		engine:   engines.NewAirport(),
		attached: attached,
	}, nil
}

// Attach implements [sources.Source].
func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	err := sources.CheckDBExists(ctx, db, engines.Ident(s.ds.Name), sources.Postgres)
	if err != nil {
		return err
	}

	path, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}
	if path.Proto != "grpc" && path.Proto != "grpc+tls" {
		return fmt.Errorf("invalid airport DSN protocol %s: %w", path.Proto, sources.ErrInvalidDataSourcePath)
	}

	location := path.Proto + "://" + path.Host
	if path.Port != "" {
		location += ":" + path.Port
	}

	token, ok := path.Params["auth_token"]
	if ok {
		// create secret
		name := strings.ReplaceAll(s.ds.Name, ".", "_")
		_, err = db.Exec(ctx,
			fmt.Sprintf(
				"CREATE OR REPLACE SECRET _%s_secret (TYPE airport, auth_token '%s', SCOPE '%s');", name, token, location))
		if err != nil {
			return err
		}
	}

	// attach with secret
	_, err = db.Exec(ctx,
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE AIRPORT, LOCATION '%s');", path.DBName, engines.Ident(s.ds.Name), location))
	if err != nil {
		return err
	}

	s.attached = true
	return nil
}

// Definition implements [sources.Source].
func (s *Source) Definition() types.DataSource {
	return s.ds
}

// Detach implements [sources.Source].
func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx,
		fmt.Sprintf("DETACH %s;", engines.Ident(s.ds.Name)))
	if err != nil {
		return err
	}
	s.attached = false
	return nil
}

// Engine implements [sources.Source].
func (s *Source) Engine() engines.Engine {
	return s.engine
}

// IsAttached implements [sources.Source].
func (s *Source) IsAttached() bool {
	return s.attached
}

// Name implements [sources.Source].
func (s *Source) Name() string {
	return s.ds.Name
}

// ReadOnly implements [sources.Source].
func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}
