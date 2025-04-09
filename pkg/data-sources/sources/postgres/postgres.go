package postgres

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Source struct {
	ds         types.DataSource
	isAttached bool

	engine engines.Engine
	c      *catalogs.Catalog
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewPostgres(),
	}, nil
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (*Source) EngineType() engines.Type {
	return engines.TypePostgres
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Catalog() *catalogs.Catalog {
	return s.c
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	// check if db is attached
	err := sources.CheckDBExists(ctx, db, s.ds.Prefix, sources.Postgres)
	if err != nil {
		return err
	}

	if s.ds.Path != "" { // add secret if dsn is provided
		dsn, err := sources.ParseDSN(s.ds.Path)
		if err != nil {
			return err
		}
		_, err = db.Exec(ctx, fmt.Sprintf(`
			CREATE OR REPLACE PERSISTENT SECRET %s_secret (
				TYPE POSTGRES,
				HOST '%s',
				PORT '%s',
				USER '%s',
				PASSWORD '%s',
				DATABASE '%s'
			); 
		`, s.ds.Name, dsn.Host, dsn.Port, dsn.User, dsn.Password, dsn.DBName,
		))
		if err != nil {
			return err
		}
	}

	_, err = db.Exec(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE POSTGRES, SECRET %[1]s_secret);", s.ds.Name))
	if err != nil {
		return err
	}
	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx, fmt.Sprintf("DETACH %s;", s.ds.Name))
	if err != nil {
		return err
	}
	s.isAttached = false
	return nil
}
