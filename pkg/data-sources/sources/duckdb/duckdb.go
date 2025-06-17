package duckdb

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Source struct {
	ds         types.DataSource
	isAttached bool

	engine engines.Engine
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) Name() string {
	return s.ds.Prefix
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (*Source) EngineType() engines.Type {
	return engines.TypeDuckDB
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) (err error) {
	if s.ds.Path != "" {
		err = sources.CheckDBExists(ctx, db, s.ds.Prefix, sources.DuckDB)
		if err != nil {
			return err
		}
	}

	if s.ds.Path == "" {
		// attach as in-memory
		s.ds.Path = ":memory:"
	}
	s.ds.Path, err = sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	sql := "ATTACH DATABASE '" + s.ds.Path + "' AS " + engines.Ident(s.ds.Name)
	if s.ds.ReadOnly {
		sql += "(READ_ONLY)"
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return err
	}

	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx, "DETACH DATABASE "+engines.Ident(s.ds.Name))
	if err != nil {
		return err
	}

	s.isAttached = false
	return nil
}
