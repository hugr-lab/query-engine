package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
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

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	// check if db is attached
	err := sources.CheckDBExists(ctx, db, engines.Ident(s.ds.Name), sources.Postgres)
	if err != nil {
		return err
	}

	path, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}
	name := strings.ReplaceAll(strings.ReplaceAll(s.ds.Name, ".", "_"), "-", "_")
	readonly := ""
	if s.ds.ReadOnly {
		readonly = ", READ_ONLY"
	}
	params := make([]string, 0, len(path.Params))
	for key, value := range path.Params {
		if value != "true" && value != "false" {
			value = fmt.Sprintf("'%s'", value)
		}
		params = append(params, fmt.Sprintf("%s %s", key, value))
	}
	ps := strings.Join(params, ", ")
	if ps != "" {
		ps = ", " + ps
	}
	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE SECRET _%s_secret (
			TYPE postgres,
			HOST '%s',
			PORT %s,
			DATABASE '%s',
			USER '%s',
			PASSWORD '%s'
			%s
		);
		ATTACH '' AS %s (TYPE POSTGRES, SECRET _%[1]s_secret %[9]s);`,
		name, path.Host, path.Port, path.DBName, path.User, path.Password, ps,
		engines.Ident(s.ds.Name),
		readonly,
	))
	if err != nil {
		return err
	}
	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx, fmt.Sprintf("DETACH %s;", engines.Ident(s.ds.Name)))
	if err != nil {
		return err
	}
	s.isAttached = false
	return nil
}
