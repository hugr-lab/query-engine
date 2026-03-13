package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

var _ sources.Source = (*Source)(nil)

type Source struct {
	engine engines.Engine
	ds     types.DataSource

	isAttached bool
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewMySql(),
	}, nil
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) (err error) {
	if s.isAttached {
		return sources.ErrDataSourceAttached
	}

	path, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}

	if path.DBName == "" || path.Host == "" {
		return sources.ErrInvalidDataSourcePath
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
	name := strings.ReplaceAll(strings.ReplaceAll(s.ds.Name, ".", "_"), "-", "_")
	readonly := ""
	if s.ds.ReadOnly {
		readonly = ", READ_ONLY"
	}
	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE SECRET _%s_secret (
			TYPE mysql,
			host '%s',
			port %s,
			database '%s',
			user '%s',
			password '%s'
			%s
		);
		ATTACH DATABASE '' AS %s (TYPE mysql, SECRET _%[1]s_secret %[9]s);
	`,
		name, path.Host, path.Port, path.DBName, path.User, path.Password, ps,
		engines.Ident(s.ds.Name),
		readonly,
	))
	if err != nil {
		return fmt.Errorf("failed to attach MySQL database %s: %w", s.ds.Name, err)
	}

	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	if !s.isAttached {
		return nil
	}

	_, err := db.Exec(ctx, fmt.Sprintf("DETACH DATABASE %s", engines.Ident(s.ds.Name)))
	if err != nil {
		return fmt.Errorf("failed to detach MySQL database %s: %w", s.ds.Name, err)
	}
	s.isAttached = false

	return nil
}
