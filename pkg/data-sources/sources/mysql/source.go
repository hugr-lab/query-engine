package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

var _ sources.Source = (*Source)(nil)

type Source struct {
	engine     *engines.DuckDB
	ds         types.DataSource
	isAttached bool
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
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

	s.ds.Path, err = sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	path, err := sources.ParseDSN(s.ds.Path)
	if err != nil {
		return err
	}

	if path.DBName == "" || path.Host == "" {
		return sources.ErrInvalidDataSourcePath
	}

	params := []string{
		fmt.Sprintf("host=%s", path.Host),
		fmt.Sprintf("database=%s", path.DBName),
	}

	if path.Port != "" {
		params = append(params, fmt.Sprintf("port=%s", path.Port))
	}
	if path.User != "" {
		params = append(params, fmt.Sprintf("user=%s", path.User))
	}
	if path.Password != "" {
		params = append(params, fmt.Sprintf("password=%s", path.Password))
	}

	for key, value := range path.Params {
		params = append(params, fmt.Sprintf("%s=%s", key, value))
	}

	cs := strings.Join(params, " ")
	_, err = db.Exec(ctx, fmt.Sprintf(
		`ATTACH DATABASE '%s' AS %s (TYPE mysql)`,
		cs,
		engines.Ident(s.ds.Name),
	))
	if err != nil {
		return fmt.Errorf("failed to attach MySQL database %s: %w", s.ds.Name, err)
	}

	if !s.ds.ReadOnly {
		// Query MySQL sequences values and create DuckDB sequences
		type sequence struct {
			TableName string `json:"table_name"`
			Sequence  int    `json:"cur_val"`
		}
		var sequences []sequence
		err = db.QueryTableToSlice(ctx, &sequences, fmt.Sprintf(`
		SELECT TABLE_NAME::VARCHAR AS table_name, auto_increment AS cur_val
		FROM mysql_query('%s', $$
			SELECT table_name, AUTO_INCREMENT
			FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_SCHEMA = '%[1]s' AND AUTO_INCREMENT IS NOT NULL;
		$$)
	`, s.ds.Name))
		if err != nil {
			return fmt.Errorf("failed to query MySQL sequences values: %w", err)
		}

		qualifyDSName := strings.ReplaceAll(strings.ToLower(s.ds.Name), ".", "_")

		for _, seq := range sequences {
			_, err = db.Exec(ctx, fmt.Sprintf(
				`CREATE OR REPLACE SEQUENCE %s_%s_seq START WITH %d INCREMENT BY 1`,
				qualifyDSName,
				seq.TableName,
				seq.Sequence,
			))
			if err != nil {
				return fmt.Errorf("failed to create sequence %s.%s: %w", s.ds.Name, seq.TableName, err)
			}
		}
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
