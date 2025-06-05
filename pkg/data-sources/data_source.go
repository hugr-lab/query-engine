package datasources

import (
	"context"
	"errors"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/formatter"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

func (s *Service) dataSource(ctx context.Context, name string) (types.DataSource, error) {
	res, err := s.qe.Query(ctx, `query($name: String!){
		core {
			data_sources_by_pk(name: $name){
				name
				type
				prefix
				path
				as_module
				self_defined
				read_only
				disabled
				catalogs{
					name
					type
					path
				}
			}
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return types.DataSource{}, err
	}
	defer res.Close()
	var ds types.DataSource
	err = res.ScanData("core.data_sources_by_pk", &ds)
	return ds, err
}

func (s *Service) LoadDataSource(ctx context.Context, name string) error {
	// read from db and if not found only source reload
	item, err := s.dataSource(ctx, name)
	if errors.Is(err, types.ErrNoData) {
		return ErrDataSourceNotFound
	}
	if err != nil {
		return err
	}

	ds, err := s.DataSource(name)
	if err == nil {
		err = s.UnloadDataSource(ctx, name)
		if err != nil {
			return err
		}
	}

	ds, err = NewDataSource(ctx, item, false)
	if err != nil {
		return err
	}

	err = s.Register(ctx, item.Name, ds)
	if err != nil {
		return nil
	}

	return s.Attach(ctx, item.Name)
}

func (s *Service) UnloadDataSource(ctx context.Context, name string) error {
	if !s.IsAttached(name) {
		s.Unregister(ctx, name)
		return errors.New("already unloaded")
	}
	err := s.Detach(ctx, name, s.db)
	if err != nil {
		return err
	}
	err = s.Unregister(ctx, name)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) DescribeDataSource(ctx context.Context, name string, self bool) (string, error) {
	ds, err := s.DataSource(name)
	if err != nil {
		return "", err
	}

	source, err := s.catalogSource(ctx, ds, self)
	if err != nil {
		return "", err
	}
	if source == nil {
		return "", nil
	}

	sd, err := source.SchemaDocument(ctx)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	formatter.NewFormatter(&sb).FormatSchemaDocument(sd)
	return sb.String(), nil
}
