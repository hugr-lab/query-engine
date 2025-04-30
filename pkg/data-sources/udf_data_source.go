package datasources

import (
	"context"
	"database/sql"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func (s *Service) registerDataSource(ctx context.Context,
	ds types.DataSource,
	persistent bool,
) *types.OperationResult {
	// load catalog sources
	if !base.IsValidIdentifier(ds.Name) {
		return types.ErrResult(errors.New("invalid data source name"))
	}
	res := types.Result("data source registered", 1, 0)
	if len(ds.Sources) == 0 && persistent {
		item, err := s.dataSource(ctx, ds.Name)
		if err != nil {
			return types.ErrResult(err)
		}
		ds.Sources = item.Sources
	}

	d, err := NewDataSource(ctx, ds, false)
	if err != nil {
		return types.ErrResult(err)
	}

	err = s.Register(ctx, ds.Name, d)
	if err != nil {
		return types.ErrResult(err)
	}
	err = s.Attach(ctx, ds.Name)
	if err != nil {
		return types.ErrResult(err)
	}
	return res
}

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
	var ds types.DataSource
	err = res.ScanData("core.data_sources_by_pk", &ds)
	return ds, err
}

func (s *Service) LoadDataSource(ctx context.Context, name string) *types.OperationResult {
	// read from db and if not found only source reload
	item, err := s.dataSource(ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.catalogs.Reload(ctx, name)
		if err != nil {
			return types.ErrResult(err)
		}
	}
	if err != nil {
		return types.ErrResult(err)
	}

	res := s.UnloadDataSource(ctx, name)
	if !res.Succeed {
		return res
	}

	res = s.registerDataSource(ctx, item, false)
	if !res.Succeed {
		return res
	}
	return types.Result("data source loaded", 1, 0)
}

func (s *Service) UnloadDataSource(ctx context.Context, name string) *types.OperationResult {
	if !s.IsAttached(name) {
		s.Unregister(ctx, name)
		return types.Result("already unloaded", 0, 0)
	}
	err := s.Detach(ctx, name, s.db)
	if err != nil {
		return types.ErrResult(err)
	}
	err = s.Unregister(ctx, name)
	if err != nil {
		return types.ErrResult(err)
	}
	return types.Result("data source unloaded", 1, 0)
}
