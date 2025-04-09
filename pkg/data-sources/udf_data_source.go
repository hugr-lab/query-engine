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
	var ds types.DataSource
	err := s.db.QueryRowToData(ctx, &ds, `
		SELECT ds.name, ds.type, ds.description, ds.prefix, ds.path, ds.self_defined, dsc.catalog_sources AS catalogs
		FROM core.data_sources AS ds
			, LATERAL (
				SELECT array_agg({
					name: cs.name, 
					type: cs.type, 
					path: cs.path
				}) AS catalog_sources
				FROM core.data_source_catalogs AS dcs
						INNER JOIN core.catalog_sources AS cs ON dcs.catalog_name = cs.name
				WHERE ds.name = dcs.data_source_name
			) AS dsc
		WHERE ds.name = $1
	`, name)
	if err != nil {
		return ds, err
	}

	return ds, err
}

func (s *Service) loadDataSource(ctx context.Context, name string) *types.OperationResult {
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

	res := s.unloadDataSource(ctx, name)
	if !res.Succeed {
		return res
	}

	res = s.registerDataSource(ctx, item, false)
	if !res.Succeed {
		return res
	}
	return types.Result("data source loaded", 1, 0)
}

func (s *Service) unloadDataSource(ctx context.Context, name string) *types.OperationResult {
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
