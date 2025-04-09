package hugr

import (
	"context"
	"errors"
	"fmt"

	"log"

	"github.com/hugr-lab/query-engine/pkg/auth"
	datasources "github.com/hugr-lab/query-engine/pkg/data-sources"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func (s *Service) loadDataSources(ctx context.Context) error {
	res, err := s.Query(auth.ContextWithFullAccess(ctx), `
		query{
			core {
				data_sources{
					name
					type
					prefix
					path
					self_defined
					read_only
					catalogs{
						name
						type
						path
					}
				}
			}
		}`, nil)
	if err != nil {
		return err
	}
	var data []types.DataSource
	err = res.ScanData("core.data_sources", &data)
	if errors.Is(err, types.ErrNoData) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, ds := range data {
		err := s.loadDataSource(ctx, ds)
		if err != nil {
			log.Printf("ERR: failed to load datasource %s: %v", ds.Name, err)
		}
	}
	return nil
}

func (s *Service) loadDataSource(ctx context.Context, ds types.DataSource) error {
	d, err := datasources.NewDataSource(ctx, ds, false)
	if err != nil {
		return fmt.Errorf("failed to create datasource: %w", err)
	}
	err = s.ds.Register(ctx, ds.Name, d)
	if err != nil {
		return fmt.Errorf("failed to register datasource: %w", err)
	}
	return s.ds.Attach(ctx, ds.Name)
}
