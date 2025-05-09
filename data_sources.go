package hugr

import (
	"context"
	"errors"

	"log"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func (s *Service) loadDataSources(ctx context.Context) error {
	ctx = auth.ContextWithFullAccess(ctx)
	res, err := s.Query(ctx, `
		query{
			core {
				data_sources(filter:{disabled:{eq: false}}){
					name
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
		err := s.LoadDataSource(ctx, ds.Name)
		if err != nil {
			log.Printf("ERR: failed to load datasource %s: %v", ds.Name, err)
		}
	}
	return nil
}

func (s *Service) RegisterDataSource(ctx context.Context, ds types.DataSource) error {
	_, err := s.Query(ctx, `mutation($data: data_sources_mut_input_data!){
		core{
			insert_data_sources(data:$data){
				name
			}
		}
	}`, map[string]any{
		"data": ds,
	})
	if err != nil {
		return err
	}
	return s.LoadDataSource(ctx, ds.Name)
}

func (s *Service) LoadDataSource(ctx context.Context, name string) error {
	return s.ds.LoadDataSource(ctx, name)
}

func (s *Service) UnloadDataSource(ctx context.Context, name string) error {
	return s.ds.UnloadDataSource(ctx, name)
}

func (s *Service) DataSourceStatus(ctx context.Context, name string) (string, error) {
	ds, err := s.ds.DataSource(name)
	if err != nil {
		return "", err
	}
	if ds.IsAttached() {
		return "attached", nil
	}
	return "detached", nil
}
