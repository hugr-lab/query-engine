package hugr

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"log"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	dssource "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/data-sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/gis"
	metainfo "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/meta-info"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func (s *Service) attachRuntimeSources(ctx context.Context) error {
	err := s.ds.AttachRuntimeSource(ctx, s.cache)
	if err != nil {
		return fmt.Errorf("attach cache source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, s.s3)
	if err != nil {
		return fmt.Errorf("attach s3 source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, dssource.New(s))
	if err != nil {
		return fmt.Errorf("attach data source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, metainfo.New())
	if err != nil {
		return fmt.Errorf("attach meta info source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, gis.New())
	if err != nil {
		return fmt.Errorf("attach GIS source: %w", err)
	}
	return nil
}

func (s *Service) loadDataSources(ctx context.Context) error {
	ctx = auth.ContextWithFullAccess(ctx)
	res, err := s.Query(ctx, `
		query{
			core {
				data_sources(filter:{disabled:{eq: false}}){
					name
					type
				}
			}
		}`, nil)
	if err != nil {
		return err
	}
	defer res.Close()
	var data []types.DataSource
	err = res.ScanData("core.data_sources", &data)
	if errors.Is(err, types.ErrNoData) {
		return nil
	}
	if err != nil {
		return err
	}

	// load first data sources than extensions
	sort.Slice(data, func(i, j int) bool {
		if data[i].Type == data[j].Type {
			return data[i].Name < data[j].Name
		}
		if data[i].Type == sources.Extension {
			return false
		}
		if data[j].Type == sources.Extension {
			return true
		}
		return data[i].Name < data[j].Name
	})
	for _, ds := range data {
		err := s.LoadDataSource(ctx, ds.Name)
		if err != nil {
			log.Printf("ERR: failed to load datasource %s: %v", ds.Name, err)
		}
		if err == nil {
			log.Printf("INFO: loaded datasource %s", ds.Name)
		}
	}

	return nil
}

func (s *Service) RegisterDataSource(ctx context.Context, ds types.DataSource) error {
	res, err := s.Query(ctx, `mutation($data: data_sources_mut_input_data!){
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
	defer res.Close()

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

// DescribeDataSource returns the formatted schema definition of a data source by its name.
func (s *Service) DescribeDataSource(ctx context.Context, name string, self bool) (string, error) {
	return s.ds.DescribeDataSource(ctx, name, self)
}
