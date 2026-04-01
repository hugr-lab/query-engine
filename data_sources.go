package hugr

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"log/slog"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/cluster"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	authrt "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/auth"
	dssource "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/data-sources"
	dbrt "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/db"
	ducklakert "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/ducklake"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/gis"
	metainfo "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/meta-info"
	"github.com/hugr-lab/query-engine/types"
)

func (s *Service) attachRuntimeSources(ctx context.Context, readonly bool) error {
	if readonly {
		return s.attachRuntimeSourcesReadonly(ctx)
	}

	err := s.ds.AttachRuntimeSource(ctx, s.cache)
	if err != nil {
		return fmt.Errorf("attach cache source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, s.s3)
	if err != nil {
		return fmt.Errorf("attach storage source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, dssource.New(s))
	if err != nil {
		return fmt.Errorf("attach data source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, metainfo.New(s))
	if err != nil {
		return fmt.Errorf("attach meta info source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, s.dbProvider.CatalogSource())
	if err != nil {
		return fmt.Errorf("attach catalog source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, authrt.New())
	if err != nil {
		return fmt.Errorf("attach auth source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, gis.New())
	if err != nil {
		return fmt.Errorf("attach GIS source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, ducklakert.New())
	if err != nil {
		return fmt.Errorf("attach ducklake management source: %w", err)
	}
	err = s.ds.AttachRuntimeSource(ctx, dbrt.New())
	if err != nil {
		return fmt.Errorf("attach db management source: %w", err)
	}

	// Attach cluster source if cluster mode is enabled.
	if s.config.Cluster.Enabled {
		s.cluster = cluster.NewSource(s.config.Cluster, s, s.dbProvider)
		if err := s.ds.AttachRuntimeSource(ctx, s.cluster); err != nil {
			return fmt.Errorf("attach cluster source: %w", err)
		}
	}

	return nil
}

// attachRuntimeSourcesReadonly attaches runtime sources in read-only mode.
// Schemas are already persisted by the writer node — we only call Attach
// (for UDF registration) and register engines for planner routing.
// Write-capable sources (storage, data-source management) are skipped.
func (s *Service) attachRuntimeSourcesReadonly(ctx context.Context) error {
	readonlySources := []sources.RuntimeSource{
		s.cache,
		metainfo.New(s),
		s.dbProvider.CatalogSource(),
		authrt.New(),
		gis.New(),
	}

	// Attach cluster source if cluster mode is enabled (workers too).
	if s.config.Cluster.Enabled {
		s.cluster = cluster.NewSource(s.config.Cluster, s, s.dbProvider)
		readonlySources = append(readonlySources, s.cluster)
	}

	for _, src := range readonlySources {
		if err := s.attachRuntimeSourceReadonly(ctx, src); err != nil {
			return fmt.Errorf("attach runtime source %q: %w", src.Name(), err)
		}
	}
	return nil
}

// attachRuntimeSourceReadonly attaches a single runtime source without schema
// compilation. Calls Attach for UDF registration and registers the engine.
func (s *Service) attachRuntimeSourceReadonly(ctx context.Context, src sources.RuntimeSource) error {
	// Setup querier if the source needs it.
	if sq, ok := src.(sources.RuntimeSourceQuerier); ok {
		sq.QueryEngineSetup(s)
	}

	// Attach for UDF registration.
	if err := src.Attach(ctx, s.db); err != nil {
		// ErrDataSourceAttached is fine — source was already attached.
		if !errors.Is(err, sources.ErrDataSourceAttached) {
			return err
		}
	}

	// Register engine for planner routing (schema already in DB).
	s.schema.RegisterEngine(src.Name(), src.Engine())
	return nil
}

// loadDataSources queries all enabled data sources and loads them.
// Catalog ops (AddCatalog/RemoveCatalog) are controlled by the
// skipCatalogOps flag on the datasources.Service.
func (s *Service) loadDataSources(ctx context.Context) (map[string]bool, error) {
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
		return nil, err
	}
	defer res.Close()
	var data []types.DataSource
	err = res.ScanData("core.data_sources", &data)
	if errors.Is(err, types.ErrNoData) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	res.Close()

	// Load base data sources before extensions.
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
	loaded := map[string]bool{}
	for _, ds := range data {
		if err := s.LoadDataSource(ctx, ds.Name); err != nil {
			slog.Error("failed to load datasource", "name", ds.Name, "error", err)
			loaded[ds.Name] = false
			continue
		}
		loaded[ds.Name] = true
		slog.Info("loaded datasource", "name", ds.Name)
	}

	return loaded, nil
}

func (s *Service) RegisterDataSource(ctx context.Context, ds types.DataSource) error {
	res, err := s.Query(ctx, `mutation($data: core_data_sources_mut_input_data!){
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
