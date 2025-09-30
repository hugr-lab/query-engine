package datasources

import (
	"context"
	"fmt"
	"os"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/types"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

func (s *Service) dataSourceCatalog(ctx context.Context, name string) (*catalogs.Catalog, error) {
	ds := s.dataSources[name]
	source, err := s.catalogSource(ctx, ds, false)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, nil
	}
	def := ds.Definition()
	return catalogs.NewCatalog(ctx, def.Name, def.Prefix, ds.Engine(), source, def.AsModule, ds.ReadOnly())
}

func (s *Service) extensionCatalog(ctx context.Context, name string) (*catalogs.Extension, error) {
	ds := s.dataSources[name]
	source, err := s.catalogSource(ctx, ds, false)
	if err != nil {
		return nil, err
	}
	def := ds.Definition()
	return s.catalogs.NewExtension(ctx, def, source)
}

func (s *Service) catalogSource(ctx context.Context, ds Source, self bool) (source sources.Source, err error) {
	var ss []sources.Source
	def := ds.Definition()
	if def.SelfDefined || self {
		if ds, ok := ds.(SelfDescriber); ok {
			source, err = ds.CatalogSource(ctx, s.db)
			if err != nil {
				return nil, err
			}
		} else {
			source, err = sources.DescribeDataSource(ctx, s.qe, def.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to describe data source %s: %w", def.Name, err)
			}
		}
		if self {
			return source, nil
		}
		if source != nil {
			ss = append(ss, source)
		}
	}
	for _, cs := range def.Sources {
		s, err := s.loadCatalogSource(ctx, cs.Type, cs.Path)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	if len(ss) == 0 {
		return nil, nil
	}
	if len(ss) == 1 {
		source = ss[0]
	}
	if len(ss) > 1 {
		source = sources.MergeSource(ss...)
	}
	return source, nil
}

func (s *Service) loadCatalogSource(ctx context.Context, t types.CatalogSourceType, path string) (sources.Source, error) {
	switch t {
	case sources.FileSourceType:
		// check if path is a valid directory
		_, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("wrong localFS catalog source type: %w", err)
		}
		s := sources.NewFileSource(path)
		return s, s.Reload(ctx)
	case sources.URISourceType:
		s := sources.NewURISource(s.db, path, false)
		return s, s.Reload(ctx)
	case sources.URIFileSourceType:
		s := sources.NewURISource(s.db, path, true)
		return s, s.Reload(ctx)
	case sources.TextSourceType:
		s := sources.NewStringSource(path)
		return s, nil
	default:
		return nil, fmt.Errorf("unknown source type %s", t)
	}
}
