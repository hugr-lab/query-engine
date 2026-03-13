package datasources

import (
	"context"
	"fmt"
	"os"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

func (s *Service) catalogSource(ctx context.Context, ds Source, self bool) (cat sources.Catalog, err error) {
	def := ds.Definition()
	opts := compileOptions(ds)

	var cc []sources.Catalog
	if def.SelfDefined || self {
		if sd, ok := ds.(SelfDescriber); ok {
			cat, err = sd.CatalogSource(ctx, s.db)
			if err != nil {
				return nil, err
			}
		} else {
			info, err := sources.DescribeDataSource(ctx, s.qe, def.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to describe data source %s: %w", def.Name, err)
			}
			if err := info.Build(ctx, ds.Engine(), opts); err != nil {
				return nil, fmt.Errorf("failed to create catalog for data source %s: %w", def.Name, err)
			}
			cat = info
		}
		if self {
			return cat, nil
		}
		if cat != nil {
			cc = append(cc, cat)
		}
	}
	for _, cs := range def.Sources {
		c, err := s.loadCatalogSource(ctx, cs.Type, cs.Path, ds.Engine(), opts)
		if err != nil {
			return nil, err
		}
		cc = append(cc, c)
	}
	if len(cc) == 0 {
		return nil, nil
	}
	if len(cc) == 1 {
		return cc[0], nil
	}
	return sources.NewMergedCatalog(ds.Engine(), opts, def.Description, cc...), nil
}

func (s *Service) loadCatalogSource(ctx context.Context, t types.CatalogSourceType, path string, engine engines.Engine, opts compiler.Options) (sources.Catalog, error) {
	switch t {
	case sources.FileSourceType:
		// check if path is a valid directory
		_, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("wrong localFS catalog source type: %w", err)
		}
		fs := sources.NewFileSource(path, engine, opts)
		return fs, fs.Reload(ctx)
	case sources.URISourceType:
		us := sources.NewURISource(s.db, path, false, engine, opts)
		return us, us.Reload(ctx)
	case sources.URIFileSourceType:
		us := sources.NewURISource(s.db, path, true, engine, opts)
		return us, us.Reload(ctx)
	case sources.TextSourceType:
		return sources.NewStringSource(path, engine, opts, path)
	default:
		return nil, fmt.Errorf("unknown source type %s", t)
	}
}

func compileOptions(ds Source) compiler.Options {
	def := ds.Definition()
	isExt := false
	if ext, ok := ds.(ExtensionSource); ok {
		isExt = ext.IsExtension()
	}
	return compiler.Options{
		Name:         def.Name,
		ReadOnly:     def.ReadOnly,
		Prefix:       def.Prefix,
		EngineType:   string(ds.Engine().Type()),
		AsModule:     def.AsModule,
		IsExtension:  isExt,
		Capabilities: ds.Engine().Capabilities(),
	}
}
