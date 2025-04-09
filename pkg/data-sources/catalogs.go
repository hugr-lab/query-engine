package datasources

import (
	"context"
	"fmt"
	"os"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/types"
)

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
	default:
		return nil, fmt.Errorf("unknown source type %s", t)
	}
}
