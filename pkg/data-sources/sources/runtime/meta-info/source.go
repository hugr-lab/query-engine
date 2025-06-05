package metainfo

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"

	_ "embed" // for embedding the schema
)

// The runtime meta-info source provides access to the metadata of DuckDB attached databases, their schemas, relations, and columns.

//go:embed schema.graphql
var schema string

type Source struct {
	db *db.Pool
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string {
	return "core.meta"
}

func (s *Source) Engine() engines.Engine {
	return engines.NewDuckDB()
}

func (s *Source) IsReadonly() bool {
	return false
}

func (s *Source) AsModule() bool {
	return true
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	return nil
}

func (s *Source) Catalog(ctx context.Context) sources.Source {
	return sources.NewStringSource("core_meta", schema)
}
