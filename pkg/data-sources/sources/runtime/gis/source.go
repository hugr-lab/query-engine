package gis

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"

	_ "embed" // for embedding the schema
)

// The GIS runtime source provides duckdb functions to work with GIS data (h3 mostly).

//go:embed schema.graphql
var schema string

//go:embed init.sql
var initSQL string

type Source struct {
	db *db.Pool
	qe types.Querier
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string {
	return "core.gis"
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

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	s.db = db
	_, err := db.Exec(ctx, initSQL)
	return err
}

func (s *Source) Catalog(ctx context.Context) sources.Source {
	return sources.NewStringSource("core_gis", schema)
}

func (s *Source) QueryEngineSetup(querier types.Querier) {
	s.qe = querier
}
