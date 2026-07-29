package dssource

import (
	"context"
	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

// Data sources management source functions
// 1. Load data sources
// 2. Unload data sources

//go:embed schema.graphql
var schema string

// catalogSchema is the curation / maintenance surface in the core.catalog
// module — the stable names for the same UDFs the core module still exposes
// under their internal `_schema_*` names.
//
//go:embed catalog_schema.graphql
var catalogSchema string

type Source struct {
	db *db.Pool
	qe types.Querier
}

func New(qe types.Querier) *Source {
	return &Source{
		qe: qe,
	}
}

func (s *Source) Name() string {
	return "core.ds"
}

func (s *Source) Engine() engines.Engine {
	return engines.NewDuckDB()
}

func (s *Source) IsReadonly() bool {
	return false
}

func (s *Source) AsModule() bool {
	return false
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	s.db = db
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	return s.registerUDF(ctx)
}

func (s *Source) Catalog(ctx context.Context) (sources.Catalog, error) {
	e := engines.NewDuckDB()
	opts := base.Options{
		Name:         s.Name(),
		ReadOnly:     s.IsReadonly(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return sources.NewStringSource(s.Name(), e, opts, schema, catalogSchema)
}
