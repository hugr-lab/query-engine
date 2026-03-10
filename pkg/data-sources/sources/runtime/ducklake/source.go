package ducklake

import (
	"context"
	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

//go:embed schema.graphql
var schema string

// Source is a runtime source that registers DuckLake management functions.
// These are global functions that operate on any attached DuckLake catalog
// by name (the ATTACH AS prefix).
type Source struct {
	db *db.Pool
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string        { return "core.ducklake" }
func (s *Source) Engine() engines.Engine { return engines.NewDuckDB() }
func (s *Source) IsReadonly() bool     { return false }
func (s *Source) AsModule() bool       { return false }

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	return s.registerUDF(ctx)
}

func (s *Source) Catalog(_ context.Context) (sources.Catalog, error) {
	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		ReadOnly:     s.IsReadonly(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return sources.NewStringSource(s.Name(), e, opts, schema)
}
