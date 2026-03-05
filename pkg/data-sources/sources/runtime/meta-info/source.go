package metainfo

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"

	_ "embed" // for embedding the schema
)

// The runtime meta-info source provides access to the metadata of DuckDB attached databases, their schemas, relations, and columns.
type Engine interface {
	types.Querier
	SchemaProvider() catalog.Provider
}

//go:embed schema.graphql
var schema string

type Source struct {
	db *db.Pool
	qe Engine
}

func New(qe Engine) *Source {
	return &Source{qe: qe}
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

func (s *Source) Attach(_ context.Context, pool *db.Pool) error {
	s.db = pool
	return nil
}

func (s *Source) Catalog(ctx context.Context) (sources.Catalog, error) {
	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		Prefix:       "core_meta",
		ReadOnly:     s.IsReadonly(),
		AsModule:     s.AsModule(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return sources.NewStringSource(s.Name(), e, opts, schema)
}
