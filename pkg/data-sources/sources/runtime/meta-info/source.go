package metainfo

import (
	"context"
	"encoding/json"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"

	_ "embed" // for embedding the schema
)

// The runtime meta-info source provides access to the metadata of DuckDB attached databases, their schemas, relations, and columns.
type Engine interface {
	types.Querier
	Schema() *ast.Schema
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

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionNoArgs[*SchemaInfo]{
		Name: "core_meta_schema_info",
		Execute: func(ctx context.Context) (*SchemaInfo, error) {
			ctx = db.ClearTxContext(ctx)
			return s.Summary(ctx)
		},
		ConvertOutput: func(out *SchemaInfo) (any, error) {
			return json.Marshal(out)
		},
		OutputType: runtime.DuckDBTypeInfoByNameMust("JSON"),
	})
	return nil
}

func (s *Source) Catalog(ctx context.Context) sources.Source {
	return sources.NewStringSource("core_meta", schema)
}
