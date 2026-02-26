package sources

import (
	"context"

	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

const (
	Postgres  types.DataSourceType = "postgres"
	DuckDB    types.DataSourceType = "duckdb"
	MySQL     types.DataSourceType = "mysql"
	MSSQL     types.DataSourceType = "mssql"
	Http      types.DataSourceType = "http"
	Runtime   types.DataSourceType = "runtime"
	Extension types.DataSourceType = "extension"
	Embedding types.DataSourceType = "embedding"

	Airport types.DataSourceType = "airport"
)

type Source interface {
	Name() string
	Definition() types.DataSource
	Engine() engines.Engine
	IsAttached() bool
	ReadOnly() bool
	Attach(ctx context.Context, db *db.Pool) error
	Detach(ctx context.Context, db *db.Pool) error
}

// The data source is a catalog extension.
type ExtensionSource interface {
	IsExtension() bool
}

type SelfDescriber interface {
	CatalogSource(ctx context.Context, db *db.Pool) (cs.Catalog, error)
}

// RuntimeSource is a data source that is attached on start and provides a catalog source.
type RuntimeSource interface {
	Name() string
	Engine() engines.Engine
	IsReadonly() bool
	AsModule() bool
	Attach(ctx context.Context, db *db.Pool) error
	Catalog(ctx context.Context) (cs.Catalog, error)
}

type RuntimeSourceQuerier interface {
	QueryEngineSetup(querier types.Querier)
}

type EmbeddingSource interface {
	CreateEmbedding(ctx context.Context, input string) (types.Vector, error)
	CreateEmbeddings(ctx context.Context, input []string) ([]types.Vector, error)
}
