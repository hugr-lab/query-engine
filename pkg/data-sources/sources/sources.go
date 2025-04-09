package sources

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

const (
	Core     types.DataSourceType = "coredb"
	Postgres types.DataSourceType = "postgres"
	DuckDB   types.DataSourceType = "duckdb"
	Http     types.DataSourceType = "http"
	Runtime  types.DataSourceType = "runtime"
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

type SelfDescriber interface {
	CatalogSource(ctx context.Context, db *db.Pool) (sources.Source, error)
}

// RuntimeSource is a data source that is attached on start and provides a catalog source.
type RuntimeSource interface {
	Name() string
	Engine() engines.Engine
	IsReadonly() bool
	Attach(ctx context.Context, db *db.Pool) error
	Catalog(ctx context.Context) sources.Source
}

type RuntimeSourceQuerier interface {
	QueryEngineSetup(querier *types.Querier)
}
