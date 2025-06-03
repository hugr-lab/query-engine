package sources

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

const (
	Postgres  types.DataSourceType = "postgres"
	DuckDB    types.DataSourceType = "duckdb"
	MySQL     types.DataSourceType = "mysql"
	Http      types.DataSourceType = "http"
	Runtime   types.DataSourceType = "runtime"
	Extension types.DataSourceType = "extension"
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
	CatalogSource(ctx context.Context, db *db.Pool) (sources.Source, error)
}

// RuntimeSource is a data source that is attached on start and provides a catalog source.
type RuntimeSource interface {
	Name() string
	Engine() engines.Engine
	IsReadonly() bool
	AsModule() bool
	Attach(ctx context.Context, db *db.Pool) error
	Catalog(ctx context.Context) sources.Source
}

type RuntimeSourceQuerier interface {
	QueryEngineSetup(querier *types.Querier)
}
