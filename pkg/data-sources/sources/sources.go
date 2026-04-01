package sources

import (
	"context"

	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
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

	Airport  types.DataSourceType = "airport"
	DuckLake types.DataSourceType = "ducklake"
	Iceberg  types.DataSourceType = "iceberg"
	HugrApp  types.DataSourceType = "hugr-app"
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

// Provisioner is implemented by sources that need to provision external
// resources (databases, schemas) after attachment. Called by the data source
// service after Attach() succeeds. Querier provides access to hugr's GraphQL
// API for registering/loading data sources and querying system configuration.
type Provisioner interface {
	Provision(ctx context.Context, querier types.Querier) error
}

// Querier provides GraphQL query access for provisioning operations.
type Querier = types.Querier

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
	CreateEmbedding(ctx context.Context, input string) (ctypes.Vector, error)
	CreateEmbeddings(ctx context.Context, input []string) ([]ctypes.Vector, error)
}
