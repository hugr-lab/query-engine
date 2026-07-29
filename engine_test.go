package hugr

import (
	"context"
	"testing"

	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	catalogstore "github.com/hugr-lab/query-engine/pkg/catalog/store"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

//go:embed pkg/data-sources/sources/runtime/core-db/schema.graphql
var testSchemaData string

// newStoreProvider spins the entity catalog storage over a fresh in-memory
// CoreDB. It is both the Provider and the CatalogManager, so AddCatalog runs
// the real write path — the storage is what compiles a schema.
func newStoreProvider(t *testing.T) *catalogstore.Store {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if err := coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store, err := catalogstore.New(ctx, pool, catalogstore.Config{VecSize: 8}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func Test_processQuery(t *testing.T) {
	ss := catalog.NewService(newStoreProvider(t))
	e := engines.NewDuckDB()
	cat, err := sources.NewStringSource("core", e, base.Options{
		Name:         "core",
		EngineType:   string(e.Type()),
		AsModule:     true,
		Capabilities: e.Capabilities(),
	}, testSchemaData)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.AddCatalog(context.Background(), "core", cat)
	if err != nil {
		t.Fatal(err)
	}

	query := `
		query test {
			__schema {
				queryType {
					name
				}
			}
			core {
				__typename
				data_sources {
					name
					type
					prefix
					description
					path
				}
			}
		}
	`

	op, err := ss.ParseQuery(context.Background(), query, nil, "test")
	if err != nil {
		t.Fatal(err)
	}

	resolvers, _ := sdl.QueryRequestInfo(op.Definition.SelectionSet)

	t.Logf("%+v", resolvers)
}
