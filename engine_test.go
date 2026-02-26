package hugr

import (
	"context"
	"testing"

	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
)

//go:embed pkg/data-sources/sources/runtime/core-db/schema.graphql
var testSchemaData string

func Test_processQuery(t *testing.T) {
	provider, err := static.New()
	if err != nil {
		t.Fatal(err)
	}
	ss := catalog.NewService(provider)
	e := engines.NewDuckDB()
	cat, err := sources.NewStringSource("test", e, compiler.Options{
		Name:         "test",
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, testSchemaData)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.AddCatalog(context.Background(), "test", cat)
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
