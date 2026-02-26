package hugr

import (
	"context"
	"testing"

	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/schema"
	"github.com/hugr-lab/query-engine/pkg/schema/sdl"
	"github.com/hugr-lab/query-engine/pkg/schema/sources"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
	"github.com/hugr-lab/query-engine/pkg/types"
)

//go:embed pkg/data-sources/sources/runtime/core-db/schema.graphql
var testSchemaData string

func Test_processQuery(t *testing.T) {
	provider, err := static.New()
	if err != nil {
		t.Fatal(err)
	}
	ss := schema.NewService(provider)
	cat, err := sources.NewCatalog(context.Background(),
		types.DataSource{Name: "test"},
		engines.NewDuckDB(),
		sources.NewStringSource(testSchemaData),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.AddCatalog(context.Background(), "test", engines.NewDuckDB(), cat)
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
