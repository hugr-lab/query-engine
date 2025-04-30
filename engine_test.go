package hugr

import (
	"context"
	"testing"

	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2"
)

//go:embed pkg/data-sources/sources/runtime/core-db/schema.graphql
var testSchemaData string

func Test_processQuery(t *testing.T) {
	cat, err := catalogs.NewCatalog(context.Background(), "test", "", engines.NewDuckDB(), sources.NewStringSource(testSchemaData), false, false)
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

	qd, errs := gqlparser.LoadQuery(cat.Schema(), query)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	resolvers, _ := compiler.QueryRequestInfo(qd.Operations[0].SelectionSet)

	t.Logf("%+v", resolvers)
}
