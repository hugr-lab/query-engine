package planner

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type testIngestValueAdapter struct {
	*engines.DuckDB
}

func (e *testIngestValueAdapter) AdaptIngestValueSQL(_ *ast.Field, valueSQL string) (string, error) {
	return "adapted(" + valueSQL + ")", nil
}

func TestAdaptIngestValueSQL(t *testing.T) {
	t.Run("direct target", func(t *testing.T) {
		got, err := adaptIngestValueSQL(engines.NewDuckDB(), nil, "staging_value")
		if err != nil {
			t.Fatal(err)
		}
		if got != "staging_value" {
			t.Fatalf("got %q, want unchanged staging expression", got)
		}
	})

	t.Run("value adapter", func(t *testing.T) {
		engine := &testIngestValueAdapter{DuckDB: engines.NewDuckDB()}
		got, err := adaptIngestValueSQL(engine, nil, "staging_value")
		if err != nil {
			t.Fatal(err)
		}
		if got != "adapted(staging_value)" {
			t.Fatalf("got %q, want adapted ingest expression", got)
		}
	})
}
