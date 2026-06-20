package planner

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type testIngestTargetCaster struct {
	*engines.DuckDB
}

func (e *testIngestTargetCaster) CastIngestValueToTarget(_ *ast.Field, stagingExpr string) (string, error) {
	return "target_cast(" + stagingExpr + ")", nil
}

func TestCastIngestValueToTarget(t *testing.T) {
	t.Run("direct target", func(t *testing.T) {
		got, err := castIngestValueToTarget(engines.NewDuckDB(), nil, "staging_value")
		if err != nil {
			t.Fatal(err)
		}
		if got != "staging_value" {
			t.Fatalf("got %q, want unchanged staging expression", got)
		}
	})

	t.Run("target caster", func(t *testing.T) {
		engine := &testIngestTargetCaster{DuckDB: engines.NewDuckDB()}
		got, err := castIngestValueToTarget(engine, nil, "staging_value")
		if err != nil {
			t.Fatal(err)
		}
		if got != "target_cast(staging_value)" {
			t.Fatalf("got %q, want target cast expression", got)
		}
	})
}
