package planner

import (
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

type testIngestValueAdapter struct {
	*engines.DuckDB
}

func (e *testIngestValueAdapter) AdaptIngestValueSQL(_ *ast.Field, valueSQL string) (string, error) {
	return "adapted(" + valueSQL + ")", nil
}

type testUnsupportedIngestEngine struct {
	*engines.DuckDB
	unsupported []string
}

func (e *testUnsupportedIngestEngine) Capabilities() *compiler.EngineCapabilities {
	caps := e.DuckDB.Capabilities()
	caps.General.UnsupportedTypes = e.unsupported
	return caps
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

func TestCheckIngestSupportedTypes(t *testing.T) {
	engine := &testUnsupportedIngestEngine{
		DuckDB:      engines.NewDuckDB(),
		unsupported: []string{base.GeometryTypeName},
	}

	t.Run("rejects unsupported arrow column type", func(t *testing.T) {
		err := checkIngestSupportedTypes(engine, nil, nil, []ingestColumn{
			{
				FieldDef: &ast.FieldDefinition{
					Name: "geom",
					Type: ast.NamedType(base.GeometryTypeName, nil),
				},
			},
		}, nil)
		if err == nil {
			t.Fatal("expected unsupported Geometry ingest error")
		}
		if !strings.Contains(err.Error(), `field "geom" of type "Geometry"`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("allows supported target type", func(t *testing.T) {
		err := checkIngestSupportedTypes(engine, nil, nil, []ingestColumn{
			{
				FieldDef: &ast.FieldDefinition{
					Name: "payload",
					Type: ast.NamedType(base.JSONTypeName, nil),
				},
			},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	})
}
