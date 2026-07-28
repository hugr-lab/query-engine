package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

// TestSelectAtCapability pins the time-travel check the planner took over from
// the SDL rule AtValidator. @at is declared `on OBJECT | FIELD`, so a client can
// pin ANY query field of ANY source — the case the SDL walk over object
// definitions never saw. Here both paths meet, so a source whose engine cannot
// time travel is refused with a plan error instead of a SQL syntax error coming
// back from the database.
func TestSelectAtCapability(t *testing.T) {
	ctx := context.Background()
	// The fixture's sources are duckdb and postgres — neither supports time
	// travel (only ducklake and iceberg do).
	q, err := testSchemaService.ValidateQuery(ctx, `
		query {
			table_object @at(version: 1) {
				field1
			}
		}
	`)
	if err != nil {
		t.Fatal(err)
	}
	var field *ast.Field
	for _, sel := range q.Operations[0].SelectionSet {
		f, ok := sel.(*ast.Field)
		if ok && f.Name == "table_object" {
			field = f
			break
		}
	}
	if field == nil {
		t.Fatal("table_object query field not found")
	}

	_, _, err = selectDataObjectNode(ctx, testSchemaService.Provider(), testService.engines, field, nil)
	if err == nil {
		t.Fatal("expected the pin to be refused, got a plan")
	}
	if !strings.Contains(err.Error(), "does not support time travel") {
		t.Fatalf("unexpected error: %v", err)
	}
}
