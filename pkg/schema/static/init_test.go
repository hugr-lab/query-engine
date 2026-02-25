package static

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	_ "github.com/hugr-lab/query-engine/pkg/schema/types"
)

func TestNew(t *testing.T) {
	p, err := New()
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ctx := context.Background()

	t.Run("built-in scalars", func(t *testing.T) {
		for _, name := range []string{"String", "Int", "Float", "Boolean", "ID"} {
			if def := p.ForName(ctx, name); def == nil {
				t.Errorf("missing built-in scalar %s", name)
			}
		}
	})

	t.Run("custom scalars", func(t *testing.T) {
		for _, name := range []string{
			"BigInt", "Date", "Timestamp", "Time", "Interval", "JSON", "Geometry",
			"IntRange", "BigIntRange", "TimestampRange", "H3Cell", "Vector",
		} {
			if def := p.ForName(ctx, name); def == nil {
				t.Errorf("missing custom scalar %s", name)
			}
		}
	})

	t.Run("system directives", func(t *testing.T) {
		for _, name := range []string{
			"table", "view", "module", "catalog", "system",
			"function", "pk", "join", "references", "dependency",
			"stats", "raw", "no_cache",
			"feature", "wfs",
		} {
			if dir := p.DirectiveForName(ctx, name); dir == nil {
				t.Errorf("missing system directive @%s", name)
			}
		}
	})

	t.Run("DDL control directives", func(t *testing.T) {
		for _, name := range []string{
			base.IfNotExistsDirectiveName,
			base.DropDirectiveName,
			base.ReplaceDirectiveName,
			base.DropDirectiveDirectiveName,
		} {
			if dir := p.DirectiveForName(ctx, name); dir == nil {
				t.Errorf("missing DDL directive @%s", name)
			}
		}
	})

	t.Run("system types", func(t *testing.T) {
		for _, name := range []string{
			"Query", "OperationResult", "OrderDirection",
			"ModuleObjectType", "GeometryType",
		} {
			if def := p.ForName(ctx, name); def == nil {
				t.Errorf("missing system type %s", name)
			}
		}
	})

	t.Run("query root type", func(t *testing.T) {
		if q := p.QueryType(ctx); q == nil {
			t.Error("Query root type not set on schema")
		}
	})
}
