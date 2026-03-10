package planner

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// helper to build an @at directive with given arguments.
func makeAtDirective(args ...*ast.Argument) *ast.Directive {
	return &ast.Directive{
		Name:      base.AtDirectiveName,
		Arguments: args,
	}
}

func makeArg(name, raw string) *ast.Argument {
	return &ast.Argument{
		Name: name,
		Value: &ast.Value{
			Raw:  raw,
			Kind: ast.StringValue,
		},
	}
}

func makeIntArg(name, raw string) *ast.Argument {
	return &ast.Argument{
		Name: name,
		Value: &ast.Value{
			Raw:  raw,
			Kind: ast.IntValue,
		},
	}
}

func TestResolveAtInfo(t *testing.T) {
	t.Run("query-level @at with version", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{
				makeAtDirective(makeIntArg("version", "5")),
			},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{},
		}
		info, err := resolveAtInfo(field, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Fatal("expected non-nil AtInfo")
		}
		if info.Version != 5 {
			t.Fatalf("expected Version=5, got %d", info.Version)
		}
		if info.Timestamp != "" {
			t.Fatalf("expected empty Timestamp, got %q", info.Timestamp)
		}
	})

	t.Run("query-level @at with timestamp", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{
				makeAtDirective(makeArg("timestamp", "2026-01-01T00:00:00Z")),
			},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{},
		}
		info, err := resolveAtInfo(field, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Fatal("expected non-nil AtInfo")
		}
		if info.Timestamp != "2026-01-01T00:00:00Z" {
			t.Fatalf("expected Timestamp=2026-01-01T00:00:00Z, got %q", info.Timestamp)
		}
		if info.Version != 0 {
			t.Fatalf("expected Version=0, got %d", info.Version)
		}
	})

	t.Run("SDL-level @at on object definition", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{}, // no query-level directive
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{
				Directives: ast.DirectiveList{
					makeAtDirective(makeIntArg("version", "3")),
				},
			},
		}
		info, err := resolveAtInfo(field, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Fatal("expected non-nil AtInfo")
		}
		if info.Version != 3 {
			t.Fatalf("expected Version=3, got %d", info.Version)
		}
	})

	t.Run("query-level overrides SDL-level", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{
				makeAtDirective(makeIntArg("version", "10")),
			},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{
				Directives: ast.DirectiveList{
					makeAtDirective(makeIntArg("version", "3")),
				},
			},
		}
		info, err := resolveAtInfo(field, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Fatal("expected non-nil AtInfo")
		}
		if info.Version != 10 {
			t.Fatalf("expected Version=10 (query-level), got %d", info.Version)
		}
	})

	t.Run("no @at directive returns nil", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{},
		}
		info, err := resolveAtInfo(field, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info != nil {
			t.Fatalf("expected nil AtInfo, got %+v", info)
		}
	})

	t.Run("version via variable", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{
				{
					Name: base.AtDirectiveName,
					Arguments: ast.ArgumentList{
						{
							Name: "version",
							Value: &ast.Value{
								Raw:  "ver",
								Kind: ast.Variable,
							},
						},
					},
				},
			},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{},
		}
		vars := map[string]any{"ver": 7}
		info, err := resolveAtInfo(field, vars)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Fatal("expected non-nil AtInfo")
		}
		if info.Version != 7 {
			t.Fatalf("expected Version=7, got %d", info.Version)
		}
	})

	t.Run("invalid version string returns error", func(t *testing.T) {
		field := &ast.Field{
			Directives: ast.DirectiveList{
				makeAtDirective(makeArg("version", "not-a-number")),
			},
			Definition: &ast.FieldDefinition{
				Type: ast.NamedType("SomeType", nil),
			},
			ObjectDefinition: &ast.Definition{},
		}
		_, err := resolveAtInfo(field, nil)
		if err == nil {
			t.Fatal("expected error for invalid version string")
		}
	})
}

func TestAtClause(t *testing.T) {
	t.Run("version only", func(t *testing.T) {
		info := &AtInfo{Version: 5}
		clause, err := atClause(info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := " AT (VERSION => 5)"
		if clause != expected {
			t.Fatalf("expected %q, got %q", expected, clause)
		}
	})

	t.Run("timestamp only", func(t *testing.T) {
		info := &AtInfo{Timestamp: "2026-01-01T00:00:00Z"}
		clause, err := atClause(info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := " AT (TIMESTAMP => '2026-01-01T00:00:00Z')"
		if clause != expected {
			t.Fatalf("expected %q, got %q", expected, clause)
		}
	})

	t.Run("nil AtInfo returns empty string", func(t *testing.T) {
		clause, err := atClause(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if clause != "" {
			t.Fatalf("expected empty string, got %q", clause)
		}
	})

	t.Run("zero-value AtInfo returns empty string", func(t *testing.T) {
		info := &AtInfo{}
		clause, err := atClause(info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if clause != "" {
			t.Fatalf("expected empty string, got %q", clause)
		}
	})

	t.Run("invalid timestamp returns error", func(t *testing.T) {
		info := &AtInfo{Timestamp: "not-a-timestamp"}
		_, err := atClause(info)
		if err == nil {
			t.Fatal("expected error for invalid timestamp")
		}
	})

	t.Run("version takes precedence over timestamp", func(t *testing.T) {
		info := &AtInfo{Version: 5, Timestamp: "2026-01-01T00:00:00Z"}
		clause, err := atClause(info)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := " AT (VERSION => 5)"
		if clause != expected {
			t.Fatalf("expected %q, got %q", expected, clause)
		}
	})
}

func TestSanitizeTimestamp(t *testing.T) {
	t.Run("valid RFC3339 UTC", func(t *testing.T) {
		ts, err := sanitizeTimestamp("2026-01-01T00:00:00Z")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ts != "2026-01-01T00:00:00Z" {
			t.Fatalf("expected 2026-01-01T00:00:00Z, got %q", ts)
		}
	})

	t.Run("valid RFC3339 with offset", func(t *testing.T) {
		ts, err := sanitizeTimestamp("2026-01-01T00:00:00+03:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ts != "2026-01-01T00:00:00+03:00" {
			t.Fatalf("expected 2026-01-01T00:00:00+03:00, got %q", ts)
		}
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		_, err := sanitizeTimestamp("not-a-timestamp")
		if err == nil {
			t.Fatal("expected error for invalid timestamp")
		}
	})

	t.Run("SQL injection attempt", func(t *testing.T) {
		_, err := sanitizeTimestamp("'; DROP TABLE users; --")
		if err == nil {
			t.Fatal("expected error for SQL injection attempt")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := sanitizeTimestamp("")
		if err == nil {
			t.Fatal("expected error for empty string")
		}
	})
}
