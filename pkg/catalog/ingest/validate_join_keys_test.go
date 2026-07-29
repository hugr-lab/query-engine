package ingest

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func namedType(name string, nonNull bool) *ast.Type {
	return &ast.Type{NamedType: name, NonNull: nonNull}
}

// TestJoinKeyCompatible pins the one widening the cross-source path forced:
// Int and BigInt are the same join key, because the width comes from the
// backend (PostgreSQL int4 → Int, DuckDB BIGINT → BigInt) and a key is compared,
// not assigned. Nothing else is interchangeable.
func TestJoinKeyCompatible(t *testing.T) {
	list := func(t *ast.Type) *ast.Type { return &ast.Type{Elem: t} }

	cases := []struct {
		name string
		a, b *ast.Type
		want bool
	}{
		{"identical", namedType("Int", false), namedType("Int", false), true},
		{"nullability ignored", namedType("Int", true), namedType("Int", false), true},
		{"Int joins BigInt", namedType("Int", false), namedType("BigInt", false), true},
		{"BigInt joins Int", namedType("BigInt", true), namedType("Int", false), true},
		{"Int does not join String", namedType("Int", false), namedType("String", false), false},
		{"Int does not join Float", namedType("Int", false), namedType("Float", false), false},
		{"BigInt does not join Timestamp", namedType("BigInt", false), namedType("Timestamp", false), false},
		// A list key is not widened: [Int] and [BigInt] are different shapes,
		// and the pairwise rule says nothing about element-wise comparison.
		{"lists are not widened", list(namedType("Int", false)), list(namedType("BigInt", false)), false},
		{"identical lists", list(namedType("Int", false)), list(namedType("Int", false)), true},
		{"nil pair", nil, nil, true},
		{"one nil", namedType("Int", false), nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := joinKeyCompatible(c.a, c.b); got != c.want {
				t.Errorf("joinKeyCompatible = %v, want %v", got, c.want)
			}
		})
	}
}
