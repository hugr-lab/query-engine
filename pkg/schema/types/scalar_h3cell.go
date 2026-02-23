package types

// Compile-time interface assertions.
var _ ScalarType = (*h3CellScalar)(nil)

type h3CellScalar struct{}

func (s *h3CellScalar) Name() string { return "H3Cell" }

func (s *h3CellScalar) SDL() string {
	return `"""
The ` + "`H3Cell`" + ` scalar type represents an H3 hexagonal hierarchical geospatial index cell identifier.
"""
scalar H3Cell`
}
