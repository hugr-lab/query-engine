package types

import "github.com/vektah/gqlparser/v2/ast"

// Compile-time interface assertions.
var (
	_ ScalarType        = (*vectorScalar)(nil)
	_ Filterable        = (*vectorScalar)(nil)
	_ ExtraFieldProvider = (*vectorScalar)(nil)
)

type vectorScalar struct{}

func (s *vectorScalar) Name() string { return "Vector" }

func (s *vectorScalar) SDL() string {
	return `"""
The ` + "`Vector`" + ` scalar type represents a fixed-length array of floating-point numbers, used for embeddings and similarity search.
Filter operators: is_null
Extra field: VectorDistance (calculates distance between vectors using a specified metric)
"""
scalar Vector`
}

func (s *vectorScalar) FilterTypeName() string { return "VectorFilter" }

func (s *vectorScalar) FilterSDL() string {
	return `input VectorFilter @system {
  is_null: Boolean
}`
}

func (s *vectorScalar) ExtraFieldName() string { return "VectorDistance" }

func (s *vectorScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateVectorExtraField(fieldName)
}
