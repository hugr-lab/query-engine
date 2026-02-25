package types

import (
	pkgtypes "github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time interface assertions.
var (
	_ ScalarType           = (*vectorScalar)(nil)
	_ Filterable           = (*vectorScalar)(nil)
	_ ExtraFieldProvider   = (*vectorScalar)(nil)
	_ ValueParser          = (*vectorScalar)(nil)
	_ SQLOutputTransformer = (*vectorScalar)(nil)
)

type vectorScalar struct{}

func (s *vectorScalar) Name() string { return "Vector" }

func (s *vectorScalar) SDL() string {
	return `"""
The ` + "`Vector`" + ` scalar type represents a fixed-length array of floating-point numbers, used for embeddings and similarity search.
Filter operators: is_null
Extra field: VectorDistance (calculates distance between vectors using a specified metric)
"""
scalar Vector

input VectorFilter @system {
  is_null: Boolean
}

input VectorSearchInput @system {
  "The name of vector field"
  name: String!
  "The vector to search"
  vector: Vector!
  "Distance type"
  distance: VectorDistanceType!
  "Limit to results"
  limit: Int!
}

enum VectorDistanceType @system {
  "L2 distance"
  L2
  "Cosine similarity"
  Cosine
  "Inner product"
  Inner
}

input SemanticSearchInput @system {
  "The text to search"
  query: String!
  "Limit to results"
  limit: Int!
}`
}

func (s *vectorScalar) FilterTypeName() string { return "VectorFilter" }

func (s *vectorScalar) ExtraFieldName() string { return "VectorDistance" }

func (s *vectorScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateVectorExtraField(fieldName)
}

func (s *vectorScalar) ParseValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return pkgtypes.ParseVector(v)
}

func (s *vectorScalar) ToOutputSQL(sql string, _ bool) string {
	return "(" + sql + ")::VARCHAR"
}

func (s *vectorScalar) ToStructFieldSQL(sql string) string {
	return "(" + sql + ")::VARCHAR"
}
