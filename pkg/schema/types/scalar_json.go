package types

import (
	pkgtypes "github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time interface assertions.
var (
	_ ScalarType             = (*jsonScalar)(nil)
	_ Filterable             = (*jsonScalar)(nil)
	_ Aggregatable           = (*jsonScalar)(nil)
	_ SubAggregatable        = (*jsonScalar)(nil)
	_ FieldArgumentsProvider = (*jsonScalar)(nil)
	_ ValueParser            = (*jsonScalar)(nil)
)

type jsonScalar struct{}

func (s *jsonScalar) Name() string { return "JSON" }

func (s *jsonScalar) SDL() string {
	return `"""
The ` + "`JSON`" + ` scalar type represents arbitrary JSON data, encoded as a JSON string.
Filter operators: eq, has, has_all, contains, is_null
Aggregation functions: count, list, any, last, sum, avg, min, max, string_agg, bool_and, bool_or (with path parameter)
"""
scalar JSON

input JSONFilter @system {
  eq: JSON
  has: String
  has_all: [String!]
  contains: JSON
  is_null: Boolean
}

type JSONAggregation @system {
  count(path: String): BigInt
  list(path: String, distinct: Boolean = false): [JSON!]
  any(path: String): JSON
  last(path: String): JSON
  sum(path: String!): Float
  avg(path: String!): Float
  min(path: String!): Float
  max(path: String!): Float
  string_agg(path: String!, sep: String!, distinct: Boolean = false): String
  bool_and(path: String!): Boolean
  bool_or(path: String!): Boolean
}

type JSONSubAggregation @system {
  count(path: String): BigIntAggregation
  sum(path: String!): FloatAggregation
  avg(path: String!): FloatAggregation
  min(path: String!): FloatAggregation
  max(path: String!): FloatAggregation
  string_agg(path: String!, sep: String!, distinct: Boolean = false): StringAggregation
  bool_and(path: String!): BooleanAggregation
  bool_or(path: String!): BooleanAggregation
}`
}

func (s *jsonScalar) FilterTypeName() string { return "JSONFilter" }

func (s *jsonScalar) AggregationTypeName() string { return "JSONAggregation" }

func (s *jsonScalar) SubAggregationTypeName() string { return "JSONSubAggregation" }

func (s *jsonScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "struct", Type: ast.NamedType("JSON", nil)},
	}
}

func (s *jsonScalar) ParseValue(v any) (any, error) {
	return pkgtypes.ParseJsonValue(v)
}
