package types

import (
	pkgtypes "github.com/hugr-lab/query-engine/types"
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
Filter operators: eq, has, has_all, contains, is_null, field, not, or, and
Aggregation functions: count, list, any, last, sum, avg, min, max, string_agg, bool_and, bool_or (with path parameter)
"""
scalar JSON

input JSONFilter @system {
  eq: JSON
  has: String
  has_all: [String!]
  contains: JSON
  is_null: Boolean
  field: [JSONFieldFilter!]
  not: JSONFilter
  or: [JSONFilter!]
  and: [JSONFilter!]
}

"""
Filter by a nested JSON field at a given path.
The path uses dot notation (e.g. "catalog.field_name").
Optional coalesce replaces NULL with a default before comparison.
"""
input JSONFieldFilter @system {
  path: String!
  coalesce: JSON
  eq: JSON
  gt: JSON
  gte: JSON
  lt: JSON
  lte: JSON
  in: [JSON!]
  not_in: [JSON!]
  like: String
  ilike: String
  regex: String
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
		{Name: "struct", Description: "Provides json structure to extract partial data from json field. Structure: {field: \"type\", field2: [\"type2\"], field3: [{field4: \"type4\"}]}.\nTypes can be: string, int, float, bool,timestamp, json, h3string", Type: ast.NamedType("JSON", nil)},
	}
}

func (s *jsonScalar) ParseValue(v any) (any, error) {
	return pkgtypes.ParseJsonValue(v)
}
