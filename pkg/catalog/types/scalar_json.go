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
Aggregation functions: count, list, any, last, sum, avg, min, max, string_agg, bool_and, bool_or (with path parameter).
"""
scalar JSON

"""Filter operators on a JSON document column. Combine several path conditions
with the parent object filter via _and / _or / _not (same as other columns)."""
input JSONFilter @system {
  "Document equals the given JSON value."
  eq: JSON
  "Top-level key with this name exists in the document."
  has: String
  "All of the given top-level keys exist in the document."
  has_all: [String!]
  "Document JSON-contains the given subtree (PostgreSQL @> / DuckDB structural superset)."
  contains: JSON
  "Matches rows where the column itself is SQL NULL (true) or NOT NULL (false)."
  is_null: Boolean
  "Filter on a nested value at a dot-path inside the document. See JSONFieldFilter."
  field: JSONFieldFilter
}

"""Filter on a single value extracted from the JSON document at a dot-path.
At most one typed sub-filter may be set; the server rejects the input otherwise.
isNull (if set) is AND-combined with the typed sub-filter."""
input JSONFieldFilter @system {
  """Dot-path to the value inside the JSON document, e.g. user.address.city. Required."""
  path: String!
  """Strict null check, independent of the typed sub-filter.
  ` + "`true`" + ` matches when the key exists at the path AND its value is JSON null;
  ` + "`false`" + ` matches when the key exists AND its value is anything other than JSON null;
  a missing key yields false in both cases.
  To match "key missing or null", combine isNull: true with _not on has at the parent JSONFilter via _or."""
  isNull: Boolean
  "Compare the extracted value as Int (engine cast)."
  int: IntFilter
  "Compare the extracted value as BigInt (engine cast)."
  bigInt: BigIntFilter
  "Compare the extracted value as Float (engine cast)."
  float: FloatFilter
  "Compare the extracted value as String."
  string: StringFilter
  "Compare the extracted value as Boolean."
  bool: BooleanFilter
  "Compare the extracted value as Date."
  date: DateFilter
  "Compare the extracted value as Time."
  time: TimeFilter
  "Compare the extracted value as DateTime (naive timestamp)."
  dateTime: DateTimeFilter
  "Compare the extracted value as Timestamp (timezone-aware)."
  timestamp: TimestampFilter
  "Compare the extracted value as Interval."
  interval: IntervalFilter
  "Compare the extracted GeoJSON value as Geometry."
  geometry: GeometryFilter
  """JSON literal used as a default when the value at ` + "`path`" + ` is missing or JSON null,
  applied BEFORE the typed sub-filter. Lets the comparison treat absent values
  as a known default (e.g. coalesce: 0 + int: { gte: 18 })."""
  coalesce: JSON
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
