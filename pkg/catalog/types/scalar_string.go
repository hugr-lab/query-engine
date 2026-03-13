package types

// Compile-time interface assertions.
var (
	_ ScalarType              = (*stringScalar)(nil)
	_ Filterable              = (*stringScalar)(nil)
	_ ListFilterable          = (*stringScalar)(nil)
	_ Aggregatable            = (*stringScalar)(nil)
	_ SubAggregatable         = (*stringScalar)(nil)
	_ MeasurementAggregatable = (*stringScalar)(nil)
	_ JSONTypeHintProvider    = (*stringScalar)(nil)
	_ ArrayParser             = (*stringScalar)(nil)
)

type stringScalar struct{}

func (s *stringScalar) Name() string { return "String" }

func (s *stringScalar) SDL() string {
	return `"""
The ` + "`String`" + ` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.
Filter operators: eq, in, like, ilike, regex, is_null
Aggregation functions: count, string_agg, list, any, last
"""
scalar String

input StringFilter @system {
  eq: String
  in: [String!]
  like: String
  ilike: String
  regex: String
  is_null: Boolean
}

input StringListFilter @system {
  eq: [String!]
  contains: [String!]
  intersects: [String!]
  is_null: Boolean
}

type StringAggregation @system {
  count: BigInt
  string_agg(sep: String!, distinct: Boolean = false): String
  list(distinct: Boolean = false): [String!]
  any: String
  last: String
}

type StringSubAggregation @system {
  count: BigIntAggregation
  string_agg: StringAggregation
}

enum StringMeasurementAggregation @system {
  ANY
}`
}

func (s *stringScalar) FilterTypeName() string { return "StringFilter" }

func (s *stringScalar) ListFilterTypeName() string { return "StringListFilter" }

func (s *stringScalar) AggregationTypeName() string { return "StringAggregation" }

func (s *stringScalar) SubAggregationTypeName() string { return "StringSubAggregation" }

func (s *stringScalar) JSONTypeHint() string { return "string" }

func (s *stringScalar) MeasurementAggregationTypeName() string {
	return "StringMeasurementAggregation"
}

func (s *stringScalar) ParseArray(v any) (any, error) {
	return ParseScalarArray[string](v)
}
