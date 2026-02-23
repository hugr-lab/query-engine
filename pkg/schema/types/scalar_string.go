package types

// Compile-time interface assertions.
var (
	_ ScalarType             = (*stringScalar)(nil)
	_ Filterable             = (*stringScalar)(nil)
	_ ListFilterable         = (*stringScalar)(nil)
	_ Aggregatable           = (*stringScalar)(nil)
	_ MeasurementAggregatable = (*stringScalar)(nil)
)

type stringScalar struct{}

func (s *stringScalar) Name() string { return "String" }

func (s *stringScalar) SDL() string {
	return `"""
The ` + "`String`" + ` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.
Filter operators: eq, in, like, ilike, regex, is_null
Aggregation functions: count, string_agg, list, any, last
"""
scalar String`
}

func (s *stringScalar) FilterTypeName() string { return "StringFilter" }

func (s *stringScalar) FilterSDL() string {
	return `input StringFilter @system {
  eq: String
  in: [String!]
  like: String
  ilike: String
  regex: String
  is_null: Boolean
}`
}

func (s *stringScalar) ListFilterTypeName() string { return "StringListFilter" }

func (s *stringScalar) ListFilterSDL() string {
	return `input StringListFilter @system {
  eq: [String!]
  contains: [String!]
  intersects: [String!]
  is_null: Boolean
}`
}

func (s *stringScalar) AggregationTypeName() string { return "StringAggregation" }

func (s *stringScalar) AggregationSDL() string {
	return `type StringAggregation @system {
  count: BigInt
  string_agg(sep: String!, distinct: Boolean = false): String
  list(distinct: Boolean = false): [String!]
  any: String
  last: String
}

type StringSubAggregation @system {
  count: BigIntAggregation
  string_agg: StringAggregation
}`
}

func (s *stringScalar) MeasurementAggregationTypeName() string {
	return "StringMeasurementAggregation"
}

func (s *stringScalar) MeasurementAggregationSDL() string {
	return `enum StringMeasurementAggregation @system {
  ANY
}`
}
