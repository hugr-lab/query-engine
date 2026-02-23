package types

// Compile-time interface assertions.
var (
	_ ScalarType             = (*intScalar)(nil)
	_ Filterable             = (*intScalar)(nil)
	_ ListFilterable         = (*intScalar)(nil)
	_ Aggregatable           = (*intScalar)(nil)
	_ MeasurementAggregatable = (*intScalar)(nil)
)

type intScalar struct{}

func (s *intScalar) Name() string { return "Int" }

func (s *intScalar) SDL() string {
	return `"""
` + "`Int`" + ` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, sum, avg, min, max, list, any, last
"""
scalar Int`
}

func (s *intScalar) FilterTypeName() string { return "IntFilter" }

func (s *intScalar) FilterSDL() string {
	return `input IntFilter @system {
  eq: Int
  gt: Int
  gte: Int
  lt: Int
  lte: Int
  in: [Int!]
  is_null: Boolean
}`
}

func (s *intScalar) ListFilterTypeName() string { return "IntListFilter" }

func (s *intScalar) ListFilterSDL() string {
	return `input IntListFilter @system {
  eq: [Int!]
  contains: [Int!]
  intersects: [Int!]
  is_null: Boolean
}`
}

func (s *intScalar) AggregationTypeName() string { return "IntAggregation" }

func (s *intScalar) AggregationSDL() string {
	return `type IntAggregation @system {
  count: BigInt
  sum: Int
  avg: Float
  min: Int
  max: Int
  list(distinct: Boolean = false): [Int!]
  any: Int
  last: Int
}

type IntSubAggregation @system {
  count: BigIntAggregation
  sum: IntAggregation
  avg: IntAggregation
  min: IntAggregation
  max: IntAggregation
}`
}

func (s *intScalar) MeasurementAggregationTypeName() string {
	return "IntMeasurementAggregation"
}

func (s *intScalar) MeasurementAggregationSDL() string {
	return `enum IntMeasurementAggregation @system {
  SUM
  AVG
  MIN
  MAX
  ANY
}`
}
