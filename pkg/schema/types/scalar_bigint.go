package types

// Compile-time interface assertions.
var (
	_ ScalarType             = (*bigIntScalar)(nil)
	_ Filterable             = (*bigIntScalar)(nil)
	_ ListFilterable         = (*bigIntScalar)(nil)
	_ Aggregatable           = (*bigIntScalar)(nil)
	_ MeasurementAggregatable = (*bigIntScalar)(nil)
)

type bigIntScalar struct{}

func (s *bigIntScalar) Name() string { return "BigInt" }

func (s *bigIntScalar) SDL() string {
	return `"""
BigInt - represents big integer type (64 bit), supports filtering, aggregation.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, sum, avg, min, max, list, any, last
"""
scalar BigInt`
}

func (s *bigIntScalar) FilterTypeName() string { return "BigIntFilter" }

func (s *bigIntScalar) FilterSDL() string {
	return `input BigIntFilter @system {
  eq: BigInt
  gt: BigInt
  gte: BigInt
  lt: BigInt
  lte: BigInt
  in: [BigInt!]
  is_null: Boolean
}`
}

func (s *bigIntScalar) ListFilterTypeName() string { return "BigIntListFilter" }

func (s *bigIntScalar) ListFilterSDL() string {
	return `input BigIntListFilter @system {
  eq: [BigInt!]
  contains: [BigInt!]
  intersects: [BigInt!]
  is_null: Boolean
}`
}

func (s *bigIntScalar) AggregationTypeName() string { return "BigIntAggregation" }

func (s *bigIntScalar) AggregationSDL() string {
	return `type BigIntAggregation @system {
  count: BigInt
  sum: BigInt
  avg: Float
  min: BigInt
  max: BigInt
  list(distinct: Boolean = false): [BigInt!]
  any: BigInt
  last: BigInt
}

type BigIntSubAggregation @system {
  count: BigIntAggregation
  sum: BigIntAggregation
  avg: BigIntAggregation
  min: BigIntAggregation
  max: BigIntAggregation
}`
}

func (s *bigIntScalar) MeasurementAggregationTypeName() string {
	return "BigIntMeasurementAggregation"
}

func (s *bigIntScalar) MeasurementAggregationSDL() string {
	return `enum BigIntMeasurementAggregation @system {
  SUM
  AVG
  MIN
  MAX
  ANY
}`
}
