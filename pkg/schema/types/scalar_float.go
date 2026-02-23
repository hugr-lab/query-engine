package types

// Compile-time interface assertions.
var (
	_ ScalarType              = (*floatScalar)(nil)
	_ Filterable              = (*floatScalar)(nil)
	_ ListFilterable          = (*floatScalar)(nil)
	_ Aggregatable            = (*floatScalar)(nil)
	_ MeasurementAggregatable = (*floatScalar)(nil)
)

type floatScalar struct{}

func (s *floatScalar) Name() string { return "Float" }

func (s *floatScalar) SDL() string {
	return `"""
The ` + "`Float`" + ` scalar type represents signed double-precision fractional values as specified by [IEEE 754](http://en.wikipedia.org/wiki/IEEE_floating_point).
Filter operators: eq, ne, gt, gte, lt, lte, in, is_null
Aggregation functions: count, sum, avg, min, max, list, any, last
"""
scalar Float

input FloatFilter @system {
  eq: Float
  ne: Float
  gt: Float
  gte: Float
  lt: Float
  lte: Float
  in: [Float!]
  is_null: Boolean
}

input FloatListFilter @system {
  eq: [Float!]
  contains: [Float!]
  intersects: [Float!]
  is_null: Boolean
}

type FloatAggregation @system {
  count: BigInt
  sum: Float
  avg: Float
  min: Float
  max: Float
  list(distinct: Boolean = false): [Float!]
  any: Float
  last: Float
}

type FloatSubAggregation @system {
  count: BigIntAggregation
  sum: FloatAggregation
  avg: FloatAggregation
  min: FloatAggregation
  max: FloatAggregation
}

enum FloatMeasurementAggregation @system {
  SUM
  AVG
  MIN
  MAX
  ANY
}`
}

func (s *floatScalar) FilterTypeName() string { return "FloatFilter" }

func (s *floatScalar) ListFilterTypeName() string { return "FloatListFilter" }

func (s *floatScalar) AggregationTypeName() string { return "FloatAggregation" }

func (s *floatScalar) MeasurementAggregationTypeName() string {
	return "FloatMeasurementAggregation"
}
