package types

// Compile-time interface assertions.
var (
	_ ScalarType              = (*timeScalar)(nil)
	_ Filterable              = (*timeScalar)(nil)
	_ ListFilterable          = (*timeScalar)(nil)
	_ Aggregatable            = (*timeScalar)(nil)
	_ MeasurementAggregatable = (*timeScalar)(nil)
)

type timeScalar struct{}

func (s *timeScalar) Name() string { return "Time" }

func (s *timeScalar) SDL() string {
	return `"""
The ` + "`Time`" + ` scalar type represents a time-of-day value encoded as an ISO 8601 time string (e.g. "15:04:05").
On input, values are parsed from ISO 8601 time strings. On output, values are formatted as ISO 8601 time strings.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, min, max, list, any, last
"""
scalar Time`
}

func (s *timeScalar) FilterTypeName() string { return "TimeFilter" }

func (s *timeScalar) FilterSDL() string {
	return `input TimeFilter @system {
  eq: Time
  gt: Time
  gte: Time
  lt: Time
  lte: Time
  in: [Time!]
  is_null: Boolean
}`
}

func (s *timeScalar) ListFilterTypeName() string { return "TimeListFilter" }

func (s *timeScalar) ListFilterSDL() string {
	return `input TimeListFilter @system {
  eq: [Time!]
  contains: [Time!]
  intersects: [Time!]
  is_null: Boolean
}`
}

func (s *timeScalar) AggregationTypeName() string { return "TimeAggregation" }

func (s *timeScalar) AggregationSDL() string {
	return `type TimeAggregation @system {
  count: BigInt
  min: Time
  max: Time
  list(distinct: Boolean = false): [Time!]
  any: Time
  last: Time
}

type TimeSubAggregation @system {
  count: BigIntAggregation
  min: TimeAggregation
  max: TimeAggregation
}`
}

func (s *timeScalar) MeasurementAggregationTypeName() string {
	return "TimeMeasurementAggregation"
}

func (s *timeScalar) MeasurementAggregationSDL() string {
	return `enum TimeMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}
