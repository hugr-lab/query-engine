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
scalar Time

input TimeFilter @system {
  eq: Time
  gt: Time
  gte: Time
  lt: Time
  lte: Time
  in: [Time!]
  is_null: Boolean
}

input TimeListFilter @system {
  eq: [Time!]
  contains: [Time!]
  intersects: [Time!]
  is_null: Boolean
}

type TimeAggregation @system {
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
}

enum TimeMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}

func (s *timeScalar) FilterTypeName() string { return "TimeFilter" }

func (s *timeScalar) ListFilterTypeName() string { return "TimeListFilter" }

func (s *timeScalar) AggregationTypeName() string { return "TimeAggregation" }

func (s *timeScalar) MeasurementAggregationTypeName() string {
	return "TimeMeasurementAggregation"
}
