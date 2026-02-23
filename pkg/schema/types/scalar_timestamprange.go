package types

// Compile-time interface assertions.
var (
	_ ScalarType = (*timestampRangeScalar)(nil)
	_ Filterable = (*timestampRangeScalar)(nil)
)

type timestampRangeScalar struct{}

func (s *timestampRangeScalar) Name() string { return "TimestampRange" }

func (s *timestampRangeScalar) SDL() string {
	return `"""
The ` + "`TimestampRange`" + ` scalar type represents a range of timestamp values with inclusive/exclusive bounds.
Filter operators: eq, contains, intersects, includes, excludes, is_null, upper, lower, upper_inclusive, lower_inclusive, upper_inf, lower_inf
"""
scalar TimestampRange`
}

func (s *timestampRangeScalar) FilterTypeName() string { return "TimestampRangeFilter" }

func (s *timestampRangeScalar) FilterSDL() string {
	return `input TimestampRangeFilter @system {
  eq: TimestampRange
  contains: Timestamp
  intersects: TimestampRange
  includes: TimestampRange
  excludes: TimestampRange
  is_null: Boolean
  upper: Timestamp
  lower: Timestamp
  upper_inclusive: Boolean
  lower_inclusive: Boolean
  upper_inf: Boolean
  lower_inf: Boolean
}`
}
