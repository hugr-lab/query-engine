package types

// Compile-time interface assertions.
var (
	_ ScalarType = (*intRangeScalar)(nil)
	_ Filterable = (*intRangeScalar)(nil)
)

type intRangeScalar struct{}

func (s *intRangeScalar) Name() string { return "IntRange" }

func (s *intRangeScalar) SDL() string {
	return `"""
The ` + "`IntRange`" + ` scalar type represents a range of integer values with inclusive/exclusive bounds.
Filter operators: eq, contains, intersects, includes, is_null, upper, lower, upper_inclusive, lower_inclusive, upper_inf, lower_inf
"""
scalar IntRange`
}

func (s *intRangeScalar) FilterTypeName() string { return "IntRangeFilter" }

func (s *intRangeScalar) FilterSDL() string {
	return `input IntRangeFilter @system {
  eq: IntRange
  contains: Int
  intersects: IntRange
  includes: IntRange
  is_null: Boolean
  upper: Int
  lower: Int
  upper_inclusive: Boolean
  lower_inclusive: Boolean
  upper_inf: Boolean
  lower_inf: Boolean
}`
}
