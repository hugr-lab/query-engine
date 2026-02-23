package types

// Compile-time interface assertions.
var (
	_ ScalarType = (*bigIntRangeScalar)(nil)
	_ Filterable = (*bigIntRangeScalar)(nil)
)

type bigIntRangeScalar struct{}

func (s *bigIntRangeScalar) Name() string { return "BigIntRange" }

func (s *bigIntRangeScalar) SDL() string {
	return `"""
The ` + "`BigIntRange`" + ` scalar type represents a range of big integer values with inclusive/exclusive bounds.
Filter operators: eq, contains, intersects, includes, excludes, is_null, upper, lower, upper_inclusive, lower_inclusive, upper_inf, lower_inf
"""
scalar BigIntRange

input BigIntRangeFilter @system {
  eq: BigIntRange
  contains: BigInt
  intersects: BigIntRange
  includes: BigIntRange
  excludes: BigIntRange
  is_null: Boolean
  upper: BigInt
  lower: BigInt
  upper_inclusive: Boolean
  lower_inclusive: Boolean
  upper_inf: Boolean
  lower_inf: Boolean
}`
}

func (s *bigIntRangeScalar) FilterTypeName() string { return "BigIntRangeFilter" }
