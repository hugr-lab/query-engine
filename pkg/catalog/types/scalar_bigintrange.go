package types

// Compile-time interface assertions.
var (
	_ ScalarType  = (*bigIntRangeScalar)(nil)
	_ Filterable  = (*bigIntRangeScalar)(nil)
	_ ValueParser = (*bigIntRangeScalar)(nil)
	_ ArrayParser = (*bigIntRangeScalar)(nil)
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

func (s *bigIntRangeScalar) ParseValue(v any) (any, error) {
	return ParseRangeValue(RangeTypeInt64, v)
}

func (s *bigIntRangeScalar) ParseArray(v any) (any, error) {
	vv, err := ParseScalarArray[string](v)
	if err != nil {
		return nil, err
	}
	out := make([]Int64Range, len(vv))
	for i, val := range vv {
		r, err := ParseRangeValue(RangeTypeInt64, val)
		if err != nil {
			return nil, err
		}
		out[i] = r.(Int64Range)
	}
	return out, nil
}
