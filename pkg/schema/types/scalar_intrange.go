package types

import pkgtypes "github.com/hugr-lab/query-engine/pkg/types"

// Compile-time interface assertions.
var (
	_ ScalarType  = (*intRangeScalar)(nil)
	_ Filterable  = (*intRangeScalar)(nil)
	_ ValueParser = (*intRangeScalar)(nil)
	_ ArrayParser = (*intRangeScalar)(nil)
)

type intRangeScalar struct{}

func (s *intRangeScalar) Name() string { return "IntRange" }

func (s *intRangeScalar) SDL() string {
	return `"""
The ` + "`IntRange`" + ` scalar type represents a range of integer values with inclusive/exclusive bounds.
Filter operators: eq, contains, intersects, includes, is_null, upper, lower, upper_inclusive, lower_inclusive, upper_inf, lower_inf
"""
scalar IntRange

input IntRangeFilter @system {
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

func (s *intRangeScalar) FilterTypeName() string { return "IntRangeFilter" }

func (s *intRangeScalar) ParseValue(v any) (any, error) {
	return pkgtypes.ParseRangeValue(pkgtypes.RangeTypeInt32, v)
}

func (s *intRangeScalar) ParseArray(v any) (any, error) {
	vv, err := pkgtypes.ParseScalarArray[string](v)
	if err != nil {
		return nil, err
	}
	out := make([]pkgtypes.Int32Range, len(vv))
	for i, val := range vv {
		r, err := pkgtypes.ParseRangeValue(pkgtypes.RangeTypeInt32, val)
		if err != nil {
			return nil, err
		}
		out[i] = r.(pkgtypes.Int32Range)
	}
	return out, nil
}
