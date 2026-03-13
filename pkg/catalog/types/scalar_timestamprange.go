package types

// Compile-time interface assertions.
var (
	_ ScalarType  = (*timestampRangeScalar)(nil)
	_ Filterable  = (*timestampRangeScalar)(nil)
	_ ValueParser = (*timestampRangeScalar)(nil)
	_ ArrayParser = (*timestampRangeScalar)(nil)
)

type timestampRangeScalar struct{}

func (s *timestampRangeScalar) Name() string { return "TimestampRange" }

func (s *timestampRangeScalar) SDL() string {
	return `"""
The ` + "`TimestampRange`" + ` scalar type represents a range of timestamp values with inclusive/exclusive bounds.
Filter operators: eq, contains, intersects, includes, excludes, is_null, upper, lower, upper_inclusive, lower_inclusive, upper_inf, lower_inf
"""
scalar TimestampRange

input TimestampRangeFilter @system {
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

func (s *timestampRangeScalar) FilterTypeName() string { return "TimestampRangeFilter" }

func (s *timestampRangeScalar) ParseValue(v any) (any, error) {
	return ParseRangeValue(RangeTypeTimestamp, v)
}

func (s *timestampRangeScalar) ParseArray(v any) (any, error) {
	vv, err := ParseScalarArray[string](v)
	if err != nil {
		return nil, err
	}
	out := make([]TimeRange, len(vv))
	for i, val := range vv {
		r, err := ParseRangeValue(RangeTypeTimestamp, val)
		if err != nil {
			return nil, err
		}
		out[i] = r.(TimeRange)
	}
	return out, nil
}
