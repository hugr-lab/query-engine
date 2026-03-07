package types

import (
	"time"

	pkgtypes "github.com/hugr-lab/query-engine/pkg/types"
)

// Compile-time interface assertions.
var (
	_ ScalarType     = (*intervalScalar)(nil)
	_ Filterable     = (*intervalScalar)(nil)
	_ ListFilterable = (*intervalScalar)(nil)
	_ ValueParser    = (*intervalScalar)(nil)
	_ ArrayParser    = (*intervalScalar)(nil)
)

type intervalScalar struct{}

func (s *intervalScalar) Name() string { return "Interval" }

func (s *intervalScalar) SDL() string {
	return `"""
The ` + "`Interval`" + ` scalar type represents a time duration (e.g. "1 year 2 months 3 days 04:05:06").
Filter operators: eq, gt, gte, lt, lte, is_null
"""
scalar Interval

input IntervalFilter @system {
  eq: Interval
  gt: Interval
  gte: Interval
  lt: Interval
  lte: Interval
  is_null: Boolean
}

input IntervalListFilter @system {
  eq: [Interval!]
  contains: [Interval!]
  intersects: [Interval!]
  is_null: Boolean
}`
}

func (s *intervalScalar) FilterTypeName() string { return "IntervalFilter" }

func (s *intervalScalar) ListFilterTypeName() string { return "IntervalListFilter" }

func (s *intervalScalar) ParseValue(v any) (any, error) {
	return pkgtypes.ParseIntervalValue(v)
}

func (s *intervalScalar) ParseArray(v any) (any, error) {
	vv, err := pkgtypes.ParseScalarArray[string](v)
	if err != nil {
		return nil, err
	}
	out := make([]time.Duration, len(vv))
	for i, val := range vv {
		r, err := pkgtypes.ParseIntervalValue(val)
		if err != nil {
			return nil, err
		}
		out[i] = r
	}
	return out, nil
}
