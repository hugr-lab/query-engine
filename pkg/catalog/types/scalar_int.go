package types

import (
	"fmt"
)

// Compile-time interface assertions.
var (
	_ ScalarType              = (*intScalar)(nil)
	_ Filterable              = (*intScalar)(nil)
	_ ListFilterable          = (*intScalar)(nil)
	_ Aggregatable            = (*intScalar)(nil)
	_ SubAggregatable         = (*intScalar)(nil)
	_ MeasurementAggregatable = (*intScalar)(nil)
	_ JSONTypeHintProvider    = (*intScalar)(nil)
	_ ValueParser             = (*intScalar)(nil)
	_ ArrayParser             = (*intScalar)(nil)
)

type intScalar struct{}

func (s *intScalar) Name() string { return "Int" }

func (s *intScalar) SDL() string {
	return `"""
` + "`Int`" + ` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, sum, avg, min, max, list, any, last
"""
scalar Int

input IntFilter @system {
  eq: Int
  gt: Int
  gte: Int
  lt: Int
  lte: Int
  in: [Int!]
  is_null: Boolean
}

input IntListFilter @system {
  eq: [Int!]
  contains: [Int!]
  intersects: [Int!]
  is_null: Boolean
}

type IntAggregation @system {
  count: BigInt
  sum: Int
  avg: Float
  min: Int
  max: Int
  list(distinct: Boolean = false): [Int!]
  any: Int
  last: Int
}

type IntSubAggregation @system {
  count: BigIntAggregation
  sum: IntAggregation
  avg: IntAggregation
  min: IntAggregation
  max: IntAggregation
}

enum IntMeasurementAggregation @system {
  SUM
  AVG
  MIN
  MAX
  ANY
}`
}

func (s *intScalar) FilterTypeName() string { return "IntFilter" }

func (s *intScalar) ListFilterTypeName() string { return "IntListFilter" }

func (s *intScalar) AggregationTypeName() string { return "IntAggregation" }

func (s *intScalar) SubAggregationTypeName() string { return "IntSubAggregation" }

func (s *intScalar) JSONTypeHint() string { return "number" }

func (s *intScalar) MeasurementAggregationTypeName() string {
	return "IntMeasurementAggregation"
}

func (s *intScalar) ParseValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	}
	return nil, fmt.Errorf("unexpected type %T for Int", v)
}

func (s *intScalar) ParseArray(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	vv, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array of Int values, got %T", v)
	}
	if len(vv) == 0 {
		return []int64{}, nil
	}
	if _, ok := vv[0].(float64); !ok {
		return ParseScalarArray[int64](vv)
	}
	dd, err := ParseScalarArray[float64](v)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(dd))
	for i, val := range dd {
		out[i] = int64(val)
	}
	return out, nil
}
