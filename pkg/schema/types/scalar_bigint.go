package types

import (
	"fmt"

	pkgtypes "github.com/hugr-lab/query-engine/pkg/types"
)

// Compile-time interface assertions.
var (
	_ ScalarType              = (*bigIntScalar)(nil)
	_ Filterable              = (*bigIntScalar)(nil)
	_ ListFilterable          = (*bigIntScalar)(nil)
	_ Aggregatable            = (*bigIntScalar)(nil)
	_ SubAggregatable         = (*bigIntScalar)(nil)
	_ MeasurementAggregatable = (*bigIntScalar)(nil)
	_ JSONTypeHintProvider    = (*bigIntScalar)(nil)
	_ ValueParser             = (*bigIntScalar)(nil)
	_ ArrayParser             = (*bigIntScalar)(nil)
)

type bigIntScalar struct{}

func (s *bigIntScalar) Name() string { return "BigInt" }

func (s *bigIntScalar) SDL() string {
	return `"""
BigInt - represents big integer type (64 bit), supports filtering, aggregation.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, sum, avg, min, max, list, any, last
"""
scalar BigInt

input BigIntFilter @system {
  eq: BigInt
  gt: BigInt
  gte: BigInt
  lt: BigInt
  lte: BigInt
  in: [BigInt!]
  is_null: Boolean
}

input BigIntListFilter @system {
  eq: [BigInt!]
  contains: [BigInt!]
  intersects: [BigInt!]
  is_null: Boolean
}

type BigIntAggregation @system {
  count: BigInt
  sum: BigInt
  avg: Float
  min: BigInt
  max: BigInt
  list(distinct: Boolean = false): [BigInt!]
  any: BigInt
  last: BigInt
}

type BigIntSubAggregation @system {
  count: BigIntAggregation
  sum: BigIntAggregation
  avg: BigIntAggregation
  min: BigIntAggregation
  max: BigIntAggregation
}

enum BigIntMeasurementAggregation @system {
  SUM
  AVG
  MIN
  MAX
  ANY
}`
}

func (s *bigIntScalar) FilterTypeName() string { return "BigIntFilter" }

func (s *bigIntScalar) ListFilterTypeName() string { return "BigIntListFilter" }

func (s *bigIntScalar) AggregationTypeName() string { return "BigIntAggregation" }

func (s *bigIntScalar) SubAggregationTypeName() string { return "BigIntSubAggregation" }

func (s *bigIntScalar) JSONTypeHint() string { return "number" }

func (s *bigIntScalar) MeasurementAggregationTypeName() string {
	return "BigIntMeasurementAggregation"
}

func (s *bigIntScalar) ParseValue(v any) (any, error) {
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
	return nil, fmt.Errorf("unexpected type %T for BigInt", v)
}

func (s *bigIntScalar) ParseArray(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	vv, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array of BigInt values, got %T", v)
	}
	if len(vv) == 0 {
		return []int64{}, nil
	}
	if _, ok := vv[0].(float64); !ok {
		return pkgtypes.ParseScalarArray[int64](vv)
	}
	dd, err := pkgtypes.ParseScalarArray[float64](v)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(dd))
	for i, val := range dd {
		out[i] = int64(val)
	}
	return out, nil
}
