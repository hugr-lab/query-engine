package types

import pkgtypes "github.com/hugr-lab/query-engine/pkg/types"

// Compile-time interface assertions.
var (
	_ ScalarType              = (*booleanScalar)(nil)
	_ Filterable              = (*booleanScalar)(nil)
	_ Aggregatable            = (*booleanScalar)(nil)
	_ SubAggregatable         = (*booleanScalar)(nil)
	_ MeasurementAggregatable = (*booleanScalar)(nil)
	_ JSONTypeHintProvider    = (*booleanScalar)(nil)
	_ ArrayParser             = (*booleanScalar)(nil)
)

// Note: booleanScalar does NOT implement ListFilterable.

type booleanScalar struct{}

func (s *booleanScalar) Name() string { return "Boolean" }

func (s *booleanScalar) SDL() string {
	return `"""
The ` + "`Boolean`" + ` scalar type represents ` + "`true`" + ` or ` + "`false`" + `.
Filter operators: eq, is_null
Aggregation functions: count, bool_and, bool_or, list, any, last
"""
scalar Boolean

input BooleanFilter @system {
  eq: Boolean
  is_null: Boolean
}

type BooleanAggregation @system {
  count: BigInt
  bool_and: Boolean
  bool_or: Boolean
  list(distinct: Boolean = false): [Boolean!]
  any: Boolean
  last: Boolean
}

type BooleanSubAggregation @system {
  count: BigIntAggregation
  bool_and: BooleanAggregation
  bool_or: BooleanAggregation
}

enum BooleanMeasurementAggregation @system {
  ANY
  OR
  AND
}`
}

func (s *booleanScalar) FilterTypeName() string { return "BooleanFilter" }

func (s *booleanScalar) AggregationTypeName() string { return "BooleanAggregation" }

func (s *booleanScalar) SubAggregationTypeName() string { return "BooleanSubAggregation" }

func (s *booleanScalar) JSONTypeHint() string { return "bool" }

func (s *booleanScalar) MeasurementAggregationTypeName() string {
	return "BooleanMeasurementAggregation"
}

func (s *booleanScalar) ParseArray(v any) (any, error) {
	return pkgtypes.ParseScalarArray[bool](v)
}
