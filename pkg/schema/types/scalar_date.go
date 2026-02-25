package types

import (
	"time"

	pkgtypes "github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time interface assertions.
var (
	_ ScalarType              = (*dateScalar)(nil)
	_ Filterable              = (*dateScalar)(nil)
	_ ListFilterable          = (*dateScalar)(nil)
	_ Aggregatable            = (*dateScalar)(nil)
	_ SubAggregatable         = (*dateScalar)(nil)
	_ MeasurementAggregatable = (*dateScalar)(nil)
	_ JSONTypeHintProvider    = (*dateScalar)(nil)
	_ ExtraFieldProvider      = (*dateScalar)(nil)
	_ FieldArgumentsProvider  = (*dateScalar)(nil)
	_ ValueParser             = (*dateScalar)(nil)
	_ ArrayParser             = (*dateScalar)(nil)
)

type dateScalar struct{}

func (s *dateScalar) Name() string { return "Date" }

func (s *dateScalar) SDL() string {
	return `"""
The ` + "`Date`" + ` scalar type represents a calendar date encoded as an ISO 8601 string (e.g. "2006-01-02").
On input, values are parsed from ISO 8601 date strings. On output, values are formatted as ISO 8601 date strings.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, min, max, list, any, last
Extra field: Extract (extracts date parts such as year, month, day)
"""
scalar Date

input DateFilter @system {
  eq: Date
  gt: Date
  gte: Date
  lt: Date
  lte: Date
  in: [Date!]
  is_null: Boolean
}

input DateListFilter @system {
  eq: [Date!]
  contains: [Date!]
  intersects: [Date!]
  is_null: Boolean
}

type DateAggregation @system {
  count: BigInt
  min: Date
  max: Date
  list(distinct: Boolean = false): [Date!]
  any: Date
  last: Date
}

type DateSubAggregation @system {
  count: BigIntAggregation
  min: DateAggregation
  max: DateAggregation
}

enum DateMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}

func (s *dateScalar) FilterTypeName() string { return "DateFilter" }

func (s *dateScalar) ListFilterTypeName() string { return "DateListFilter" }

func (s *dateScalar) AggregationTypeName() string { return "DateAggregation" }

func (s *dateScalar) SubAggregationTypeName() string { return "DateSubAggregation" }

func (s *dateScalar) JSONTypeHint() string { return "timestamp" }

func (s *dateScalar) MeasurementAggregationTypeName() string {
	return "DateMeasurementAggregation"
}

func (s *dateScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "bucket", Type: ast.NamedType("TimeBucket", nil)},
	}
}

func (s *dateScalar) ExtraFieldName() string { return "Extract" }

func (s *dateScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateTimestampExtraField(fieldName, "Timestamp")
}

func (s *dateScalar) ParseValue(v any) (any, error) {
	return pkgtypes.ParseTimeValue(v)
}

func (s *dateScalar) ParseArray(v any) (any, error) {
	return pkgtypes.ParseScalarArray[time.Time](v)
}
