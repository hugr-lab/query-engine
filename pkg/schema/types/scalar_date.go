package types

import "github.com/vektah/gqlparser/v2/ast"

// Compile-time interface assertions.
var (
	_ ScalarType              = (*dateScalar)(nil)
	_ Filterable              = (*dateScalar)(nil)
	_ ListFilterable          = (*dateScalar)(nil)
	_ Aggregatable            = (*dateScalar)(nil)
	_ MeasurementAggregatable = (*dateScalar)(nil)
	_ ExtraFieldProvider      = (*dateScalar)(nil)
	_ FieldArgumentsProvider  = (*dateScalar)(nil)
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
scalar Date`
}

func (s *dateScalar) FilterTypeName() string { return "DateFilter" }

func (s *dateScalar) FilterSDL() string {
	return `input DateFilter @system {
  eq: Date
  gt: Date
  gte: Date
  lt: Date
  lte: Date
  in: [Date!]
  is_null: Boolean
}`
}

func (s *dateScalar) ListFilterTypeName() string { return "DateListFilter" }

func (s *dateScalar) ListFilterSDL() string {
	return `input DateListFilter @system {
  eq: [Date!]
  contains: [Date!]
  intersects: [Date!]
  is_null: Boolean
}`
}

func (s *dateScalar) AggregationTypeName() string { return "DateAggregation" }

func (s *dateScalar) AggregationSDL() string {
	return `type DateAggregation @system {
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
}`
}

func (s *dateScalar) MeasurementAggregationTypeName() string {
	return "DateMeasurementAggregation"
}

func (s *dateScalar) MeasurementAggregationSDL() string {
	return `enum DateMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
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
