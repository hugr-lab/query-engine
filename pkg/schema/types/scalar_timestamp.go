package types

import "github.com/vektah/gqlparser/v2/ast"

// Compile-time interface assertions.
var (
	_ ScalarType              = (*timestampScalar)(nil)
	_ Filterable              = (*timestampScalar)(nil)
	_ ListFilterable          = (*timestampScalar)(nil)
	_ Aggregatable            = (*timestampScalar)(nil)
	_ MeasurementAggregatable = (*timestampScalar)(nil)
	_ ExtraFieldProvider      = (*timestampScalar)(nil)
	_ FieldArgumentsProvider  = (*timestampScalar)(nil)
)

type timestampScalar struct{}

func (s *timestampScalar) Name() string { return "Timestamp" }

func (s *timestampScalar) SDL() string {
	return `"""
The ` + "`Timestamp`" + ` scalar type represents a date-time value with timezone, encoded as an RFC 3339 string (e.g. "2006-01-02T15:04:05Z07:00").
On input, values are parsed from RFC 3339 strings. On output, values are formatted as RFC 3339 strings.
Filter operators: eq, gt, gte, lt, lte, is_null
Aggregation functions: count, min, max, list, any, last
Extra field: Extract (extracts date/time parts such as year, month, day, hour, minute, second)
"""
scalar Timestamp

input TimestampFilter @system {
  eq: Timestamp
  gt: Timestamp
  gte: Timestamp
  lt: Timestamp
  lte: Timestamp
  is_null: Boolean
}

input TimestampListFilter @system {
  eq: [Timestamp!]
  contains: [Timestamp!]
  intersects: [Timestamp!]
  is_null: Boolean
}

type TimestampAggregation @system {
  count: BigInt
  min: Timestamp
  max: Timestamp
  list(distinct: Boolean = false): [Timestamp!]
  any: Timestamp
  last: Timestamp
}

type TimestampSubAggregation @system {
  count: BigIntAggregation
  min: TimestampAggregation
  max: TimestampAggregation
}

enum TimestampMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}

func (s *timestampScalar) FilterTypeName() string { return "TimestampFilter" }

func (s *timestampScalar) ListFilterTypeName() string { return "TimestampListFilter" }

func (s *timestampScalar) AggregationTypeName() string { return "TimestampAggregation" }

func (s *timestampScalar) MeasurementAggregationTypeName() string {
	return "TimestampMeasurementAggregation"
}

func (s *timestampScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "bucket", Type: ast.NamedType("TimeBucket", nil)},
		{Name: "bucket_interval", Type: ast.NamedType("Interval", nil)},
	}
}

func (s *timestampScalar) ExtraFieldName() string { return "Extract" }

func (s *timestampScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateTimestampExtraField(fieldName, "Timestamp")
}
