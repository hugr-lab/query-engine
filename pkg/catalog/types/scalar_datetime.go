package types

import (
	"fmt"

	pkgtypes "github.com/hugr-lab/query-engine/types"

	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time interface assertions.
var (
	_ ScalarType              = (*datetimeScalar)(nil)
	_ Filterable              = (*datetimeScalar)(nil)
	_ ListFilterable          = (*datetimeScalar)(nil)
	_ Aggregatable            = (*datetimeScalar)(nil)
	_ SubAggregatable         = (*datetimeScalar)(nil)
	_ MeasurementAggregatable = (*datetimeScalar)(nil)
	_ JSONTypeHintProvider    = (*datetimeScalar)(nil)
	_ ExtraFieldProvider      = (*datetimeScalar)(nil)
	_ FieldArgumentsProvider  = (*datetimeScalar)(nil)
	_ ValueParser             = (*datetimeScalar)(nil)
	_ ArrayParser             = (*datetimeScalar)(nil)
	_ SQLOutputTransformer    = (*datetimeScalar)(nil)
)

type datetimeScalar struct{}

func (s *datetimeScalar) Name() string { return "DateTime" }

func (s *datetimeScalar) SDL() string {
	return `"""
The ` + "`DateTime`" + ` scalar type represents a naive date-time value WITHOUT timezone, encoded as an RFC 3339 string (e.g. "2006-01-02T15:04:05").
On input, values are parsed from RFC 3339 strings. On output, values are formatted as RFC 3339 strings.
SET TimeZone has no effect on DateTime values.
Filter operators: eq, gt, gte, lt, lte, is_null
Aggregation functions: count, min, max, list, any, last
Extra field: Extract (extracts date/time parts such as year, month, day, hour, minute, second)
"""
scalar DateTime

input DateTimeFilter @system {
  eq: DateTime
  gt: DateTime
  gte: DateTime
  lt: DateTime
  lte: DateTime
  is_null: Boolean
}

input DateTimeListFilter @system {
  eq: [DateTime!]
  contains: [DateTime!]
  intersects: [DateTime!]
  is_null: Boolean
}

type DateTimeAggregation @system {
  count: BigInt
  min: DateTime
  max: DateTime
  list(distinct: Boolean = false): [DateTime!]
  any: DateTime
  last: DateTime
}

type DateTimeSubAggregation @system {
  count: BigIntAggregation
  min: DateTimeAggregation
  max: DateTimeAggregation
}

enum DateTimeMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}

func (s *datetimeScalar) FilterTypeName() string { return "DateTimeFilter" }

func (s *datetimeScalar) ListFilterTypeName() string { return "DateTimeListFilter" }

func (s *datetimeScalar) AggregationTypeName() string { return "DateTimeAggregation" }

func (s *datetimeScalar) SubAggregationTypeName() string { return "DateTimeSubAggregation" }

func (s *datetimeScalar) JSONTypeHint() string { return "datetime" }

func (s *datetimeScalar) MeasurementAggregationTypeName() string {
	return "DateTimeMeasurementAggregation"
}

func (s *datetimeScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "bucket", Description: "Truncate to the specified part of the datetime. Possible values: 'year', 'month', 'day', 'hour', 'minute', 'second'.", Type: ast.NamedType("TimeBucket", nil)},
		{Name: "bucket_interval", Description: "Truncate the specified part of the datetime", Type: ast.NamedType("Interval", nil)},
	}
}

func (s *datetimeScalar) ExtraFieldName() string { return "Extract" }

func (s *datetimeScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateTimestampExtraField(fieldName, "DateTime")
}

func (s *datetimeScalar) ParseValue(v any) (any, error) {
	t, err := ParseTimeValue(v)
	if err != nil {
		return nil, err
	}
	return pkgtypes.DateTime(t), nil
}

func (s *datetimeScalar) ParseArray(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch v := v.(type) {
	case []pkgtypes.DateTime:
		return v, nil
	case []interface{}:
		a := make([]pkgtypes.DateTime, len(v))
		for i, e := range v {
			t, err := ParseTimeValue(e)
			if err != nil {
				return nil, err
			}
			a[i] = pkgtypes.DateTime(t)
		}
		return a, nil
	default:
		return nil, fmt.Errorf("invalid DateTime array value: %v", v)
	}
}

// ToOutputSQL emits the naive date-time in a canonical RFC 3339 form
// without a timezone offset — the format matches what
// RecordToJSON.emitTimestamp produces for a types.DateTime (an Arrow
// Timestamp whose TimeZone field is empty):
//
//	"YYYY-MM-DDTHH:MM:SS.ffffff"
//
// raw=true returns the column unchanged; the native-Arrow table path
// formats from the Arrow cell on the Go side using the same layout.
func (s *datetimeScalar) ToOutputSQL(sql string, raw bool) string {
	if raw {
		return sql
	}
	return datetimeSQL(sql)
}

// ToStructFieldSQL applies the same RFC 3339 formatting inside a
// STRUCT_PACK nested field.
func (s *datetimeScalar) ToStructFieldSQL(sql string) string {
	return datetimeSQL(sql)
}

// datetimeSQL emits a naive DateTime as RFC3339Nano-with-Z-suffix: the
// wall-clock time is rendered as-if-UTC so the string is strictly-valid
// RFC 3339 and parseable by Go's time.Time.UnmarshalJSON. Matches
// record_json.go:emitTimestamp when the Arrow column has no TimeZone.
// Trailing fractional zeros are trimmed.
func datetimeSQL(sql string) string {
	return fmt.Sprintf(
		`rtrim(rtrim(strftime(%s, '%%Y-%%m-%%dT%%H:%%M:%%S.%%f'), '0'), '.') || 'Z'`,
		sql)
}
