package types

import (
	"fmt"
	"strings"
	"time"
)

// Compile-time interface assertions.
var (
	_ ScalarType              = (*timeScalar)(nil)
	_ Filterable              = (*timeScalar)(nil)
	_ ListFilterable          = (*timeScalar)(nil)
	_ Aggregatable            = (*timeScalar)(nil)
	_ SubAggregatable         = (*timeScalar)(nil)
	_ MeasurementAggregatable = (*timeScalar)(nil)
	_ JSONTypeHintProvider    = (*timeScalar)(nil)
	_ ValueParser             = (*timeScalar)(nil)
	_ ArrayParser             = (*timeScalar)(nil)
)

type timeScalar struct{}

func (s *timeScalar) Name() string { return "Time" }

func (s *timeScalar) SDL() string {
	return `"""
The ` + "`Time`" + ` scalar type represents a time-of-day value encoded as an ISO 8601 time string (e.g. "15:04:05").
On input, values are parsed from ISO 8601 time strings. On output, values are formatted as ISO 8601 time strings.
Filter operators: eq, gt, gte, lt, lte, in, is_null
Aggregation functions: count, min, max, list, any, last
"""
scalar Time

input TimeFilter @system {
  eq: Time
  gt: Time
  gte: Time
  lt: Time
  lte: Time
  in: [Time!]
  is_null: Boolean
}

input TimeListFilter @system {
  eq: [Time!]
  contains: [Time!]
  intersects: [Time!]
  is_null: Boolean
}

type TimeAggregation @system {
  count: BigInt
  min: Time
  max: Time
  list(distinct: Boolean = false): [Time!]
  any: Time
  last: Time
}

type TimeSubAggregation @system {
  count: BigIntAggregation
  min: TimeAggregation
  max: TimeAggregation
}

enum TimeMeasurementAggregation @system {
  MIN
  MAX
  ANY
}`
}

func (s *timeScalar) FilterTypeName() string { return "TimeFilter" }

func (s *timeScalar) ListFilterTypeName() string { return "TimeListFilter" }

func (s *timeScalar) AggregationTypeName() string { return "TimeAggregation" }

func (s *timeScalar) SubAggregationTypeName() string { return "TimeSubAggregation" }

func (s *timeScalar) JSONTypeHint() string { return "timestamp" }

func (s *timeScalar) MeasurementAggregationTypeName() string {
	return "TimeMeasurementAggregation"
}

func (s *timeScalar) ParseValue(v any) (any, error) {
	return ParseTimeValue(v)
}

func (s *timeScalar) ParseArray(v any) (any, error) {
	return ParseScalarArray[time.Time](v)
}

func ParseTimeValue(v any) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}
	switch v := v.(type) {
	case time.Time:
		return v, nil
	case int:
		return time.Unix(int64(v), 0), nil
	case int64:
		return time.Unix(int64(v), 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	case string:
		switch len(v) {
		case 0:
			return time.Time{}, nil
		case 4: // 2025
			return time.Parse("2006", v)
		case 7: // 2025-01
			return time.Parse("2006-01", v)
		case 8: // HH:mm:ss
			return time.Parse("15:04:05", v)
		case 10: // 2025-01-01
			if strings.Contains(v, ".") { // 02.01.2006
				return time.Parse("02.01.2006", v)
			}
			return time.Parse("2006-01-02", v)
		case 16: // 2025-01-01 15:04
			return time.Parse("2006-01-02 15:04", v)
		case 19: // 2025-01-01T15:04:05 or 2025-01-01 15:04:05
			if strings.Contains(v, "T") {
				return time.Parse("2006-01-02T15:04:05", v)
			}
			return time.Parse("2006-01-02 15:04:05", v)
		default:
			return time.Parse(time.RFC3339, v)
		}
	default:
		return time.Time{}, fmt.Errorf("invalid time value: %v", v)
	}
}
