package types

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type (
	RangeType   int
	RangeDetail int
)

const (
	RangeTypeInt32 RangeType = iota + 1
	RangeTypeInt64
	RangeTypeTimestamp
)

const (
	RangeEmpty RangeDetail = 1 << iota
	RangeUpperInfinity
	RangeLowerInfinity
	RangeUpperInclusive
	RangeLowerInclusive
)

type Int32Range struct {
	Lower, Upper int32
	Detail       RangeDetail
}

type Int64Range struct {
	Lower, Upper int64
	Detail       RangeDetail
}

type TimeRange struct {
	Lower, Upper time.Time
	Detail       RangeDetail
}

func ParseRangeDetail(v any) (RangeDetail, error) {
	if v == nil {
		return 0, nil
	}
	switch v := v.(type) {
	case int:
		return RangeDetail(v), nil
	case string:
		switch v {
		case "empty":
			return RangeEmpty, nil
		case "[]":
			return RangeLowerInclusive | RangeUpperInclusive, nil
		case "[)":
			return RangeLowerInclusive | RangeUpperInfinity, nil
		case "(]":
			return RangeLowerInfinity | RangeUpperInclusive, nil
		case "()":
			return 0, nil
		}
	}
	return 0, fmt.Errorf("invalid range detail: %v", v)
}

func ParseRangeValue(t RangeType, v any) (any, error) {
	var err error
	var parse map[string]any
	switch v := v.(type) {
	case string:
		parse, err = parseStringRange(v)
	case []any:
		if len(v) < 2 {
			return nil, fmt.Errorf("invalid range value")
		}
		parse = map[string]any{
			"type":  RangeLowerInclusive | RangeUpperInclusive,
			"lower": v[0],
			"upper": v[1],
		}
	case map[string]any:
		parse = map[string]any{
			"lower": v["lower"],
			"upper": v["upper"],
		}
		parse["type"], err = ParseRangeDetail(v["type"])
	case Int32Range, Int64Range, TimeRange:
		return v, nil
	case *Int32Range, *Int64Range, *TimeRange:
	}
	if err != nil {
		return nil, err
	}
	if parse == nil {
		return nil, fmt.Errorf("invalid range value")
	}
	b := BaseRange{
		Type:   t,
		Lower:  parse["lower"],
		Upper:  parse["upper"],
		Detail: parse["type"].(RangeDetail),
	}
	if t == RangeTypeTimestamp && b.Lower != nil && b.Lower.(string) != "" {
		b.Lower, err = time.Parse(time.RFC3339, b.Lower.(string))
		if err != nil {
			return nil, fmt.Errorf("invalid lower bound: %w", err)
		}
	}
	if _, ok := b.Lower.(int); t == RangeTypeTimestamp && b.Upper != nil && ok {
		b.Upper = time.Unix(int64(b.Lower.(int)), 0)
	}
	if t == RangeTypeTimestamp && b.Upper != nil && b.Upper.(string) != "" {
		b.Upper, err = time.Parse(time.RFC3339, b.Upper.(string))
		if err != nil {
			return nil, fmt.Errorf("invalid upper bound: %w", err)
		}
	}
	if _, ok := b.Upper.(int); t == RangeTypeTimestamp && b.Upper != nil && ok {
		b.Upper = time.Unix(int64(b.Upper.(int)), 0)
	}

	if b.Lower == nil {
		b.Detail |= RangeLowerInfinity
	}
	if b.Upper == nil {
		b.Detail |= RangeUpperInfinity
	}

	switch t {
	case RangeTypeInt32:
		if _, ok := b.Upper.(int); b.Upper != nil && !ok {
			return nil, fmt.Errorf("invalid upper bound")
		}
		if _, ok := b.Lower.(int); b.Lower != nil && !ok {
			return nil, fmt.Errorf("invalid lower bound")
		}
		return b.ToInt32Range()
	case RangeTypeInt64:
		if _, ok := b.Upper.(float64); b.Upper != nil && !ok {
			return nil, fmt.Errorf("invalid upper bound")
		}
		if _, ok := b.Lower.(float64); b.Lower != nil && !ok {
			return nil, fmt.Errorf("invalid lower bound")
		}
		return b.ToInt64Range()
	case RangeTypeTimestamp:
		if _, ok := b.Upper.(time.Time); b.Upper != nil && !ok {
			return nil, fmt.Errorf("invalid upper bound")
		}
		if _, ok := b.Lower.(time.Time); b.Lower != nil && !ok {
			return nil, fmt.Errorf("invalid lower bound")
		}
		return b.ToTimestampRange()
	}

	return b, nil
}

type BaseRange struct {
	Type         RangeType
	Lower, Upper any
	Detail       RangeDetail
}

func (t RangeDetail) IsEmpty() bool {
	return t&RangeEmpty != 0
}

func (t *RangeDetail) Clear() {
	*t = RangeEmpty
}

func (t RangeDetail) IsUpperInfinity() bool {
	return t&RangeUpperInfinity != 0 && !t.IsEmpty()
}

func (t RangeDetail) IsLowerInfinity() bool {
	return t&RangeLowerInfinity != 0 && !t.IsEmpty()
}

func (t RangeDetail) IsLowerInclusive() bool {
	return t&RangeLowerInclusive != 0 && !t.IsEmpty()
}

func (t RangeDetail) IsUpperInclusive() bool {
	return t&RangeUpperInclusive != 0 && !t.IsEmpty()
}

func (t BaseRange) ToInt32Range() (Int32Range, error) {
	if t.Type != RangeTypeInt32 {
		return Int32Range{}, fmt.Errorf("invalid range type")
	}
	return Int32Range{
		Detail: t.Detail,
		Lower:  int32(t.Lower.(int)),
		Upper:  int32(t.Upper.(int)),
	}, nil
}

func (t BaseRange) ToInt64Range() (Int64Range, error) {
	if t.Type != RangeTypeInt64 {
		return Int64Range{}, fmt.Errorf("invalid range type")
	}
	return Int64Range{
		Detail: t.Detail,
		Lower:  int64(t.Lower.(int)),
		Upper:  int64(t.Upper.(int)),
	}, nil
}

func (t BaseRange) ToTimestampRange() (TimeRange, error) {
	if t.Type != RangeTypeTimestamp {
		return TimeRange{}, fmt.Errorf("invalid range type")
	}
	return TimeRange{
		Detail: t.Detail,
		Lower:  t.Lower.(time.Time),
		Upper:  t.Upper.(time.Time),
	}, nil
}

var rangeRE = regexp.MustCompile(`([\[\(])\s*([^,\s]*)\s*,\s*([^,\s]*)\s*([\]\)])`)

func parseStringRange(input string) (map[string]interface{}, error) {
	// Проверяем совпадение
	matches := rangeRE.FindStringSubmatch(input)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid format")
	}

	// Определяем тип диапазона
	leftBracket := matches[1]
	rightBracket := matches[4]

	// Функция для обработки значений диапазона (числа или строки)
	parseValue := func(value string) (interface{}, error) {
		switch {
		case value == "":
			return nil, nil
		case strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'"):
			// Убираем одинарные кавычки из строкового значения
			return strings.Trim(value, "'"), nil
		case strings.Contains(value, "."):
			v, err := strconv.ParseFloat(value, 64)
			return int(v), err
		default:
			return strconv.Atoi(value)
		}
	}

	// Парсим начало и конец диапазона
	lower, err := parseValue(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid start value: %w", err)
	}
	upper, err := parseValue(matches[3])
	if err != nil {
		return nil, fmt.Errorf("invalid end value: %w", err)
	}

	var rangeType RangeDetail
	if leftBracket == "[" {
		rangeType |= RangeLowerInclusive
	}
	if rightBracket == "]" {
		rangeType |= RangeUpperInclusive
	}
	if lower == nil {
		rangeType |= RangeLowerInfinity
	}
	if upper == nil {
		rangeType |= RangeUpperInfinity
	}
	if lower == nil && upper == nil {
		rangeType = RangeEmpty
	}

	// Формируем результат
	result := map[string]interface{}{
		"type":  rangeType,
		"lower": lower,
		"upper": upper,
	}

	return result, nil
}
