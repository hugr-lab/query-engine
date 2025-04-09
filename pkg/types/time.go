package types

import (
	"fmt"
	"time"
)

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
		return time.Parse(time.RFC3339, v)
	default:
		return time.Time{}, fmt.Errorf("invalid time value: %v", v)
	}
}
