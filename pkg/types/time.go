package types

import (
	"fmt"
	"strings"
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
