package jq

import (
	"fmt"
	"time"

	"github.com/itchyny/gojq"
)

func timeFuncs() []gojq.CompilerOption {
	return []gojq.CompilerOption{
		gojq.WithFunction("localTime", 0, 0, func(_ any, _ []any) any {
			return time.Now()
		}),
		gojq.WithFunction("utcTime", 0, 0, func(_ any, _ []any) any {
			return time.Now().UTC()
		}),
		gojq.WithFunction("unixTime", 0, 1, func(a1 any, _ []any) any {
			if a1 == nil {
				return time.Now().UTC().Unix()
			}
			t, err := parseTime(a1, nil)
			if err != nil {
				return err
			}
			return t.Unix()
		}),
		gojq.WithFunction("roundTime", 1, 2, func(a1 any, a2 []any) any {
			if len(a2) < 1 || len(a2) > 2 {
				return ErrInvalidFunctionParam
			}
			durStr, ok := a2[0].(string)
			if !ok {
				return ErrInvalidFunctionParam
			}
			var tz *time.Location
			if len(a2) == 2 {
				tzStr, ok := a2[1].(string)
				if !ok {
					return ErrInvalidFunctionParam
				}
				loc, err := time.LoadLocation(tzStr)
				if err != nil {
					return fmt.Errorf("invalid timezone for roundTime: %v", err)
				}
				tz = loc
			}
			t, err := parseTime(a1, tz)
			if err != nil {
				return err
			}
			y, m, d := t.Date()
			wd := int(t.Weekday())
			if wd == 0 {
				wd = 7
			}
			switch durStr {
			case "second", "seconds":
				return t.Round(time.Second)
			case "minute", "minutes":
				return t.Round(time.Minute)
			case "hour", "hours":
				return time.Date(y, m, d, t.Hour(), 0, 0, 0, t.Location())
			case "day":
				return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
			case "month", "months":
				return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
			case "monday":
				// round to the start of the week (Monday)
				wd := int(t.Weekday())
				if wd == 0 {
					wd = 7
				}
				return time.Date(y, m, d-(wd-1), 0, 0, 0, 0, t.Location())
			case "tuesday":
				// round to the nearest Tuesday (start of ISO week)
				offset := 2 - wd
				if offset > 2 {
					offset -= 7
				}
				return time.Date(y, m, d+offset, 0, 0, 0, 0, t.Location())
			case "wednesday":
				// round to the nearest Wednesday (middle of the week)
				offset := 3 - wd
				if offset > 3 {
					offset -= 7
				}
				return time.Date(y, m, d+offset, 0, 0, 0, 0, t.Location())
			case "thursday":
				// round to the nearest Thursday (middle of the week)
				offset := 4 - wd
				if offset > 4 {
					offset -= 7
				}
				return time.Date(y, m, d+offset, 0, 0, 0, 0, t.Location())
			case "friday":
				// round to the nearest Friday (end of the week)
				offset := 5 - wd
				if offset > 5 {
					offset -= 7
				}
				return time.Date(y, m, d+offset, 0, 0, 0, 0, t.Location())
			case "saturday":
				// round to the nearest Saturday (end of the week)
				offset := 6 - wd
				if offset > 6 {
					offset -= 7
				}
				return time.Date(y, m, d+offset, 0, 0, 0, 0, t.Location())
			case "year", "years":
				return time.Date(y, 1, 1, 0, 0, 0, 0, t.Location())
			default:
				dur, err := time.ParseDuration(durStr)
				if err != nil {
					return fmt.Errorf("invalid duration for roundTime: %v", err)
				}
				return t.Round(dur)
			}
		}),
		gojq.WithFunction("timeAdd", 1, 2, func(a1 any, a2 []any) any {
			if len(a2) < 1 || len(a2) > 2 {
				return ErrInvalidFunctionParam
			}
			durStr, ok := a2[0].(string)
			if !ok {
				return ErrInvalidFunctionParam
			}
			var tz *time.Location
			if len(a2) == 2 {
				tzStr, ok := a2[1].(string)
				if !ok {
					return ErrInvalidFunctionParam
				}
				loc, err := time.LoadLocation(tzStr)
				if err != nil {
					return fmt.Errorf("invalid timezone for timeAdd: %v", err)
				}
				tz = loc
			}
			t, err := parseTime(a1, tz)
			if err != nil {
				return err
			}
			dur, err := time.ParseDuration(durStr)
			if err != nil {
				return fmt.Errorf("invalid duration for timeAdd: %v", err)
			}
			return t.Add(dur)
		}),
		gojq.WithFunction("datePart", 1, 2, func(a1 any, a2 []any) any {
			if len(a2) < 1 || len(a2) > 2 {
				return ErrInvalidFunctionParam
			}
			part, ok := a2[0].(string)
			if !ok {
				return ErrInvalidFunctionParam
			}
			var tz *time.Location
			if len(a2) == 2 {
				tzStr, ok := a2[1].(string)
				if !ok {
					return ErrInvalidFunctionParam
				}
				loc, err := time.LoadLocation(tzStr)
				if err != nil {
					return fmt.Errorf("invalid timezone for datePart: %v", err)
				}
				tz = loc
			}
			t, err := parseTime(a1, tz)
			if err != nil {
				return err
			}
			switch part {
			case "second":
				return t.Second()
			case "minute":
				return t.Minute()
			case "hour":
				return t.Hour()
			case "day":
				return t.Day()
			case "month":
				return int(t.Month())
			case "year":
				return t.Year()
			case "weekday":
				wd := int(t.Weekday())
				if wd == 0 {
					wd = 7
				}
				return wd
			case "yearday":
				return t.YearDay()
			default:
				return fmt.Errorf("invalid part for datePart: %s", part)
			}
		}),
	}
}

func parseTime(v any, tz *time.Location) (time.Time, error) {
	switch t := v.(type) {
	case string:
		return time.Parse(time.RFC3339, t)
	case time.Time:
		return t, nil
	case float64:
		ts := int64(t)
		ns := int64((t - float64(ts)) * float64(time.Second))
		if tz != nil {
			return time.Unix(ts, ns).In(tz), nil
		}
		return time.Unix(ts, ns), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time value: %v", v)
	}
}
