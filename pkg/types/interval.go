package types

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ParseIntervalValue(v any) (time.Duration, error) {
	if v == nil {
		return 0, nil
	}
	switch v := v.(type) {
	case string:
		d, err := ParseSQLInterval(v)
		if err != nil {
			return 0, err
		}
		return d, nil
	case int:
		return time.Duration(v) * time.Second, nil
	case time.Duration:
		return v, nil
	default:
		return 0, fmt.Errorf("unexpected type %T for interval", v)
	}
}

var intervalRegex = regexp.MustCompile(`(?i)(\d+)\s*(day|hour|minute|second)s?`)

func ParseSQLInterval(s string) (time.Duration, error) {
	matches := intervalRegex.FindAllStringSubmatch(s, -1)
	if matches == nil {
		return 0, fmt.Errorf("invalid interval format: %s", s)
	}

	var totalDuration time.Duration
	for _, match := range matches {
		value, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, fmt.Errorf("invalid number in interval: %s", match[1])
		}

		unit := strings.ToLower(match[2])
		switch unit {
		case "day", "days", "d":
			totalDuration += time.Duration(value) * 24 * time.Hour
		case "hour", "hours", "h":
			totalDuration += time.Duration(value) * time.Hour
		case "minute", "minutes", "m":
			totalDuration += time.Duration(value) * time.Minute
		case "second", "seconds", "s":
			totalDuration += time.Duration(value) * time.Second
		default:
			return 0, fmt.Errorf("invalid time unit in interval: %s", unit)
		}
	}

	return totalDuration, nil
}

func IntervalToSQLValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	d, ok := v.(time.Duration)
	if !ok {
		var err error
		d, err = ParseIntervalValue(v)
		if err != nil {
			return "", err
		}
	}
	hours := int(math.Floor(d.Hours()))
	minutes := int(math.Floor(d.Minutes())) % 60
	seconds := int(d.Seconds()) % 60
	switch {
	case hours != 0 && minutes != 0 && seconds != 0:
		return fmt.Sprintf("'%d hours %d minutes %d seconds'::INTERVAL", hours, minutes, seconds), nil
	case hours != 0 && minutes != 0:
		return fmt.Sprintf("'%d hours %d minutes'::INTERVAL", hours, minutes), nil
	case hours != 0 && seconds != 0:
		return fmt.Sprintf("'%d hours %d seconds'::INTERVAL", hours, seconds), nil
	case minutes != 0 && seconds != 0:
		return fmt.Sprintf("'%d minutes %d seconds'::INTERVAL", minutes, seconds), nil
	case hours != 0:
		return fmt.Sprintf("'%d hours'::INTERVAL", hours), nil
	case minutes != 0:
		return fmt.Sprintf("'%d minutes'::INTERVAL", minutes), nil
	case seconds != 0:
		return fmt.Sprintf("'%d seconds'::INTERVAL", seconds), nil
	default:
		return "'0 seconds'", nil
	}
}
