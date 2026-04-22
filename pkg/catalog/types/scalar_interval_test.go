package types

import (
	"testing"
	"time"
)

func TestParseSQLInterval_ISO8601(t *testing.T) {
	const day = 24 * time.Hour
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"PT0S", 0},
		{"PT2H", 2 * time.Hour},
		{"PT1H30M", time.Hour + 30*time.Minute},
		{"PT1M30S", time.Minute + 30*time.Second},
		{"PT1.5S", 1500 * time.Millisecond},
		{"PT0.5S", 500 * time.Millisecond},
		{"P1D", day},
		{"P1DT2H", day + 2*time.Hour},
		{"P2M", 60 * day},
		{"P1Y", 365 * day},
		{"P1Y2M3DT4H5M6.789S", 365*day + 60*day + 3*day + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond},
		{"P0M0DT7200.000000000S", 2 * time.Hour}, // legacy non-normalised form
		{"-PT1H", -time.Hour},
		{"PT-1H", -time.Hour}, // emitter form for negative MonthDayNano
		{"-P1DT2H", -(day + 2*time.Hour)},
	}
	for _, c := range cases {
		got, err := ParseSQLInterval(c.in)
		if err != nil {
			t.Errorf("ParseSQLInterval(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSQLInterval(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestParseSQLInterval_Invalid(t *testing.T) {
	invalid := []string{
		"P",       // no components
		"PT",      // no components
		"P1H",     // hours must be in T portion
		"PT1D",    // days must be before T
		"P-1D",    // negation only allowed on whole or after T
		"1H",      // missing P
		"garbage", // unrelated
	}
	for _, s := range invalid {
		if _, err := parseISO8601Duration(s); err == nil {
			t.Errorf("parseISO8601Duration(%q) expected error, got nil", s)
		}
	}
}

func TestParseSQLInterval_NonISOStillWorks(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"1h30m", time.Hour + 30*time.Minute},       // Go format
		{"10 minutes", 10 * time.Minute},            // SQL format
		{"1 hour 30 seconds", time.Hour + 30*time.Second},
		{"5", 5 * time.Second}, // bare number
	}
	for _, c := range cases {
		got, err := ParseSQLInterval(c.in)
		if err != nil {
			t.Errorf("ParseSQLInterval(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSQLInterval(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
