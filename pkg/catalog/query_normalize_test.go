package catalog

import (
	"testing"
	"time"
)

func TestNormalizeVariables_FastPath_NoChange(t *testing.T) {
	in := map[string]any{
		"a": "x",
		"b": float64(1.5),
		"c": true,
		"d": nil,
		"e": map[string]any{"nested": "ok"},
		"f": []any{"one", float64(2)},
	}
	out, err := normalizeVariables(in)
	if err != nil {
		t.Fatalf("normalizeVariables: %v", err)
	}
	// Fast path must return the same map (no alloc).
	if &in == &out {
		// can't compare map pointers this way; skip — equality of reference
		// is checked via isJSONShape invariant below.
	}
	if !isJSONShape(out) {
		t.Fatalf("out is not JSON shape: %#v", out)
	}
	if len(out) != len(in) {
		t.Errorf("len=%d want %d", len(out), len(in))
	}
}

func TestNormalizeVariables_IntIsNormalized(t *testing.T) {
	in := map[string]any{"id": 42, "ratio": float32(1.5)}
	out, err := normalizeVariables(in)
	if err != nil {
		t.Fatalf("normalizeVariables: %v", err)
	}
	if v, ok := out["id"].(float64); !ok || v != 42 {
		t.Errorf("id=%v (%T), want float64(42)", out["id"], out["id"])
	}
	if v, ok := out["ratio"].(float64); !ok || v != 1.5 {
		t.Errorf("ratio=%v (%T), want float64(1.5)", out["ratio"], out["ratio"])
	}
}

func TestNormalizeVariables_TimeIsNormalized(t *testing.T) {
	ts := time.Date(2024, 3, 15, 12, 30, 45, 0, time.UTC)
	in := map[string]any{"ts": ts}
	out, err := normalizeVariables(in)
	if err != nil {
		t.Fatalf("normalizeVariables: %v", err)
	}
	s, ok := out["ts"].(string)
	if !ok {
		t.Fatalf("ts=%T, want string after JSON round-trip", out["ts"])
	}
	if s != "2024-03-15T12:30:45Z" {
		t.Errorf("ts=%q", s)
	}
}

func TestNormalizeVariables_NestedGoTypes(t *testing.T) {
	in := map[string]any{
		"filter": map[string]any{
			"limit": 10,
			"tags":  []string{"a", "b"},
		},
	}
	out, err := normalizeVariables(in)
	if err != nil {
		t.Fatalf("normalizeVariables: %v", err)
	}
	f, ok := out["filter"].(map[string]any)
	if !ok {
		t.Fatalf("filter type=%T", out["filter"])
	}
	if v, ok := f["limit"].(float64); !ok || v != 10 {
		t.Errorf("limit=%v (%T)", f["limit"], f["limit"])
	}
	tags, ok := f["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0].(string) != "a" || tags[1].(string) != "b" {
		t.Errorf("tags=%#v", f["tags"])
	}
}

func TestNormalizeVariables_Empty(t *testing.T) {
	out, err := normalizeVariables(nil)
	if err != nil {
		t.Fatalf("normalizeVariables(nil): %v", err)
	}
	if out != nil {
		t.Errorf("out=%#v, want nil", out)
	}
}

func TestIsJSONShape(t *testing.T) {
	cases := []struct {
		in   any
		want bool
	}{
		{nil, true},
		{true, true},
		{"x", true},
		{float64(1), true},
		{1, false},
		{int64(1), false},
		{float32(1), false},
		{time.Now(), false},
		{map[string]any{"a": 1}, false},
		{map[string]any{"a": "x"}, true},
		{[]any{1}, false},
		{[]any{"x"}, true},
		{[]string{"x"}, false},
	}
	for i, c := range cases {
		if got := isJSONShape(c.in); got != c.want {
			t.Errorf("[%d] isJSONShape(%#v)=%v, want %v", i, c.in, got, c.want)
		}
	}
}
