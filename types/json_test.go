package types

import (
	"encoding/json"
	"testing"
)

// TestParseJsonValue_StringLiteralForms shows the trap that confuses callers:
// `"123"` and `` `123` `` are the SAME Go string (3 bytes: 1,2,3),
// while `` `"123"` `` (or `"\"123\""`) is a DIFFERENT 5-byte Go string
// with literal double-quote characters inside.
// json.Unmarshal sees only the bytes, so the result type differs.
func TestParseJsonValue_StringLiteralForms(t *testing.T) {
	// Sanity: the two "bare" forms are byte-identical Go strings.
	if "123" != `123` {
		t.Fatalf(`"123" and ` + "`123`" + ` must be the same Go string`)
	}
	if `"123"` != "\"123\"" {
		t.Fatalf("backtick `\"123\"` and double-quoted \"\\\"123\\\"\" must be the same Go string")
	}

	cases := []struct {
		name     string
		input    string
		wantKind string // "number", "string"
		wantVal  any
	}{
		{
			name:     `bare digits "123" → JSON number`,
			input:    "123", // 3 bytes: 1,2,3
			wantKind: "number",
			wantVal:  float64(123), // json.Unmarshal into any always decodes numbers as float64
		},
		{
			name:     "bare digits `123` (raw) → identical to above",
			input:    `123`, // also 3 bytes: 1,2,3
			wantKind: "number",
			wantVal:  float64(123),
		},
		{
			name:     "raw `\"123\"` → JSON string",
			input:    `"123"`, // 5 bytes: ",1,2,3,"
			wantKind: "string",
			wantVal:  "123", // Go string, not a number
		},
		{
			name:     `escaped "\"123\"" → identical to raw form above`,
			input:    "\"123\"", // also 5 bytes: ",1,2,3,"
			wantKind: "string",
			wantVal:  "123",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseJsonValue(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantVal {
				t.Fatalf("value: got %v (%T), want %v (%T)", got, got, tc.wantVal, tc.wantVal)
			}
			switch tc.wantKind {
			case "number":
				if _, ok := got.(float64); !ok {
					t.Fatalf("expected float64 (JSON number), got %T", got)
				}
			case "string":
				if _, ok := got.(string); !ok {
					t.Fatalf("expected string (JSON string), got %T", got)
				}
			}
		})
	}
}

// TestParseJsonValue_StringContentsMustBeValidJSON shows that a Go string
// is parsed as a JSON literal — its content must satisfy JSON grammar.
// Bare identifiers without quotes are NOT valid JSON.
func TestParseJsonValue_StringContentsMustBeValidJSON(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
		wantVal any
	}{
		{name: "bare hello — not valid JSON", input: "hello", wantErr: true},
		{name: `quoted "hello" — valid JSON string`, input: `"hello"`, wantVal: "hello"},
		{name: "true — valid JSON bool", input: "true", wantVal: true},
		{name: "false — valid JSON bool", input: "false", wantVal: false},
		{name: "null — valid JSON, decodes to Go nil", input: "null", wantVal: nil},
		{name: "empty string — invalid JSON", input: "", wantErr: true},
		{name: "JSON array literal", input: "[1,2,3]", wantVal: []any{float64(1), float64(2), float64(3)}},
		{name: `JSON object literal`, input: `{"a":1}`, wantVal: map[string]any{"a": float64(1)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseJsonValue(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got value %v", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if !jsonEqual(got, tc.wantVal) {
				t.Fatalf("value: got %#v, want %#v", got, tc.wantVal)
			}
		})
	}
}

// TestParseJsonValue_NonStringPassthrough shows that any non-string, non-nil
// value is returned AS-IS without any conversion. This is how driver-decoded
// values (pgx, duckdb) flow through unchanged.
//
// Critical asymmetry: ParseJsonValue(123) keeps Go int; ParseJsonValue("123")
// returns float64. Same conceptual number, different Go types.
func TestParseJsonValue_NonStringPassthrough(t *testing.T) {
	t.Run("Go int 123 stays int", func(t *testing.T) {
		got, err := ParseJsonValue(123)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v, ok := got.(int); !ok || v != 123 {
			t.Fatalf("got %v (%T), want int 123", got, got)
		}
	})

	t.Run(`Go string "123" becomes float64 (JSON-decoded)`, func(t *testing.T) {
		got, err := ParseJsonValue("123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v, ok := got.(float64); !ok || v != 123 {
			t.Fatalf("got %v (%T), want float64 123", got, got)
		}
	})

	t.Run("Go slice passes through untouched", func(t *testing.T) {
		in := []int{1, 2, 3}
		got, err := ParseJsonValue(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Same underlying slice — not re-decoded into []any{float64,...}
		out, ok := got.([]int)
		if !ok {
			t.Fatalf("expected []int, got %T", got)
		}
		if len(out) != 3 || out[0] != 1 || out[2] != 3 {
			t.Fatalf("slice contents changed: %v", out)
		}
	})

	t.Run("already-decoded map passes through", func(t *testing.T) {
		in := map[string]any{"k": "v"}
		got, err := ParseJsonValue(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", got)
		}
		if out["k"] != "v" {
			t.Fatalf("map contents changed: %v", out)
		}
	})

	t.Run("nil returns nil with no error", func(t *testing.T) {
		got, err := ParseJsonValue(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
}

// jsonEqual compares two values using JSON serialisation — convenient for
// values that include nested slices/maps where == doesn't work.
func jsonEqual(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}
