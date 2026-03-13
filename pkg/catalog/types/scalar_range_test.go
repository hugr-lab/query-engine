package types

import (
	"reflect"
	"testing"
)

func TestParseStringRange(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]interface{}
		err      bool
	}{
		{
			input: "[1,10]",
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive | RangeUpperInclusive,
				"lower": 1,
				"upper": 10,
			},
			err: false,
		},
		{
			input: "(1,10]",
			expected: map[string]interface{}{
				"type":  RangeUpperInclusive,
				"lower": 1,
				"upper": 10,
			},
			err: false,
		},
		{
			input: "[1,10)",
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive,
				"lower": 1,
				"upper": 10,
			},
			err: false,
		},
		{
			input: "(1,10)",
			expected: map[string]interface{}{
				"type":  RangeDetail(0),
				"lower": 1,
				"upper": 10,
			},
			err: false,
		},
		{
			input: "[,]",
			expected: map[string]interface{}{
				"type":  RangeEmpty,
				"lower": nil,
				"upper": nil,
			},
			err: false,
		},
		{
			input: "[1.5,10.5]", // float range types are not supported
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive | RangeUpperInclusive,
				"lower": 1,
				"upper": 10,
			},
			err: false,
		},
		{
			input:    "invalid",
			expected: nil,
			err:      true,
		},
		{
			input: "['a','z']",
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive | RangeUpperInclusive,
				"lower": "a",
				"upper": "z",
			},
			err: false,
		},
		{
			input: "('a','z']",
			expected: map[string]interface{}{
				"type":  RangeUpperInclusive,
				"lower": "a",
				"upper": "z",
			},
			err: false,
		},
		{
			input: "['a','z')",
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive,
				"lower": "a",
				"upper": "z",
			},
			err: false,
		},
		{
			input: "('a','z')",
			expected: map[string]interface{}{
				"type":  RangeDetail(0),
				"lower": "a",
				"upper": "z",
			},
			err: false,
		},
		{
			input: "['','']",
			expected: map[string]interface{}{
				"type":  RangeLowerInclusive | RangeUpperInclusive,
				"lower": "",
				"upper": "",
			},
			err: false,
		},
	}

	for _, test := range tests {
		result, err := parseStringRange(test.input)
		if (err != nil) != test.err {
			t.Errorf("parseStringRange(%q) error = %v, expected error = %v", test.input, err, test.err)
			continue
		}
		if !reflect.DeepEqual(result, test.expected) {
			t.Errorf("parseStringRange(%q) = %v, expected %v", test.input, result, test.expected)
		}
	}
}
