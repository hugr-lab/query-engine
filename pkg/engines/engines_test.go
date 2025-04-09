package engines

import (
	"testing"
)

func Test_ApplyQueryParams(t *testing.T) {
	engine := &DuckDB{}

	tests := []struct {
		name     string
		query    string
		params   []any
		applied  int
		expected string
		err      bool
	}{
		{
			name:     "no params",
			query:    "SELECT * FROM table",
			params:   []any{},
			applied:  0,
			expected: "SELECT * FROM table",
		},
		{
			name:     "no params",
			query:    "SELECT * FROM table",
			params:   []any{1, 2, 3},
			applied:  3,
			expected: "SELECT * FROM table",
		},
		{
			name:     "single param",
			query:    "SELECT * FROM table WHERE id = $1",
			params:   []any{1},
			applied:  0,
			expected: "SELECT * FROM table WHERE id = 1",
		},
		{
			name:     "multiple params",
			query:    "SELECT * FROM table WHERE id = $1 AND name = $2",
			params:   []any{1, "test"},
			applied:  0,
			expected: "SELECT * FROM table WHERE id = 1 AND name = 'test'",
		},
		{
			name:     "start index",
			query:    "SELECT * FROM table WHERE id = $3 AND name = $2",
			params:   []any{1, "test", 23},
			applied:  1,
			expected: "SELECT * FROM table WHERE id = 23 AND name = 'test'",
		},
		{
			name:     "not enough params",
			query:    "SELECT * FROM table WHERE id = $1 AND name = $2",
			params:   []any{1},
			applied:  0,
			expected: "",
			err:      true,
		},
		{
			name:     "too many params",
			query:    "SELECT * FROM table WHERE id = $1",
			params:   []any{1, "test"},
			expected: "",
			applied:  0,
			err:      true,
		},
		{
			name:     "only last params",
			query:    "SELECT * FROM table WHERE id = $2",
			params:   []any{1, "test"},
			applied:  1,
			expected: "SELECT * FROM table WHERE id = 'test'",
		},
		{
			name:     "only last params",
			query:    "SELECT $3, $4 FROM table WHERE id = $2",
			params:   []any{1, "test", 23, 44},
			applied:  1,
			expected: "SELECT 23, 44 FROM table WHERE id = 'test'",
		},
		{
			name:     "not last params",
			query:    "SELECT $3 FROM table WHERE id = $2",
			params:   []any{1, "test", 23, 44},
			applied:  0,
			expected: "",
			err:      true,
		},
		{
			name:     "not sequential last params",
			query:    "SELECT $4 FROM table WHERE id = $2",
			params:   []any{1, "test", 23, 44},
			applied:  0,
			expected: "",
			err:      true,
		},
		{
			name:     "complex case inside string",
			query:    "SELECT ('$3', $4, '''$2') FROM table WHERE id = $4",
			params:   []any{1, "test", 23, 44},
			applied:  3,
			expected: "SELECT ('$3', 44, '''$2') FROM table WHERE id = 44",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, applied, err := ApplyQueryParams(engine, tt.query, tt.params)
			if result != tt.expected ||
				applied != tt.applied ||
				tt.err != (err != nil) {
				t.Errorf("applyParams() = %v, %d, %v; want %v, %d, %v", result, applied, err, tt.expected, tt.applied, tt.err)
			}
		})
	}
}
