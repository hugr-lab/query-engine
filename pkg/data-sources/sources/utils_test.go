package sources

import "testing"

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string

		wantErr bool
	}{
		{
			name:     "valid dsn",
			dsn:      "postgres://user:password@localhost:5432/dbname",
			expected: "postgres://user:password@localhost:5432/dbname",
		},
		{
			name:     "valid dsn with params",
			dsn:      "postgres://user:password@localhost:5432/dbname?sslmode=disable",
			expected: "postgres://user:password@localhost:5432/dbname?sslmode=disable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseDSN(test.dsn)
			if (err != nil) != test.wantErr {
				t.Fatalf("expected error: %v, got: %v", test.wantErr, err)
			}
			if test.wantErr {
				return
			}
			if result.String() != test.expected {
				t.Errorf("expected %s, got %s", test.expected, result)
			}
		})
	}
}
