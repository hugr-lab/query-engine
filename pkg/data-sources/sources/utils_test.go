package sources

import (
	"os"
	"strings"
	"testing"
)

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

func TestApplyEnvVars(t *testing.T) {
	origEnv := os.Environ()
	restoreEnv := func() {
		os.Clearenv()
		for _, kv := range origEnv {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				os.Setenv(parts[0], parts[1])
			}
		}
	}
	defer restoreEnv()

	tests := []struct {
		name      string
		dsn       string
		envKey    string
		envValue  string
		want      string
		wantError bool
	}{
		{
			name:      "no env var in dsn",
			dsn:       "postgres://user:pass@localhost:5432/db",
			want:      "postgres://user:pass@localhost:5432/db",
			wantError: false,
		},
		{
			name:      "env var present and set",
			dsn:       "postgres://user:[$PASSWORD]@localhost:5432/db",
			envKey:    "PASSWORD",
			envValue:  "secret",
			want:      "postgres://user:secret@localhost:5432/db",
			wantError: false,
		},
		{
			name:      "env var present but not set",
			dsn:       "postgres://user:[$MISSING]@localhost:5432/db",
			envKey:    "",
			envValue:  "",
			want:      "",
			wantError: true,
		},
		{
			name:      "multiple env vars",
			dsn:       "postgres://[$USER]:[$PASSWORD]@localhost:5432/db",
			envKey:    "",
			envValue:  "",
			want:      "postgres://testuser:testpass@localhost:5432/db",
			wantError: false,
		},
		{
			name:      "env var with no $ prefix",
			dsn:       "postgres://user:[PASSWORD]@localhost:5432/db",
			want:      "postgres://user:[PASSWORD]@localhost:5432/db",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreEnv()
			if tt.name == "multiple env vars" {
				os.Setenv("USER", "testuser")
				os.Setenv("PASSWORD", "testpass")
			} else if tt.envKey != "" {
				os.Setenv(tt.envKey, tt.envValue)
			}
			got, err := ApplyEnvVars(tt.dsn)
			if (err != nil) != tt.wantError {
				t.Fatalf("expected error: %v, got: %v", tt.wantError, err)
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
