package ducklake

import (
	"testing"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantMeta     string
		wantData     string
		wantType     string
		wantSecret   string
		wantIsSecret bool
		wantPgConn   string
		wantPgHost   string
		wantPgPass   string
		wantErr      bool
	}{
		{
			name:         "simple secret reference",
			raw:          "ducklake:my_secret",
			wantMeta:     "my_secret",
			wantIsSecret: true,
		},
		{
			name:         "full path with all params",
			raw:          "ducklake:/path/to/meta.db?data_path=s3://bucket/&metadata_type=postgres&metadata_secret=pg_secret",
			wantMeta:     "/path/to/meta.db",
			wantData:     "s3://bucket/",
			wantType:     "postgres",
			wantSecret:   "pg_secret",
			wantIsSecret: false,
		},
		{
			name:         "path without query params",
			raw:          "ducklake:/path/to/meta.db",
			wantMeta:     "/path/to/meta.db",
			wantIsSecret: false, // has '/' so not a secret ref
		},
		{
			name:         "secret ref without ducklake prefix still works",
			raw:          "my_secret",
			wantMeta:     "my_secret",
			wantIsSecret: true,
		},
		{
			name:         "path with dots is not a secret ref",
			raw:          "ducklake:meta.db",
			wantMeta:     "meta.db",
			wantIsSecret: false,
		},
		{
			name:         "path with data_path only",
			raw:          "ducklake:/meta.db?data_path=/data/",
			wantMeta:     "/meta.db",
			wantData:     "/data/",
			wantIsSecret: false,
		},
		// PostgreSQL URI-style paths
		{
			name:       "postgres URI with data_path",
			raw:        "postgres://user:pass@localhost:5432/mydb?data_path=s3://bucket/data/",
			wantData:   "s3://bucket/data/",
			wantType:   "postgres",
			wantPgConn: "dbname=mydb host=localhost port=5432 user=user",
			wantPgHost: "localhost",
			wantPgPass: "pass",
		},
		{
			name:       "postgres URI minimal",
			raw:        "postgres://localhost/mydb",
			wantType:   "postgres",
			wantPgConn: "dbname=mydb host=localhost port=5432",
		},
		{
			name:       "postgres URI with metadata_secret",
			raw:        "postgres://host:5432/db?data_path=/data/&metadata_secret=my_pg_secret",
			wantData:   "/data/",
			wantType:   "postgres",
			wantSecret: "my_pg_secret",
			wantPgConn: "dbname=db host=host port=5432",
		},
		{
			name:       "postgresql:// prefix also works",
			raw:        "postgresql://user@host/db?data_path=/data/",
			wantData:   "/data/",
			wantType:   "postgres",
			wantPgConn: "dbname=db host=host port=5432 user=user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePath(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.MetadataPath != tt.wantMeta {
				t.Errorf("MetadataPath = %q, want %q", got.MetadataPath, tt.wantMeta)
			}
			if got.DataPath != tt.wantData {
				t.Errorf("DataPath = %q, want %q", got.DataPath, tt.wantData)
			}
			if got.MetadataType != tt.wantType {
				t.Errorf("MetadataType = %q, want %q", got.MetadataType, tt.wantType)
			}
			if got.MetadataSecret != tt.wantSecret {
				t.Errorf("MetadataSecret = %q, want %q", got.MetadataSecret, tt.wantSecret)
			}
			if got.IsSecretRef != tt.wantIsSecret {
				t.Errorf("IsSecretRef = %v, want %v", got.IsSecretRef, tt.wantIsSecret)
			}
			if got.PgConnStr != tt.wantPgConn {
				t.Errorf("PgConnStr = %q, want %q", got.PgConnStr, tt.wantPgConn)
			}
			if tt.wantPgHost != "" && (got.pgParams == nil || got.pgParams.Host != tt.wantPgHost) {
				t.Errorf("pgParams.Host = %q, want %q", got.pgParams.Host, tt.wantPgHost)
			}
			if tt.wantPgPass != "" && (got.pgParams == nil || got.pgParams.Password != tt.wantPgPass) {
				t.Errorf("pgParams.Password = %q, want %q", got.pgParams.Password, tt.wantPgPass)
			}
		})
	}
}

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{"a'b'c", "a''b''c"},
		{"", ""},
	}
	for _, tt := range tests {
		got := escapeSQLString(tt.in)
		if got != tt.want {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
