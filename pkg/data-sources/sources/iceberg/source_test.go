package iceberg

import (
	"testing"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantErr  bool
		check    func(t *testing.T, p *icebergParams)
	}{
		{
			name: "REST catalog with credentials",
			raw:  "iceberg://localhost:8181/warehouse?client_id=admin&client_secret=password&oauth2_server_uri=http://localhost:8181/v1/oauth/tokens",
			check: func(t *testing.T, p *icebergParams) {
				if p.Endpoint != "https://localhost:8181" {
					t.Errorf("endpoint = %q, want %q", p.Endpoint, "https://localhost:8181")
				}
				if p.Warehouse != "warehouse" {
					t.Errorf("warehouse = %q, want %q", p.Warehouse, "warehouse")
				}
				if p.ClientID != "admin" {
					t.Errorf("client_id = %q, want %q", p.ClientID, "admin")
				}
				if p.ClientSecret != "password" {
					t.Errorf("client_secret = %q, want %q", p.ClientSecret, "password")
				}
				if p.OAuth2ServerURI != "http://localhost:8181/v1/oauth/tokens" {
					t.Errorf("oauth2_server_uri = %q, want %q", p.OAuth2ServerURI, "http://localhost:8181/v1/oauth/tokens")
				}
				if p.IsSecretRef {
					t.Error("should not be secret ref")
				}
				if p.EndpointType != "" {
					t.Errorf("endpoint_type = %q, want empty", p.EndpointType)
				}
			},
		},
		{
			name: "REST catalog with bearer token",
			raw:  "iceberg://catalog.example.com/my_warehouse?token=my_bearer_token",
			check: func(t *testing.T, p *icebergParams) {
				if p.Endpoint != "https://catalog.example.com" {
					t.Errorf("endpoint = %q, want %q", p.Endpoint, "https://catalog.example.com")
				}
				if p.Warehouse != "my_warehouse" {
					t.Errorf("warehouse = %q, want %q", p.Warehouse, "my_warehouse")
				}
				if p.Token != "my_bearer_token" {
					t.Errorf("token = %q, want %q", p.Token, "my_bearer_token")
				}
			},
		},
		{
			name: "REST catalog with filters",
			raw:  "iceberg://localhost:8181/warehouse?schema_filter=^default$&table_filter=^users",
			check: func(t *testing.T, p *icebergParams) {
				if p.SchemaFilter != "^default$" {
					t.Errorf("schema_filter = %q, want %q", p.SchemaFilter, "^default$")
				}
				if p.TableFilter != "^users" {
					t.Errorf("table_filter = %q, want %q", p.TableFilter, "^users")
				}
			},
		},
		{
			name: "REST catalog with oauth2_scope",
			raw:  "iceberg://lakekeeper:8181/warehouse?client_id=admin&client_secret=pass&oauth2_scope=PRINCIPAL_ROLE:ALL&oauth2_server_uri=http://lakekeeper:8181/v1/oauth/tokens",
			check: func(t *testing.T, p *icebergParams) {
				if p.OAuth2Scope != "PRINCIPAL_ROLE:ALL" {
					t.Errorf("oauth2_scope = %q, want %q", p.OAuth2Scope, "PRINCIPAL_ROLE:ALL")
				}
			},
		},
		{
			name: "AWS Glue",
			raw:  "iceberg+glue://123456789?region=us-east-1",
			check: func(t *testing.T, p *icebergParams) {
				if p.Warehouse != "123456789" {
					t.Errorf("warehouse = %q, want %q", p.Warehouse, "123456789")
				}
				if p.EndpointType != "glue" {
					t.Errorf("endpoint_type = %q, want %q", p.EndpointType, "glue")
				}
				if p.Region != "us-east-1" {
					t.Errorf("region = %q, want %q", p.Region, "us-east-1")
				}
			},
		},
		{
			name: "AWS S3 Tables",
			raw:  "iceberg+s3tables://arn:aws:s3tables:us-east-1:123456789:bucket/my-bucket?region=us-east-1",
			check: func(t *testing.T, p *icebergParams) {
				if p.EndpointType != "s3_tables" {
					t.Errorf("endpoint_type = %q, want %q", p.EndpointType, "s3_tables")
				}
				if p.Region != "us-east-1" {
					t.Errorf("region = %q, want %q", p.Region, "us-east-1")
				}
			},
		},
		{
			name: "secret reference",
			raw:  "my_iceberg_secret",
			check: func(t *testing.T, p *icebergParams) {
				if !p.IsSecretRef {
					t.Error("should be secret ref")
				}
				if p.Warehouse != "my_iceberg_secret" {
					t.Errorf("warehouse = %q, want %q", p.Warehouse, "my_iceberg_secret")
				}
			},
		},
		{
			name: "REST catalog with HTTP scheme",
			raw:  "iceberg+http://localhost:8181/warehouse?client_id=admin&client_secret=pass",
			check: func(t *testing.T, p *icebergParams) {
				if p.Endpoint != "http://localhost:8181" {
					t.Errorf("endpoint = %q, want %q", p.Endpoint, "http://localhost:8181")
				}
				if p.Warehouse != "warehouse" {
					t.Errorf("warehouse = %q, want %q", p.Warehouse, "warehouse")
				}
				if p.ClientID != "admin" {
					t.Errorf("client_id = %q, want %q", p.ClientID, "admin")
				}
			},
		},
		{
			name: "s3_secret param",
			raw:  "iceberg://localhost:8181/warehouse?s3_secret=my_s3_creds",
			check: func(t *testing.T, p *icebergParams) {
				if p.S3Secret != "my_s3_creds" {
					t.Errorf("s3_secret = %q, want %q", p.S3Secret, "my_s3_creds")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parsePath(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, p)
			}
		})
	}
}
