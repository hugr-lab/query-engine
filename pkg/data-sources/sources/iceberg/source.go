package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Source struct {
	ds         types.DataSource
	isAttached bool

	engine       engines.Engine
	schemaFilter string
	tableFilter  string
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewIceberg(),
	}, nil
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) prefix() string {
	if s.ds.Prefix != "" {
		return s.ds.Prefix
	}
	return s.ds.Name
}

// icebergParams holds parsed Iceberg connection parameters.
//
// Supported path formats:
//
//  1. REST Catalog (HTTPS):
//     iceberg://endpoint/warehouse?client_id=...&client_secret=...&oauth2_server_uri=...
//
//  1b. REST Catalog (HTTP, for local/dev):
//     iceberg+http://endpoint/warehouse?params
//
//  2. AWS Glue:
//     iceberg+glue://account_id?region=us-east-1
//
//  3. AWS S3 Tables:
//     iceberg+s3tables://arn?region=us-east-1
//
//  4. Secret reference (simple identifier):
//     my_secret_name
//
// Query parameters:
//   - client_id:          OAuth2 client ID
//   - client_secret:      OAuth2 client secret
//   - oauth2_server_uri:  OAuth2 token endpoint
//   - oauth2_scope:       OAuth2 scope
//   - token:              Bearer token (alternative to OAuth2)
//   - region:                  AWS region
//   - access_delegation_mode:  "vended_credentials" for Polaris (optional)
//   - schema_filter:           Regexp to filter namespaces
//   - table_filter:            Regexp to filter tables
type icebergParams struct {
	Warehouse    string // First arg to ATTACH (warehouse name or ARN)
	Endpoint     string // REST endpoint URL
	EndpointType string // "glue" or "s3_tables" or empty (REST)
	IsSecretRef  bool   // true if path references an existing secret
	// Auth
	ClientID        string
	ClientSecret    string
	OAuth2ServerURI string
	OAuth2Scope     string
	Token           string
	// Storage
	Region               string
	AccessDelegationMode string // e.g. "vended_credentials" for Polaris
	// Filters
	SchemaFilter string
	TableFilter  string
}

func parsePath(raw string) (*icebergParams, error) {
	p := &icebergParams{}

	switch {
	case strings.HasPrefix(raw, "iceberg+glue://"):
		return parseGluePath(raw)
	case strings.HasPrefix(raw, "iceberg+s3tables://"):
		return parseS3TablesPath(raw)
	case strings.HasPrefix(raw, "iceberg+http://"):
		return parseRESTPath(raw, "http")
	case strings.HasPrefix(raw, "iceberg://"):
		return parseRESTPath(raw, "https")
	default:
		// Check if simple secret reference (no special chars)
		if !strings.Contains(raw, "/") &&
			!strings.Contains(raw, ":") &&
			!strings.Contains(raw, "?") {
			p.IsSecretRef = true
			p.Warehouse = raw
			return p, nil
		}
		return nil, fmt.Errorf("iceberg: unsupported path format: %s", raw)
	}
}

// parseRESTPath parses iceberg://endpoint/warehouse?params or iceberg+http://endpoint/warehouse?params
func parseRESTPath(raw string, scheme string) (*icebergParams, error) {
	// Replace custom scheme with https:// for URL parsing
	var normalized string
	if scheme == "http" {
		normalized = "https://" + strings.TrimPrefix(raw, "iceberg+http://")
	} else {
		normalized = "https://" + strings.TrimPrefix(raw, "iceberg://")
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("iceberg: invalid REST catalog URI: %w", err)
	}

	// Split path into endpoint prefix and warehouse name.
	// The last path segment is the warehouse; earlier segments (if any)
	// become part of the endpoint URL (e.g. /api/catalog for Apache Polaris).
	pathStr := strings.TrimPrefix(u.Path, "/")
	endpoint := fmt.Sprintf("%s://%s", scheme, u.Host)
	warehouse := pathStr
	if idx := strings.LastIndex(pathStr, "/"); idx >= 0 {
		endpoint += "/" + pathStr[:idx]
		warehouse = pathStr[idx+1:]
	}

	p := &icebergParams{
		Endpoint:  endpoint,
		Warehouse: warehouse,
	}

	parseQueryParams(p, u.Query())
	return p, nil
}

// parseGluePath parses iceberg+glue://account_id?params
func parseGluePath(raw string) (*icebergParams, error) {
	u, err := url.Parse("https://" + strings.TrimPrefix(raw, "iceberg+glue://"))
	if err != nil {
		return nil, fmt.Errorf("iceberg: invalid Glue URI: %w", err)
	}

	p := &icebergParams{
		Warehouse:    u.Host,
		EndpointType: "glue",
	}

	parseQueryParams(p, u.Query())
	return p, nil
}

// parseS3TablesPath parses iceberg+s3tables://arn?params
// ARNs contain colons (e.g. arn:aws:s3tables:...) which break standard URL parsing,
// so we parse manually.
func parseS3TablesPath(raw string) (*icebergParams, error) {
	rest := strings.TrimPrefix(raw, "iceberg+s3tables://")

	p := &icebergParams{
		EndpointType: "s3_tables",
	}

	if idx := strings.Index(rest, "?"); idx >= 0 {
		p.Warehouse = rest[:idx]
		q, err := url.ParseQuery(rest[idx+1:])
		if err != nil {
			return nil, fmt.Errorf("iceberg: invalid S3 Tables query params: %w", err)
		}
		parseQueryParams(p, q)
	} else {
		p.Warehouse = rest
	}

	return p, nil
}

func parseQueryParams(p *icebergParams, q url.Values) {
	p.ClientID = q.Get("client_id")
	p.ClientSecret = q.Get("client_secret")
	p.OAuth2ServerURI = q.Get("oauth2_server_uri")
	p.OAuth2Scope = q.Get("oauth2_scope")
	p.Token = q.Get("token")
	p.Region = q.Get("region")
	p.AccessDelegationMode = q.Get("access_delegation_mode")
	p.SchemaFilter = q.Get("schema_filter")
	p.TableFilter = q.Get("table_filter")
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	path, err := sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return fmt.Errorf("iceberg: failed to apply env vars: %w", err)
	}

	params, err := parsePath(path)
	if err != nil {
		return err
	}

	// Store filter settings for self-describe
	s.schemaFilter = params.SchemaFilter
	s.tableFilter = params.TableFilter

	prefix := s.prefix()
	if prefix == "" {
		return fmt.Errorf("iceberg: prefix or name must be set")
	}
	name := strings.ReplaceAll(strings.ReplaceAll(prefix, ".", "_"), "-", "_")

	// If referencing an existing secret, attach directly
	if params.IsSecretRef {
		return s.attachWithSecret(ctx, pool, params.Warehouse, prefix)
	}

	// Create iceberg secret from inline credentials
	secretName, err := s.createSecret(ctx, pool, params, name)
	if err != nil {
		return err
	}

	// Build ATTACH statement
	return s.attachCatalog(ctx, pool, params, prefix, secretName)
}

func (s *Source) attachWithSecret(ctx context.Context, pool *db.Pool, secretName, prefix string) error {
	var attachParts []string
	attachParts = append(attachParts, "TYPE iceberg")
	attachParts = append(attachParts, fmt.Sprintf("SECRET %s", secretName))
	if s.ds.ReadOnly {
		attachParts = append(attachParts, "READ_ONLY")
	}

	// When using a secret reference, the warehouse name is the secret name itself
	sql := fmt.Sprintf("ATTACH '%s' AS %s (%s);",
		escapeSQLString(secretName),
		engines.Ident(prefix),
		strings.Join(attachParts, ", "),
	)
	_, err := pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("iceberg: attach with secret failed: %w", err)
	}
	s.isAttached = true
	return nil
}

func (s *Source) createSecret(ctx context.Context, pool *db.Pool, params *icebergParams, name string) (string, error) {
	secretName := fmt.Sprintf("_%s_iceberg_secret", name)

	var parts []string
	parts = append(parts, "TYPE iceberg")

	if params.ClientID != "" {
		parts = append(parts, fmt.Sprintf("CLIENT_ID '%s'", escapeSQLString(params.ClientID)))
	}
	if params.ClientSecret != "" {
		parts = append(parts, fmt.Sprintf("CLIENT_SECRET '%s'", escapeSQLString(params.ClientSecret)))
	}
	if params.OAuth2ServerURI != "" {
		parts = append(parts, fmt.Sprintf("OAUTH2_SERVER_URI '%s'", escapeSQLString(params.OAuth2ServerURI)))
	}
	if params.OAuth2Scope != "" {
		parts = append(parts, fmt.Sprintf("OAUTH2_SCOPE '%s'", escapeSQLString(params.OAuth2Scope)))
	}
	if params.Token != "" {
		parts = append(parts, fmt.Sprintf("TOKEN '%s'", escapeSQLString(params.Token)))
	}
	if params.Region != "" {
		parts = append(parts, fmt.Sprintf("REGION '%s'", escapeSQLString(params.Region)))
	}

	// Only create secret if there are auth params
	if len(parts) <= 1 {
		return "", nil
	}

	sql := fmt.Sprintf("CREATE OR REPLACE SECRET %s (\n\t%s\n);",
		secretName, strings.Join(parts, ",\n\t"),
	)
	_, err := pool.Exec(ctx, sql)
	if err != nil {
		return "", fmt.Errorf("iceberg: create secret failed: %w", err)
	}
	return secretName, nil
}

func (s *Source) attachCatalog(ctx context.Context, pool *db.Pool, params *icebergParams, prefix, secretName string) error {
	var attachParts []string
	attachParts = append(attachParts, "TYPE iceberg")

	if params.Endpoint != "" {
		attachParts = append(attachParts, fmt.Sprintf("ENDPOINT '%s'", escapeSQLString(params.Endpoint)))
	}
	if params.EndpointType != "" {
		attachParts = append(attachParts, fmt.Sprintf("ENDPOINT_TYPE '%s'", escapeSQLString(params.EndpointType)))
	}
	if secretName != "" {
		attachParts = append(attachParts, fmt.Sprintf("SECRET %s", secretName))
	} else {
		// No auth credentials — tell DuckDB to skip OAuth2 handshake
		attachParts = append(attachParts, "AUTHORIZATION_TYPE 'none'")
	}
	if params.AccessDelegationMode != "" {
		attachParts = append(attachParts, fmt.Sprintf("ACCESS_DELEGATION_MODE '%s'", escapeSQLString(params.AccessDelegationMode)))
	}
	if s.ds.ReadOnly {
		attachParts = append(attachParts, "READ_ONLY")
	}

	sql := fmt.Sprintf("ATTACH '%s' AS %s (%s);",
		escapeSQLString(params.Warehouse),
		engines.Ident(prefix),
		strings.Join(attachParts, ", "),
	)
	_, err := pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("iceberg: attach failed: %w", err)
	}
	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, pool *db.Pool) error {
	prefix := s.prefix()
	_, err := pool.Exec(ctx, fmt.Sprintf("DETACH DATABASE %s;", engines.Ident(prefix)))
	if err != nil {
		return err
	}
	s.isAttached = false

	// Clean up secrets created during Attach to avoid orphaned secrets.
	name := strings.ReplaceAll(strings.ReplaceAll(prefix, ".", "_"), "-", "_")
	// Best-effort: ignore errors since secrets may not exist (e.g. secret-ref mode).
	pool.Exec(ctx, fmt.Sprintf("DROP SECRET IF EXISTS _%s_iceberg_secret;", name)) //nolint:errcheck

	return nil
}

// escapeSQLString is a local alias for sources.EscapeSQLString.
var escapeSQLString = sources.EscapeSQLString
