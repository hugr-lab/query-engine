package ducklake

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
	schemaFilter string // regexp filter for schema names in self-describe
	tableFilter  string // regexp filter for table names in self-describe
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckLake(),
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

// ducklakeParams holds parsed DuckLake connection parameters.
//
// Supported path formats:
//
//  1. Secret reference (simple identifier):
//     my_secret_name
//
//  2. DuckDB file metadata:
//     /path/to/meta.duckdb?data_path=/path/to/data/
//     meta.duckdb?data_path=s3://bucket/path/
//
//  3. PostgreSQL metadata (URI-style, mirrors Hugr's postgres source format):
//     postgres://user:pass@host:port/dbname?data_path=s3://bucket/path/
//     The PostgreSQL credentials are handled via a DuckDB postgres secret.
//
// Query parameters (on formats 2 and 3):
//   - data_path:            Storage location for data files
//   - metadata_secret:      Name of existing DuckDB secret for remote metadata credentials
//   - schema_filter:        Regexp to filter schemas for self-describe (e.g. "^(public|analytics)$")
//   - table_filter:         Regexp to filter tables for self-describe (e.g. "^(users|orders)$")
//   - automatic_migration:  Set to "true" to enable automatic DuckLake catalog version migration
type ducklakeParams struct {
	MetadataPath       string          // DuckDB file path or empty (for PG via secret)
	DataPath           string          // DATA_PATH for file storage
	MetadataType       string          // Remote metadata backend type: "postgres", "mysql", "sqlite"
	MetadataSecret     string          // DuckDB secret name for remote metadata credentials
	AutomaticMigration bool            // Enable automatic DuckLake catalog version migration
	EnsureDB           bool            // If true, auto-create PostgreSQL database if it doesn't exist
	IsSecretRef        bool            // true if path references an existing DuckLake secret
	PgConnStr          string          // PostgreSQL connection string for logging (parsed from postgres:// URI)
	pgParams           *pgSecretParams // PostgreSQL connection fields (parsed from postgres:// URI)
	SchemaFilter       string          // Regexp to filter schemas for self-describe
	TableFilter        string          // Regexp to filter tables for self-describe
}

func parsePath(raw string) (*ducklakeParams, error) {
	raw = strings.TrimPrefix(raw, "ducklake:")

	p := &ducklakeParams{}

	// Check for PostgreSQL URI: postgres://...
	if strings.HasPrefix(raw, "postgres://") || strings.HasPrefix(raw, "postgresql://") {
		return parsePgPath(raw)
	}

	// Check if there are query params
	if idx := strings.Index(raw, "?"); idx >= 0 {
		p.MetadataPath = raw[:idx]
		query, err := url.ParseQuery(raw[idx+1:])
		if err != nil {
			return nil, fmt.Errorf("ducklake: invalid query parameters: %w", err)
		}
		p.DataPath = query.Get("data_path")
		p.MetadataType = query.Get("metadata_type")
		p.MetadataSecret = query.Get("metadata_secret")
		p.SchemaFilter = query.Get("schema_filter")
		p.TableFilter = query.Get("table_filter")
		p.AutomaticMigration = strings.EqualFold(query.Get("automatic_migration"), "true")
		p.EnsureDB = strings.EqualFold(query.Get("ensure_db"), "true")
	} else {
		p.MetadataPath = raw
	}

	// Determine if this is a secret reference (simple name without path separators or protocol markers)
	if !strings.Contains(p.MetadataPath, "/") &&
		!strings.Contains(p.MetadataPath, ":") &&
		!strings.Contains(p.MetadataPath, ".") &&
		p.DataPath == "" && p.MetadataType == "" {
		p.IsSecretRef = true
	}

	return p, nil
}

// pgSecretParams holds parsed PostgreSQL connection fields for DuckDB secret creation.
type pgSecretParams struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

// parsePgPath parses a postgres://user:pass@host:port/dbname?data_path=... URI.
func parsePgPath(raw string) (*ducklakeParams, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("ducklake: invalid postgres URI: %w", err)
	}

	p := &ducklakeParams{
		MetadataType: "postgres",
	}

	// Extract query params that are hugr-specific (not PG connection params)
	if u.Query().Has("data_path") {
		p.DataPath = u.Query().Get("data_path")
	}
	if u.Query().Has("metadata_secret") {
		p.MetadataSecret = u.Query().Get("metadata_secret")
	}
	p.SchemaFilter = u.Query().Get("schema_filter")
	p.TableFilter = u.Query().Get("table_filter")
	p.AutomaticMigration = strings.EqualFold(u.Query().Get("automatic_migration"), "true")
	p.EnsureDB = strings.EqualFold(u.Query().Get("ensure_db"), "true")

	// Parse PostgreSQL connection fields from URI
	pg := pgSecretParams{
		Host:     u.Hostname(),
		Port:     u.Port(),
		Database: strings.TrimPrefix(u.Path, "/"),
	}
	if u.User != nil {
		pg.User = u.User.Username()
		pg.Password, _ = u.User.Password()
	}
	if pg.Port == "" {
		pg.Port = "5432"
	}

	p.PgConnStr = pg.toConnStr()
	p.pgParams = &pg

	return p, nil
}

// toConnStr returns a libpq-style connection string for logging/debugging.
func (pg *pgSecretParams) toConnStr() string {
	var parts []string
	if pg.Database != "" {
		parts = append(parts, "dbname="+pg.Database)
	}
	if pg.Host != "" {
		parts = append(parts, "host="+pg.Host)
	}
	if pg.Port != "" {
		parts = append(parts, "port="+pg.Port)
	}
	if pg.User != "" {
		parts = append(parts, "user="+pg.User)
	}
	return strings.Join(parts, " ")
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	path, err := sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return fmt.Errorf("ducklake: failed to apply env vars: %w", err)
	}

	params, err := parsePath(path)
	if err != nil {
		return err
	}

	// Auto-create PostgreSQL database if ensure_db=true
	if params.EnsureDB && params.pgParams != nil {
		if err := sources.EnsurePgDatabase(ctx, params.pgParams.Host, params.pgParams.Port, params.pgParams.User, params.pgParams.Password, params.pgParams.Database); err != nil {
			return fmt.Errorf("ducklake: %w", err)
		}
	}

	// Store filter settings for self-describe
	s.schemaFilter = params.SchemaFilter
	s.tableFilter = params.TableFilter

	prefix := s.prefix()
	if prefix == "" {
		return fmt.Errorf("ducklake: prefix or name must be set")
	}
	name := strings.ReplaceAll(strings.ReplaceAll(prefix, ".", "_"), "-", "_")

	attachOpts := s.attachOptions(params)

	// If referencing an existing secret, attach directly
	if params.IsSecretRef {
		sql := fmt.Sprintf("ATTACH 'ducklake:%s' AS %s%s;",
			escapeSQLString(params.MetadataPath),
			engines.Ident(prefix),
			attachOpts,
		)
		_, err = pool.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("ducklake: attach failed: %w", err)
		}
		s.isAttached = true
		return nil
	}

	// PostgreSQL metadata path: create a postgres secret + ducklake secret
	if params.PgConnStr != "" {
		return s.attachWithPgMetadata(ctx, pool, params, prefix, name, attachOpts)
	}

	// DuckDB file metadata: create ducklake secret
	return s.attachWithFileMetadata(ctx, pool, params, prefix, name, attachOpts)
}

// attachWithPgMetadata creates a PostgreSQL secret for metadata and a DuckLake secret referencing it.
func (s *Source) attachWithPgMetadata(ctx context.Context, pool *db.Pool, params *ducklakeParams, prefix, name, attachOpts string) error {
	var stmts []string

	pgSecretName := params.MetadataSecret
	if pgSecretName == "" {
		// Create a postgres secret for the metadata connection using individual fields
		pgSecretName = fmt.Sprintf("_%s_pg_secret", name)
		pg := params.pgParams
		var pgParts []string
		pgParts = append(pgParts, "TYPE postgres")
		if pg.Host != "" {
			pgParts = append(pgParts, fmt.Sprintf("HOST '%s'", escapeSQLString(pg.Host)))
		}
		if pg.Port != "" {
			pgParts = append(pgParts, fmt.Sprintf("PORT '%s'", escapeSQLString(pg.Port)))
		}
		if pg.Database != "" {
			pgParts = append(pgParts, fmt.Sprintf("DATABASE '%s'", escapeSQLString(pg.Database)))
		}
		if pg.User != "" {
			pgParts = append(pgParts, fmt.Sprintf("USER '%s'", escapeSQLString(pg.User)))
		}
		if pg.Password != "" {
			pgParts = append(pgParts, fmt.Sprintf("PASSWORD '%s'", escapeSQLString(pg.Password)))
		}
		stmts = append(stmts, fmt.Sprintf(
			"CREATE OR REPLACE SECRET %s (\n\t%s\n);",
			pgSecretName, strings.Join(pgParts, ",\n\t"),
		))
	}

	// Create DuckLake secret with PostgreSQL metadata parameters
	dlSecretName := fmt.Sprintf("_%s_ducklake_secret", name)
	secretParts := []string{
		"TYPE ducklake",
		"METADATA_PATH ''",
		fmt.Sprintf("METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': '%s'}", escapeSQLString(pgSecretName)),
	}
	if params.DataPath != "" {
		secretParts = append(secretParts, fmt.Sprintf("DATA_PATH '%s'", escapeSQLString(params.DataPath)))
	}

	stmts = append(stmts,
		fmt.Sprintf("CREATE OR REPLACE SECRET %s (\n\t%s\n);",
			dlSecretName, strings.Join(secretParts, ",\n\t"),
		),
		fmt.Sprintf("ATTACH 'ducklake:%s' AS %s%s;",
			dlSecretName, engines.Ident(prefix), attachOpts,
		),
	)

	_, err := pool.Exec(ctx, strings.Join(stmts, "\n"))
	if err != nil {
		return fmt.Errorf("ducklake: attach with pg metadata failed: %w", err)
	}
	s.isAttached = true
	return nil
}

// attachWithFileMetadata creates a DuckLake secret for file-based metadata.
func (s *Source) attachWithFileMetadata(ctx context.Context, pool *db.Pool, params *ducklakeParams, prefix, name, attachOpts string) error {
	secretParts := []string{
		"TYPE ducklake",
		fmt.Sprintf("METADATA_PATH '%s'", escapeSQLString(params.MetadataPath)),
	}
	if params.DataPath != "" {
		secretParts = append(secretParts, fmt.Sprintf("DATA_PATH '%s'", escapeSQLString(params.DataPath)))
	}
	if params.MetadataType != "" {
		metaParams := fmt.Sprintf("'TYPE': '%s'", escapeSQLString(params.MetadataType))
		if params.MetadataSecret != "" {
			metaParams += fmt.Sprintf(", 'SECRET': '%s'", escapeSQLString(params.MetadataSecret))
		}
		secretParts = append(secretParts, fmt.Sprintf("METADATA_PARAMETERS MAP {%s}", metaParams))
	}
	secretName := fmt.Sprintf("_%s_ducklake_secret", name)
	secretSQL := fmt.Sprintf("CREATE OR REPLACE SECRET %s (\n\t%s\n);",
		secretName, strings.Join(secretParts, ",\n\t"),
	)
	attachSQL := fmt.Sprintf("ATTACH 'ducklake:%s' AS %s%s;",
		secretName, engines.Ident(prefix), attachOpts,
	)

	_, err := pool.Exec(ctx, secretSQL+"\n"+attachSQL)
	if err != nil {
		return fmt.Errorf("ducklake: attach failed: %w", err)
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
	pool.Exec(ctx, fmt.Sprintf("DROP SECRET IF EXISTS _%s_ducklake_secret;", name)) //nolint:errcheck
	pool.Exec(ctx, fmt.Sprintf("DROP SECRET IF EXISTS _%s_pg_secret;", name))       //nolint:errcheck

	return nil
}

// attachOptions builds the ATTACH statement options string (e.g. " (READ_ONLY, AUTOMATIC_MIGRATION TRUE)").
func (s *Source) attachOptions(params *ducklakeParams) string {
	var opts []string
	if s.ds.ReadOnly {
		opts = append(opts, "READ_ONLY")
	}
	if params.AutomaticMigration {
		opts = append(opts, "AUTOMATIC_MIGRATION TRUE")
	}
	if len(opts) == 0 {
		return ""
	}
	return fmt.Sprintf(" (%s)", strings.Join(opts, ", "))
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
