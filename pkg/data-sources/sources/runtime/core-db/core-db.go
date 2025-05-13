package coredb

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"

	cs "github.com/hugr-lab/query-engine/pkg/catalogs/sources"
)

const (
	Version = "0.0.8"
	dbName  = "core"
)

var (
	ErrDBIsNotInitialized   = errors.New("db is not initialized")
	ErrDBIsDifferentVersion = errors.New("db is different version")
)

//go:embed schema.sql
var InitSchema string

//go:embed schema.graphql
var schema string

type Config struct {
	Path     string `json:"path"`
	ReadOnly bool   `json:"read_only"`

	// S3 for now only supports s3://
	S3Region   string `json:"s3_region"`
	S3Key      string `json:"s3_key"`
	S3Secret   string `json:"s3_secret"`
	S3UseSSL   bool   `json:"s3_use_ssl"`
	S3Endpoint string `json:"s3_endpoint"`
}

type Source struct {
	c        Config
	dbType   types.DataSourceType
	s3Source bool
	e        engines.Engine
}

func New(c Config) *Source {
	if strings.HasPrefix(c.Path, "postgres://") {
		return &Source{
			c:        c,
			dbType:   sources.Postgres,
			s3Source: strings.HasPrefix(c.Path, "s3://"),
			e:        engines.NewPostgres(),
		}
	}
	return &Source{
		c:        c,
		dbType:   sources.DuckDB,
		s3Source: strings.HasPrefix(c.Path, "s3://"),
		e:        engines.NewDuckDB(),
	}
}

type Info struct {
	Version string               `json:"version"`
	Type    types.DataSourceType `json:"type"`
}

func (s *Source) Info() Info {
	return Info{
		Version: Version,
		Type:    s.dbType,
	}
}

func (s *Source) Name() string {
	return dbName
}

func (s *Source) Engine() engines.Engine {
	return s.e
}

func (s *Source) IsReadonly() bool {
	return s.c.ReadOnly || s.s3Source
}

func (s *Source) AsModule() bool {
	return false
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	// check if db is attached
	err := sources.CheckDBExists(ctx, db, s.Name(), s.dbType)
	if err != nil {
		return err
	}

	if s.c.Path == "" {
		// attach as in-memory
		s.c.Path = ":memory:"
	}

	// check if db is on s3
	if s.s3Source {
		err = s.registerS3Secret(ctx, db)
		if err != nil {
			return fmt.Errorf("register core-db s3 secret: %w", err)
		}
	}

	sql := "ATTACH DATABASE '" + s.c.Path + "' AS " + s.Name()
	switch {
	case s.dbType == sources.DuckDB && s.IsReadonly():
		sql += " (READ_ONLY)"
	case s.dbType == sources.Postgres && !s.IsReadonly():
		sql += " (TYPE POSTGRES)"
	case s.dbType == sources.Postgres && s.IsReadonly():
		sql += " (TYPE POSTGRES, READ_ONLY)"
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return err
	}

	// check if db is not initialized apply schema
	err = checkDBVersion(ctx, db)
	if errors.Is(err, ErrDBIsNotInitialized) && !s.IsReadonly() {
		return s.applySchema(ctx, db)
	}

	return err
}

func (s *Source) registerS3Secret(ctx context.Context, db *db.Pool) error {
	_, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE PERSISTENT SECRET coredb_s3 (
			TYPE s3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			USE_SSL %v,
			URL_STYLE 'path',
			SCOPE '%s'
		);
	`, s.c.S3Key, s.c.S3Secret, s.c.S3Region, s.c.S3Endpoint, s.c.S3UseSSL, s.c.Path))
	if err != nil {
		return err
	}
	return err
}

func (s *Source) Catalog(ctx context.Context) cs.Source {
	return cs.NewStringSource(schema)
}

func checkDBVersion(ctx context.Context, db *db.Pool) error {
	// check if the db is already initialized
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("core db initialization: %w", err)
	}
	defer conn.Close()
	var isExists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(FROM duckdb_tables() WHERE database_name = '"+dbName+"' AND table_name = 'version');",
	).Scan(&isExists)
	if err != nil {
		return fmt.Errorf("core db check version: %w", err)
	}
	if !isExists {
		return ErrDBIsNotInitialized
	}

	var version *string
	err = conn.QueryRow(ctx, `SELECT "version" FROM core."version" LIMIT 1;`).Scan(&version)
	if err != nil {
		return err
	}
	if version == nil {
		return ErrDBIsNotInitialized
	}
	if *version != Version {
		return ErrDBIsDifferentVersion
	}
	return nil
}

func (s *Source) applySchema(ctx context.Context, pool *db.Pool) error {
	dbType := db.SDBAttachedDuckDB
	if s.dbType == sources.Postgres {
		dbType = db.SDBAttachedPostgres
	}
	sql, err := db.ParseSQLScriptTemplate(dbType, InitSchema)
	if err != nil {
		return fmt.Errorf("core db initialization: %w", err)
	}
	if s.dbType == sources.Postgres {
		sql = "FROM " + s.e.(engines.EngineQueryScanner).WrapExec(s.Name(), sql)
	}
	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("core db initialization: %w", err)
	}
	return nil
}
